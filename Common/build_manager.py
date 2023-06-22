import os
import yaml
import argparse
import datetime
import requests
import json
from xxhash import xxh64_hexdigest
from collections import defaultdict
from Common.utils import LoggingUtil, quick_jsonl_file_iterator
from Common.data_sources import get_available_data_sources
from Common.load_manager import SourceDataManager
from Common.kgx_file_merger import KGXFileMerger
from Common.neo4j_tools import Neo4jTools
from Common.kgxmodel import GraphSpec, SubGraphSource, DataSource, NormalizationScheme
from Common.metadata import Metadata, GraphMetadata, SourceMetadata
from Common.supplementation import SequenceVariantSupplementation
from Common.node_types import PRIMARY_KNOWLEDGE_SOURCE, PREDICATE
from Common.meta_kg import MetaKnowledgeGraphBuilder, META_KG_FILENAME, TEST_DATA_FILENAME

NODES_FILENAME = 'nodes.jsonl'
EDGES_FILENAME = 'edges.jsonl'


class GraphBuilder:

    def __init__(self):

        self.logger = LoggingUtil.init_logging("Data_services.Common.GraphBuilder",
                                               line_format='medium',
                                               log_file_path=os.environ['DATA_SERVICES_LOGS'])

        self.current_graph_versions = {}
        self.graphs_dir = self.init_graphs_dir()  # path to the graphs output directory
        self.source_data_manager = SourceDataManager()  # access to the data sources and their metadata
        self.graph_specs = self.load_graph_specs()  # list of graphs to build (GraphSpec objects)
        self.build_results = {}

    def build_graph(self, graph_id: str):

        self.logger.info(f'Building graph {graph_id}. Checking dependencies...')
        graph_spec = self.get_graph_spec(graph_id)
        if self.build_dependencies(graph_spec):
            self.logger.info(f'Building graph {graph_id}. Dependencies are ready...')
        else:
            self.logger.warning(f'Aborting graph {graph_spec.graph_id}, building dependencies failed.')
            return

        # check the status for previous builds of this version
        graph_version = graph_spec.graph_version
        graph_metadata = self.get_graph_metadata(graph_id, graph_version)
        build_status = graph_metadata.get_build_status()
        if build_status == Metadata.IN_PROGRESS:
            self.logger.info(f'Graph {graph_id} version {graph_version} is already in progress. Skipping..')
            return

        if build_status == Metadata.BROKEN or build_status == Metadata.FAILED:
            self.logger.info(f'Graph {graph_id} version {graph_version} previously failed to build. Skipping..')
            return

        graph_output_dir = self.get_graph_dir_path(graph_id, graph_version)
        if build_status != Metadata.STABLE:

            # if we get here we need to build the graph
            self.logger.info(f'Building graph {graph_id} version {graph_version}. Merging sources...')
            graph_metadata.set_build_status(Metadata.IN_PROGRESS)
            graph_metadata.set_graph_version(graph_version)
            graph_metadata.set_graph_name(graph_spec.graph_name)
            graph_metadata.set_graph_description(graph_spec.graph_description)
            graph_metadata.set_graph_url(graph_spec.graph_url)
            graph_metadata.set_graph_spec(graph_spec.get_metadata_representation())

            # merge the sources and write the finalized graph kgx files
            source_merger = KGXFileMerger(output_directory=graph_output_dir)
            merge_metadata = source_merger.merge(graph_spec,
                                                 nodes_output_filename=NODES_FILENAME,
                                                 edges_output_filename=EDGES_FILENAME)

            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            if "merge_error" in merge_metadata:
                graph_metadata.set_build_error(merge_metadata["merge_error"], current_time)
                graph_metadata.set_build_status(Metadata.FAILED)
                self.logger.error(f'Error building graph {graph_id}.')
                return

            graph_metadata.set_build_info(merge_metadata, current_time)
            graph_metadata.set_build_status(Metadata.STABLE)
            self.logger.info(f'Building graph {graph_id} complete!')
            self.build_results[graph_id] = {'version': graph_version, 'success': True}
        else:
            self.logger.info(f'Graph {graph_id} version {graph_version} was already built.')
            self.build_results[graph_id] = {'version': graph_version, 'success': False}

        if not graph_metadata.has_qc():
            self.logger.info(f'Running QC for graph {graph_id}...')
            qc_results = self.run_qc(graph_id, graph_version, graph_directory=graph_output_dir)
            graph_metadata.set_qc_results(qc_results)
            # TODO - bail if qc fails
            self.logger.info(f'QC complete for graph {graph_id}.')

        needs_meta_kg = not self.has_meta_kg(graph_directory=graph_output_dir)
        needs_test_data = not self.has_test_data(graph_directory=graph_output_dir)
        if needs_meta_kg or needs_test_data:
            self.logger.info(f'Generating MetaKG and test data for {graph_id}...')
            self.generate_meta_kg_and_test_data(graph_directory=graph_output_dir,
                                                generate_meta_kg=needs_meta_kg,
                                                generate_test_data=needs_test_data)

        if 'neo4j' in graph_spec.graph_output_format.lower():
            self.logger.info(f'Starting Neo4j dump pipeline for {graph_id}...')
            neo4j_tools = Neo4jTools(graph_id=graph_id,
                                     graph_version=graph_version)
            dump_success = neo4j_tools.create_neo4j_dump(graph_id=graph_id,
                                                         graph_version=graph_version,
                                                         graph_directory=graph_output_dir,
                                                         nodes_filename=NODES_FILENAME,
                                                         edges_filename=EDGES_FILENAME)
            if dump_success:
                graph_output_url = self.get_graph_output_URL(graph_id, graph_version)
                graph_metadata.set_dump_url(f'{graph_output_url}graph_{graph_version}.db.dump')

    def build_dependencies(self, graph_spec: GraphSpec):
        for subgraph_source in graph_spec.subgraphs:
            subgraph_id = subgraph_source.id
            subgraph_version = subgraph_source.version
            if self.check_for_existing_graph_dir(subgraph_id, subgraph_version):
                # load previous metadata
                graph_metadata = self.get_graph_metadata(subgraph_id, subgraph_version)
                subgraph_source.graph_metadata = graph_metadata.metadata
            elif self.current_graph_versions[subgraph_id] == subgraph_version:
                self.logger.warning(f'For graph {graph_spec.graph_id} subgraph dependency '
                                    f'{subgraph_id} version {subgraph_version} is not ready. Building now...')
                self.build_graph(subgraph_id)
            else:
                self.logger.warning(f'Building graph {graph_spec.graph_id} failed, '
                                    f'subgraph {subgraph_id} had version {subgraph_version} specified, '
                                    f'but that version of the graph was not found in the graphs directory.')
                return False

            graph_metadata = self.get_graph_metadata(subgraph_id, subgraph_version)
            if graph_metadata.get_build_status() == Metadata.STABLE:
                # we found the sub graph and it's stable - update the GraphSource in preparation for building the graph
                subgraph_dir = self.get_graph_dir_path(subgraph_id, subgraph_version)
                subgraph_nodes_path = self.get_graph_nodes_file_path(subgraph_dir)
                subgraph_edges_path = self.get_graph_edges_file_path(subgraph_dir)
                subgraph_source.file_paths = [subgraph_nodes_path, subgraph_edges_path]
            else:
                self.logger.warning(
                    f'Attempting to build graph {graph_spec.graph_id} failed, dependency '
                    f'subgraph {subgraph_id} version {subgraph_version} was not built successfully.')
                return False

        for data_source in graph_spec.sources:
            source_id = data_source.id
            if source_id not in get_available_data_sources():
                self.logger.warning(
                    f'Attempting to build graph {graph_spec.graph_id} failed: '
                    f'{source_id} is not a valid data source id. ')
                return False

            source_metadata: SourceMetadata = self.source_data_manager.get_source_metadata(source_id,
                                                                                           data_source.source_version)
            release_version = source_metadata.get_release_version(parsing_version=data_source.parsing_version,
                                                                  normalization_version=data_source.normalization_scheme.get_composite_normalization_version(),
                                                                  supplementation_version=data_source.supplementation_version)
            if release_version is None:
                self.logger.info(
                    f'Attempting to build graph {graph_spec.graph_id}, '
                    f'dependency {source_id} is not ready. Building now...')
                release_version = self.source_data_manager.run_pipeline(source_id,
                                                                        source_version=data_source.source_version,
                                                                        parsing_version=data_source.parsing_version,
                                                                        normalization_scheme=data_source.normalization_scheme,
                                                                        supplementation_version=data_source.supplementation_version)
                if not release_version:
                    self.logger.info(
                        f'While attempting to build {graph_spec.graph_id}, dependency pipeline failed for {source_id}...')
                    return False

            data_source.version = release_version
            data_source.release_info = source_metadata.get_release_info(release_version)
            data_source.file_paths = self.source_data_manager.get_final_file_paths(source_id,
                                                                                   data_source.source_version,
                                                                                   data_source.parsing_version,
                                                                                   data_source.normalization_scheme.get_composite_normalization_version(),
                                                                                   data_source.supplementation_version)
        return True

    def has_meta_kg(self, graph_directory: str):
        if os.path.exists(os.path.join(graph_directory, META_KG_FILENAME)):
            return True
        else:
            return False

    def has_test_data(self, graph_directory: str):
        if os.path.exists(os.path.join(graph_directory, TEST_DATA_FILENAME)):
            return True
        else:
            return False

    def generate_meta_kg_and_test_data(self,
                                       graph_directory: str,
                                       generate_meta_kg: bool = True,
                                       generate_test_data: bool = True):
        graph_nodes_file_path = os.path.join(graph_directory, NODES_FILENAME)
        graph_edges_file_path = os.path.join(graph_directory, EDGES_FILENAME)
        mkgb = MetaKnowledgeGraphBuilder(nodes_file_path=graph_nodes_file_path,
                                         edges_file_path=graph_edges_file_path,
                                         logger=self.logger)
        if generate_meta_kg:
            meta_kg_file_path = os.path.join(graph_directory, META_KG_FILENAME)
            mkgb.write_meta_kg_to_file(meta_kg_file_path)
        if generate_test_data:
            test_data_file_path = os.path.join(graph_directory, TEST_DATA_FILENAME)
            mkgb.write_test_data_to_file(test_data_file_path)

    def run_qc(self,
               graph_id: str,
               graph_version: str,
               graph_directory: str):

        knowledge_sources = set()
        edge_properties = set()
        predicate_counts = defaultdict(int)
        graph_edges_file_path = os.path.join(graph_directory, EDGES_FILENAME)
        for edge_json in quick_jsonl_file_iterator(graph_edges_file_path):
            knowledge_sources.add(edge_json[PRIMARY_KNOWLEDGE_SOURCE])
            for key in edge_json.keys():
                edge_properties.add(key)
            predicate_counts[edge_json[PREDICATE]] += 1
        qc_metadata = {
            'primary_knowledge_sources': list(knowledge_sources),
            'edge_properties': list(edge_properties),
            'predicate_counts': {k: v for k, v in predicate_counts.items()}
        }
        return qc_metadata

    def load_graph_specs(self):
        if 'DATA_SERVICES_GRAPH_SPEC' in os.environ and os.environ['DATA_SERVICES_GRAPH_SPEC']:
            # this is a messy way to find the graph spec path, mainly for testing - URL is preferred
            graph_spec_file = os.environ['DATA_SERVICES_GRAPH_SPEC']
            graph_spec_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'graph_specs', graph_spec_file)
            if os.path.exists(graph_spec_path):
                self.logger.info(f'Loading graph spec: {graph_spec_file}')
                with open(graph_spec_path) as graph_spec_file:
                    graph_spec_yaml = yaml.full_load(graph_spec_file)
                    return self.parse_graph_spec(graph_spec_yaml)
            else:
                raise Exception(f'Configuration Error - Graph Spec could not be found: {graph_spec_file}')
        elif 'DATA_SERVICES_GRAPH_SPEC_URL' in os.environ:
            graph_spec_url = os.environ['DATA_SERVICES_GRAPH_SPEC_URL']
            graph_spec_request = requests.get(graph_spec_url)
            graph_spec_request.raise_for_status()
            graph_spec_yaml = yaml.full_load(graph_spec_request.text)
            return self.parse_graph_spec(graph_spec_yaml)
        else:
            raise Exception(f'Configuration Error - No Graph Spec was configured. Set the environment variable '
                            f'DATA_SERVICES_GRAPH_SPEC_URL to a URL with a valid Graph Spec yaml file. '
                            f'See the README for more info.')

    def parse_graph_spec(self, graph_spec_yaml):
        graph_specs = []
        graph_id = ""
        try:
            for graph_yaml in graph_spec_yaml['graphs']:
                graph_id = graph_yaml['graph_id']
                graph_name = graph_yaml['graph_name'] if 'graph_name' in graph_yaml else ""
                graph_description = graph_yaml['graph_description'] if 'graph_description' in graph_yaml else ""
                graph_url = graph_yaml['graph_url'] if 'graph_url' in graph_yaml else ""

                # parse the list of data sources
                data_sources = [self.parse_data_source_spec(data_source) for data_source in graph_yaml['sources']] \
                    if 'sources' in graph_yaml else []

                # parse the list of subgraphs
                subgraph_sources = [self.parse_subgraph_spec(subgraph) for subgraph in graph_yaml['subgraphs']] \
                    if 'subgraphs' in graph_yaml else []

                if not data_sources and not subgraph_sources:
                    self.logger.error(f'Error: No sources were provided for graph: {graph_id}.')
                    continue

                # take any normalization scheme parameters specified at the graph level
                graph_wide_node_norm_version = graph_yaml['node_normalization_version'] \
                    if 'node_normalization_version' in graph_yaml else None
                if graph_wide_node_norm_version == 'latest':
                    graph_wide_node_norm_version = self.source_data_manager.get_latest_node_normalization_version()
                graph_wide_edge_norm_version = graph_yaml['edge_normalization_version'] \
                    if 'edge_normalization_version' in graph_yaml else None
                if graph_wide_edge_norm_version == 'latest':
                    graph_wide_edge_norm_version = self.source_data_manager.get_latest_edge_normalization_version()
                graph_wide_conflation = graph_yaml['conflation'] \
                    if 'conflation' in graph_yaml else None
                graph_wide_strict_norm = graph_yaml['strict_normalization'] \
                    if 'strict_normalization' in graph_yaml else None

                # apply them to all of the data sources, this will overwrite anything defined at the source level
                for data_source in data_sources:
                    if graph_wide_node_norm_version is not None:
                        data_source.normalization_scheme.node_normalization_version = graph_wide_node_norm_version
                    if graph_wide_edge_norm_version is not None:
                        data_source.normalization_scheme.edge_normalization_version = graph_wide_edge_norm_version
                    if graph_wide_conflation is not None:
                        data_source.normalization_scheme.conflation = graph_wide_conflation
                    if graph_wide_strict_norm is not None:
                        data_source.normalization_scheme.strict = graph_wide_strict_norm

                graph_output_format = graph_yaml['output_format'] if 'output_format' in graph_yaml else ""
                current_graph_spec = GraphSpec(graph_id=graph_id,
                                               graph_name=graph_name,
                                               graph_description=graph_description,
                                               graph_url=graph_url,
                                               graph_version=None,  # this will get populated later
                                               graph_output_format=graph_output_format,
                                               subgraphs=subgraph_sources,
                                               sources=data_sources)
                graph_version = self.generate_graph_version(current_graph_spec)
                current_graph_spec.graph_version = graph_version
                self.current_graph_versions[graph_id] = graph_version
                graph_specs.append(current_graph_spec)
        except Exception as e:
            self.logger.error(f'Error parsing Graph Spec ({graph_id}), formatting error or missing information: {repr(e)}')
            raise e
        return graph_specs

    def parse_subgraph_spec(self, subgraph_yml):
        subgraph_id = subgraph_yml['graph_id']
        subgraph_version = subgraph_yml['graph_version'] if 'graph_version' in subgraph_yml else 'current'
        if subgraph_version == 'current':
            if subgraph_id in self.current_graph_versions:
                subgraph_version = self.current_graph_versions[subgraph_id]
            else:
                raise Exception(f'Graph Spec Error - Could not determine version of subgraph {subgraph_id}. '
                                f'Either specify an existing version, already built in your graphs directory, '
                                f'or the subgraph must be defined previously in the same Graph Spec.')
        merge_strategy = subgraph_yml['merge_strategy'] if 'merge_strategy' in subgraph_yml else 'default'
        subgraph_source = SubGraphSource(id=subgraph_id,
                                         version=subgraph_version,
                                         merge_strategy=merge_strategy)
        return subgraph_source

    def parse_data_source_spec(self, source_yml):
        source_id = source_yml['source_id']
        if source_id not in get_available_data_sources():
            error_message = f'Data source {source_id} is not a valid data source id.'
            self.logger.error(error_message + " " +
                              f'Valid sources are: {", ".join(get_available_data_sources())}')
            raise Exception(error_message)

        source_version = source_yml['source_version'] if 'source_version' in source_yml \
            else self.source_data_manager.get_latest_source_version(source_id)
        if source_version is None:
            # TODO it would be great if we could default to the last stable version already built somehow
            error_message = f'Data source {source_id} could not determine the latest version. The service may be down.'
            raise Exception(error_message)

        parsing_version = source_yml['parsing_version'] if 'parsing_version' in source_yml \
            else self.source_data_manager.get_latest_parsing_version(source_id)
        merge_strategy = source_yml['merge_strategy'] if 'merge_strategy' in source_yml else 'default'
        node_normalization_version = source_yml['node_normalization_version'] \
            if 'node_normalization_version' in source_yml \
            else self.source_data_manager.get_latest_node_normalization_version()
        edge_normalization_version = source_yml['edge_normalization_version'] \
            if 'edge_normalization_version' in source_yml \
            else self.source_data_manager.get_latest_edge_normalization_version()
        strict_normalization = source_yml['strict_normalization'] \
            if 'strict_normalization' in source_yml else True
        conflation = source_yml['conflation'] \
            if 'conflation' in source_yml else False
        normalization_scheme = NormalizationScheme(node_normalization_version=node_normalization_version,
                                                   edge_normalization_version=edge_normalization_version,
                                                   strict=strict_normalization,
                                                   conflation=conflation)
        supplementation_version = SequenceVariantSupplementation.SUPPLEMENTATION_VERSION
        graph_source = DataSource(id=source_id,
                                  version=None,  # this will get populated later in build_dependencies
                                  source_version=source_version,
                                  merge_strategy=merge_strategy,
                                  normalization_scheme=normalization_scheme,
                                  parsing_version=parsing_version,
                                  supplementation_version=supplementation_version)
        return graph_source

    def get_graph_spec(self, graph_id: str):
        for graph_spec in self.graph_specs:
            if graph_spec.graph_id == graph_id:
                return graph_spec
        return None

    def get_graph_dir_path(self, graph_id: str, graph_version: str):
        return os.path.join(self.graphs_dir, graph_id, graph_version)

    def get_graph_output_URL(self, graph_id: str, graph_version: str):
        graph_output_url = os.environ['DATA_SERVICES_OUTPUT_URL']
        if graph_output_url[-1] != '/':
            graph_output_url += '/'
        return f'{graph_output_url}{graph_id}/{graph_version}/'

    def get_graph_nodes_file_path(self, graph_output_dir: str):
        return os.path.join(graph_output_dir, NODES_FILENAME)

    def get_graph_edges_file_path(self, graph_output_dir: str):
        return os.path.join(graph_output_dir, EDGES_FILENAME)

    def check_for_existing_graph_dir(self, graph_id: str, graph_version: str):
        graph_output_dir = self.get_graph_dir_path(graph_id, graph_version)
        if not os.path.isdir(graph_output_dir):
            return False
        return True

    def get_graph_metadata(self, graph_id: str, graph_version: str):
        # make sure the output directory exists (where we check for existing GraphMetadata)
        graph_output_dir = self.get_graph_dir_path(graph_id, graph_version)
        if not os.path.isdir(graph_output_dir):
            os.makedirs(graph_output_dir)

        # load existing or create new metadata file
        return GraphMetadata(graph_id, graph_output_dir)

    @staticmethod
    def generate_graph_version(graph_spec: GraphSpec):
        sources_string = ''.join(
            [json.dumps(graph_source.get_metadata_representation())
             for graph_source in graph_spec.sources])
        subgraphs_string = ''.join(
            [''.join([subgraph.id, subgraph.version, subgraph.merge_strategy])
             for subgraph in graph_spec.subgraphs])
        graph_version = xxh64_hexdigest(sources_string + subgraphs_string)
        return graph_version

    @staticmethod
    def init_graphs_dir():
        # use the directory specified by the environment variable DATA_SERVICES_GRAPHS
        if 'DATA_SERVICES_GRAPHS' in os.environ and os.path.isdir(os.environ['DATA_SERVICES_GRAPHS']):
            return os.environ['DATA_SERVICES_GRAPHS']
        else:
            # if graph dir is invalid or not specified back out
            raise IOError(
                'GraphBuilder graphs directory not found. '
                'Specify a valid directory with environment variable DATA_SERVICES_GRAPHS.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Merge data source files into complete graphs.")
    parser.add_argument('graph_id',
                        help='ID of the graph to build. Must match an ID from the configured Graph Spec.')
    parser.add_argument('-v', '--version',
                        action='store_true',
                        help='Only retrieve a generated version for graphs from the graph spec.')
    args = parser.parse_args()
    graph_id_arg = args.graph_id
    retrieve_version = args.version

    graph_builder = GraphBuilder()
    if graph_id_arg == "all":
        if retrieve_version:
            graph_versions = [graph_spec.graph_version for graph_spec in graph_builder.graph_specs]
            print('\n'.join(graph_versions))
        else:
            for g_id in [graph_spec.graph_id for graph_spec in graph_builder.graph_specs]:
                graph_builder.build_graph(g_id)
    else:
        graph_spec = graph_builder.get_graph_spec(graph_id_arg)
        if graph_spec:
            if retrieve_version:
                print(graph_spec.graph_version)
            else:
                graph_builder.build_graph(graph_id_arg)
        else:
            print(f'Invalid graph spec requested: {graph_id_arg}')
    for results_graph_id, results in graph_builder.build_results.items():
        if results['success']:
            print(f'{results_graph_id}\t{results["version"]}')
