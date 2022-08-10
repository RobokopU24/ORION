import os
import yaml
import argparse
import datetime
import requests
import json
import Common.kgx_file_converter as kgx_file_converter
from xxhash import xxh64_hexdigest
from Common.utils import LoggingUtil
from Common.load_manager import SourceDataManager, SOURCE_DATA_LOADER_CLASSES
from Common.kgx_file_merger import KGXFileMerger
from Common.neo4j_tools import Neo4jTools
from Common.kgxmodel import GraphSpec, SubGraphSource, DataSource, NormalizationScheme
from Common.metadata import Metadata, GraphMetadata, SourceMetadata
from Common.supplementation import SequenceVariantSupplementation

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

    def build_graph(self, graph_id: str, create_neo4j_dump: bool = False):

        graph_spec = self.get_graph_spec(graph_id)
        if not self.build_dependencies(graph_spec):
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
            self.logger.info(f'Building graph {graph_id} version {graph_version}')
            graph_metadata.set_build_status(Metadata.IN_PROGRESS)
            graph_metadata.set_graph_version(graph_version)
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
                self.logger.info(f'Error building graph {graph_id}.')
                return

            graph_metadata.set_build_info(merge_metadata, current_time)
            graph_metadata.set_build_status(Metadata.STABLE)
            self.logger.info(f'Building graph {graph_id} complete!')
        else:
            self.logger.info(f'Graph {graph_id} version {graph_version} was already built.')

        if create_neo4j_dump:
            self.logger.info(f'Starting Neo4j dump pipeline for {graph_id}...')
            self.create_neo4j_dump(graph_id=graph_id,
                                   graph_directory=graph_output_dir)

    def build_dependencies(self, graph_spec: GraphSpec):

        graph_id = graph_spec.graph_id
        for subgraph_source in graph_spec.subgraphs:
            subgraph_id = subgraph_source.id
            subgraph_version = subgraph_source.version
            if self.check_for_existing_graph_dir(subgraph_id, subgraph_version):
                # load previous metadata
                graph_metadata = self.get_graph_metadata(subgraph_id, subgraph_version)
                subgraph_source.graph_metadata = graph_metadata.metadata
            else:
                self.logger.warning(f'Attempting to build graph {graph_id} failed, '
                                    f'subgraph {subgraph_id} version {subgraph_version} not found.')
                return False

            if graph_metadata.get_build_status() == Metadata.STABLE:
                # we found the sub graph and it's stable - update the GraphSource in preparation for building the graph
                subgraph_dir = self.get_graph_dir_path(subgraph_id, subgraph_version)
                subgraph_nodes_path = self.get_graph_nodes_file_path(subgraph_dir)
                subgraph_edges_path = self.get_graph_edges_file_path(subgraph_dir)
                subgraph_source.file_paths = [subgraph_nodes_path, subgraph_edges_path]
            else:
                self.logger.warning(
                    f'Attempting to build graph {graph_id} failed, sub graph {subgraph_id} version {subgraph_version} is not stable.')
                return False

        for data_source in graph_spec.sources:
            source_id = data_source.id
            if source_id not in SOURCE_DATA_LOADER_CLASSES.keys():
                self.logger.warning(
                    f'Attempting to build graph {graph_id} failed: {source_id} is not a valid data source id. ')
                return False

            source_metadata: SourceMetadata = self.source_data_manager.get_source_metadata(source_id,
                                                                                           data_source.version)
            if not source_metadata.is_stable(parsing_version=data_source.parsing_version,
                                             normalization_version=data_source.normalization_scheme.get_composite_normalization_version(),
                                             supplementation_version=data_source.supplementation_version):
                self.logger.info(
                    f'Attempting to build graph {graph_id}, dependency {source_id} is not ready. Building now...')
                success = self.source_data_manager.run_pipeline(source_id,
                                                                source_version=data_source.version,
                                                                parsing_version=data_source.parsing_version,
                                                                normalization_scheme=data_source.normalization_scheme,
                                                                supplementation_version=data_source.supplementation_version)
                if not success:
                    self.logger.info(
                        f'Attempting to build graph {graph_id}, building dependency {source_id} failed. ...')
                    return False

            data_source.file_paths = self.source_data_manager.get_final_file_paths(source_id,
                                                                                   data_source.version,
                                                                                   data_source.parsing_version,
                                                                                   data_source.normalization_scheme.get_composite_normalization_version(),
                                                                                   data_source.supplementation_version)
        return True

    def create_neo4j_dump(self,
                          graph_id: str,
                          graph_directory: str):

        graph_spec = self.get_graph_spec(graph_id)
        graph_version = graph_spec.graph_version

        nodes_csv_filename = f'{NODES_FILENAME}.csv'
        edges_csv_filename = f'{EDGES_FILENAME}.csv'
        graph_nodes_file_path = os.path.join(graph_directory, NODES_FILENAME)
        graph_edges_file_path = os.path.join(graph_directory, EDGES_FILENAME)
        csv_nodes_file_path = os.path.join(graph_directory, nodes_csv_filename)
        csv_edges_file_path = os.path.join(graph_directory, edges_csv_filename)
        if os.path.exists(csv_nodes_file_path) and os.path.exists(csv_edges_file_path):
            self.logger.info(f'CSV files were already created for {graph_id}({graph_version})')
        else:
            self.__convert_kgx_to_neo4j(graph_id=graph_id,
                                        graph_version=graph_version,
                                        nodes_input_file=graph_nodes_file_path,
                                        edges_input_file=graph_edges_file_path,
                                        nodes_output_file=csv_nodes_file_path,
                                        edges_output_file=csv_edges_file_path)

        graph_dump_file_path = os.path.join(graph_directory, f'graph_{graph_version}.db.dump')
        if os.path.exists(graph_dump_file_path):
            self.logger.info(f'Neo4j dump already exists for {graph_id}({graph_version})')
            return

        neo4j_tools = Neo4jTools(graph_id=graph_id, graph_version=graph_version)
        password_exit_code = neo4j_tools.set_initial_password()
        if password_exit_code != 0:
            return

        import_exit_code = neo4j_tools.import_csv_files(graph_directory=graph_directory,
                                                        csv_nodes_filename=nodes_csv_filename,
                                                        csv_edges_filename=edges_csv_filename)
        if import_exit_code != 0:
            return

        start_exit_code = neo4j_tools.start_neo4j()
        if start_exit_code != 0:
            return

        waiting_exit_code = neo4j_tools.wait_for_neo4j_initialization()
        if waiting_exit_code != 0:
            return

        indexes_exit_code = neo4j_tools.add_db_indexes()
        if indexes_exit_code != 0:
            return

        stop_exit_code = neo4j_tools.stop_neo4j()
        if stop_exit_code != 0:
            return

        dump_exit_code = neo4j_tools.create_backup_dump(graph_dump_file_path)
        if dump_exit_code != 0:
            return

        self.logger.info(f'Success! Neo4j dump created with indexes for {graph_id}({graph_version})')

    def __convert_kgx_to_neo4j(self,
                               graph_id: str,
                               graph_version: str,
                               nodes_input_file: str,
                               edges_input_file: str,
                               nodes_output_file: str,
                               edges_output_file: str):
        self.logger.info(f'Creating CSV files for {graph_id}({graph_version})...')
        kgx_file_converter.convert_jsonl_to_neo4j_csv(nodes_input_file=nodes_input_file,
                                                      edges_input_file=edges_input_file,
                                                      nodes_output_file=nodes_output_file,
                                                      edges_output_file=edges_output_file)
        self.logger.info(f'CSV files created for {graph_id}({graph_version})...')

    def load_graph_specs(self):
        if 'DATA_SERVICES_GRAPH_SPEC' in os.environ:
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
            graph_spec_url = os.environ['DATA_SERVICES_GRAPH_SPEC']
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
        try:
            for graph_yaml in graph_spec_yaml['graphs']:
                graph_id = graph_yaml['graph_id']

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

                # we don't support other output formats yet
                # graph_output_format = graph_yaml['output_format'] if 'output_format' in graph_yaml else 'jsonl'
                graph_output_format = 'jsonl'
                current_graph_spec = GraphSpec(graph_id=graph_id,
                                               graph_version=None,
                                               graph_output_format=graph_output_format,
                                               subgraphs=subgraph_sources,
                                               sources=data_sources)
                graph_version = self.generate_graph_version(current_graph_spec)
                current_graph_spec.graph_version = graph_version
                self.current_graph_versions[graph_id] = graph_version
                graph_specs.append(current_graph_spec)
        except KeyError as e:
            self.logger.error(f'Error parsing Graph Spec, formatting error or missing information: {e}')

        return graph_specs

    def parse_subgraph_spec(self, subgraph_yml):
        subgraph_id = subgraph_yml['graph_id']
        subgraph_version = subgraph_yml['graph_version'] if 'graph_version' in subgraph_yml else 'current'
        if subgraph_version == 'current':
            if subgraph_id in self.current_graph_versions:
                subgraph_version = self.current_graph_versions[subgraph_id]
            else:
                raise Exception(f'Graph Spec Error - Could not determine version of subgraph {subgraph_id}. '
                                f'Either specify an existing version, already built in your graphs storage directory, '
                                f'or the subgraph must be defined previously in the same Graph Spec.')
        merge_strategy = subgraph_yml['merge_strategy'] if 'merge_strategy' in subgraph_yml else 'default'
        subgraph_source = SubGraphSource(id=subgraph_id,
                                         version=subgraph_version,
                                         merge_strategy=merge_strategy)
        return subgraph_source

    def parse_data_source_spec(self, source_yml):
        source_id = source_yml['source_id']
        source_version = source_yml['source_version'] if 'source_version' in source_yml \
            else self.source_data_manager.get_latest_source_version(source_id)
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
                                  version=source_version,
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
            [''.join(subgraph.id, subgraph.version, subgraph.merge_strategy)
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
    parser.add_argument('-n', '--neo4j_dump',
                        action='store_true',
                        help='Flag that indicates a neo4j database dump should also be created.')
    parser.add_argument('-v', '--version',
                        action='store_true',
                        help='Only retrieve the current version of the graph.')
    args = parser.parse_args()
    graph_id_arg = args.graph_id
    retrieve_version = args.version
    neo4j_dump_bool = args.neo4j_dump

    graph_builder = GraphBuilder()
    if graph_id_arg == "all":
        if retrieve_version:
            graph_versions = [graph_spec.graph_version for graph_spec in graph_builder.graph_specs]
            print('\n'.join(graph_versions))
        else:
            for g_id in [graph_spec.graph_id for graph_spec in graph_builder.graph_specs]:
                graph_builder.build_graph(g_id, create_neo4j_dump=neo4j_dump_bool)
    else:
        graph_spec = graph_builder.get_graph_spec(graph_id_arg)
        if graph_spec:
            if retrieve_version:
                print(graph_spec.graph_version)
            else:
                graph_builder.build_graph(graph_id_arg, create_neo4j_dump=neo4j_dump_bool)
        else:
            print(f'Invalid graph id requested: {graph_id_arg}')

