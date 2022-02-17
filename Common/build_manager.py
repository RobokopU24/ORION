
import os
import yaml
import argparse
import datetime
from xxhash import xxh64_hexdigest
from Common.utils import LoggingUtil
from Common.load_manager import SourceDataManager
from Common.kgx_file_merger import KGXFileMerger
from Common.kgxmodel import GraphSpec, SourceDataSpec, SubGraphSpec, NormalizationScheme
from Common.metadata import Metadata, GraphMetadata, SourceMetadata
from Common.supplementation import SequenceVariantSupplementation


class GraphBuilder:

    def __init__(self):

        self.logger = LoggingUtil.init_logging("Data_services.Common.GraphBuilder",
                                               line_format='medium',
                                               log_file_path=os.environ['DATA_SERVICES_LOGS'])

        self.graphs_dir = self.init_graphs_dir()  # path to the graphs output directory
        self.graph_specs = self.load_graph_specs()  # list of graphs to build (GraphSpec objects)
        self.source_data_manager = SourceDataManager()  # access to the data sources and their metadata

    def build_all_graphs(self):
        for graph_spec in self.graph_specs:
            self.build_graph(graph_spec)

    def build_graph(self, graph_spec: GraphSpec):

        if not self.build_dependencies(graph_spec):
            self.logger.warning(f'Aborting graph {graph_spec.graph_id}, building dependencies failed.')
            return

        graph_id = graph_spec.graph_id
        graph_version = self.generate_graph_version(graph_spec)
        graph_metadata = self.get_graph_metadata(graph_id, graph_version)

        # check the status for previous builds of this version
        build_status = graph_metadata.get_build_status()
        if build_status == Metadata.IN_PROGRESS:
            self.logger.info(f'Graph {graph_id} version {graph_version} is already in progress. Skipping..')
            return

        if build_status == Metadata.BROKEN or build_status == Metadata.FAILED:
            self.logger.info(f'Graph {graph_id} version {graph_version} previously failed to build. Skipping..')
            return

        if build_status == Metadata.STABLE:
            self.logger.info(f'Graph {graph_id} version {graph_version} was already built.')
            return

        # if we get here we need to build the graph
        self.logger.info(f'Building graph {graph_id} version {graph_version}')
        graph_metadata.set_build_status(Metadata.IN_PROGRESS)
        graph_metadata.set_graph_version(graph_version)
        graph_metadata.set_graph_spec(graph_spec.get_metadata_representation())

        # determine output file paths
        graph_output_dir = self.get_graph_dir_path(graph_id, graph_version)
        nodes_output_path = self.get_graph_nodes_file_path(graph_output_dir)
        edges_output_path = self.get_graph_edges_file_path(graph_output_dir)

        # merge the sources and write the finalized graph kgx files
        source_merger = KGXFileMerger()
        merge_metadata = source_merger.merge(graph_spec,
                                             nodes_output_file_path=nodes_output_path,
                                             edges_output_file_path=edges_output_path)

        if "merge_error" in merge_metadata:
            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            graph_metadata.set_build_error(merge_metadata["merge_error"], current_time)
            graph_metadata.set_build_status(Metadata.FAILED)
            self.logger.info(f'Error building graph {graph_id}.')
        else:
            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            graph_metadata.set_build_info(merge_metadata, current_time)
            graph_metadata.set_build_status(Metadata.STABLE)
            self.logger.info(f'Building graph {graph_id} complete!')

            # create a symlink for accessing 'latest'
            # latest_graph_dir = self.get_graph_dir_path(graph_id, 'latest')
            # os.remove(latest_graph_dir)
            # os.symlink(graph_output_dir, latest_graph_dir, target_is_directory=True)

    def build_dependencies(self, graph_spec: GraphSpec):

        graph_id = graph_spec.graph_id
        for subgraph_source in graph_spec.subgraphs:
            subgraph_id = subgraph_source.graph_id
            subgraph_version = subgraph_source.graph_version
            if self.check_for_existing_graph_dir(subgraph_id, subgraph_version):
                # load previous metadata
                graph_metadata = self.get_graph_metadata(subgraph_id, subgraph_version)

                # grab the graph version from the metadata - this is necessary to replace 'latest' with a real one
                subgraph_version = graph_metadata.get_graph_version()
                subgraph_source.graph_version = subgraph_version
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
            source_id = data_source.source_id

            if data_source.source_version == 'latest':
                data_source.source_version = self.source_data_manager.get_latest_source_version(source_id)
            if data_source.parsing_version == 'latest':
                data_source.parsing_version = self.source_data_manager.get_latest_parsing_version(source_id)

            normalization_scheme = data_source.normalization_scheme
            if normalization_scheme.node_normalization_version == 'latest':
                normalization_scheme.node_normalization_version = self.source_data_manager.get_latest_node_normalization_version()
            if normalization_scheme.edge_normalization_version == 'latest':
                normalization_scheme.edge_normalization_version = self.source_data_manager.get_latest_edge_normalization_version()

            data_source.supplementation_version = SequenceVariantSupplementation.SUPPLEMENTATION_VERSION

            source_metadata: SourceMetadata = self.source_data_manager.get_source_metadata(source_id,
                                                                                           data_source.source_version)
            if not source_metadata.is_stable(parsing_version=data_source.parsing_version,
                                             normalization_version=data_source.normalization_scheme.get_composite_normalization_version(),
                                             supplementation_version=data_source.supplementation_version):
                self.logger.info(
                    f'Attempting to build graph {graph_id}, dependency {source_id} is not ready. Building now...')
                success = self.source_data_manager.run_pipeline(source_id,
                                                                source_version=data_source.source_version,
                                                                parsing_version=data_source.parsing_version,
                                                                normalization_scheme=data_source.normalization_scheme,
                                                                supplementation_version=data_source.supplementation_version)
                if not success:
                    self.logger.info(
                        f'Attempting to build graph {graph_id}, building dependency {source_id} failed. ...')
                    return False

            data_source.file_paths = self.source_data_manager.get_final_file_paths(source_id,
                                                                                   data_source.source_version,
                                                                                   data_source.parsing_version,
                                                                                   data_source.normalization_scheme.get_composite_normalization_version(),
                                                                                   data_source.supplementation_version)
        return True

    def load_graph_specs(self):
        if 'DATA_SERVICES_GRAPH_SPEC' in os.environ:
            graph_spec_file = os.environ['DATA_SERVICES_GRAPH_SPEC']
            graph_spec_path = os.path.join(self.graphs_dir, graph_spec_file)
            if os.path.exists(graph_spec_path):
                self.logger.info(f'Loading graph spec: {graph_spec_file}')
                with open(graph_spec_path) as graph_spec_file:
                    graph_spec_yaml = yaml.full_load(graph_spec_file)
                    return self.parse_graph_spec(graph_spec_yaml)
            else:
                raise Exception(f'Configuration Error - Graph Spec could not be found: {graph_spec_file}')
        else:
            raise Exception(f'Configuration Error - No Graph Spec was configured. Set the environment variable '
                            f'DATA_SERVICES_GRAPH_SPEC to a valid Graph Spec file in your Graphs directory. '
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
                graph_wide_edge_norm_version = graph_yaml['edge_normalization_version'] \
                    if 'edge_normalization_version' in graph_yaml else None
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
                graph_specs.append(current_graph_spec)
        except KeyError as e:
            self.logger.error(f'Error parsing Graph Spec, formatting error or missing information: {e}')

        return graph_specs

    def parse_subgraph_spec(self, subgraph_yml):
        graph_id = subgraph_yml['graph_id']
        graph_version = subgraph_yml['graph_version'] if 'graph_version' in subgraph_yml else 'latest'
        merge_strategy = subgraph_yml['merge_strategy'] if 'merge_strategy' in subgraph_yml else 'default'
        subgraph_source = SubGraphSpec(graph_id=graph_id,
                                       graph_version=graph_version,
                                       merge_strategy=merge_strategy)
        return subgraph_source

    def parse_data_source_spec(self, source_yml):
        source_id = source_yml['source_id']
        source_version = source_yml['source_version'] if 'source_version' in source_yml else 'latest'
        parsing_version = source_yml['parsing_version'] if 'parsing_version' in source_yml else 'latest'
        merge_strategy = source_yml['merge_strategy'] if 'merge_strategy' in source_yml else 'default'
        node_normalization_version = source_yml['node_normalization_version'] \
            if 'node_normalization_version' in source_yml else 'latest'
        edge_normalization_version = source_yml['edge_normalization_version'] \
            if 'edge_normalization_version' in source_yml else 'latest'
        strict_normalization = source_yml['strict_normalization'] \
            if 'strict_normalization' in source_yml else True
        conflation = source_yml['conflation'] \
            if 'conflation' in source_yml else False
        normalization_scheme = NormalizationScheme(node_normalization_version=node_normalization_version,
                                                   edge_normalization_version=edge_normalization_version,
                                                   strict=strict_normalization,
                                                   conflation=conflation)
        graph_source = SourceDataSpec(source_id=source_id,
                                      source_version=source_version,
                                      normalization_scheme=normalization_scheme,
                                      parsing_version=parsing_version,
                                      merge_strategy=merge_strategy)
        return graph_source

    def get_graph_spec(self, graph_id: str):
        for graph_spec in self.graph_specs:
            if graph_spec.graph_id == graph_id:
                return graph_spec
        return None

    def get_graph_dir_path(self, graph_id: str, graph_version: str):
        return os.path.join(self.graphs_dir, graph_id, graph_version)

    def get_graph_nodes_file_path(self, graph_output_dir: str):
        return os.path.join(graph_output_dir, f'nodes.jsonl')

    def get_graph_edges_file_path(self, graph_output_dir: str):
        return os.path.join(graph_output_dir, f'edges.jsonl')

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
        graph_version_string = ''.join(
            [f'{graph_source.source_id}{graph_source.source_version}{graph_source.merge_strategy}'
             for graph_source in graph_spec.sources])
        graph_version = xxh64_hexdigest(graph_version_string)
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
    parser = argparse.ArgumentParser(description="Merge KGX files into neo4j graphs.")
    parser.add_argument('-g', '--graph_id', default='all',
                        help=f'Select a single graph to load by the graph id.')
    args = parser.parse_args()

    graph_id_arg = args.graph_id
    graph_builder = GraphBuilder()
    if graph_id_arg == "all":
        graph_builder.build_all_graphs()
    else:
        spec = graph_builder.get_graph_spec(graph_id_arg)
        if spec:
            graph_builder.build_graph(spec)
        else:
            print(f'Invalid graph id requested: {graph_id_arg}')

