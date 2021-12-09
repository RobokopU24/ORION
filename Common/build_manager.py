
import os
import yaml
import argparse
import datetime
from xxhash import xxh64_hexdigest
from collections import defaultdict
from Common.utils import LoggingUtil
from Common.load_manager import SourceDataManager
from Common.kgx_file_merger import KGXFileMerger
from Common.kgxmodel import GraphSource, GraphSpec, KNOWLEDGE_SOURCE, SUBGRAPH
from Common.metadata import Metadata, GraphMetadata, SourceMetadata
from Common.supplementation import SequenceVariantSupplementation

class GraphBuilder:

    def __init__(self):

        self.logger = LoggingUtil.init_logging("Data_services.Common.GraphBuilder",
                                               line_format='medium',
                                               log_file_path=os.environ['DATA_SERVICES_LOGS'])

        self.graphs_dir = self.init_graphs_dir()  # path to the graphs output directory
        self.source_data_manager = SourceDataManager()  # access to the data sources and their metadata
        self.graph_metadata = defaultdict(lambda: dict())
        self.current_graph_versions = {}
        self.graph_specs = self.load_graph_specs()  # list of GraphSpec

    def build_all_graphs(self):
        for graph_spec in self.graph_specs:
            self.build_graph(graph_spec)

    def build_graph(self, graph_spec: GraphSpec):

        graph_id = graph_spec.graph_id

        success_w_dependencies = self.build_dependencies(graph_spec)
        if not success_w_dependencies:
            self.logger.warning(f'Aborting graph {graph_id}, loading dependencies failed.')
            return

        graph_version = self.generate_graph_version(graph_spec)
        self.current_graph_versions[graph_id] = graph_version
        graph_metadata = self.get_graph_metadata(graph_id, graph_version)
        self.graph_metadata[graph_id][graph_version] = graph_metadata

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

    def load_graph_specs(self):

        if 'DATA_SERVICES_GRAPH_SPEC' in os.environ:
            graph_spec_file = os.environ['DATA_SERVICES_GRAPH_SPEC']
            graph_spec_path = os.path.join(self.graphs_dir, graph_spec_file)
            self.logger.info(f'Loading custom graph spec - {graph_spec_file}')
        else:
            graph_spec_path = os.path.dirname(os.path.abspath(__file__)) + '/../default-graph-spec.yml'
            self.logger.info(f'Loading default graph spec..')

        with open(graph_spec_path) as graph_spec_file:
            graph_spec_yaml = yaml.full_load(graph_spec_file)
            graph_specs = []
            if not graph_spec_yaml['graphs']:
                self.logger.error(f'Error: No graphs were found in the graph spec.')
                return graph_specs

            for graph_yaml in graph_spec_yaml['graphs']:
                graph_id = graph_yaml['graph_id']
                graph_sources = []
                for source in graph_yaml['sources']:
                    graph_sources.append(self.parse_source(source))

                if not graph_sources:
                    self.logger.error(f'Error: No sources were provided for graph: {graph_id}.')
                    continue

                graph_output_format = graph_yaml['output_format'] if 'output_format' in graph_yaml else 'jsonl'
                current_graph_spec = GraphSpec(graph_id=graph_id,
                                               graph_version=None,  # the version is will be generated later
                                               graph_output_format=graph_output_format,
                                               sources=graph_sources)
                graph_specs.append(current_graph_spec)

        return graph_specs

    def parse_source(self, source: dict):

        if 'source_id' in source:
            source_id = source['source_id']
            source_type = KNOWLEDGE_SOURCE
        elif 'subgraph_id' in source:
            source_id = source['subgraph_id']
            source_type = SUBGRAPH
        else:
            raise Exception(f'Error parsing Graph Spec source: {source}')

        source_version = source['version'] if 'version' in source else 'latest'
        node_normalization_version = source['node_normalization_version'] if 'node_normalization_version' in source else 'latest'
        edge_normalization_version = source['edge_normalization_version'] if 'edge_normalization_version' in source else 'latest'
        strict_normalization = source['strict_normalization'] if 'strict_normalization' in source else True
        parsing_version = source['parsing_version'] if 'parsing_version' in source else 'latest'
        merge_strategy = source['merge_strategy'] if 'merge_strategy' in source else 'default'
        graph_source = GraphSource(source_id=source_id,
                                   source_type=source_type,
                                   source_version=source_version,
                                   parsing_version=parsing_version,
                                   node_normalization_version=node_normalization_version,
                                   edge_normalization_version=edge_normalization_version,
                                   strict_normalization=strict_normalization,
                                   merge_strategy=merge_strategy)
        return graph_source

    def build_dependencies(self, graph_spec: GraphSpec):

        graph_id = graph_spec.graph_id

        for subgraph_source in [graph_source for graph_source in graph_spec.sources
                                if graph_source.source_type == SUBGRAPH]:
            subgraph_id = subgraph_source.source_id
            if subgraph_source.source_version == 'latest':
                if subgraph_id in self.current_graph_versions:
                    subgraph_version = self.current_graph_versions[subgraph_id]
                    subgraph_source.source_version = subgraph_version
                else:
                    self.logger.warning(f'Attempting to build graph {graph_id} failed, sub graph {subgraph_id} failed or was not previously defined.')
                    return False
            else:
                subgraph_version = subgraph_source.source_version
                if self.check_for_existing_build(subgraph_id, subgraph_version):
                    # load previous metadata
                    graph_metadata = self.get_graph_metadata(subgraph_id, subgraph_version)
                    self.graph_metadata[subgraph_id][subgraph_version] = graph_metadata
                else:
                    self.logger.warning(f'Attempting to build graph {graph_id} failed, sub graph {subgraph_id} previous version {subgraph_version} not found.')
                    return False

            if self.graph_metadata[subgraph_id][subgraph_version].get_build_status() == Metadata.STABLE:
                # we found the sub graph and it's stable - update the GraphSource in preparation for building the graph
                subgraph_output_dir = self.get_graph_dir_path(subgraph_id, subgraph_version)
                nodes_output_path = self.get_graph_nodes_file_path(subgraph_output_dir)
                edges_output_path = self.get_graph_edges_file_path(subgraph_output_dir)
                subgraph_source.file_paths = [nodes_output_path, edges_output_path]
            else:
                self.logger.warning(
                    f'Attempting to build graph {graph_id} failed, sub graph {subgraph_id} version {sub_graph_source.source_version} is not stable.')
                return False

        for graph_source in [graph_source for graph_source in graph_spec.sources
                             if graph_source.source_type == KNOWLEDGE_SOURCE]:
            source_id = graph_source.source_id
            strict_normalization = graph_source.strict_normalization

            if graph_source.source_version == 'latest':
                graph_source.source_version = self.source_data_manager.get_latest_source_version(source_id)
            if graph_source.parsing_version == 'latest':
                graph_source.parsing_version = self.source_data_manager.get_latest_parsing_version(source_id)
            if graph_source.node_normalization_version == 'latest':
                graph_source.node_normalization_version = self.source_data_manager.get_latest_node_normalization_version()
            if graph_source.edge_normalization_version == 'latest':
                graph_source.edge_normalization_version = self.source_data_manager.get_latest_edge_normalization_version()
            graph_source.supplementation_version = SequenceVariantSupplementation.SUPPLEMENTATION_VERSION

            normalization_version = self.source_data_manager.generate_normalization_version(graph_source.node_normalization_version,
                                                                                            graph_source.edge_normalization_version,
                                                                                            strict_normalization)
            source_metadata: SourceMetadata = self.source_data_manager.get_source_metadata(source_id,
                                                                                           graph_source.source_version)
            if not source_metadata.is_stable(parsing_version=graph_source.parsing_version,
                                             normalization_version=normalization_version,
                                             supplementation_version=graph_source.supplementation_version):
                self.logger.info(
                    f'Attempting to build graph {graph_id}, dependency {source_id} is not ready. Building now...')
                success = self.source_data_manager.run_pipeline(source_id)
                if not success:
                    self.logger.info(
                        f'Attempting to build graph {graph_id}, building dependency {source_id} failed. Aborting...')
                    return False

            finalized_file_paths = self.source_data_manager.get_final_file_paths(source_id,
                                                                                 graph_source.source_version,
                                                                                 graph_source.parsing_version,
                                                                                 normalization_version,
                                                                                 graph_source.supplementation_version)
            graph_source.file_paths = finalized_file_paths

        return True

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

    def check_for_existing_build(self, graph_id: str, graph_version: str):
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

