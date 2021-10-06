
import os
import yaml
import argparse
from Common.utils import LoggingUtil
from Common.load_manager import SourceDataLoadManager
from Common.kgx_file_merger import KGXFileMerger
from Common.kgxmodel import GraphSource, GraphSpec


class GraphBuilder:

    def __init__(self):

        self.logger = LoggingUtil.init_logging("Data_services.Common.GraphBuilder",
                                               line_format='medium',
                                               log_file_path=os.environ['DATA_SERVICES_LOGS'])

        self.data_manager = SourceDataLoadManager()
        self.graphs_dir = self.init_graphs_dir()
        self.graph_specs = self.load_graph_specs()

    def build_all_graphs(self):
        for graph_spec in self.graph_specs:
            self.build_graph(graph_spec)

    def build_graph(self, graph_spec: GraphSpec):

        self.logger.info(f'Building graph {graph_spec.graph_id}..')
        source_merger = KGXFileMerger()
        source_merger.merge(graph_spec)
        # if we wanted to use KGX
        # source_merger.merge_using_kgx(graph_spec)
        self.logger.info(f'Building graph {graph_spec.graph_id} complete!')

    def load_graph_specs(self):
        if 'DATA_SERVICES_GRAPH_SPEC' in os.environ:
            graph_spec_file = os.environ['DATA_SERVICES_GRAPH_SPEC']
            graph_spec_path = os.path.join(self.graphs_dir, graph_spec_file)
            self.logger.info(f'Loaded custom graph spec at {graph_spec_file}.')
        else:
            graph_spec_path = os.path.dirname(os.path.abspath(__file__)) + '/../default-graph-spec.yml'
            self.logger.debug(f'Loaded default graph spec.')

        with open(graph_spec_path) as graph_spec_file:
            graph_spec_yaml = yaml.full_load(graph_spec_file)
            graph_specs = []
            if not graph_spec_yaml['graphs']:
                self.logger.warning(f'Warning: No graphs were found in the graph spec.')
                return graph_specs

            for graph_yaml in graph_spec_yaml['graphs']:
                graph_id = graph_yaml['graph_id']
                graph_sources = []
                for source in graph_yaml['sources']:
                    source_id = source['source_id']
                    if source_id not in self.data_manager.metadata:
                        self.logger.info(
                            f'Could not build graph {graph_id} because {source_id} is not active.')
                        graph_sources = None
                        break

                    load_version = source['load_version'] if 'load_version' in source else 'latest'
                    merge_strategy = source['merge_strategy'] if 'merge_strategy' in source else 'default'
                    source_metadata = self.data_manager.metadata[source_id]
                    if not source_metadata.is_ready_to_build():
                        self.logger.info(
                            f'Could not build graph {graph_id} because {source_id} is not stable.')
                        graph_sources = None
                        break
                    else:
                        file_paths = list()
                        file_paths.append(
                            self.data_manager.get_normalized_node_file_path(source_id, load_version))
                        file_paths.append(
                            self.data_manager.get_normalized_edge_file_path(source_id, load_version))
                        if source_metadata.has_supplemental_data():
                            file_paths.append(self.data_manager.get_normalized_supp_node_file_path(source_id,
                                                                                                   load_version))
                            file_paths.append(
                                self.data_manager.get_normalized_supplemental_edge_file_path(source_id,
                                                                                             load_version))
                        graph_sources.append(GraphSource(source_id=source_id,
                                                         load_version=load_version,
                                                         file_paths=file_paths,
                                                         merge_strategy=merge_strategy))
                if graph_sources:
                    graph_output_format = graph_yaml['output_format'] if 'output_format' in graph_yaml else 'jsonl'
                    current_graph_spec = GraphSpec(graph_id=graph_id,
                                                   graph_version=1,
                                                   graph_output_format=graph_output_format,
                                                   graph_output_dir=os.path.join(self.graphs_dir, graph_id),
                                                   sources=graph_sources)
                    graph_specs.append(current_graph_spec)
            self.logger.debug(f'Loaded {len(graph_specs)} graph specs ..')
        return graph_specs

    def get_graph_spec(self, graph_id: str):
        for graph_spec in self.graph_specs:
            if graph_spec.graph_id == graph_id:
                return graph_spec
        return None

    def init_graphs_dir(self):
        # use the storage directory specified by the environment variable DATA_SERVICES_STORAGE
        # create or verify graph directory is at the top level of the storage directory
        if 'DATA_SERVICES_GRAPHS' in os.environ and os.path.isdir(os.environ['DATA_SERVICES_GRAPHS']):
            return os.environ['DATA_SERVICES_GRAPHS']
        else:
            # if graph dir is invalid or not specified back out
            raise IOError(
                'GraphBuilder graphs directory not found. '
                'Specify the graphs directory with environment variable DATA_SERVICES_GRAPHS.')


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

