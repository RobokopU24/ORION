
import os
import yaml
import argparse
from dataclasses import dataclass
from kgx.cli.cli_utils import merge as kgx_merge
from Common.utils import LoggingUtil
from Common.load_manager import SourceDataLoadManager


@dataclass
class GraphSpec:
    graph_id: str
    graph_version: str
    graph_output_format: str
    graph_output_file: str
    sources: list


@dataclass
class GraphSource:
    source_id: str
    load_version: str


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

        source_dict = {}
        for source in graph_spec.sources:
            source_id = source.source_id
            source_load_version = source.load_version
            source_metadata = self.data_manager.metadata[source_id]
            if source_metadata.is_ready_to_build():
                file_paths = list()
                file_paths.append(self.data_manager.get_normalized_node_file_path(source_id, source_load_version))
                file_paths.append(self.data_manager.get_normalized_edge_file_path(source_id, source_load_version))
                if source_metadata.has_supplemental_data():
                    file_paths.append(self.data_manager.get_normalized_supp_node_file_path(source_id, source_load_version))
                    file_paths.append(self.data_manager.get_normalized_supplemental_edge_file_path(source_id, source_load_version))
                source_dict[source.source_id] = {'input': {'name': source_id,
                                                           'format': 'jsonl',
                                                           'filename': file_paths}}
            else:
                self.logger.info(f'Could not build graph {graph_spec.graph_id} because {source_id} is not stable.')
                return


        """
        saving this in case we want to merge straight into a graph in the future
        destination_dict = {'output': {'format': 'neo4j',
                                       'compression': self.neo4j_uri,
                                       'username': self.neo4j_user,
                                       'password': self.neo4j_password}}
        """
        destination_dict = {'output': {'format': graph_spec.graph_output_format,
                                       'filename': [graph_spec.graph_output_file]}}

        operations_dict = [{'name': 'kgx.graph_operations.summarize_graph.generate_graph_stats',
                            'args': {'graph_name': graph_spec.graph_id,
                                     'filename': f'{os.path.join(self.graphs_dir, graph_spec.graph_output_file)}.yaml'}}]

        merged_graph = {'name': graph_spec.graph_id,
                        'source': source_dict,
                        'operations': operations_dict,
                        'destination': destination_dict}

        configuration_dict = {
            "output_directory": self.graphs_dir,
            "checkpoint": False,
            "prefix_map": None,
            "predicate_mappings": None,
            "property_types": None,
            "reverse_prefix_map": None,
            "reverse_predicate_mappings": None,
        }

        kgx_merge_config = {'configuration': configuration_dict,
                            'merged_graph': merged_graph}

        kgx_merge_config_filename = f'{graph_spec.graph_id}_{graph_spec.graph_version}_merge_kgx_config.yml'
        kgx_merge_config_filepath = os.path.join(self.graphs_dir, kgx_merge_config_filename)
        with open(kgx_merge_config_filepath, 'w') as merge_cfg_file:
            yaml.dump(kgx_merge_config, merge_cfg_file)
        kgx_merge(kgx_merge_config_filepath)
        os.remove(kgx_merge_config_filepath)

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
            for graph_yaml in graph_spec_yaml['graphs']:
                graph_id = graph_yaml['graph_id']
                graph_sources = []
                for source in graph_yaml['sources']:
                    load_version = source['load_version'] if 'load_version' in source else 'latest'
                    graph_sources.append(GraphSource(source_id=source['source_id'], load_version=load_version))
                    self.logger.info(f'Adding source {source["source_id"]}')
                graph_output_format = graph_yaml['output_format'] if 'output_format' in graph_yaml else 'jsonl'
                graph_output_file = graph_yaml['output_file_name'] if 'output_file_name' in graph_yaml else graph_id
                current_graph_spec = GraphSpec(graph_id=graph_id,
                                               graph_version=1,
                                               graph_output_format=graph_output_format,
                                               graph_output_file=graph_output_file,
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
        if not spec:
            print(f'Invalid graph id requested: {graph_id_arg}')
        graph_builder.build_graph(spec)
