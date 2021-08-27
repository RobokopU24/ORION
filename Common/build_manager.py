
import os
import yaml
import argparse
from dataclasses import dataclass
from kgx.cli.cli_utils import merge as kgx_merge
from kgx.utils.kgx_utils import prepare_data_dict as kgx_dict_merge
from Common.utils import LoggingUtil
from Common.load_manager import SourceDataLoadManager
from Common.kgx_file_writer import KGXFileWriter
import jsonlines, json, hashlib
import redis

@dataclass
class GraphSpec:
    graph_id: str
    graph_version: str
    graph_output_format: str
    graph_output_file: str
    sources: list
    merger: str = "custom"


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
        self.redis_con = None
        try:
            self.redis_con = redis.Redis(
                host=os.environ['DATA_SERVICES_REDIS_HOST'],
                port=os.environ['DATA_SERVICES_REDIS_PORT'],
                password=os.environ['DATA_SERVICES_REDIS_PASSWORD']
            )
        except Exception as e:
            self.logger.warning("Creating redis connection failed with error {e}; " \
                                "Some merging types will not work")

    def build_all_graphs(self):
        for graph_spec in self.graph_specs:
            self.build_graph(graph_spec)

    def build_graph_kgx(self, source_dict: dict, graph_id: str, graph_version: str, graph_output_format: str, graph_output_file: str, data_dir: str):
        """
        saving this in case we want to merge straight into a graph in the future
        destination_dict = {'output': {'format': 'neo4j',
                                       'uri': self.neo4j_uri,
                                       'username': self.neo4j_user,
                                       'password': self.neo4j_password}}
        """

        destination_dict = {'output': {'format': graph_output_format,
                                       'filename': [graph_output_file]}}

        operations_dict = [{'name': 'kgx.graph_operations.summarize_graph.generate_graph_stats',
                            'args': {'graph_name': graph_id,
                                     'filename': f'{os.path.join(self.graphs_dir, graph_output_file)}.yaml'}}]

        merged_graph = {'name': graph_id,
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

        kgx_merge_config_filename = f'{graph_id}_{graph_version}_merge_kgx_config.yml'
        kgx_merge_config_filepath = os.path.join(self.graphs_dir, kgx_merge_config_filename)
        with open(kgx_merge_config_filepath, 'w') as merge_cfg_file:
            yaml.dump(kgx_merge_config, merge_cfg_file)
        kgx_merge(kgx_merge_config_filepath)
        os.remove(kgx_merge_config_filepath)

    def build_graph_custom(self, source_dict: dict, graph_id: str, graph_version: str, graph_output_format: str, graph_output_file: str, data_dir: str):

        self.redis_con: redis.Redis

        assert self.redis_con, "Error Merger needs redis connection"

        # Test redis connection

        self.redis_con.ping()

        data_sets_processed = set()

        NODE_KEY_PREFIX='node-'
        EDGE_KEY_PREFIX='edge-'

        for data_source_name, spec in source_dict.items():

            format = spec['input']['format']
            files = spec['input']['filename']
            self.logger.info(f"Processing {data_source_name}")

            if format == 'jsonl':
                assert len(files) == 2, f"Error Expected jsonl formatted files to come in pairs, " \
                                         f"Dataset {data_source_name} resolved to {files}."
                try:
                    nodes_file = [file_name for file_name in files if "nodes" in file_name][0]
                    edges_file = [file_name for file_name in files if "edges" in file_name][0]
                except IndexError:
                    raise ValueError(f"Could not detect edges and/or nodes file from {files} "
                                     f"for Dataset {data_source_name}")

                # fun part Merging

                # merging nodes

                with jsonlines.open(nodes_file) as nodes:
                    # this is our merge condition
                    id_functor = lambda n: n['id']
                    # this is how we want to merge
                    merge_functor = kgx_dict_merge
                    matched_nodes = self.merge_redis(
                        nodes,
                        id_functor=id_functor,
                        merge_functor=merge_functor,
                        redis_key_prefix=NODE_KEY_PREFIX
                    )
                    self.logger.info(f"Matched {matched_nodes} nodes from redis for data set {data_source_name}. "
                                     f"Datasets processed thus far : {data_sets_processed}")


                # merge edges

                with jsonlines.open(edges_file) as edges:
                    counter = 0
                    edges_from_file = {}
                    matched_edges = 0
                    # if we want certain datasets to be merged we just need to control how this id is generated
                    #  eg say we wanted to merge biolink and ctd edges . we can  if biolink in mergergable sets
                    # we just need to adjust how we compute that salt str(set('biolink', 'ctd')) etc ... would
                    # have that effect

                    edge_id_uniqueness_salt = data_source_name
                    edge_id_compute = lambda edge: hashlib.md5(
                        str(edge['subject'] +
                        edge['predicate'] +
                        edge['object'] +
                        edge_id_uniqueness_salt).encode('utf-8')
                    ).hexdigest()

                    # for now we practically don't merge on edges.
                    merge_functor = lambda edge_1, edge_2: edge_2

                    matched_edges = self.merge_redis(items=edges,
                                                     id_functor=edge_id_compute,
                                                     merge_functor=merge_functor,
                                                     redis_key_prefix=EDGE_KEY_PREFIX,
                                                     re_adjust_id=True)

                    self.logger.info(f"Matched {matched_edges} edges from redis for data set {data_source_name}. "
                                     f"Datasets processed thus far : {data_sets_processed}")
                data_sets_processed.add(data_source_name)


        # create output dir
        output_dir = os.path.join(data_dir, graph_id)
        os.makedirs(output_dir, exist_ok=True)

        self.write_redis_back_to_file(f'{NODE_KEY_PREFIX}*', os.path.join(output_dir, 'nodes.jsonl'))
        self.write_redis_back_to_file(f'{EDGE_KEY_PREFIX}*', os.path.join(output_dir, 'edges.jsonl'))

    def merge_redis(self, items: iter, id_functor, merge_functor, re_adjust_id=False, redis_key_prefix=""):
        counter = 0
        flush_interval = 100_000
        items_from_data_set = {}
        matched_items = 0
        for item in items:
            if re_adjust_id:
                item['id'] = id_functor(item)
            # we can scan redis for {redis_key_prefix}* and write those to output jsonl file.
            items_from_data_set[f'{redis_key_prefix}{id_functor(item)}'] = item

            counter += 1
            if counter % flush_interval == 0:
                matched_items += self.merge_redis_single_batch(items_from_data_set, merge_functor)
                # reset after flush
                items_from_data_set = {}
        # flush out remaining
        if len(items_from_data_set):
            matched_items += self.merge_redis_single_batch(items_from_data_set, merge_functor)
        return matched_items

    def merge_redis_single_batch(self, items_from_data_set, merge_functor):
        matched_items = 0
        items_from_redis = self.read_items_from_redis(list(items_from_data_set.keys()))
        matched_items += len(items_from_redis)
        # merge them together on id using kgx merger
        for item_id in items_from_redis:
            items_from_data_set[item_id] = merge_functor(
                items_from_redis[item_id],
                items_from_data_set[item_id]
            )
            # write all nodes for this batch to redis
        self.write_items_to_redis(items_from_data_set)
        return matched_items

    def read_items_from_redis(self, ids):
        chunk_size = 10_000  # batch for pipeline
        pipeline = self.redis_con.pipeline()
        response = {}
        chunked_ids = [ids[start: start + chunk_size] for start in range(0, len(ids), chunk_size)]
        for ids in chunked_ids:
            for i in ids:
                pipeline.get(i)
            result = pipeline.execute()
            for i, res in zip(ids, result):
                if res:
                    response.update({i: json.loads(res)})
        return response

    def write_items_to_redis(self, items):
        chunk_size = 10_000  # batch for redis beyond this cap it might not be optimal, according to redis docs
        pipeline = self.redis_con.pipeline()
        all_keys = list(items.keys())
        chunked_keys = [all_keys[start: start + chunk_size] for start in
                        range(0, len(all_keys), chunk_size)]
        for keys in chunked_keys:
            for key in keys:
                pipeline.set(key, json.dumps(items[key]))
            pipeline.execute()

    def delete_keys(self, items):
        # deletes keys
        chunk_size = 10_000
        pipeline = self.redis_con.pipeline()
        all_keys = list(items)
        chunked_keys = [all_keys[start: start + chunk_size] for start in range(0, len(all_keys), chunk_size)]
        for keys in chunked_keys:
            for key in keys:
                pipeline.delete(key)
            pipeline.execute()

    def write_redis_back_to_file(self, redis_key_pattern, output_file_name):
        with open(output_file_name, 'w') as stream:
            self.logger.info(f'Grabbing {redis_key_pattern} from redis,...')
            keys = self.redis_con.keys(redis_key_pattern)
            self.logger.info(f'found {len(keys)} items in redis...')
            chunk_size = 500_000
            chunked_keys = [keys[start: start + chunk_size] for start in range(0, len(keys), chunk_size) ]
            for chunk in chunked_keys:
                items = self.read_items_from_redis(chunk)
                self.delete_keys(chunk)
                items = [json.dumps(items[x]) + '\n' for x in items]
                stream.writelines(items)
                self.logger.info(f"wrote : {len(items)}")
            self.logger.info(f"Done writing {redis_key_pattern}")

    def build_graph(self, graph_spec: GraphSpec):

        self.logger.info(f'Building graph {graph_spec.graph_id}..')

        source_dict = {}
        for source in graph_spec.sources:
            source_id = source.source_id
            source_load_version = source.load_version
            if source_id not in self.data_manager.metadata:
                self.logger.info(f'Could not build graph {graph_spec.graph_id} because {source_id} is not active.')
                return
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

        if graph_spec.merger == 'KGX':
            self.build_graph_kgx(
                source_dict=source_dict,
                graph_id=graph_spec.graph_id,
                graph_version=graph_spec.graph_version,
                graph_output_format=graph_spec.graph_output_format,
                graph_output_file=graph_spec.graph_output_file,
                data_dir=self.graphs_dir
            )
        else:
            self.build_graph_custom(
                source_dict=source_dict,
                graph_id=graph_spec.graph_id,
                graph_version=graph_spec.graph_version,
                graph_output_format=graph_spec.graph_output_format,
                graph_output_file=graph_spec.graph_output_file,
                data_dir=self.graphs_dir
            )

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
