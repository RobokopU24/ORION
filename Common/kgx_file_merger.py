import redis
import yaml
import os
import jsonlines
import json
import hashlib
from Common.utils import LoggingUtil

from kgx.cli.cli_utils import merge as kgx_merge
from kgx.utils.kgx_utils import prepare_data_dict as kgx_dict_merge
from Common.kgxmodel import GraphSpec, GraphSource
from Common.node_types import ORIGINAL_KNOWLEDGE_SOURCE, PRIMARY_KNOWLEDGE_SOURCE, SUBJECT_ID, OBJECT_ID, PREDICATE


class KGXFileMerger:

    def __init__(self):
        self.logger = LoggingUtil.init_logging("Data_services.Common.KGXFileMerger",
                                               line_format='medium',
                                               log_file_path=os.environ['DATA_SERVICES_LOGS'])
        self.redis_con = None
        try:
            self.redis_con = redis.Redis(
                host=os.environ['DATA_SERVICES_REDIS_HOST'],
                port=os.environ['DATA_SERVICES_REDIS_PORT'],
                password=os.environ['DATA_SERVICES_REDIS_PASSWORD']
            )
        except Exception as e:
            self.logger.warning(f"Creating redis connection failed with error {e}; " \
                                "Some merging types will not work")

    def merge(self,
              graph_spec: GraphSpec):
        self.redis_con: redis.Redis

        assert self.redis_con, "Error Merger needs redis connection"

        # Test redis connection

        self.redis_con.ping()

        data_sets_processed = set()

        NODE_KEY_PREFIX = 'node-'
        EDGE_KEY_PREFIX = 'edge-'

        # fun part Merging
        for graph_source in graph_spec.sources:
            if graph_source.merge_strategy == 'all':

                self.logger.info(f"Processing {graph_source.source_id}")
                for file_path in graph_source.file_paths:
                    if "nodes" in file_path:
                        # merging nodes
                        with jsonlines.open(file_path) as nodes:
                            # this is our merge condition
                            id_functor = lambda n: n['id']
                            # this is how we want to merge
                            merge_functor = kgx_dict_merge
                            matched_nodes = self.__merge_redis(
                                items=nodes,
                                id_functor=id_functor,
                                merge_functor=merge_functor,
                                redis_key_prefix=NODE_KEY_PREFIX)
                            self.logger.info(f"Matched {matched_nodes} nodes from redis for data set {graph_source.source_id}. "
                                             f"Datasets processed thus far : {data_sets_processed}")

                    elif "edges" in file_path:
                        # merge edges
                        with jsonlines.open(file_path) as edges:

                            # if we want certain datasets to be merged we just need to control how this id is generated
                            #  eg say we wanted to merge biolink and ctd edges . we can  if biolink in mergergable sets
                            # we just need to adjust how we compute that salt str(set('biolink', 'ctd')) etc ... would
                            # have that effect

                            edge_id_compute = lambda edge: hashlib.md5(
                                str(edge[SUBJECT_ID] +
                                    edge[PREDICATE] +
                                    edge[OBJECT_ID] +
                                    f'{edge.get(ORIGINAL_KNOWLEDGE_SOURCE, "")}{edge.get(PRIMARY_KNOWLEDGE_SOURCE, "")}'
                                    if f'{edge.get(ORIGINAL_KNOWLEDGE_SOURCE, "")}{edge.get(PRIMARY_KNOWLEDGE_SOURCE, "")}'
                                    else graph_source.source_id).encode('utf-8')
                            ).hexdigest()

                            # for now we practically don't merge on edges.
                            merge_functor = lambda edge_1, edge_2: edge_2

                            matched_edges = self.__merge_redis(items=edges,
                                                               id_functor=edge_id_compute,
                                                               merge_functor=merge_functor,
                                                               redis_key_prefix=EDGE_KEY_PREFIX,
                                                               re_adjust_id=True)

                            self.logger.info(f"Matched {matched_edges} edges from redis for data set {graph_source.source_id}. "
                                             f"Datasets processed thus far : {data_sets_processed}")
                    else:
                        raise ValueError(f"Did not recognize file {file_path} for merging "
                                         f"from data source {graph_source.source_id}.")

                data_sets_processed.add(graph_source.source_id)

        # create output dir
        os.makedirs(graph_spec.graph_output_dir, exist_ok=True)

        nodes_output_file_path = os.path.join(graph_spec.graph_output_dir, 'nodes.jsonl')
        edges_output_file_path = os.path.join(graph_spec.graph_output_dir, 'edges.jsonl')
        self.__write_redis_back_to_file(f'{NODE_KEY_PREFIX}*', nodes_output_file_path)
        self.__write_redis_back_to_file(f'{EDGE_KEY_PREFIX}*', edges_output_file_path)

        for graph_source in graph_spec.sources:
            if graph_source.merge_strategy == 'connected_edge_subset':
                file_path_iterator = iter(graph_source.file_paths)
                for file_path in file_path_iterator:
                    connected_edge_subset_nodes_file = file_path
                    connected_edge_subset_edges_file = next(file_path_iterator)
                    temp_nodes_file = f'{nodes_output_file_path}_temp'
                    temp_edges_file = f'{edges_output_file_path}_temp'
                    self.kgx_a_subset_b(nodes_output_file_path,
                                        edges_output_file_path,
                                        connected_edge_subset_nodes_file,
                                        connected_edge_subset_edges_file,
                                        temp_nodes_file,
                                        temp_edges_file)
                    os.remove(nodes_output_file_path)
                    os.remove(edges_output_file_path)
                    os.rename(temp_nodes_file, nodes_output_file_path)
                    os.rename(temp_edges_file, edges_output_file_path)
                data_sets_processed.add(graph_source.source_id)
                self.logger.info(f"Merged {graph_source.source_id} using connected_edge_subset merge strategy."
                                 f"Datasets processed thus far : {data_sets_processed}")
        if len(data_sets_processed) != len(graph_spec.sources):
            all_source_ids = [graph_source.source_id for graph_source in graph_spec.sources]
            missing_data_sets = [source_id for source_id in all_source_ids if source_id not in data_sets_processed]
            self.logger.error(f"Error merging graph {graph_spec.graph_id}! could not merge: {missing_data_sets}")

    def __merge_redis(self, items: iter, id_functor, merge_functor, re_adjust_id=False, redis_key_prefix=""):
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
                matched_items += self.__merge_redis_single_batch(items_from_data_set, merge_functor)
                # reset after flush
                items_from_data_set = {}
        # flush out remaining
        if len(items_from_data_set):
            matched_items += self.__merge_redis_single_batch(items_from_data_set, merge_functor)
        return matched_items

    def __merge_redis_single_batch(self, items_from_data_set, merge_functor):
        matched_items = 0
        items_from_redis = self.__read_items_from_redis(list(items_from_data_set.keys()))
        matched_items += len(items_from_redis)
        # merge them together on id using kgx merger
        for item_id in items_from_redis:
            items_from_data_set[item_id] = merge_functor(
                items_from_redis[item_id],
                items_from_data_set[item_id]
            )
            # write all nodes for this batch to redis
        self.__write_items_to_redis(items_from_data_set)
        return matched_items

    def __read_items_from_redis(self, ids):
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

    def __write_items_to_redis(self, items):
        chunk_size = 10_000  # batch for redis beyond this cap it might not be optimal, according to redis docs
        pipeline = self.redis_con.pipeline()
        all_keys = list(items.keys())
        chunked_keys = [all_keys[start: start + chunk_size] for start in
                        range(0, len(all_keys), chunk_size)]
        for keys in chunked_keys:
            for key in keys:
                pipeline.set(key, json.dumps(items[key]))
            pipeline.execute()

    def __delete_keys(self, items):
        # deletes keys
        chunk_size = 10_000
        pipeline = self.redis_con.pipeline()
        all_keys = list(items)
        chunked_keys = [all_keys[start: start + chunk_size] for start in range(0, len(all_keys), chunk_size)]
        for keys in chunked_keys:
            for key in keys:
                pipeline.delete(key)
            pipeline.execute()

    def __write_redis_back_to_file(self, redis_key_pattern, output_file_name):
        with open(output_file_name, 'w') as stream:
            self.logger.debug(f'Grabbing {redis_key_pattern} from redis,...')
            keys = self.redis_con.keys(redis_key_pattern)
            self.logger.debug(f'found {len(keys)} items in redis...')
            chunk_size = 500_000
            chunked_keys = [keys[start: start + chunk_size] for start in range(0, len(keys), chunk_size)]
            for chunk in chunked_keys:
                items = self.__read_items_from_redis(chunk)
                self.__delete_keys(chunk)
                items = [json.dumps(items[x]) + '\n' for x in items]
                stream.writelines(items)
                self.logger.debug(f"wrote : {len(items)}")
            self.logger.debug(f"Done writing {redis_key_pattern}")

    def jsonl_file_iterator(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as stream:
            for line in stream:
                yield json.loads(line)

    """
    Given two kgx sets, A and B, this program generates a new kgx file that contains:

    All nodes from A
    All edges from A
    All edges from B that have either subject node or object node that exists in A.
    All nodes in B that have connections to nodes in A.
    """
    def kgx_a_subset_b(self, node_file_a, edge_file_a, node_file_b, edge_file_b, new_node_file, new_edge_file):
        # first read all nodes from fileset A
        nodes = self.jsonl_file_iterator(node_file_a)
        node_ids = set([node['id'] for node in nodes])

        # filter edges from B that contain A
        edges = self.jsonl_file_iterator(edge_file_b)
        filtered_edges = [edge for edge in edges
                          if edge[SUBJECT_ID] in node_ids or edge[OBJECT_ID] in node_ids]

        self.logger.info(f'Found new {len(filtered_edges)} possible edges to add')

        # find node ids that filtered edges from B connect to not present in A
        filtered_edges_node_ids = set()
        for edge in filtered_edges:
            filtered_edges_node_ids.add(edge[SUBJECT_ID])
            filtered_edges_node_ids.add(edge[OBJECT_ID])
        filtered_edges_node_ids = filtered_edges_node_ids - node_ids

        self.logger.info(f'Found new {len(filtered_edges_node_ids)} node ids that are connected with edges from B')

        # get node data from B
        filtered_nodes_from_B = [node for node in self.jsonl_file_iterator(node_file_b)
                                 if node['id'] in filtered_edges_node_ids]

        # write out nodes
        with open(new_node_file, 'w', encoding='utf-8') as stream:
            # write out nodes of A
            for node in self.jsonl_file_iterator(node_file_a):
                stream.write(json.dumps(node) + '\n')
            # write new nodes from B
            for node in filtered_nodes_from_B:
                stream.write(json.dumps(node) + '\n')

        # write out edges
        with open(new_edge_file, 'w', encoding='utf-8') as stream:
            # write out edges from A
            for edge in self.jsonl_file_iterator(edge_file_a):
                stream.write(json.dumps(edge) + '\n')
            # write out new edges from B
            for edge in filtered_edges:
                stream.write(json.dumps(edge) + '\n')

    @staticmethod
    def merge_using_kgx(graph_spec: GraphSpec):

        source_dict = {}
        for source in graph_spec.sources:
            source_dict[source.source_id] = {'input': {'name': source.source_id,
                                                       'format': graph_spec.graph_output_format,
                                                       'filename': source.file_paths}}

        """
        saving this in case we want to merge straight into a graph in the future
        destination_dict = {'output': {'format': 'neo4j',
                                       'uri': self.neo4j_uri,
                                       'username': self.neo4j_user,
                                       'password': self.neo4j_password}}
        """

        destination_dict = {'output': {'format': graph_spec.graph_output_format,
                                       'filename': [graph_spec.graph_id]}}

        operations_dict = [{'name': 'kgx.graph_operations.summarize_graph.generate_graph_stats',
                            'args': {'graph_name': graph_spec.graph_id,
                                     'filename': f'{os.path.join(graph_spec.graph_output_dir, graph_spec.graph_id)}_stats.yaml'}}]

        merged_graph = {'name': graph_spec.graph_id,
                        'source': source_dict,
                        'operations': operations_dict,
                        'destination': destination_dict}

        configuration_dict = {
            "output_directory": graph_spec.graph_output_dir,
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
        kgx_merge_config_filepath = os.path.join(graph_spec.graph_output_dir, kgx_merge_config_filename)
        with open(kgx_merge_config_filepath, 'w') as merge_cfg_file:
            yaml.dump(kgx_merge_config, merge_cfg_file)
        kgx_merge(kgx_merge_config_filepath)
        os.remove(kgx_merge_config_filepath)
