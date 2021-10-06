import redis
import os
import jsonlines
import orjson
import hashlib
from Common.utils import LoggingUtil
from kgx.utils.kgx_utils import prepare_data_dict as kgx_dict_merge
from Common.kgxmodel import GraphSpec, GraphSource
from Common.node_types import ORIGINAL_KNOWLEDGE_SOURCE, PRIMARY_KNOWLEDGE_SOURCE, SUBJECT_ID, OBJECT_ID, PREDICATE
# from kgx.cli.cli_utils import merge as kgx_merge
# from Common.kgx_file_writer import KGXFileWriter


def quick_json_dumps(item):
    return str(orjson.dumps(item), encoding='utf-8')


def quick_json_loads(item):
    return orjson.loads(item)


def quick_jsonl_file_iterator(json_file):
    with open(json_file, 'r', encoding='utf-8') as stream:
        for line in stream:
            yield orjson.loads(line)


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

        if not graph_spec.sources:
            self.logger.error(f'Merge attempted but {graph_spec.graph_id} had no sources to merge.')
            return

        nodes_output_file_path = os.path.join(graph_spec.graph_output_dir, 'nodes.jsonl')
        edges_output_file_path = os.path.join(graph_spec.graph_output_dir, 'edges.jsonl')
        if os.path.exists(nodes_output_file_path) or os.path.exists(edges_output_file_path):
            self.logger.error(f'Merge attempted for {graph_spec.graph_id} but merged files already existed!')
            return

        # create output dir
        os.makedirs(graph_spec.graph_output_dir, exist_ok=True)

        # group the sources based on their merge strategy, we'll process the primary sources first
        processed_sources = []
        primary_sources = []
        secondary_sources = []
        for graph_source in graph_spec.sources:
            if graph_source.merge_strategy == 'default':
                primary_sources.append(graph_source)
            elif graph_source.merge_strategy == 'connected_edge_subset':
                secondary_sources.append(graph_source)

        # TODO we should be able to process a single primary source more efficiently
        # if len(primary_sources) == 1:
        #    self.process_single_source(primary_sources[0], nodes_output_file_path, edges_output_file_path)
        #    processed_sources = [primary_sources[0]]
        # else:
        primary_sources_processed = self.process_many_sources(primary_sources, nodes_output_file_path, edges_output_file_path)
        processed_sources.extend(primary_sources_processed)

        for graph_source in secondary_sources:
            if graph_source.merge_strategy == 'connected_edge_subset':
                self.logger.info(f"Merging {graph_source.source_id} using connected_edge_subset merge strategy. "
                                 f"Datasets processed thus far : {processed_sources}")

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
                processed_sources.append(graph_source.source_id)
                self.logger.info(f"Merged {graph_source.source_id} using connected_edge_subset merge strategy. "
                                 f"Datasets processed thus far : {processed_sources}")
        if len(processed_sources) != len(graph_spec.sources):
            all_source_ids = [graph_source.source_id for graph_source in graph_spec.sources]
            missing_data_sets = [source_id for source_id in all_source_ids if source_id not in processed_sources]
            self.logger.error(f"Error merging graph {graph_spec.graph_id}! could not merge: {missing_data_sets}")

    def process_many_sources(self, graph_sources: list, nodes_out_file: str, edges_out_file: str):
        node_key_prefix = 'node-'
        edge_key_prefix = 'edge-'
        processed_sources = []
        for graph_source in graph_sources:
            self.logger.info(f"Processing {graph_source.source_id}. Sources processed thus far : {processed_sources}")
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
                            redis_key_prefix=node_key_prefix)
                        self.logger.info(f"Matched {matched_nodes} nodes from redis for {graph_source.source_id} - {file_path}.")

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
                                if ((ORIGINAL_KNOWLEDGE_SOURCE in edge) or (PRIMARY_KNOWLEDGE_SOURCE in edge))
                                else graph_source.source_id).encode('utf-8')
                        ).hexdigest()

                        # for now we practically don't merge on edges.
                        merge_functor = lambda edge_1, edge_2: edge_2

                        matched_edges = self.__merge_redis(items=edges,
                                                           id_functor=edge_id_compute,
                                                           merge_functor=merge_functor,
                                                           redis_key_prefix=edge_key_prefix,
                                                           re_adjust_id=True)

                        self.logger.info(f"Matched {matched_edges} edges from redis for {graph_source.source_id} - {file_path}.")
                else:
                    raise ValueError(f"Did not recognize file {file_path} for merging "
                                     f"from data source {graph_source.source_id}.")
            processed_sources.append(graph_source.source_id)
        self.__write_redis_back_to_file(f'{node_key_prefix}*', nodes_out_file)
        self.__write_redis_back_to_file(f'{edge_key_prefix}*', edges_out_file)
        return processed_sources

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
        # merge the items from the data set with the items in redis
        for item_id in items_from_redis:
            items_from_data_set[item_id] = merge_functor(
                items_from_redis[item_id],
                items_from_data_set[item_id]
            )
        # write all of the merged items back to redis
        self.__write_items_to_redis(items_from_data_set)
        return matched_items

    def __read_items_from_redis(self, ids, return_json_objects=True):
        chunk_size = 10_000  # batch for pipeline
        response = {}
        chunked_ids = [ids[start: start + chunk_size] for start in range(0, len(ids), chunk_size)]
        for ids in chunked_ids:
            result = self.redis_con.mget(ids)
            for i, res in zip(ids, result):
                if res:
                    if return_json_objects:
                        # if return_json_objects desired parse the response with loads
                        response.update({i: quick_json_loads(res)})
                    else:
                        # otherwise return the response straight from redis (bytes as of now)
                        response.update({i: res})
        return response

    def __write_items_to_redis(self, items):
        chunk_size = 10_000  # batch for redis beyond this cap it might not be optimal, according to redis docs
        all_keys = list(items.keys())
        chunked_keys = [all_keys[start: start + chunk_size] for start in
                        range(0, len(all_keys), chunk_size)]
        for keys in chunked_keys:
            items_to_write = {key: quick_json_dumps(items[key]) for key in keys}
            self.redis_con.mset(items_to_write)

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
            self.logger.debug(f'Grabbing {redis_key_pattern} from redis...')
            keys = self.redis_con.keys(redis_key_pattern)
            self.logger.debug(f'found {len(keys)} items in redis...')
            chunk_size = 500_000
            chunked_keys = [keys[start: start + chunk_size] for start in range(0, len(keys), chunk_size)]
            for chunk in chunked_keys:
                # to avoid parsing the json into an object for no reason use return_json_objects=False
                # but then items will be a dictionary of {key: bytes_object}
                # so we need to decode the bytes before we write to file
                items = self.__read_items_from_redis(chunk, return_json_objects=False)
                items = [f'{items[x].decode("utf-8") }\n' for x in items]
                stream.writelines(items)
                self.__delete_keys(chunk)
                self.logger.debug(f"wrote : {len(items)}")
            self.logger.debug(f"Done writing {redis_key_pattern}")

    """
    This is on hold / TBD - we should be able to process individual sources more efficiently
    def process_single_source(self, graph_source: GraphSource, nodes_out_file: str, edges_out_file: str):
        self.logger.info(f"Processing single primary source {graph_source.source_id}.")
        files_processed = 0
        files_to_process = graph_source.file_paths
        if len(files_to_process) <= 2:
            # TODO just copy them over, never needed to merge
            # maybe split nodes and edges file paths in GraphSource to make that more flexible
        else:
            # merge all the files from just one source - probably dont need to check duplicates
    """

    """
    Given two kgx sets, A and B, generate a new kgx file that contains:
    All nodes and edges from A
    Edges from B that connect to nodes in A, and any nodes from B that they connect to.
    """
    def kgx_a_subset_b(self, node_file_a, edge_file_a, node_file_b, edge_file_b, new_node_file, new_edge_file):
        # first read all nodes from fileset A
        nodes_reader = quick_jsonl_file_iterator(node_file_a)
        node_ids = set([node['id'] for node in nodes_reader])

        # filter edges from B that contain nodes from A
        edges_reader = quick_jsonl_file_iterator(edge_file_b)
        filtered_edges = [edge for edge in edges_reader
                          if edge[SUBJECT_ID] in node_ids or edge[OBJECT_ID] in node_ids]

        self.logger.info(f'Found {len(filtered_edges)} new edges to add')

        # find node ids that filtered edges from B connect to not present in A
        filtered_edges_node_ids = set()
        for edge in filtered_edges:
            filtered_edges_node_ids.add(edge[SUBJECT_ID])
            filtered_edges_node_ids.add(edge[OBJECT_ID])
        filtered_edges_node_ids = filtered_edges_node_ids - node_ids

        self.logger.info(f'Found {len(filtered_edges_node_ids)} new nodes from connected edges')

        # get node data from B
        nodes_reader = quick_jsonl_file_iterator(node_file_b)
        filtered_nodes_from_b = [node for node in nodes_reader
                                 if node['id'] in filtered_edges_node_ids]

        # write out nodes
        with open(new_node_file, 'w', encoding='utf-8') as stream:
            # write out jsonl nodes of A
            with open(node_file_a) as node_file_a_reader:
                stream.writelines(node_file_a_reader)
            # write new nodes from B
            for node in filtered_nodes_from_b:
                stream.write(quick_json_dumps(node) + '\n')

        # write out edges
        with open(new_edge_file, 'w', encoding='utf-8') as stream:
            # write out jsonl edges from A
            with open(edge_file_a) as edge_file_a_reader:
                stream.writelines(edge_file_a_reader)
            # write out new edges from B
            for edge in filtered_edges:
                stream.write(quick_json_dumps(edge) + '\n')

    """
    # this is probably going to be removed later
    # saving in case we want to load kgx files straight into a graph
    @staticmethod
    def merge_using_kgx(graph_spec: GraphSpec):

        source_dict = {}
        for source in graph_spec.sources:
            source_dict[source.source_id] = {'input': {'name': source.source_id,
                                                       'format': graph_spec.graph_output_format,
                                                       'filename': source.file_paths}}
         
        destination_dict = {'output': {'format': 'neo4j',
                                       'uri': self.neo4j_uri,
                                       'username': self.neo4j_user,
                                       'password': self.neo4j_password}}

        #destination_dict = {'output': {'format': graph_spec.graph_output_format,
        #                               'filename': [graph_spec.graph_id]}}

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
    """
