import redis
import os
import jsonlines
import orjson
from xxhash import xxh64_hexdigest
from Common.utils import LoggingUtil
from kgx.utils.kgx_utils import prepare_data_dict as kgx_dict_merge
from Common.kgxmodel import GraphSpec, GraphSource
from Common.node_types import ORIGINAL_KNOWLEDGE_SOURCE, PRIMARY_KNOWLEDGE_SOURCE, SUBJECT_ID, OBJECT_ID, PREDICATE


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
              graph_spec: GraphSpec,
              nodes_output_file_path: str,
              edges_output_file_path: str):

        # Test redis connection
        self.redis_con: redis.Redis
        assert self.redis_con, "Error Merger needs redis connection"
        self.redis_con.ping()

        if not graph_spec.sources:
            merge_error_msg = f'Merge attempted but {graph_spec.graph_id} had no sources to merge.'
            self.logger.error(merge_error_msg)
            return {'merge_error': merge_error_msg}

        if os.path.exists(nodes_output_file_path) or os.path.exists(edges_output_file_path):
            merge_error_msg = f'Merge attempted for {graph_spec.graph_id} but merged files already existed!'
            self.logger.error(merge_error_msg)
            return {'merge_error': merge_error_msg}

        # group the sources based on their merge strategy, we'll process the primary sources first
        primary_sources = []
        secondary_sources = []
        for graph_source in graph_spec.sources:
            if graph_source.merge_strategy == 'default':
                primary_sources.append(graph_source)
            elif graph_source.merge_strategy == 'connected_edge_subset':
                secondary_sources.append(graph_source)

        # TODO we should be able to process a single primary source more efficiently (ie copy and paste it)
        # if len(primary_sources) == 1:
        #    self.process_single_source(primary_sources[0], nodes_output_file_path, edges_output_file_path)
        # else:

        merge_metadata = {
            'sources': {},
            'final_node_count': 0,
            'final_edge_count': 0
        }

        self.merge_primary_sources(primary_sources,
                                   nodes_output_file_path,
                                   edges_output_file_path,
                                   merge_metadata)

        self.merge_secondary_sources(secondary_sources,
                                     nodes_output_file_path,
                                     edges_output_file_path,
                                     merge_metadata)

        if len(merge_metadata['sources']) != len(graph_spec.sources):
            all_source_ids = [graph_source.source_id for graph_source in graph_spec.sources]
            missing_data_sets = [source_id for source_id in all_source_ids if source_id not in merge_metadata['sources'].keys()]
            self.logger.error(f"Error merging graph {graph_spec.graph_id}! could not merge: {missing_data_sets}")

        return merge_metadata

    def merge_primary_sources(self,
                              graph_sources: list,
                              nodes_out_file: str,
                              edges_out_file: str,
                              merge_metadata: dict):

        # these are the callback functions used to merge nodes
        node_id_function = lambda n: n['id']
        node_merge_function = kgx_dict_merge

        node_key_prefix = 'node-'
        edge_key_prefix = 'edge-'
        for i, graph_source in enumerate(graph_sources, start=1):
            self.logger.info(f"Processing {graph_source.source_id}. (primary source {i}/{len(graph_sources)})")
            merge_metadata["sources"][graph_source.source_id] = {'source_version': graph_source.source_version}

            # these are the callback functions used to merge edges
            # need to declare this edge id function again for every graph source because the source id is included
            edge_id_function = lambda edge: xxh64_hexdigest(
                str(f'{edge[SUBJECT_ID]}{edge[PREDICATE]}{edge[OBJECT_ID]}' +
                    (f'{edge.get(ORIGINAL_KNOWLEDGE_SOURCE, "")}{edge.get(PRIMARY_KNOWLEDGE_SOURCE, "")}'
                    if ((ORIGINAL_KNOWLEDGE_SOURCE in edge) or (PRIMARY_KNOWLEDGE_SOURCE in edge))
                    else graph_source.source_id)))
            edge_merge_function = lambda edge_1, edge_2: edge_1

            for file_path in graph_source.file_paths:
                source_filename = file_path.rsplit('/')[-1]
                merge_metadata["sources"][graph_source.source_id][source_filename] = {}
                if "nodes" in file_path:
                    with jsonlines.open(file_path) as nodes:
                        nodes_count, merged_nodes_count = self.__merge_redis(items=nodes,
                                                                             id_function=node_id_function,
                                                                             merge_function=node_merge_function,
                                                                             redis_key_prefix=node_key_prefix)
                        merge_metadata["sources"][graph_source.source_id][source_filename]["nodes"] = nodes_count
                        merge_metadata["sources"][graph_source.source_id][source_filename]["nodes_merged"] = merged_nodes_count
                        if merged_nodes_count:
                            self.logger.info(f"Merged {merged_nodes_count} nodes from {graph_source.source_id}"
                                             f" - {file_path}.")

                elif "edges" in file_path:
                    with jsonlines.open(file_path) as edges:

                        # for now we don't merge edges, just return one of them
                        edges_count, merged_edge_count = self.__merge_redis(items=edges,
                                                                            id_function=edge_id_function,
                                                                            merge_function=edge_merge_function,
                                                                            redis_key_prefix=edge_key_prefix,
                                                                            re_adjust_id=True)
                        merge_metadata["sources"][graph_source.source_id][source_filename]["edges"] = edges_count
                        merge_metadata["sources"][graph_source.source_id][source_filename]["edges_merged"] = merged_edge_count
                        if merged_edge_count:
                            self.logger.info(f"Merged {merged_edge_count} edges from {graph_source.source_id}"
                                             f" - {file_path}.")
                else:
                    raise ValueError(f"Did not recognize file {file_path} for merging "
                                     f"from data source {graph_source.source_id}.")

        nodes_written = self.__write_redis_back_to_file(f'{node_key_prefix}*', nodes_out_file)
        edges_written = self.__write_redis_back_to_file(f'{edge_key_prefix}*', edges_out_file)
        self.redis_con.flushdb()
        merge_metadata['final_node_count'] += nodes_written
        merge_metadata['final_edge_count'] += edges_written
        return True

    def merge_secondary_sources(self,
                                graph_sources: list,
                                nodes_output_file_path: str,
                                edges_output_file_path: str,
                                merge_metadata: dict):
        for i, graph_source in enumerate(graph_sources, start=1):
            self.logger.info(f"Processing {graph_source.source_id}. (secondary source {i}/{len(graph_sources)})")
            if graph_source.merge_strategy == 'connected_edge_subset':
                self.logger.info(f"Merging {graph_source.source_id} using connected_edge_subset merge strategy.")

                merge_metadata["sources"][graph_source.source_id] = {'source_version': graph_source.source_version}

                file_path_iterator = iter(graph_source.file_paths)
                for file_path in file_path_iterator:
                    connected_edge_subset_nodes_file = file_path
                    connected_edge_subset_edges_file = next(file_path_iterator)
                    if not (('nodes' in connected_edge_subset_nodes_file) and
                        ('edges' in connected_edge_subset_edges_file)):
                        raise IOError(f'File paths were not in node, edge ordered pairs: {connected_edge_subset_nodes_file},{connected_edge_subset_edges_file}')
                    temp_nodes_file = f'{nodes_output_file_path}_temp'
                    temp_edges_file = f'{edges_output_file_path}_temp'
                    new_node_count, new_edge_count = self.kgx_a_subset_b(nodes_output_file_path,
                                                                         edges_output_file_path,
                                                                         connected_edge_subset_nodes_file,
                                                                         connected_edge_subset_edges_file,
                                                                         temp_nodes_file,
                                                                         temp_edges_file)
                    os.remove(nodes_output_file_path)
                    os.remove(edges_output_file_path)
                    os.rename(temp_nodes_file, nodes_output_file_path)
                    os.rename(temp_edges_file, edges_output_file_path)
                    nodes_source_filename = connected_edge_subset_nodes_file.rsplit('/')[-1]
                    edges_source_filename = connected_edge_subset_edges_file.rsplit('/')[-1]

                    # due to the algorithm implemented in kgx_a_subset_b there are no mergers to log
                    merge_metadata["sources"][graph_source.source_id][nodes_source_filename] = {
                        "nodes": new_node_count,
                        "nodes_merged": 0,
                    }
                    merge_metadata["sources"][graph_source.source_id][edges_source_filename] = {
                        "edges": new_edge_count,
                        "edges_merged": 0
                    }
                    merge_metadata['final_node_count'] += new_node_count
                    merge_metadata['final_edge_count'] += new_edge_count

    def __merge_redis(self, items: iter, id_function, merge_function, re_adjust_id=False, redis_key_prefix=""):
        counter = 0
        flush_interval = 100_000
        items_from_data_set = {}
        matched_items = 0
        for item in items:
            new_item_id = id_function(item)
            if re_adjust_id:
                item['id'] = new_item_id
            # we can scan redis for {redis_key_prefix}* and write those to output jsonl file.
            items_from_data_set[f'{redis_key_prefix}{new_item_id}'] = item

            counter += 1
            if counter % flush_interval == 0:
                matched_items += self.__merge_redis_single_batch(items_from_data_set, merge_function)
                # reset after flush
                items_from_data_set = {}
        # flush out remaining
        if len(items_from_data_set):
            matched_items += self.__merge_redis_single_batch(items_from_data_set, merge_function)
        return counter, matched_items

    def __merge_redis_single_batch(self, items_from_data_set, merge_function):
        matched_items = 0
        items_from_redis = self.__read_items_from_redis(list(items_from_data_set.keys()))
        matched_items += len(items_from_redis)
        # merge the items from the data set with the items in redis
        for item_id in items_from_redis:
            items_from_data_set[item_id] = merge_function(
                items_from_redis[item_id],
                items_from_data_set[item_id]
            )
        # write all of the merged items back to redis
        self.__write_items_to_redis(items_from_data_set)
        return matched_items

    def __read_items_from_redis(self, ids, return_values_for_writing=False):
        chunk_size = 10_000  # batch for pipeline
        dict_response = {}
        values_response = []
        chunked_ids = [ids[start: start + chunk_size] for start in range(0, len(ids), chunk_size)]
        for ids in chunked_ids:
            result = self.redis_con.mget(ids)
            if return_values_for_writing:
                values_response.extend(result)
            else:
                for i, res in zip(ids, result):
                    if res:
                        dict_response[i] = quick_json_loads(res)

        if return_values_for_writing:
            return values_response
        else:
            return dict_response

    def __write_items_to_redis(self, items):
        chunk_size = 10_000  # batch for redis beyond this cap it might not be optimal, according to redis docs
        all_keys = list(items.keys())
        chunked_keys = [all_keys[start: start + chunk_size] for start in
                        range(0, len(all_keys), chunk_size)]
        for keys in chunked_keys:
            items_to_write = {key: quick_json_dumps(items[key]) for key in keys}
            self.redis_con.mset(items_to_write)

    def __write_redis_back_to_file(self, redis_key_pattern, output_file_name):
        items_written = 0
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
                items = self.__read_items_from_redis(chunk, return_values_for_writing=True)
                items = [f'{item.decode("utf-8")}\n' for item in items]
                stream.writelines(items)
                items_written += len(items)
                # self.logger.debug(f"wrote : {len(items)}")
            self.logger.debug(f"Done writing {redis_key_pattern}")
        return items_written


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
        new_edge_count = len(filtered_edges)
        self.logger.debug(f'Found {new_edge_count} new connected edges')

        # find node ids that filtered edges from B connect to not present in A
        filtered_edges_node_ids = set()
        for edge in filtered_edges:
            filtered_edges_node_ids.add(edge[SUBJECT_ID])
            filtered_edges_node_ids.add(edge[OBJECT_ID])
        filtered_edges_node_ids = filtered_edges_node_ids - node_ids

        new_node_count = len(filtered_edges_node_ids)
        self.logger.debug(f'Found {new_node_count} new nodes from connected edges')

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
            stream.writelines([quick_json_dumps(node) + '\n' for node in filtered_nodes_from_b])

        # write out edges
        with open(new_edge_file, 'w', encoding='utf-8') as stream:
            # write out jsonl edges from A
            with open(edge_file_a) as edge_file_a_reader:
                stream.writelines(edge_file_a_reader)
            # write out new edges from B
            stream.writelines([quick_json_dumps(edge) + '\n' for edge in filtered_edges])

        return new_node_count, new_edge_count


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
