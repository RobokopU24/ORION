import os
import jsonlines
import orjson
from Common.utils import LoggingUtil
from Common.kgxmodel import GraphSpec
from Common.node_types import SUBJECT_ID, OBJECT_ID
from Common.merging import GraphMerger

# import line_profiler
# import atexit
# profile = line_profiler.LineProfiler()
# atexit.register(profile.print_stats)


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

    def merge(self,
              graph_spec: GraphSpec,
              nodes_output_file_path: str,
              edges_output_file_path: str):

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
            missing_data_sets = [source_id for source_id in all_source_ids if
                                 source_id not in merge_metadata['sources'].keys()]
            self.logger.error(f"Error merging graph {graph_spec.graph_id}! could not merge: {missing_data_sets}")

        return merge_metadata

    def merge_primary_sources(self,
                              graph_sources: list,
                              nodes_out_file: str,
                              edges_out_file: str,
                              merge_metadata: dict):

        graph_merger = GraphMerger()
        for i, graph_source in enumerate(graph_sources, start=1):
            self.logger.info(f"Processing {graph_source.source_id}. (primary source {i}/{len(graph_sources)})")
            merge_metadata["sources"][graph_source.source_id] = {'source_version': graph_source.source_version}

            for file_path in graph_source.file_paths:
                source_filename = file_path.rsplit('/')[-1]
                merge_metadata["sources"][graph_source.source_id][source_filename] = {}
                if "nodes" in file_path:
                    with jsonlines.open(file_path) as nodes:
                        nodes_count, merged_nodes_count = graph_merger.merge_nodes(nodes)
                        merge_metadata["sources"][graph_source.source_id][source_filename]["nodes"] = nodes_count
                        merge_metadata["sources"][graph_source.source_id][source_filename][
                            "nodes_merged"] = merged_nodes_count
                        if merged_nodes_count:
                            self.logger.info(f"Merged {merged_nodes_count} nodes from {graph_source.source_id}"
                                             f" - {file_path}.")

                elif "edges" in file_path:
                    with jsonlines.open(file_path) as edges:
                        edges_count, merged_edge_count = graph_merger.merge_edges(edges, overwrite=True)
                        merge_metadata["sources"][graph_source.source_id][source_filename]["edges"] = edges_count
                        merge_metadata["sources"][graph_source.source_id][source_filename][
                            "edges_merged"] = merged_edge_count
                        if merged_edge_count:
                            self.logger.info(f"Merged {merged_edge_count} edges from {graph_source.source_id}"
                                             f" - {file_path}.")
                else:
                    raise ValueError(f"Did not recognize file {file_path} for merging "
                                     f"from data source {graph_source.source_id}.")

        nodes_written, edges_written = self.__write_back_to_file(graph_merger,
                                                                 nodes_out_file,
                                                                 edges_out_file)
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
                        raise IOError(
                            f'File paths were not in node, edge ordered pairs: {connected_edge_subset_nodes_file},{connected_edge_subset_edges_file}')
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

    def __write_back_to_file(self,
                             graph_merger: GraphMerger,
                             nodes_out_file: str,
                             edges_out_file: str):

        self.logger.debug(f'Writing merged nodes to file...')
        nodes_written = 0
        with open(nodes_out_file, 'w') as nodes_out:
            for node_line in graph_merger.get_merged_nodes_lines():
                nodes_out.write(node_line)
                nodes_written += 1

        self.logger.debug(f'Writing merged edges to file...')
        edges_written = 0
        with open(edges_out_file, 'w') as edges_out:
            for edge_line in graph_merger.get_merged_edges_lines():
                edges_out.write(edge_line)
                edges_written += 1

        return nodes_written, edges_written

    """
    This is on hold / TBD - we should be able to process individual sources more efficiently
    def process_single_source(self, graph_source: SourceDataSpec, nodes_out_file: str, edges_out_file: str):
        self.logger.info(f"Processing single primary source {graph_source.source_id}.")
        files_processed = 0
        files_to_process = graph_source.file_paths
        if len(files_to_process) <= 2:
            # TODO just copy them over, never needed to merge
            # maybe split nodes and edges file paths in SourceDataSpec to make that more flexible
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

