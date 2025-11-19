import os
import jsonlines
import json
from datetime import datetime
from itertools import chain
from orion.utils import LoggingUtil, quick_jsonl_file_iterator
from orion.kgxmodel import GraphSpec, GraphSource, SubGraphSource
from orion.biolink_constants import SUBJECT_ID, OBJECT_ID
from orion.merging import GraphMerger, DiskGraphMerger, MemoryGraphMerger
from orion.load_manager import RESOURCE_HOGS

logger = LoggingUtil.init_logging("ORION.Common.KGXFileMerger",
                                  line_format='medium',
                                  log_file_path=os.getenv('ORION_LOGS'))

CONNECTED_EDGE_SUBSET = 'connected_edge_subset'
DONT_MERGE = 'dont_merge_edges'
SECONDARY_MERGE_STRATEGIES = [CONNECTED_EDGE_SUBSET]


class KGXFileMerger:

    def __init__(self,
                 graph_spec: GraphSpec,
                 output_directory: str = None,
                 nodes_output_filename: str = None,
                 edges_output_filename: str = None,
                 save_memory: bool = False):
        self.graph_spec = graph_spec
        self.output_directory = output_directory
        self.nodes_output_filename = nodes_output_filename
        self.edges_output_filename = edges_output_filename
        self.merge_metadata = self.init_merge_metadata()
        self.edge_graph_merger: GraphMerger = self.init_edge_graph_merger(save_memory=save_memory)
        self.node_graph_merger = MemoryGraphMerger() if not save_memory \
            else DiskGraphMerger(temp_directory=self.output_directory)
        # these will be edge files that have a dont_merge merge strategy
        self.unmerged_edge_files = {}

    def merge(self):
        if not (self.graph_spec.sources or self.graph_spec.subgraphs):
            merge_error_msg = f'Merge attempted but {self.graph_spec.graph_id} had no sources to merge.'
            logger.error(merge_error_msg)
            self.merge_metadata['merge_error'] = merge_error_msg
            return

        # group the sources based on their merge strategy
        primary_sources = []
        secondary_sources = []
        dont_merge_sources = []
        for graph_source in chain(self.graph_spec.sources, self.graph_spec.subgraphs):
            if not graph_source.merge_strategy:
                primary_sources.append(graph_source)
            elif graph_source.merge_strategy in SECONDARY_MERGE_STRATEGIES:
                secondary_sources.append(graph_source)
            elif graph_source.merge_strategy == DONT_MERGE:
                dont_merge_sources.append(graph_source)
            else:
                self.merge_metadata['merge_error'] = f'Unsupported merge strategy specified: ' \
                                                     f'{graph_source.merge_strategy}'
                return

        self.merge_primary_sources(primary_sources)
        self.merge_secondary_sources(secondary_sources)
        self.merge_dont_merge_sources(dont_merge_sources)

        # sources are added to self.merge_metadata['sources'] as they get merged in,
        # this roughly checks that all the sources that should be merged were processed
        if len(self.merge_metadata['sources']) != \
                len(self.graph_spec.sources) + len(self.graph_spec.subgraphs):
            all_source_ids = [graph_source.id for graph_source in chain(self.graph_spec.sources,
                                                                        self.graph_spec.subgraphs)]
            missing_data_sets = [source_id for source_id in all_source_ids if
                                 source_id not in self.merge_metadata['sources'].keys()]
            error_message = f"Error merging graph {self.graph_spec.graph_id}! could not merge: {missing_data_sets}"
            logger.error(error_message)
            self.merge_metadata["merge_error"] = error_message
            return

        # NOTE about metadata counts:
        # The implementation of DiskGraphMerger makes determining final and merging counts impossible until the output
        # files are written because that's when the merging actually happens. So while you could use this without
        # writing files, if using DiskGraphMerger the counts won't get updated.
        if self.nodes_output_filename and self.edges_output_filename:
            merged_nodes_written, merged_edges_written = self.__write_merged_graph_to_file()
            unmerged_edges_written = self.__write_unmerged_edges_to_file()
            self.merge_metadata['unmerged_edge_count'] = unmerged_edges_written
            self.merge_metadata['final_node_count'] += merged_nodes_written
            self.merge_metadata['final_edge_count'] += merged_edges_written + unmerged_edges_written
            self.merge_metadata['merged_nodes'] += self.node_graph_merger.merged_node_counter
            self.merge_metadata['merged_edges'] += self.edge_graph_merger.merged_edge_counter

    def merge_primary_sources(self,
                              graph_sources: list):

        for i, graph_source in enumerate(graph_sources, start=1):
            logger.info(f"Processing {graph_source.id}. (primary source {i}/{len(graph_sources)})")
            self.merge_metadata["sources"][graph_source.id] = {'release_version': graph_source.version}

            for file_path in graph_source.get_node_file_paths():
                with jsonlines.open(file_path) as nodes:
                    nodes_count = self.node_graph_merger.merge_nodes(nodes)
                source_filename = file_path.rsplit('/')[-1]
                self.merge_metadata["sources"][graph_source.id][source_filename] = {"nodes": nodes_count}

            for file_path in graph_source.get_edge_file_paths():
                with jsonlines.open(file_path) as edges:
                    edges_count = self.edge_graph_merger.merge_edges(
                        edges, additional_edge_attributes=graph_source.edge_merging_attributes,
                        add_edge_id=graph_source.edge_id_addition)
                source_filename = file_path.rsplit('/')[-1]
                self.merge_metadata["sources"][graph_source.id][source_filename] = {"edges": edges_count}
        return True

    def merge_secondary_sources(self,
                                graph_sources: list):
        primary_node_ids = None
        for i, graph_source in enumerate(graph_sources, start=1):
            logger.info(f"Processing {graph_source.id}. (secondary source {i}/{len(graph_sources)})")
            if graph_source.merge_strategy == CONNECTED_EDGE_SUBSET:
                logger.info(f"Merging {graph_source.id} using {CONNECTED_EDGE_SUBSET} merge strategy.")

                self.merge_metadata["sources"][graph_source.id] = {'release_version': graph_source.version}

                # For connected_edge_subset, only merge edges that connect to nodes in primary sources.
                # Here we establish that list once, before any connected_edge_subset sources are merged in, so we don't
                # include edges from one connected_edge_subset that are only connected to another connected_edge_subset.
                if not primary_node_ids:
                    primary_node_ids = set(self.node_graph_merger.nodes.keys())

                nodes_to_add = set()
                for edge_file in graph_source.get_edge_file_paths():
                    edge_counter = 0
                    additional_edge_attributes = graph_source.edge_merging_attributes
                    add_edge_id = graph_source.edge_id_addition
                    for edge in quick_jsonl_file_iterator(edge_file):
                        edge_subject_connected = edge[SUBJECT_ID] in primary_node_ids
                        edge_object_connected = edge[OBJECT_ID] in primary_node_ids
                        if edge_subject_connected or edge_object_connected:
                            edge_counter += 1
                            self.edge_graph_merger.merge_edge(edge,
                                                              additional_edge_attributes=additional_edge_attributes,
                                                              add_edge_id=add_edge_id)
                            if not edge_subject_connected:
                                nodes_to_add.add(edge[SUBJECT_ID])
                            elif not edge_object_connected:
                                nodes_to_add.add(edge[OBJECT_ID])
                    source_filename = edge_file.rsplit('/')[-1]
                    self.merge_metadata["sources"][graph_source.id][source_filename] = {"edges": edge_counter}

                for node_file in graph_source.get_node_file_paths():
                    node_counter = 0
                    for node in quick_jsonl_file_iterator(node_file):
                        if node['id'] in nodes_to_add:
                            node_counter += 1
                            self.node_graph_merger.merge_node(node)
                    source_filename = node_file.rsplit('/')[-1]
                    self.merge_metadata["sources"][graph_source.id][source_filename] = {"nodes": node_counter}

    def merge_dont_merge_sources(self, graph_sources: list):
        for graph_source in graph_sources:
            # merge in the nodes
            for file_path in graph_source.get_node_file_paths():
                with jsonlines.open(file_path) as nodes:
                    nodes_count = self.node_graph_merger.merge_nodes(nodes)
                source_filename = file_path.rsplit('/')[-1]
                self.merge_metadata["sources"][graph_source.id][source_filename] = {"nodes": nodes_count}

            # just queue up the edges files
            self.unmerged_edge_files[graph_source.id] = graph_source.get_edge_file_paths()

    def __write_merged_graph_to_file(self):
        nodes_output_file_path = os.path.join(self.output_directory, self.nodes_output_filename)
        edges_output_file_path = os.path.join(self.output_directory, self.edges_output_filename)
        if os.path.exists(nodes_output_file_path) or os.path.exists(edges_output_file_path):
            merge_error_msg = f'Merge attempted for {self.graph_spec.graph_id} but merged files already existed!'
            logger.error(merge_error_msg)
            self.merge_metadata['merge_error'] = merge_error_msg
            return 0, 0

        logger.info(f'Writing merged nodes to file...')
        nodes_written = 0
        with open(nodes_output_file_path, 'w') as nodes_out:
            for node_line in self.node_graph_merger.get_merged_nodes_jsonl():
                nodes_out.write(node_line)
                nodes_written += 1

        logger.info(f'Writing merged edges to file...')
        edges_written = 0
        with open(edges_output_file_path, 'w') as edges_out:
            for edge_line in self.edge_graph_merger.get_merged_edges_jsonl():
                edges_out.write(edge_line)
                edges_written += 1

        return nodes_written, edges_written

    def __write_unmerged_edges_to_file(self):
        all_unmerged_edges_count = 0
        edges_output_file_path = os.path.join(self.output_directory, self.edges_output_filename)
        with open(edges_output_file_path, 'a') as edges_out:
            for graph_source_id, edges_files in self.unmerged_edge_files.items():
                for edges_file in edges_files:
                    edges_count = 0
                    with open(edges_file) as edges:
                        for edge in edges:
                            edges_out.write(edge)
                            edges_count += 1
                    edges_filename = edges_file.rsplit('/')[-1]
                    self.merge_metadata["sources"][graph_source_id][edges_filename]["edges"] = edges_count
                    all_unmerged_edges_count += edges_count
        return all_unmerged_edges_count

    def init_edge_graph_merger(self, save_memory: bool = False) -> GraphMerger:
        needs_on_disk_merge = True
        if not save_memory:
            needs_on_disk_merge = False
            for graph_source in chain(self.graph_spec.sources, self.graph_spec.subgraphs):
                if isinstance(graph_source, SubGraphSource):
                    for source_id in graph_source.graph_metadata.get_source_ids():
                        if source_id in RESOURCE_HOGS:
                            needs_on_disk_merge = True
                            break
                elif graph_source.id in RESOURCE_HOGS:
                    needs_on_disk_merge = True
                    break
        if needs_on_disk_merge:
            if self.output_directory is None:
                raise IOError(f'DiskGraphMerger attempted but no output directory was specified.')
            return DiskGraphMerger(temp_directory=self.output_directory)
        else:
            return MemoryGraphMerger()

    @staticmethod
    def init_merge_metadata():
        return {'sources': {},
                'merged_nodes': 0,
                'merged_edges': 0,
                'final_node_count': 0,
                'final_edge_count': 0}

    def get_merge_metadata(self):
        return self.merge_metadata


# This was moved over from the cli implementation - it's a hacky way to merge files without a graph spec
#
# given a list of kgx jsonl node files and edge files,
# create a simple GraphSpec and use KGXFileMerge to merge the files into one node file and one edge file
def merge_kgx_files(output_dir: str,
                    nodes_files: list = None,
                    edges_files: list = None,
                    graph_id: str = "merged_graph"):
    if not nodes_files:
        nodes_files = []
    else:
        for node_file in nodes_files:
            if 'node' not in node_file:
                print('All node files must contain the text "node" in their file name.')
                return False

    if not edges_files:
        edges_files = []
    else:
        for edge_file in edges_files:
            if 'edge' not in edge_file:
                print(f'All edge files must contain the text "edge" in their file name. This file does not: {edge_file}')
                return False

    current_time = datetime.now()
    timestamp = current_time.strftime("%Y/%m/%d %H:%M:%S")
    graph_source = GraphSource(id='cli_merge',
                               file_paths=nodes_files + edges_files)
    graph_spec = GraphSpec(
        graph_id='cli_merge',
        graph_name='',
        graph_description=f'Merged on {timestamp}',
        graph_url='',
        graph_version=graph_id,
        graph_output_format='jsonl',
        sources=[graph_source],
        subgraphs=[]
    )
    file_merger = KGXFileMerger(graph_spec=graph_spec,
                                output_directory=output_dir,
                                nodes_output_filename=f'{graph_id}_nodes.jsonl',
                                edges_output_filename=f'{graph_id}_edges.jsonl')
    file_merger.merge()

    merge_metadata = file_merger.get_merge_metadata()
    if "merge_error" in merge_metadata:
        logger.error(f'Merge error occured: {merge_metadata["merge_error"]}')
        return False
    else:
        metadata_output = os.path.join(output_dir, f"{graph_id}_metadata.json")
        with open(metadata_output, 'w') as metadata_file:
            metadata_file.write(json.dumps(merge_metadata, indent=4))
        return True
