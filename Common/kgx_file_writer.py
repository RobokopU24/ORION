import os
import jsonlines
import logging

from Common.utils import LoggingUtil
from Common.kgxmodel import kgxnode, kgxedge
from Common.node_types import ORIGINAL_KNOWLEDGE_SOURCE, PRIMARY_KNOWLEDGE_SOURCE, AGGREGATOR_KNOWLEDGE_SOURCES, \
    SUBJECT_ID, OBJECT_ID, PREDICATE


class KGXFileWriter:

    logger = LoggingUtil.init_logging("Data_services.Common.KGXFileWriter",
                                      line_format='medium',
                                      level=logging.DEBUG,
                                      log_file_path=os.environ['DATA_SERVICES_LOGS'])
    """
    constructor
    :param nodes_output_file_path: the file path for the nodes file
    :param edges_output_file_path: the file path for the edes file
    """
    def __init__(self,
                 nodes_output_file_path: str = None,
                 edges_output_file_path: str = None,
                 buffer_size: int = 20000):
        self.edges_to_write = []
        self.edges_buffer_size = buffer_size

        # written nodes is a set of node ids used for preventing duplicate node writes
        self.written_nodes = set()
        self.nodes_to_write = []
        self.nodes_buffer_size = buffer_size
        self.repeat_node_count = 0

        self.nodes_output_file_handler = None
        if nodes_output_file_path:
            if os.path.isfile(nodes_output_file_path):
                # TODO verify - do we really want to overwrite existing files? we could remove them on previous errors instead
                self.logger.warning(f'KGXFileWriter warning.. file already existed: {nodes_output_file_path}! Overwriting it!')
            self.nodes_output_file_handler = open(nodes_output_file_path, 'w')
            self.nodes_jsonl_writer = jsonlines.Writer(self.nodes_output_file_handler)

        self.edges_output_file_handler = None
        if edges_output_file_path:
            if os.path.isfile(edges_output_file_path):
                # TODO verify - do we really want to overwrite existing files? we could remove them on previous errors instead
                self.logger.warning(f'KGXFileWriter warning.. file already existed: {edges_output_file_path}! Overwriting it!')
            self.edges_output_file_handler = open(edges_output_file_path, 'w')
            self.edges_jsonl_writer = jsonlines.Writer(self.edges_output_file_handler)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.nodes_output_file_handler:
            self.__write_nodes_to_file()
            self.nodes_jsonl_writer.close()
            self.nodes_output_file_handler.close()
        if self.edges_output_file_handler:
            self.__write_edges_to_file()
            self.edges_jsonl_writer.close()
            self.edges_output_file_handler.close()

    def write_node(self, node_id: str, node_name: str, node_types: list, node_properties: dict = None, uniquify: bool = True):
        if uniquify:
            if node_id in self.written_nodes:
                self.repeat_node_count += 1
                return
            self.written_nodes.add(node_id)

        node_object = {'id': node_id, 'name': node_name, 'category': node_types}
        if node_properties:
            node_object.update(node_properties)

        self.nodes_to_write.append(node_object)
        self.check_node_buffer_for_flush()

    def write_kgx_node(self, node: kgxnode):
        self.write_node(node.identifier,
                        node_name=node.name,
                        node_types=node.categories,
                        node_properties=node.properties)

    def write_normalized_node(self, node_json: dict, uniquify: bool = True):
        if uniquify:
            if node_json['id'] in self.written_nodes:
                self.repeat_node_count += 1
                return
            self.written_nodes.add(node_json['id'])

        self.nodes_to_write.append(node_json)
        self.check_node_buffer_for_flush()

    def write_normalized_nodes(self, nodes: iter, uniquify: bool = True):
        for node in nodes:
            self.write_normalized_node(node, uniquify)

    def check_node_buffer_for_flush(self):
        if len(self.nodes_to_write) >= self.nodes_buffer_size:
            self.__write_nodes_to_file()

    def __write_nodes_to_file(self):
        try:
            self.nodes_jsonl_writer.write_all(self.nodes_to_write)
        except jsonlines.InvalidLineError as e:
            self.logger.error(f'KGXFileWriter: Failed to write json data: {e.line}.')
            raise e

        self.nodes_to_write.clear()

    def write_edge(self,
                   subject_id: str,
                   object_id: str,
                   predicate: str = None,
                   original_knowledge_source: str = None,
                   primary_knowledge_source: str = None,
                   aggregator_knowledge_sources: list = None,
                   edge_properties: dict = None,
                   edge_id: str = None):
        if edge_id:
            edge_object = {'id': edge_id,
                           SUBJECT_ID: subject_id,
                           PREDICATE: predicate,
                           OBJECT_ID: object_id}
        else:
            edge_object = {SUBJECT_ID: subject_id,
                           PREDICATE: predicate,
                           OBJECT_ID: object_id}

        if original_knowledge_source is not None:
            edge_object[ORIGINAL_KNOWLEDGE_SOURCE] = original_knowledge_source

        if primary_knowledge_source is not None:
            edge_object[PRIMARY_KNOWLEDGE_SOURCE] = primary_knowledge_source

        if aggregator_knowledge_sources is not None:
            edge_object[AGGREGATOR_KNOWLEDGE_SOURCES] = aggregator_knowledge_sources

        if edge_properties is not None:
            edge_object.update(edge_properties)

        self.edges_to_write.append(edge_object)
        self.check_edge_buffer_for_flush()

    def write_kgx_edge(self, edge: kgxedge):
        self.write_edge(subject_id=edge.subjectid,
                        object_id=edge.objectid,
                        predicate=edge.predicate,
                        original_knowledge_source=edge.original_knowledge_source,
                        primary_knowledge_source=edge.primary_knowledge_source,
                        aggregator_knowledge_sources=edge.aggregator_knowledge_sources,
                        edge_properties=edge.properties)

    def write_normalized_edges(self, edges: iter):
        for edge in edges:
            self.edges_to_write.append(edge)
            self.check_edge_buffer_for_flush()

    def check_edge_buffer_for_flush(self):
        if len(self.edges_to_write) >= self.edges_buffer_size:
            self.__write_edges_to_file()

    def __write_edges_to_file(self):
        try:
            self.edges_jsonl_writer.write_all(self.edges_to_write)
        except jsonlines.InvalidLineError as e:
            self.logger.error(f'KGXFileWriter: Failed to write json data: {e.line}.')
            raise e

        self.edges_to_write.clear()
