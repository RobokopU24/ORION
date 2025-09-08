import os
import jsonlines
import logging

from orion.utils import LoggingUtil
from orion.kgxmodel import kgxnode, kgxedge
from orion.biolink_constants import PRIMARY_KNOWLEDGE_SOURCE, AGGREGATOR_KNOWLEDGE_SOURCES, \
    SUBJECT_ID, OBJECT_ID, PREDICATE


class KGXFileWriter:

    logger = LoggingUtil.init_logging("ORION.Common.KGXFileWriter",
                                      line_format='medium',
                                      level=logging.INFO,
                                      log_file_path=os.getenv('ORION_LOGS'))
    """
    constructor
    :param nodes_output_file_path: the file path for the nodes file
    :param edges_output_file_path: the file path for the edes file
    """
    def __init__(self,
                 nodes_output_file_path: str = None,
                 edges_output_file_path: str = None):
        self.edges_to_write = []
        self.edges_written = 0

        # written nodes is a set of node ids used for preventing duplicate node writes
        self.written_nodes = set()
        self.nodes_to_write = []
        self.nodes_written = 0
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
        self.close()

    def close(self):
        if self.nodes_output_file_handler:
            self.nodes_jsonl_writer.close()
            self.nodes_output_file_handler.close()
            self.nodes_output_file_handler = None
        if self.edges_output_file_handler:
            self.edges_jsonl_writer.close()
            self.edges_output_file_handler.close()
            self.edges_output_file_handler = None

    def write_node(self, node_id: str, node_name: str = "", node_types: list = None, node_properties: dict = None, uniquify: bool = True):
        if uniquify:
            if node_id in self.written_nodes:
                self.repeat_node_count += 1
                return
            self.written_nodes.add(node_id)

        if node_types is None:
            node_types = []

        node_object = {'id': node_id, 'name': node_name, 'category': node_types}
        if node_properties:
            node_object.update(node_properties)

        self.__write_node_to_file(node_object)

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

        self.__write_node_to_file(node_json)

    def write_normalized_nodes(self, nodes: iter, uniquify: bool = True):
        for node in nodes:
            self.write_normalized_node(node, uniquify)

    def __write_node_to_file(self, node):
        try:
            self.nodes_jsonl_writer.write(node)
            self.nodes_written += 1
        except jsonlines.InvalidLineError as e:
            self.logger.error(f'KGXFileWriter: Failed to write json data: {e.line}.')
            raise e

    def write_edge(self,
                   subject_id: str,
                   object_id: str,
                   predicate: str = None,
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

        if primary_knowledge_source is not None:
            edge_object[PRIMARY_KNOWLEDGE_SOURCE] = primary_knowledge_source

        if aggregator_knowledge_sources is not None:
            edge_object[AGGREGATOR_KNOWLEDGE_SOURCES] = aggregator_knowledge_sources

        if edge_properties is not None:
            edge_object.update({k: v for k, v in edge_properties.items() if v})

        self.__write_edge_to_file(edge_object)

    def write_kgx_edge(self, edge: kgxedge):
        self.write_edge(subject_id=edge.subjectid,
                        object_id=edge.objectid,
                        predicate=edge.predicate,
                        primary_knowledge_source=edge.primary_knowledge_source,
                        aggregator_knowledge_sources=edge.aggregator_knowledge_sources,
                        edge_properties=edge.properties)

    def write_normalized_edge(self, edge: dict):
        self.__write_edge_to_file(edge)

    def write_normalized_edges(self, edges: iter):
        for edge in edges:
            self.__write_edge_to_file(edge)

    def __write_edge_to_file(self, edge):
        try:
            self.edges_jsonl_writer.write(edge)
            self.edges_written += 1
        except jsonlines.InvalidLineError as e:
            self.logger.error(f'KGXFileWriter: Failed to write json data: {e.line}.')
            raise e
