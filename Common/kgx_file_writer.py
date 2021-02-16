import hashlib
import os
import json
from Common.utils import LoggingUtil


class KGXFileWriter:

    logger = LoggingUtil.init_logging("Data_services.Common.KGXFileWriter",
                                      line_format='medium',
                                      log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def __init__(self, nodes_output_file_path: str = None, edges_output_file_path: str = None, streaming: bool = False):
        self.written_nodes = set()
        self.streaming = streaming
        self.edges_to_write = []
        self.edges_buffer_size = 10000
        self.edges_written_flag = False

        self.nodes_to_write = []
        self.nodes_buffer_size = 10000
        self.nodes_written_flag = False

        self.nodes_output_file_handler = None
        if nodes_output_file_path:
            if os.path.isfile(nodes_output_file_path):
                self.logger.warning(f'KGXFileWriter error.. file already exists: {nodes_output_file_path} - overwriting!')
            self.nodes_output_file_handler = open(nodes_output_file_path, 'w')
            if streaming:
                self.nodes_output_file_handler.write('{"nodes": [\n')

        self.edges_output_file_handler = None
        if edges_output_file_path:
            if os.path.isfile(edges_output_file_path):
                self.logger.warning(f'KGXFileWriter error.. file already exists: {edges_output_file_path} - overwriting!')
            self.edges_output_file_handler = open(edges_output_file_path, 'w')
            if streaming:
                self.edges_output_file_handler.write('{"edges": [\n')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.nodes_output_file_handler:
            self.__write_nodes_to_file()
            if self.streaming:
                self.nodes_output_file_handler.write('\n]}')
            self.nodes_output_file_handler.close()
        if self.edges_output_file_handler:
            self.__write_edges_to_file()
            if self.streaming:
                self.edges_output_file_handler.write('\n]}')
            self.edges_output_file_handler.close()

    def write_node(self, node_id: str, node_name: str, node_types: list, node_properties: dict = None):
        if node_id in self.written_nodes:
            return

        self.written_nodes.add(node_id)
        node_object = {'id': node_id, 'name': node_name, 'category': node_types}
        if node_properties:
            for p in node_properties:
                node_object[p] = node_properties[p]

        self.nodes_to_write.append(node_object)
        self.check_node_buffer_for_flush()

    def write_normalized_node(self, node_json: dict):
        if node_json['id'] in self.written_nodes:
            return

        self.written_nodes.add(node_json['id'])
        self.nodes_to_write.append(node_json)
        self.check_node_buffer_for_flush()

    def write_normalized_nodes(self, nodes: list):
        for node in nodes:
            self.write_normalized_node(node)

    def check_node_buffer_for_flush(self):
        if self.streaming and len(self.nodes_to_write) >= self.nodes_buffer_size:
            self.__write_nodes_to_file()

    def __write_nodes_to_file(self):
        if self.nodes_to_write:
            if self.streaming:
                prefix = ",\n" if self.nodes_written_flag else ""
                next_chunk_to_write = prefix + ",\n".join([json.dumps(node) for node in self.nodes_to_write])
                self.nodes_output_file_handler.write(next_chunk_to_write)
                self.nodes_written_flag = True
                self.nodes_to_write = []
            else:
                nodes_json_object = {"nodes": self.nodes_to_write}
                self.nodes_output_file_handler.write(json.dumps(nodes_json_object, indent=4))

    def write_edge(self,
                   subject_id: str,
                   object_id: str,
                   relation: str,
                   predicate: str,
                   edge_properties: dict = None,
                   edge_id: str = None):
        if edge_id is None:
            composite_id = f'{object_id}{predicate}{subject_id}'
            edge_id = hashlib.md5(composite_id.encode("utf-8")).hexdigest()
        edge_object = {'id': edge_id,
                       'subject': subject_id,
                       'predicate': predicate,
                       'object': object_id,
                       'relation': relation
                       }

        if edge_properties is not None:
            for p in edge_properties:
                if p not in edge_object:
                    edge_object[p] = edge_properties[p]

        self.edges_to_write.append(edge_object)
        self.check_edge_buffer_for_flush()

    def check_edge_buffer_for_flush(self):
        if self.streaming and len(self.edges_to_write) >= self.edges_buffer_size:
            self.__write_edges_to_file()

    def __write_edges_to_file(self):
        if self.edges_to_write:
            if self.streaming:
                prefix = ",\n" if self.edges_written_flag else ""
                next_chunk_to_write = prefix + ",\n".join([json.dumps(edge) for edge in self.edges_to_write])
                self.edges_output_file_handler.write(next_chunk_to_write)
                self.edges_written_flag = True
                self.edges_to_write = []
            else:
                edges_json_object = {"edges": self.edges_to_write}
                self.edges_output_file_handler.write(json.dumps(edges_json_object, indent=4))
