
import hashlib
import os
import json
from Common.utils import LoggingUtil
from pathlib import Path


class KGXFileWriter:

    logger = LoggingUtil.init_logging("Data_services.Common.KGXFileWriter",
                                      line_format='medium',
                                      log_file_path=os.path.join(Path(__file__).parents[1], 'logs'))

    def __init__(self, output_directory: str, out_file_name: str):
        self.written_nodes = set()

        self.edges_to_write = []
        self.nodes_to_write = []

        self.nodes_output_file_handler = None
        nodes_output_file_path = os.path.join(output_directory, f'{out_file_name}_nodes.json')
        if os.path.isfile(nodes_output_file_path):
            self.logger.error(f'KGXFileWriter error.. file already exists: {nodes_output_file_path}')
        else:
            self.nodes_output_file_handler = open(nodes_output_file_path, 'w')

        self.edges_output_file_handler = None
        edges_output_file_path = os.path.join(output_directory, f'{out_file_name}_edges.json')
        if os.path.isfile(edges_output_file_path):
            self.logger.error(f'KGXFileWriter error.. file already exists: {edges_output_file_path}')
        else:
            self.edges_output_file_handler = open(edges_output_file_path, 'w')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.nodes_output_file_handler:
            self.write_nodes_to_file()
            self.nodes_output_file_handler.close()
        if self.edges_output_file_handler:
            self.write_edges_to_file()
            self.edges_output_file_handler.close()

    def write_node(self, node_id: str, node_name: str, node_type: str):
        if node_id in self.written_nodes:
            return

        self.written_nodes.add(node_id)
        node_object = {'id': node_id, 'name': node_name, 'category': node_type}
        self.nodes_to_write.append(node_object)

    def write_nodes_to_file(self):
        if self.nodes_to_write:
            nodes_json_object = {"nodes": self.nodes_to_write}
            self.nodes_output_file_handler.write(json.dumps(nodes_json_object, indent=4))

    def write_edge(self,
                   subject_id: str,
                   object_id: str,
                   relation: str,
                   edge_label: str,
                   edge_properties: dict = None,
                   edge_id: str = None):
        if edge_id is None:
            composite_id = f'{object_id}{edge_label}{subject_id}'
            edge_id = hashlib.md5(composite_id.encode("utf-8")).hexdigest()
        edge_object = {'id': edge_id,
                       'subject': subject_id,
                       'edge_label': edge_label,
                       'object': object_id,
                       'relation': relation
                       }
        for p in edge_properties:
            edge_object[p] = edge_properties[p]
        self.edges_to_write.append(edge_object)

    def write_edges_to_file(self):
        if self.edges_to_write:
            edges_json_object = {"edges": self.edges_to_write}
            self.edges_output_file_handler.write(json.dumps(edges_json_object, indent=4))
