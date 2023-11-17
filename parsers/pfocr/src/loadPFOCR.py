
import os
import requests
import yaml

from Common.loader_interface import SourceDataLoader
from Common.node_types import PRIMARY_KNOWLEDGE_SOURCE, NODE_TYPES, SUBJECT_ID, OBJECT_ID, PREDICATE, PUBLICATIONS
from Common.utils import GetData, quick_jsonl_file_iterator


##############
# Class: PFOCR source loader
#
# Desc: Class that loads/parses the PFOCR data.
##############
class PFOCRLoader(SourceDataLoader):

    source_id: str = 'PFOCR'
    provenance_id: str = 'infores:pfocr'
    description = "Pathway Figure OCR is an open science project dedicated to extracting pathway information from " \
                  "the published literature to be freely used by anyone."
    source_data_url = "https://pfocr.wikipathways.org/"
    parsing_version: str = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_url = 'https://stars.renci.org/var/data_services/pfocr/'
        self.pfocr_nodes_file = 'pfocr_nodes.jsonl.gz'
        self.pfocr_edges_file = 'pfocr_edges.jsonl.gz'
        self.pfocr_metadata_file = 'pfocr-kg.yaml'
        self.data_files = [self.pfocr_nodes_file,
                           self.pfocr_edges_file]

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data
        :return:
        """
        version_file_url = f"{self.data_url}{self.pfocr_metadata_file}"
        r = requests.get(version_file_url)
        version_yaml = yaml.full_load(r.text)
        build_version = str(version_yaml['build'])
        return build_version

    def get_data(self) -> bool:
        for data_file in self.data_files:
            source_data_url = f'{self.data_url}{data_file}'
            data_puller = GetData()
            data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        nodes_file_path: str = os.path.join(self.data_path, self.pfocr_nodes_file)
        for pfocr_node in quick_jsonl_file_iterator(nodes_file_path, is_gzip=True):
            node_id = pfocr_node.pop('id')
            node_name = pfocr_node.pop('name')
            node_types = pfocr_node.pop(NODE_TYPES)
            self.output_file_writer.write_node(node_id=node_id,
                                               node_name=node_name,
                                               node_types=node_types,
                                               node_properties=pfocr_node)
        final_record_count = 0
        final_skipped_record_count = 0
        edges_file_path: str = os.path.join(self.data_path, self.pfocr_edges_file)
        for pfocr_edge in quick_jsonl_file_iterator(edges_file_path, is_gzip=True):
            try:
                # looks unnecessary to write nodes here,
                # but there are node ids in the edges file that aren't in the nodes file
                self.output_file_writer.write_node(pfocr_edge[SUBJECT_ID])
                self.output_file_writer.write_node(pfocr_edge[OBJECT_ID])
                self.output_file_writer.write_edge(
                    subject_id=pfocr_edge[SUBJECT_ID],
                    predicate=pfocr_edge[PREDICATE],
                    object_id=pfocr_edge[OBJECT_ID],
                    primary_knowledge_source=pfocr_edge[PRIMARY_KNOWLEDGE_SOURCE],
                    edge_properties={
                        PUBLICATIONS: pfocr_edge[PUBLICATIONS]
                    }
                )
            except KeyError:
                final_skipped_record_count += 1
            final_record_count += 1
        load_metadata: dict = {
            'num_source_lines': final_record_count,
            'unusable_source_lines': 0
        }
        return load_metadata
