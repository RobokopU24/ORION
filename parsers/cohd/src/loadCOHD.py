
import os
import requests
import yaml

from Common.loader_interface import SourceDataLoader
from Common.utils import GetData, quick_jsonl_file_iterator


##############
# Class: COHD source loader
#
# Desc: Class that loads/parses the COHD data.
##############
class COHDLoader(SourceDataLoader):

    source_id: str = 'COHD'
    provenance_id: str = 'infores:cohd'
    parsing_version: str = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url = 'https://stars.renci.org/var/data_services/cohd_2/'
        self.version_file = 'cohd.yaml'
        self.cohd_nodes = 'cohd_nodes.jsonl'
        self.cohd_edges = 'cohd_edges.jsonl'
        self.data_files = [self.cohd_nodes, self.cohd_edges]

    def get_latest_source_version(self) -> str:
        version_file_url = f"{self.data_url}{self.version_file}"
        r = requests.get(version_file_url)
        if not r.ok:
            r.raise_for_status()
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
        record_counter = 0
        skipped_record_counter = 0

        nodes_file_path: str = os.path.join(self.data_path, self.cohd_nodes)
        for node_json in quick_jsonl_file_iterator(nodes_file_path):
            self.output_file_writer.write_normalized_node(node_json)

        edges_file_path: str = os.path.join(self.data_path, self.cohd_edges)
        for edge_json in quick_jsonl_file_iterator(edges_file_path):
            sources = edge_json.pop("sources")
            for source in sources:
                edge_json[source["resource_role"]] = source["resource_id"]
            self.output_file_writer.write_normalized_edge(edge_json)
            record_counter += 1

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter}
        return load_metadata
