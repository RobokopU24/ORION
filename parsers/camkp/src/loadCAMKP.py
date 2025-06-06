import os
import argparse
import enum
import requests
import yaml
import json

from Common.utils import GetData
from Common.kgxmodel import kgxnode, kgxedge
from Common.biolink_constants import XREFS, KNOWLEDGE_LEVEL, KNOWLEDGE_ASSERTION, AGENT_TYPE, MANUAL_AGENT
from Common.loader_interface import SourceDataLoader

from gzip import GzipFile


# The CAM KP triplet file:
class CAMDATACOLS(enum.IntEnum):
    SUBJECT_ID = 0
    PREDICATE = 1
    OBJECT_ID = 2
    PROVENANCE_URL = 3
    PROVENANCE_ID = 4
    QUALIFIERS = 5


##############
# Class: CAM-KP loader
# Desc: Class that loads/parses the CAM-KP data.
##############
class CAMKPLoader(SourceDataLoader):

    source_id: str = "CAMKP"
    provenance_id: str = "infores:go-cam"
    aggregator_knowledge_source: str = "infores:cam-kp"
    parsing_version = "1.5"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.source_db: str = 'CAM KP'
        self.data_url: str = 'https://stars.renci.org/var/cam-kp/'
        self.data_file: str = 'cam-kg.tsv.gz'
        self.version_file = 'cam-kg.yaml'

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data
        :return:
        """
        version_file_url = f"{self.data_url}{self.version_file}"
        r = requests.get(version_file_url)
        version_yaml = yaml.full_load(r.text)
        build_version = str(version_yaml['build'])
        return build_version

    def get_data(self) -> int:
        """
        Gets the TextMiningKP data.
        """
        data_puller = GetData()
        source_url = f"{self.data_url}{self.data_file}"
        data_puller.pull_via_http(source_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        :return: ret_val: record counts
        """

        record_counter = 0
        skipped_record_counter = 0
        
        node_file_path: str = os.path.join(self.data_path, self.data_file)
        with GzipFile(node_file_path) as zf:
            for bytesline in zf:
                lines = bytesline.decode('utf-8')
                line = lines.strip().split('\t')

                subject_id = self.sanitize_cam_node_id(line[CAMDATACOLS.SUBJECT_ID.value])
                subject_node = kgxnode(subject_id)
                self.output_file_writer.write_kgx_node(subject_node)

                object_id = self.sanitize_cam_node_id(line[CAMDATACOLS.OBJECT_ID.value])
                object_node = kgxnode(object_id)
                self.output_file_writer.write_kgx_node(object_node)

                predicate = line[CAMDATACOLS.PREDICATE.value]
                edge_provenance_id = line[CAMDATACOLS.PROVENANCE_ID.value]
                edge_provenance_url = line[CAMDATACOLS.PROVENANCE_URL.value]

                # The following qualifier section is hacky and bad.
                # This is mostly because the source data can have multiple instances of the same qualifier
                # on the same edge, this splits those into multiple edges.
                # Request to change the source data has been made and this should be changed after.

                # If it has enough columns to have a qualifier
                if len(line) >= CAMDATACOLS.QUALIFIERS.value + 1:
                    # qualifier format looks like:
                    # (biolink:anatomical_context_qualifier=GO:0005634)&&(biolink:anatomical_context_qualifier=CL:0008019)
                    # split on && then remove the parentheses
                    qualifier_strings = [qualifier.strip("()") for qualifier in line[CAMDATACOLS.QUALIFIERS.value].split('&&')]

                    # parse the strings which are like biolink:anatomical_context_qualifier=GO:0005634
                    qualifiers = []
                    for qualifier_string in qualifier_strings:
                        qualifier_list = qualifier_string.split('=')
                        q_key = qualifier_list[0].removeprefix("biolink:")
                        if q_key != "anatomical_context_qualifier":
                            raise RuntimeError(f'Unsupported qualifier: {q_key}')
                        q_value = qualifier_list[1]
                        qualifiers.append({q_key: q_value})

                    # if the source data changes as mentioned above we could just do this and not split them up
                    # make a dict of qualifier_key: qualifier_value, remove the biolink prefix
                    # qualifiers = {qual[0].removeprefix("biolink:"): qual[1] for qual in
                    #             [qualifier.split('=') for qualifier in qualifiers]}
                    # edge_properties.update(qualifiers)
                else:
                    qualifiers = [{}]

                for qualifier in qualifiers:
                    edge_properties = {
                        XREFS: [edge_provenance_url],
                        KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                        AGENT_TYPE: MANUAL_AGENT,
                        **qualifier
                    }
                    new_edge = kgxedge(subject_id=subject_id,
                                       object_id=object_id,
                                       predicate=predicate,
                                       primary_knowledge_source=edge_provenance_id,
                                       aggregator_knowledge_sources=[self.aggregator_knowledge_source],
                                       edgeprops=edge_properties)
                    self.output_file_writer.write_kgx_edge(new_edge)
                    record_counter += 1
        
        load_metadata: dict = {'num_source_lines': record_counter,
                               'unusable_source_lines': skipped_record_counter}
        return load_metadata

    def sanitize_cam_node_id(self, node_id):
        node_id = node_id.strip('\"')
        if node_id.startswith("MGI:"):
            node_id = node_id[4:]
        return node_id
