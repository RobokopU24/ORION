import os
import enum

from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from Common.biolink_constants import *
from Common.prefixes import PUBCHEM_COMPOUND
from Common.utils import GetData


class GENERICDATACOLS(enum.IntEnum):
    SOURCE_ID = 2
    SOURCE_LABEL = 3
    TARGET_ID = 5
    TARGET_LABEL = 6
    PREDICATE = 7


PREDICATE_MAPPING = {
    "in_similarity_relationship_with": "biolink:chemically_similar_to",
    "negatively_regulates": "RO:0002212",
    "positively_regulates": "RO:0002213"
}


##############
# Class: LINCS loader
#
# By: James Chung
# Date: 10/30/2024
# Desc: Class that loads/parses the data in Library of Integrated Network-Based Cellular Signatures.
# 
##############
class LINCSLoader(SourceDataLoader):

    source_id: str = 'LINCS'
    provenance_id: str = 'infores:lincs'
    parsing_version: str = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url = 'https://stars.renci.org/var/data_services/LINCS/'
        self.edge_file = "LINCS.lookup.edges.csv"
        self.data_files = [self.edge_file]

    def get_latest_source_version(self) -> str:
        # The KG was generated from Data Distillery KG. There was no version defined.
        latest_version = 'v1.0'
        return latest_version

    def get_data(self) -> bool:
        source_data_url = f'{self.data_url}{self.edge_file}'
        data_puller = GetData()
        data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        extractor = Extractor(file_writer=self.output_file_writer)
        lincs_file: str = os.path.join(self.data_path, self.edge_file)
        with open(lincs_file, 'rt') as fp:
            extractor.csv_extract(fp,
                                  lambda line: self.resolve_id(line[GENERICDATACOLS.SOURCE_ID.value]),  # source id
                                  lambda line: self.resolve_id(line[GENERICDATACOLS.TARGET_ID.value]),  # target id
                                  lambda line: PREDICATE_MAPPING[line[GENERICDATACOLS.PREDICATE.value]],  # predicate extractor
                                  lambda line: {},  # subject properties
                                  lambda line: {},  # object properties
                                  lambda line: self.get_edge_properties(),  # edge properties
                                  comment_character='#',
                                  delim=',',
                                  has_header_row=True)
        return extractor.load_metadata

    @staticmethod
    def resolve_id(idstring: str):
        if idstring.startswith("PUBCHEM"):
            return idstring.replace("PUBCHEM", PUBCHEM_COMPOUND)
        return idstring
        
    def get_edge_properties(self):
        properties = {
            PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id,
            KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
            AGENT_TYPE: DATA_PIPELINE
        }
        return properties
