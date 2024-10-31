import os
import enum

from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge
from Common.neo4j_tools import Neo4jTools
from Common.biolink_constants import *
from Common.prefixes import PUBCHEM_COMPOUND, KNOWLEDGE_LEVEL, KNOWLEDGE_ASSERTION, AGENT_TYPE, DATA_PIPELINE
from Common.utils import GetData


# if parsing a tsv or csv type file with columns, use a enum to represent each field
class GENERICDATACOLS(enum.IntEnum):
    SOURCE_ID = 2
    SOURCE_LABEL = 3
    TARGET_ID = 5
    TARGET_LABEL = 6
    PREDICATE = 7

PREDICATE_MAPPING = {
                    "in_similarity_relationship_with": "biolink:chemically_similar_to",
                    "negatively_regulates": {
                        "RO:0002448": {
                            OBJECT_DIRECTION_QUALIFIER: "downregulated"}},
                     "positively_regulates": {
                        "RO:0002448": {
                            OBJECT_DIRECTION_QUALIFIER: "upregulated"}}
                     }



##############
# Class: LINCS loader
#
# By: James Chung
# Date: 10/30/2023
# Desc: Class that loads/parses the data in Library of Integrated Network-Based Cellular Signatures.
# 
##############


class LINCSLoader(SourceDataLoader):

    source_id: str = 'LINCS'
    # this should be a valid infores curie from the biolink infores catalog
    provenance_id: str = 'infores:lincs'
    # increment parsing_version whenever changes are made to the parser that would result in changes to parsing output
    parsing_version: str = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.lincs_url = 'https://stars.renci.org/var/data_services/LINCS/'
        self.edge_file = "LINCS.lookup.edges.csv"
        self.data_files = [self.edge_file]

    def get_latest_source_version(self) -> str:
        # if possible go to the source and retrieve a string that is the latest version of the source data
        # The KG was generated from Data Distillery KG. There was no version defined.
        latest_version = 'v1.0'
        return latest_version

    def get_data(self) -> bool:
        # get_data is responsible for fetching the files in self.data_files and saving them to self.data_path
        # Not used for LINCS so far.
        source_data_url = f'{self.example_url}{self.edge_file}'
        data_puller = GetData()
        data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        # This is a made up example of how one might extract nodes and edges from a tsv file
        # In this case it's taking the subject ID from column 1 and the object ID from column 3,
        # prepending them with a curie prefix. The predicate comes from column 3. The value in column 4
        # is set as a property on the edge.
        extractor = Extractor(file_writer=self.output_file_writer)
        lincs_file: str = os.path.join(self.lincs_url, self.edge_file)
        with open(lincs_file, 'rt') as fp:
            extractor.csv_extract(fp,
                                  lambda line: self.resolve_id(line[GENERICDATACOLS.SOURCE_ID.value]),  # source id
                                  lambda line: self.resolve_id(line[GENERICDATACOLS.TARGET_ID.value]),  # target id
                                  lambda line: PREDICATE_MAPPING[line[GENERICDATACOLS.PREDICATE.value]].key,  # predicate extractor
                                  lambda line: {line[GENERICDATACOLS.SOURCE_LABEL.value]},  # subject properties
                                  lambda line: {line[GENERICDATACOLS.TARGET_LABEL.value]},  # object properties
                                  lambda line: self.format_edge_properties(line[GENERICDATACOLS.PREDICATE.value]),  # edge properties
                                  comment_character='#',
                                  delim='\t',
                                  has_header_row=True)
        return extractor.load_metadata

    def resolve_id(self, idstring: str):
        if idstring.startswith("PUBCHEM"):
            return f"{PUBCHEM_COMPOUND}{idstring.replace("PUBCHEM","")}"
        elif idstring.startswith("HGNC"):
            return idstring
        
    def format_edge_properties(self, predicate: str):
        properties = PREDICATE_MAPPING[predicate].value
        
        properties.update({
            PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id,
            KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
            AGENT_TYPE: DATA_PIPELINE
        })
        
        return properties