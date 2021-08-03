import os
import argparse
import logging
import datetime
import enum
import requests

from Common.utils import LoggingUtil, GetData
from Common.loader_interface import SourceDataLoader, SourceDataBrokenError
from Common.extractor import Extractor
from Common.node_types import node_types, AGGREGATOR_KNOWLEDGE_SOURCES


# the data header columns for the nodes file are:
class NODESDATACOLS(enum.IntEnum):
    ID = 0
    CATEGORY = 1
    NAME = 2

# the data header columns for the edges file are:
class EDGESDATACOLS(enum.IntEnum):
    ID = 0
    SUBJECT = 1
    PREDICATE = 2
    OBJECT = 3
    RELATION = 5
    # SOURCE = 17


##############
# Class: Biolink model loader
#
# By: Phil Owen
# Date: 4/5/2021
# Desc: Class that loads/parses the biolink model data.
##############
class BLLoader(SourceDataLoader):

    source_id: str = 'Biolink'
    provenance_id: str = 'infores:biolink'

    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super(SourceDataLoader, self).__init__()

        self.bl_edges_file_name = 'sri-reference-kg_edges.tsv'
        self.bl_nodes_file_name = 'sri-reference-kg_nodes.tsv'
        self.data_path: str = os.path.join(os.environ['DATA_SERVICES_STORAGE'], self.source_id)
        self.data_files: list = [self.bl_edges_file_name, self.bl_nodes_file_name]
        self.test_mode: bool = test_mode

        # the final output lists of nodes and edges
        self.final_node_list: list = []
        self.final_edge_list: list = []

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.biolink.BLLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        bl_edges_url = f'https://archive.monarchinitiative.org/latest/kgx/{self.bl_edges_file_name}'
        req = requests.head(bl_edges_url)
        modified_time = req.headers['last-modified']
        return modified_time

    def get_data(self) -> int:
        """
        Gets the biolink data.

        """
        for file_name in self.data_files:
            bl_data_url = f'https://archive.monarchinitiative.org/latest/kgx/{file_name}'
            data_puller = GetData()
            data_puller.pull_via_http(bl_data_url, self.data_path)

        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor()

        # parse the nodes file
        nodes_file: str = os.path.join(self.data_path, self.bl_nodes_file_name)
        with open(nodes_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: get_bl_node_id(line),
                                  # extract subject id,
                                  lambda line: None,  # extract object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: get_bl_node_properties(line),  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {},  # edge props
                                  comment_character=None,
                                  delim='\t',
                                  has_header_row=True)

        # parse the edges file
        edges_file: str = os.path.join(self.data_path, self.bl_edges_file_name)
        with open(edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[EDGESDATACOLS.SUBJECT.value],
                                  # extract subject id,
                                  lambda line: line[EDGESDATACOLS.OBJECT.value],  # extract object id
                                  lambda line: get_bl_edge_predicate(line),  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {'relation': line[EDGESDATACOLS.RELATION.value].split('|')[0],
                                                AGGREGATOR_KNOWLEDGE_SOURCES: ['infores:sri-reference-kg']},#edgeprops
                                  comment_character=None,
                                  delim='\t',
                                  has_header_row=True)

        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata

UNDESIRED_NODE_PREFIXES = {
    'FlyBase',
    'ZP',
    'ZFS',
    'ZFA',
    'ZFIN',
    'BGD',
    'MONARCH_BNODE',
    'PomBase',
    'http'
}

def get_bl_node_id(line: list):
    node_id = line[NODESDATACOLS.ID.value]
    curie = node_id.split(':')[0]
    if len(curie) == 1 or curie in UNDESIRED_NODE_PREFIXES:
        return None
    else:
        return node_id

def get_bl_node_properties(line: list):
    categories = line[NODESDATACOLS.CATEGORY.value].split('|')
    name = line[NODESDATACOLS.NAME.value]
    return {'categories': categories, 'name': name}

DESIRED_BL_PREDICATES = {
    'biolink:biomarker_for',
    'biolink:contributes_to',
    'biolink:correlated_with',
    'biolink:gene_associated_with_condition',
    'biolink:has_phenotype',
    'biolink:interacts_with',
    'biolink:orthologous_to',
    'biolink:treats'
}

def get_bl_edge_predicate(line: list):
    predicate = line[EDGESDATACOLS.PREDICATE.value]
    if predicate in DESIRED_BL_PREDICATES:
        return predicate
    else:
        return None

if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load biolink data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the biolink data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    HMDB_data_dir: str = args['data_dir']

    # get a reference to the processor
    bl = BLLoader()

    # load the data files and create KGX output
    bl.load(HMDB_data_dir, HMDB_data_dir)
