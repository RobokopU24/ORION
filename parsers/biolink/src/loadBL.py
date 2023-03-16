import os
import argparse
import enum

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import PRIMARY_KNOWLEDGE_SOURCE, NODE_TYPES


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
    CATEGORY = 4
    RELATION = 5
    # SOURCE = 17


##############
# Class: Biolink loader - This format of this source is no longer supported so we host a version at RENCI
#
# By: Phil Owen
# Date: 4/5/2021
# Desc: Class that loads/parses the biolink model data.
##############
class BLLoader(SourceDataLoader):

    source_id: str = 'Biolink'
    provenance_id: str = 'infores:sri-reference-kg'
    description = "A graph based on the Monarch API (https://api.monarchinitiative.org/)."
    source_data_url = "https://api.monarchinitiative.org/api/"
    license = "https://monarchinitiative.org/about/licensing"
    attribution = "https://monarchinitiative.org/about/monarch"
    parsing_version: str = '1.2'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.bl_edges_file_name = 'sri-reference-kg_edges.tsv'
        # self.bl_nodes_file_name = 'sri-reference-kg_nodes.tsv'
        # self.data_files: list = [self.bl_edges_file_name, self.bl_nodes_file_name]
        self.data_files: list = [self.bl_edges_file_name]
        self.data_url = 'https://stars.renci.org/var/data_services/Biolink/'

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        # No more updates are coming so we host this version at RENCI
        return "7_28_2021"

    def get_data(self) -> int:
        """
        Gets the biolink data.

        """
        for file_name in self.data_files:
            bl_data_url = f'{self.data_url}{file_name}'
            data_puller = GetData()
            data_puller.pull_via_http(bl_data_url, self.data_path)

        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor(file_writer=self.output_file_writer)

        """
        # commented out - with strict normalization we aren't using names or node types from this file,
        # so skipping this is more efficient, we get the node IDs from the edges file
        # parse the nodes file
        nodes_file: str = os.path.join(self.data_path, self.bl_nodes_file_name)
        with open(nodes_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: get_bl_node_id(line, NODESDATACOLS.ID.value),
                                  # extract subject id,
                                  lambda line: None,  # extract object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: get_bl_node_properties(line),  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {},  # edge props
                                  comment_character=None,
                                  delim='\t',
                                  has_header_row=True)
        """

        # parse the edges file
        edges_file: str = os.path.join(self.data_path, self.bl_edges_file_name)
        with open(edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: get_bl_node_id(line, EDGESDATACOLS.SUBJECT.value),  # subject id
                                  lambda line: get_bl_node_id(line, EDGESDATACOLS.OBJECT.value),  # object id
                                  lambda line: get_bl_edge_predicate(line),  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id},  # edgeprops
                                  comment_character=None,
                                  delim='\t',
                                  has_header_row=True,
                                  exclude_unconnected_nodes=True)

        return extractor.load_metadata


UNDESIRED_NODE_PREFIXES = {
    'MONARCH_BNODE',
    'http',
    'https'
}


def get_bl_node_id(line: list, id_index: int):
    node_id = line[id_index]
    split_node_id = node_id.split(':')
    if len(split_node_id) == 1 or split_node_id[0] in UNDESIRED_NODE_PREFIXES:
        return None
    else:
        return node_id


def get_bl_node_properties(line: list):
    categories = line[NODESDATACOLS.CATEGORY.value].split('|')
    name = line[NODESDATACOLS.NAME.value]
    return {NODE_TYPES: categories, 'name': name}


def passes_qc_filter(line: list):
    return not (line[EDGESDATACOLS.SUBJECT.value].startswith('MESH') and
                line[EDGESDATACOLS.OBJECT.value].startswith('MESH') and
                line[EDGESDATACOLS.CATEGORY.value] ==
                'biolink:Association|biolink:InformationContentEntity')

# you might think these should be constants but this source is not updated and these won't change
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
    if predicate == 'biolink:biomarker_for' and not passes_qc_filter(line):
        return None
    else:
        return predicate if predicate in DESIRED_BL_PREDICATES else None


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
