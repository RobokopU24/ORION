import os
import argparse
import enum
import gzip
import logging

from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from io import TextIOWrapper
from Common.utils import LoggingUtil, GetData
from Common.node_types import PRIMARY_KNOWLEDGE_SOURCE


# the data header columns are:
class DATACOLS(enum.IntEnum):
    DB = 0
    DB_Object_ID = 1
    DB_Object_Symbol = 2
    Qualifier = 3
    GO_ID = 4
    DB_Reference = 5
    Evidence_Code = 6
    With_From = 7
    Aspect = 8
    DB_Object_Name = 9
    DB_Object_Synonym = 10
    DB_Object_Type = 11
    Taxon_Interacting_taxon = 12
    Date = 13
    Assigned_By = 14
    Annotation_Extension = 15
    Gene_Product_Form_ID = 16


##############
# Class: UniProtKB GOA loader
#
# By: Phil Owen
# Date: 7/6/2020
# Desc: Class that loads the UniProtKB GOA data and creates KGX files for importing into a Neo4j graph.
##############
class GOALoader(SourceDataLoader):

    provenance_id = 'infores:goa'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        # set global variables
        self.data_file = 'goa_human.gaf.gz'

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        # init the return
        ret_val: str = 'Not found'

        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # the name of the file that has the version date
        data_file_name: str = 'summary.txt'

        # get the summary file
        byte_count: int = gd.pull_via_http(f'http://current.geneontology.org/{data_file_name}', self.data_path)

        # did we get the file
        if byte_count > 0:
            with open(os.path.join(self.data_path, data_file_name), 'r') as inf:
                # read all the lines
                lines = inf.readlines()

                # what to look for in the file
                search_text = 'Start date: '

                # for each line
                for line in lines:
                    # is this the line we are looking for
                    if line.startswith(search_text):
                        # save the date
                        ret_val = line.split(search_text)[1].strip()

            # remove the file
            os.remove(os.path.join(self.data_path, data_file_name))

        # return to the caller
        return ret_val

    def get_data(self) -> (int):
        """
        Gets goa data.

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # do the real thing if we arent in test mode
        if not self.test_mode:
            # get the GOA data file
            byte_count: int = gd.pull_via_http(self.data_url + self.data_file, self.data_path)
        else:
            # TODO create test data for GOA
            byte_count: int = 0

        # return the byte count to the caller
        return byte_count

    def parse_data(self) -> dict:
        """
        Parses the data file for nodes/edges

        :return: dict of parsing metadata results
        """

        infile_path = os.path.join(self.data_path, self.data_file)

        extractor = Extractor( )

        with (gzip.open if infile_path.endswith(".gz") else open)(infile_path) as zf:
            extractor.csv_extract(TextIOWrapper(zf, "utf-8"),
                                  lambda line: f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}',
                                  # extract subject id,
                                  lambda line: f'{line[DATACOLS.GO_ID.value]}',  # extract object id
                                  lambda line: get_goa_predicate(line),  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id},  # edge props
                                  comment_character = "!", delim = '\t' )
        # return to the caller
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata


goa_predicates = {'enables':'RO:0002327',
                  'involved_in':'RO:0002331',
                  'located_in':'RO:0001025',
                  'contributes_to':'RO:0002326',
                  'acts_upstream_of':'RO:0002263',
                  'part_of':'BFO:0000050',
                  'acts_upstream_of_positive_effect':'RO:0004034',
                  'is_active_in':'RO:0002432',
                  'acts_upstream_of_negative_effect':'RO:0004035',
                  'colocalizes_with':'RO:0002325',
                  'acts_upstream_of_or_within':'RO:0002264',
                  'acts_upstream_of_or_within_positive_effect':'RO:0004032',
                  'acts_upstream_of_or_within_negative_effect':'RO:0004033'}


def get_goa_predicate(line: list):
    supplied_qualifier = line[DATACOLS.Qualifier.value]
    if "|" in supplied_qualifier:
        return None
    else:
        return goa_predicates[supplied_qualifier]


class HumanGOALoader(GOALoader):

    source_id = 'HumanGOA'

    def __init__(self, test_mode: bool = False):
        super().__init__(test_mode)
        self.data_file = 'goa_human.gaf.gz'
        self.data_url = 'http://current.geneontology.org/annotations/'


class PlantGOALoader(GOALoader):

    source_id = 'PlantGOA'

    def __init__(self, test_mode: bool = False):
        super().__init__(test_mode)
        self.data_url = 'http://current.geneontology.org/annotations/'
        self.data_file = 'goa_uniprot_test.gaf.gz' # 'goa_uniprot_all.gaf.gz' #
        self.plant_taxa_file = 'plant_taxa.txt'

        self.plant_taxa_path = os.path.join(self.data_path, self.plant_taxa_file)


    def parse_data(self) -> dict:
        """
        Parses the data file for nodes/edges

        :return: dict of parsing metadata results
        """

        infile_path = os.path.join(self.data_path, self.data_file)

        extractor = Extractor( )

        with open(self.plant_taxa_path) as plant_taxa:
            plant_taxa_set = set()
            for line in plant_taxa:
                plant_taxa_set.add(line.strip())



        with (gzip.open if infile_path.endswith(".gz") else open)(infile_path) as goa_file:
            extractor.csv_extract(TextIOWrapper(goa_file, "utf-8"),
                                          lambda line: f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}',
                                          # extract subject id,
                                          lambda line: f'{line[DATACOLS.GO_ID.value]}',  # extract object id
                                          lambda line: get_goa_predicate(line),  # predicate extractor
                                          lambda line: {},  # subject props
                                          lambda line: {},  # object props
                                          lambda line: {PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id},  # edge props
                                          plant_taxa_set,
                                          DATACOLS.Taxon_Interacting_taxon.value,
                                          comment_character = "!", delim = '\t' )
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata

if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load plant or human GOA files and create KGX import files.')

    # command line should be like: python loadGOA.py -p goa_storage_path
    ap.add_argument('-p', '--data_dir', required=True, help='The location to save the KGX files')

    # parse the arguments
    args = vars(ap.parse_args())

    # get the params
    data_dir = args['data_dir']

    # TODO - very low priority - add an argument for specifying Human vs Plant and use the appropriate loader class
    goa = PlantGOALoader(False)

    # load the data files and create KGX output
    goa.load(f"{data_dir}/nodes", f"{data_dir}/edges")
