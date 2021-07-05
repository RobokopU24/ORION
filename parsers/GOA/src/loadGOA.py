import os
import argparse
import enum
import pandas as pd
import gzip
import logging
import requests

from Common.loader_interface import SourceDataLoader
from Common.csv_extractor import extract
from io import TextIOWrapper
from csv import reader
from Common.utils import LoggingUtil, GetData


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
    # the final output lists of nodes and edges
    final_node_list: list = []
    final_edge_list: list = []

    node_norm_failures: list = []

    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super(SourceDataLoader, self).__init__()

        # set global variables
        self.data_path = os.environ['DATA_SERVICES_STORAGE']
        self.data_file = 'goa_human.gaf.gz'
        self.test_mode = test_mode
        self.source_id = 'GeneOntologyAnnotations'
        self.source_db = 'GeneOntologyAnnotations'
        self.provenance_id = 'infores:goa'

        self.predicates = {'enables':'RO:0002327',
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

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.GOA.GOALoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])


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
                        # save teh date
                        ret_val = line.split(search_text)[1].strip()

            # remove the file
            os.remove(os.path.join(self.data_path, data_file_name))

        # return to the caller
        return ret_val

    def get_data(self) -> (int):
        """
        Gets the human goa data.

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # do the real thing if we arent in debug mode
        if not self.test_mode:
            # get the GOA data file
            byte_count: int = gd.get_goa_http_file(self.data_path, self.data_file)
        else:
            byte_count: int = 1

        # return the byte count to the caller
        return byte_count


    def parse_data_file(self, infile_path: str) -> dict:
        """
        Parses the data file for nodes/edges

        :param infile_path: the name of the GOA file to process
        :return: parsing meta data results
        """

        with gzip.open(infile_path, 'r') as zf:
            self.final_node_list, self.final_edge_list, load_metadata = \
                extract(TextIOWrapper(zf, "utf-8"),
                        lambda line: f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}',  #extract subject id,
                        lambda line: f'{line[DATACOLS.GO_ID.value]}',  #extract object id
                        lambda line: self.predicates[ line[DATACOLS.Qualifier] ],  #predicate extractor
                        lambda line: {},  #subject props
                        lambda line: {},  # object props
                        lambda line: {},  # edge props
                        comment_character = "!", delim = '\t'
                        )

        # return to the caller
        return load_metadata

if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB human data files and create KGX import files.')

    # command line should be like: python loadGOA.py -p /projects/stars/Data_services/UniProtKB_data -g goa_human.gaf.gz -m json
    ap.add_argument('-p', '--data_dir', required=True, help='The location of the UniProtKB data files')

    # parse the arguments
    args = vars(ap.parse_args())

    # get the params
    data_dir = args['data_dir']

    # get a reference to the processor
    goa = GOALoader(False)

    # load the data files and create KGX output
    goa.load(f"{data_dir}/nodes", f"{data_dir}/edges")
