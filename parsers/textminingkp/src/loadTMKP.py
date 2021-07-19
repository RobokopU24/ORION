import os
import argparse
import logging
import datetime

from Common.utils import LoggingUtil
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader


##############
# Class: TextMiningKP loader
#
# By: Phil Owen
# Date: 4/5/2021
# Desc: Class that loads/parses the TextMiningKP data.
##############
class TMKPLoader(SourceDataLoader):


    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super(SourceDataLoader, self).__init__()

        # set global variables
        self.data_path: str = os.environ['DATA_SERVICES_STORAGE']
        self.data_file: str = ''
        self.test_mode: bool = test_mode
        self.source_id: str = ''
        self.source_db: str = 'Text Mining KP'
        self.provenance_id: str = 'infores:textminingkp'

        # the final output lists of nodes and edges
        self.final_node_list: list = []
        self.final_edge_list: list = []

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.textminingkp.KPLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return datetime.datetime.now().strftime("%m/%d/%Y")

    def get_data(self) -> int:
        """
        Gets the TextMiningKP data.

        """
        # get a reference to the data gathering class
        # gd: GetData = GetData(self.logger.level)

        pass

    def parse_data(self, data_file_path: str, data_file_name: str) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        :param data_file_path: the path to the HMDB zip file
        :param data_file_name: the name of the HMDB zip file
        :return: ret_val: record counts
        """
        # get the path to the data file
        infile_path: str = os.path.join(data_file_path, data_file_name)

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        self.logger.debug(f'Parsing data file complete.')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # return to the caller
        return load_metadata


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load TextMiningKP data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the TextMiningKP data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    data_dir: str = args['data_dir']

    # get a reference to the processor
    ldr = TMKPLoader()

    # load the data files and create KGX output
    ldr.load(data_dir, data_dir)
