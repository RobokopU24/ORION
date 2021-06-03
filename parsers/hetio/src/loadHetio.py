import os
import argparse
import logging
import datetime

from Common.utils import LoggingUtil
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader


##############
# Class: Hetio loader
#
# By: Phil Owen
# Date: 4/5/2021
# Desc: Class that loads/parses the Hetio data.
##############
class HetioLoader(SourceDataLoader):
    # the final output lists of nodes and edges
    final_node_list: list = []
    final_edge_list: list = []

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
        self.source_db: str = 'Hetio'

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services..Loader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return datetime.datetime.now().strftime("%m/%d/%Y")

    def write_to_file(self, nodes_output_file_path: str, edges_output_file_path: str) -> None:
        """
        sends the data over to the KGX writer to create the node/edge files

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return: Nothing
        """
        # get a KGX file writer
        with KGXFileWriter(nodes_output_file_path, edges_output_file_path) as file_writer:
            # for each node captured
            for node in self.final_node_list:
                # write out the node
                file_writer.write_node(node['id'], node_name=node['name'], node_types=[], node_properties=None)

            # for each edge captured
            for edge in self.final_edge_list:
                # write out the edge data
                file_writer.write_edge(subject_id=edge['subject'], object_id=edge['object'], relation=edge['relation'], edge_properties=edge['properties'], predicate='')

    def get_hetio_data(self) -> int:
        """
        Gets the Hetio data.

        """
        # get a reference to the data gathering class
        # gd: GetData = GetData(self.logger.level)

        pass

    def load(self, nodes_output_file_path: str, edges_output_file_path: str) -> dict:
        """
        parses the Hetio data file gathered

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return the parsed metadata stats
        """

        self.logger.info(f' - Start of  data processing.')

        # get the list of taxons to process
        file_count = self.get_hetio_data()

        # init the return
        load_metadata: dict = {}

        # get the intact archive
        if file_count == 1:
            self.logger.debug(f'{self.data_file} archive retrieved. Parsing data.')

            # parse the data
            load_metadata = self.parse_data_file(self.data_path, self.data_file)

            self.logger.info(f'Hetio - {self.data_file} Processing complete.')

            # write out the data
            self.write_to_file(nodes_output_file_path, edges_output_file_path)

            self.logger.info(f'Hetio - Processing complete.')
        else:
            self.logger.error(f'Error: Retrieving  archive failed.')

        # return the metadata to the caller
        return load_metadata

    def parse_data_file(self, data_file_path: str, data_file_name: str) -> dict:
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
    ap = argparse.ArgumentParser(description='Load  data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the  data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    data_dir: str = args['data_dir']

    # get a reference to the processor
    ldr = HetioLoader()

    # load the data files and create KGX output
    ldr.load(data_dir, data_dir)
