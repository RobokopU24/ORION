import os
import csv
import argparse
import logging
import datetime

from Common.utils import LoggingUtil, GetData
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader


##############
# Class: Panther loader
#
# By: Phil Owen
# Date: 4/5/2021
# Desc: Class that loads/parses the Panther data.
##############
class PLoader(SourceDataLoader):
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
        self.data_version: str = '16.0'
        self.data_file: str = f'PTHR{self.data_version}_human'
        self.test_mode: bool = test_mode
        self.source_id: str = 'PANTHER'
        self.source_db: str = 'Protein ANalysis THrough Evolutionary Relationships'

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.Panther.PLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

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

    def get_panther_data(self) -> int:
        """
        Gets the Panther data.

        """
        # get a reference to the data gathering class
        gd: GetData = GetData(self.logger.level)

        # do the real thing if we arent in debug mode
        if not self.test_mode:
            # get the complete data set
            file_count: int = gd.pull_via_ftp('ftp.pantherdb.org', f'/sequence_classifications/{self.data_version}/PANTHER_Sequence_Classification_files/', [self.data_file], self.data_path)
        else:
            file_count: int = 1

        # return the file count to the caller
        return file_count

    def load(self, nodes_output_file_path: str, edges_output_file_path: str) -> dict:
        """
        parses the Panther data file gathered

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return the parsed metadata stats
        """

        self.logger.info(f' - Start of Panther data processing.')

        # get the list of taxons to process
        file_count = self.get_panther_data()

        # init the return
        load_metadata: dict = {}

        # get the intact archive
        if file_count == 1:
            self.logger.debug(f'{self.data_file} archive retrieved. Parsing data.')

            # parse the data
            load_metadata = self.parse_data_file(self.data_path, self.data_file)

            self.logger.info(f'Panther - {self.data_file} Processing complete.')

            # write out the data
            self.write_to_file(nodes_output_file_path, edges_output_file_path)

            self.logger.info(f'Panther - Processing complete.')
        else:
            self.logger.error(f'Error: Retrieving  archive failed.')

        # return the metadata to the caller
        return load_metadata

    def parse_data_file(self, data_file_path: str, data_file_name: str) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        :param data_file_path: the path to the PANTHER file
        :param data_file_name: the name of the PANTHER file
        :return: ret_val: record counts
        """
        # get the path to the data file
        infile_path: str = os.path.join(data_file_path, data_file_name)

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # open up the file
        with open(infile_path, 'r', encoding="utf-8") as fp:
            # the list of columns in the data
            cols = ['gene_identifier', 'protein_id', 'gene_name', 'panther_sf_id', 'panther_family_name',
                    'panther_subfamily_name', 'panther_molecular_func', 'panther_biological_process',
                    'cellular_components', 'protein_class', 'pathway']

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '?', fp), delimiter='\t', fieldnames=cols)

            # go through the data. no column header in this data
            for r in data:
                # increment the counter
                record_counter += 1

            fam_id, sub_id = r['panther_sf_id'].split(':')
            fam_name = r['panther_family_name']

            self.get_gene_family_by_gene_family(fam_name, fam_id, sub_id, r)
            self.get_gene_by_gene_family(fam_name, fam_id, r)
            self.get_cellular_component_by_gene_family(fam_name, fam_id, r)
            self.get_biological_process_or_activity_by_gene_family(fam_name, fam_id, r)
            self.get_pathway_by_gene_family(fam_name, fam_id, r)

        self.logger.debug(f'Parsing data file complete.')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # return to the caller
        return load_metadata

    def get_gene_family_by_gene_family(self, fam_name, fam_id, sub_id, r):
        # get the family name
        family_name = r['panther_family_name']

        # create the gene family node
        self.final_node_list.append({'id': family_name, 'name': '', 'properties': {}})

        # create the sub gene family name
        self.final_node_list.append({'id': '', 'name': '', 'properties': {}})

        # create the edge
        self.final_edge_list.append({})

    def get_gene_by_gene_family(self, r):
        # get the family name
        family_name = r['panther_family_name']

        # create the gene family node
        self.final_node_list.append({'id': family_name, 'name': '', 'properties': {}})

        # create the sub gene family name
        self.final_node_list.append({'id': '', 'name': '', 'properties': {}})

        # create the edge
        self.final_edge_list.append({})


    def get_cellular_component_by_gene_family(self):
        # create the gene node
        self.final_node_list.append({'id': '', 'name': '', 'properties': {}})

        pass

    def get_biological_process_or_activity_by_gene_family(self):
        # create the gene node
        self.final_node_list.append({'id': '', 'name': '', 'properties': {}})

        pass

    def get_pathway_by_gene_family(self):
        # create the gene node
        self.final_node_list.append({'id': '', 'name': '', 'properties': {}})

        pass

if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load Panther data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the Panther data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    data_dir: str = args['data_dir']

    # get a reference to the processor
    ldr = PLoader()

    # load the data files and create KGX output
    ldr.load(data_dir, data_dir)
