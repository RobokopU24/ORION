import os
import argparse
import logging
import datetime
import time

from rdflib import Graph
from Common.utils import LoggingUtil, GetData
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader


##############
# Class: UberGraph data loader
#
# By: Phil Owen
# Date: 6/12/2020
# Desc: Class that loads the UberGraph data and creates KGX files for importing into a Neo4j graph.
##############
class UGLoader(SourceDataLoader):
    # the final output lists of nodes and edges
    final_node_list: list = []
    final_edge_list: list = []

    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # set global variables
        self.data_path = os.environ['DATA_SERVICES_STORAGE']
        self.test_mode = test_mode
        self.source_id = 'UberGraph'
        self.source_db = 'properties-nonredundant.ttl'
        self.file_size = 200000

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.UberGraph.UGLoader", level=logging.DEBUG, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_name(self):
        """
        returns the name of this class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        return datetime.datetime.now().strftime("%m/%d/%Y")

    def get_ug_data(self):
        """
        Gets the uberon graph data.

        """
        pass

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
                file_writer.write_node(node['id'], node_name=node['name'], node_types=[], node_properties=node['properties'])

            # for each edge captured
            for edge in self.final_edge_list:
                # write out the edge data
                file_writer.write_edge(subject_id=edge['subject'], object_id=edge['object'], relation=edge['relation'], edge_properties=edge['properties'], predicate='')

    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        """
        Loads/parsers the UberGraph data file to produce node/edge KGX files for importation into a graph database.

        :param: nodes_output_file_path - path to node file
        :param: edges_output_file_path - path to edge file
        :return: None
        """
        self.logger.info(f'UGLoader - Start of UberGraph data processing.')

        # split the input file names
        file_names = ['properties-nonredundant.ttl']

        # loop through the data files
        for file_name in file_names:
            self.logger.info(f'Splitting UberGraph data file: {file_name}. {self.file_size} records per file + remainder')

            # parse the data
            self.parse_data_file(self.data_path, file_name)

            # do not remove the file if in debug mode
            # split_files = self.parse_data_file(nodes_output_file_path, file_name)
            # if self.logger.level != logging.DEBUG:
            #     # remove the data file
            #     os.remove(os.path.join(nodes_output_file_path, file_name))
            #
            #     # remove all the intermediate files
            #     for file in split_files:
            #         os.remove(file)


        self.write_to_file(nodes_output_file_path, edges_output_file_path)

        self.logger.info(f'UGLoader - Processing complete.')

    def parse_data_file(self, data_file_path: str, data_file_name: str) -> list:
        """
        Parses the data file for graph nodes/edges and writes them out the KGX tsv files.

        :param data_file_path: the path to the UberGraph data file
        :param data_file_name: the name of the UberGraph file
        :return: split_files: the temporary files created of the input file
        """

        # get a reference to the data handler object
        gd = GetData(self.logger.level)

        # init a list for the output data
        triple: list = []

        # get the source database name
        source_database = 'UberGraph ' + data_file_name.split('.')[0]

        # split the file into pieces
        split_files: list = gd.split_file(data_file_path, data_file_name, self.file_size)

        # parse each file
        for file in split_files:
            self.logger.info(f'Working file: {file}')

            # get a time stamp
            tm_start = time.time()

            # get the biolink json-ld data
            g: Graph = gd.get_biolink_graph(file)

            # get the triples
            g_t = g.triples((None, None, None))

            # for every triple in the input data
            for t in g_t:
                # clear before use
                triple.clear()

                # get the curie for each element in the triple
                for n in t:
                    try:
                        # get the value
                        val: str = g.compute_qname(n)[2]

                        # if string is all lower it is not a curie
                        if not val.islower():
                            # replace the underscores to create a curie
                            val = g.compute_qname(n)[2].replace('_', ':')
                    except Exception as e:
                        # save it anyway
                        val = n
                        self.logger.warning(f'Exception parsing RDF qname {val}. {e}')

                    # add it to the group
                    triple.append(val)

                # create the nodes
                self.final_node_list.append({'id': triple[0], 'name': triple[0], 'properties': None})
                self.final_edge_list.append({'subject': triple[0], 'predicate': triple[1], 'relation': triple[1], 'object': triple[2], 'properties': {'source_database': source_database}})
                self.final_node_list.append({'id': triple[2], 'name': triple[2], 'properties': None})

            self.logger.debug(f'Loading complete for file {file.split(".")[2]} of {len(split_files)} in {round(time.time() - tm_start, 0)} seconds.')

        # return the split file names so they can be removed if desired
        return split_files


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load IntAct virus interaction data file and create KGX import files.')

    # command line should be like: python loadIA.py -d E:/Data_services/IntAct_data
    ap.add_argument('-u', '--data_dir', required=True, help='The UberGraph data file directory.')

    # parse the arguments
    args = vars(ap.parse_args())

    # UG_data_dir = 'E:/Data_services/UberGraph'
    UG_data_dir = args['data_dir']

    # get a reference to the processor logging.DEBUG
    ug = UGLoader()

    # load the data files and create KGX output files
    ug.load(UG_data_dir, UG_data_dir)
