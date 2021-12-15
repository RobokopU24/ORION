import os
import argparse
import logging
import datetime
import time

from rdflib import Graph
from Common.utils import LoggingUtil, GetData
from Common.loader_interface import SourceDataLoader
from Common.prefixes import HGNC
from Common.kgxmodel import kgxnode, kgxedge


##############
# Class: UberGraph data loader
#
# By: Phil Owen
# Date: 6/12/2020
# Desc: Class that loads the UberGraph data and creates KGX files for importing into a Neo4j graph.
##############
class UGLoader(SourceDataLoader):

    source_id = 'UberGraph'
    provenance_id = 'infores:ubergraph'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        # this will be dynamically populated after extracting and splitting the data files
        self.split_file_paths = []
        # this is the name of the archive file the source files will come from
        self.data_file = 'properties-nonredundant.zip'

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        return datetime.datetime.now().strftime("%m_%Y")

    def get_data(self):
        """
        Gets the uberon graph data.

        """
        # get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        byte_count: int = gd.pull_via_http(f'https://stars.renci.org/var/data_services/{self.data_file}',
                                           self.data_path, False)

        # unzip the archive and split the file into pieces of size file_size
        file_size = 250000
        data_file_inside_archive = 'properties-nonredundant.ttl'
        self.split_file_paths: list = gd.split_file(self.data_file, self.data_path, data_file_inside_archive, file_size)

        if byte_count > 0:
            return True
        else:
            return False

    def parse_data(self):
        """
        Parses the data file for graph nodes/edges.
        """

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # init a list for the output data
        triple: list = []

        # get a reference to the data handler object
        gd: GetData = GetData(self.logger.level)

        # parse each file
        for file in self.split_file_paths:
            self.logger.info(f'Working file: {file}')

            # get a time stamp
            tm_start = time.time()

            # get the biolink json-ld data
            g: Graph = gd.get_biolink_graph(file)

            # get the triples
            g_t = g.triples((None, None, None))

            # for every triple in the input data
            for t in g_t:
                # increment the record counter
                record_counter += 1

                # clear before use
                triple.clear()

                # get the curie for each element in the triple
                for n in t:
                    # init the value storage
                    val = None

                    try:
                        # get the value
                        qname = g.compute_qname(n)

                        # HGNC must be handled differently that the others
                        if qname[1].find('hgnc') > 0:
                            val = f"{HGNC}:" + qname[2]
                        # if string is all lower it is not a curie
                        elif not qname[2].islower():
                            # replace the underscores to create a curie
                            val = qname[2].replace('_', ':')

                    except Exception as e:
                        self.logger.warning(f'Exception parsing RDF {t}. {e}')

                    # did we get a valid value
                    if val is not None:
                        # add it to the group
                        triple.append(val)

                # make sure we have all 3 entries
                if len(triple) == 3:
                    # create the nodes
                    node_1 = kgxnode(triple[0], name=triple[0])
                    self.final_node_list.append(node_1)
                    new_edge = kgxedge(subject_id=triple[0],
                                       object_id=triple[2],
                                       relation=triple[1],
                                       original_knowledge_source=self.provenance_id)
                    self.final_edge_list.append(new_edge)
                    node_2 = kgxnode(triple[2], name=triple[2])
                    self.final_node_list.append(node_2)
                    record_counter += 1
                else:
                    skipped_record_counter += 1

            self.logger.debug(f'Loading complete for file {file.split(".")[2]} of {len(self.split_file_paths)} in {round(time.time() - tm_start, 0)} seconds.')

        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # return to the caller
        return load_metadata

    def clean_up(self):
        # these split files are generated during parsing and don't fit the normal mold, custom clean up here
        if self.split_file_paths:
            for file_to_remove in self.split_file_paths:
                if os.path.exists(file_to_remove):
                    os.remove(file_to_remove)
        super().clean_up()


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
