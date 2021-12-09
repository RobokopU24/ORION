import os
import argparse
import logging
import datetime
import time

from rdflib import Graph
from Common.utils import LoggingUtil, GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge
from Common.prefixes import HGNC


##############
# Class: Ontological-Hierarchy loader
#
# By: Phil Owen
# Date: 4/5/2021
# Desc: Class that loads/parses the Ontological-Hierarchy data.
##############
class OHLoader(SourceDataLoader):

    source_id: str = 'OntologicalHierarchy'
    provenance_id: str = 'infores:ontological-hierarchy'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        # set global variables
        self.data_url: str = 'https://stars.renci.org/var/data_services/'
        self.data_file: str = 'properties-redundant.zip'
        self.source_db: str = 'properties-redundant.ttl'
        self.subclass_predicate = 'biolink:subclass_of'

        self.file_size = 500000

    def get_latest_source_version(self) -> str:
        """
        gets the latest available version of the data

        :return:
        """
        file_url = f'{self.data_url}{self.data_file}'
        gd = GetData(self.logger.level)
        latest_source_version = gd.get_http_file_modified_date(file_url)
        return latest_source_version

    def get_data(self) -> int:
        """
        Gets the ontological-hierarchy data.

        """
        # get a reference to the data gathering class
        # gd: GetData = GetData(self.logger.level)

        # get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        byte_count: int = gd.pull_via_http(f'{self.data_url}{self.data_file}',
                                           self.data_path, False)

        if byte_count > 0:
            return True

    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        """
        Loads/parsers the UberGraph data file to produce node/edge KGX files for importation into a graph database.

        :param: nodes_output_file_path - path to node file
        :param: edges_output_file_path - path to edge file
        :return: None
        """
        self.logger.info(f'OHLoader - Start of UberGraph Ontological hierarchy data processing.')

        self.get_data()

        # split the input file names
        file_name = self.data_file

        self.logger.info(f'Parsing OntologicalHierarchy data file: {file_name}. {self.file_size} records per file + remainder')

        # parse the data
        split_files, final_record_count, final_skipped_count, final_skipped_non_subclass = \
            self.parse_data_file(file_name)

        # remove all the intermediate files
        for file in split_files:
            os.remove(file)

        # remove the data file
        os.remove(os.path.join(self.data_path, file_name ))

        self.write_to_file(nodes_output_file_path, edges_output_file_path)

        self.logger.info(f'OntologicalHierarchy loader - Processing complete.')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': final_record_count,
            'unusable_source_lines': final_skipped_count,
            'non_subclass_source_lines': final_skipped_non_subclass
        }

        # return the metadata to the caller
        return load_metadata

    def parse_data_file(self, data_file_name: str) -> (list, int, int):
        """
        Parses the data file for graph nodes/edges and writes them out the KGX tsv files.

        :param data_file_path: the path to the UberGraph data file
        :param data_file_name: the name of the UberGraph file
        :return: split_files: the temporary files created of the input file and the parsed metadata
        """
        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0
        skipped_non_subclass_record_counter: int = 0

        # get a reference to the data handler object
        gd: GetData = GetData(self.logger.level)

        # init a list for the output data
        triple: list = []

        # split the file into pieces
        split_files: list = gd.split_file(os.path.join(self.data_path, f'{data_file_name}'), self.data_path,
                                          data_file_name.replace('.zip', '.ttl'), self.file_size)

        # parse each file

        # test mode
        if self.test_mode:
            # use the first few files
            files_to_parse = split_files[0:2]
        else:
            files_to_parse = split_files

        for file in files_to_parse:
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
                    # Filter only subclass edges
                    if triple[1] == 'subClassOf':
                        # create the nodes and edges
                        self.final_node_list.append(kgxnode(triple[0], name=triple[0]))
                        self.final_node_list.append(kgxnode(triple[2], name=triple[2]))
                        self.final_edge_list.append(kgxedge(subject_id=triple[0],
                                                            object_id=triple[2],
                                                            relation=self.subclass_predicate,
                                                            predicate=self.subclass_predicate))
                    else:
                        skipped_non_subclass_record_counter += 1
                else:
                    skipped_record_counter += 1

            self.logger.debug(
                f'Loading complete for file {file.split(".")[2]} of {len(split_files)} in {round(time.time() - tm_start, 0)} seconds.')

        # return the split file names so they can be removed if desired
        return split_files, record_counter, skipped_record_counter, skipped_non_subclass_record_counter


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load Ontological-Hierarchy data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the Ontological-Hierarchy data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    data_dir: str = args['data_dir']

    # get a reference to the processor
    ldr = OHLoader()

    # load the data files and create KGX output
    ldr.load(data_dir + '/nodes.jsonl', data_dir + '/edges.jsonl')
