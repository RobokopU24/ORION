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

    def parse_data(self):
        """
        Parses the data file for graph nodes/edges
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
        split_files: list = gd.split_file(archive_file_path=os.path.join(self.data_path, f'{self.data_file}'),
                                          output_dir=self.data_path,
                                          data_file_name=self.data_file.replace('.zip', '.ttl'))

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
                                                            predicate=self.subclass_predicate))
                    else:
                        skipped_non_subclass_record_counter += 1
                else:
                    skipped_record_counter += 1

            self.logger.debug(
                f'Loading complete for file {file.split(".")[2]} of {len(split_files)} in {round(time.time() - tm_start, 0)} seconds.')

        # remove all the intermediate files
        for file in split_files:
            os.remove(file)

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter,
            'non_subclass_source_lines': skipped_non_subclass_record_counter
            }

        # return the split file names so they can be removed if desired
        return load_metadata


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
