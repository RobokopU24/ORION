import os
import argparse
import datetime
import pyoxigraph

from zipfile import ZipFile
from Common.utils import GetData
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
    parsing_version: str = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        # this is the name of the archive file the source files will come from
        self.data_file = 'properties-nonredundant.zip'
        self.data_url: str = 'https://stars.renci.org/var/data_services/'

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        file_url = f'{self.data_url}{self.data_file}'
        gd = GetData(self.logger.level)
        latest_source_version = gd.get_http_file_modified_date(file_url)
        return latest_source_version

    def get_data(self):
        """
        Gets the uberon graph data.

        """
        # get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        byte_count: int = gd.pull_via_http(f'{self.data_url}{self.data_file}',
                                           self.data_path, False)
        if byte_count > 0:
            return True
        else:
            return False

    def parse_data(self):
        """
        Parses the data file for graph nodes/edges
        """

        def convert_iri_to_curie(iri):
            id_portion = iri.rsplit('/')[-1].rsplit('#')[-1]
            # HGNC must be handled differently that the others
            if iri.find('hgnc') > 0:
                return f"{HGNC}:" + id_portion
            # if string is all lower it is not a curie
            elif not id_portion.islower():
                # replace the underscores to create a curie
                return id_portion.replace('_', ':')
            return None

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        archive_file_path = os.path.join(self.data_path, f'{self.data_file}')
        with ZipFile(archive_file_path) as zf:
            # open the hmdb xml file
            with zf.open(self.data_file.replace('.zip', '.ttl')) as ttl_file:

                # for every triple in the input data
                for ttl_triple in pyoxigraph.parse(ttl_file, mime_type='text/turtle'):

                    # increment the record counter
                    record_counter += 1

                    if self.test_mode and record_counter == 2000:
                        break

                    predicate_curie = convert_iri_to_curie(ttl_triple.predicate.value)
                    subject_curie = convert_iri_to_curie(ttl_triple.subject.value)
                    object_curie = convert_iri_to_curie(ttl_triple.object.value)

                    # make sure we have all 3 entries
                    if subject_curie and object_curie and predicate_curie:
                        # create the nodes and edges
                        self.output_file_writer.write_kgx_node(kgxnode(subject_curie))
                        self.output_file_writer.write_kgx_node(kgxnode(object_curie))
                        self.output_file_writer.write_kgx_edge(kgxedge(subject_id=subject_curie,
                                                                       object_id=object_curie,
                                                                       predicate=predicate_curie,
                                                                       primary_knowledge_source=self.provenance_id))
                    else:
                        skipped_record_counter += 1

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # return the split file names so they can be removed if desired
        return load_metadata


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
