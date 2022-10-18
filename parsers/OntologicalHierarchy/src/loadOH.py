import os
import argparse
import pyoxigraph

from zipfile import ZipFile
from Common.utils import GetData
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
        skipped_non_subclass_record_counter: int = 0

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

                    # Filter only subclass edges
                    if 'subClassOf' not in ttl_triple.predicate.value:
                        skipped_non_subclass_record_counter += 1
                        continue

                    subject_curie = convert_iri_to_curie(ttl_triple.subject.value)
                    object_curie = convert_iri_to_curie(ttl_triple.object.value)

                    # make sure we have all 3 entries
                    if subject_curie and object_curie:
                        # create the nodes and edges
                        self.output_file_writer.write_kgx_node(kgxnode(subject_curie))
                        self.output_file_writer.write_kgx_node(kgxnode(object_curie))
                        self.output_file_writer.write_kgx_edge(kgxedge(subject_id=subject_curie,
                                                                       object_id=object_curie,
                                                                       predicate=self.subclass_predicate,
                                                                       primary_knowledge_source=self.provenance_id))
                    else:
                        skipped_record_counter += 1

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
