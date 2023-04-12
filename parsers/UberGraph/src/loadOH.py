import os
import argparse
import tarfile
from io import TextIOWrapper

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader, SourceDataBrokenError
from parsers.UberGraph.src.ubergraph import UberGraphTools


##############
# Class: Ontological-Hierarchy loader
#
# By: Phil Owen
# Date: 4/5/2021
# Desc: Class that loads/parses the Ontological-Hierarchy data.
##############
class OHLoader(SourceDataLoader):

    source_id: str = 'OntologicalHierarchy'
    provenance_id: str = 'infores:sri-ontology'
    description = "Subclass relationships from the redundant version of Ubergraph. The redundant version of Ubergraph is the complete inference closure for all subclass and existential relations. This includes all transitive, reflexive subclass relations."
    source_data_url = "https://github.com/INCATools/ubergraph"
    license = "https://raw.githubusercontent.com/INCATools/ubergraph/master/LICENSE.txt"
    attribution = "https://github.com/INCATools/ubergraph"
    parsing_version: str = '1.3'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        # set global variables
        self.base_url = 'https://ubergraph.apps.renci.org'
        self.data_file = 'redundant-graph-table.tgz'
        self.data_url: str = f'{self.base_url}/downloads/current/'
        self.redundant_graph_path = 'redundant-graph-table'
        self.subclass_predicate = 'rdfs:subClassOf'

    def get_latest_source_version(self) -> str:
        """
        gets the latest available version of the data

        this is a terrible way to grab the latest version but it works until we get Jim to make it easier
        :return:
        """
        latest_source_version = UberGraphTools.get_latest_source_version(ubergraph_url=self.base_url)
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

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0
        skipped_non_subclass_record_counter: int = 0

        ubergraph_archive_path = os.path.join(self.data_path, self.data_file)
        ubergraph_tools = UberGraphTools(ubergraph_archive_path,
                                         graph_base_path=self.redundant_graph_path)
        with tarfile.open(ubergraph_archive_path, 'r') as tar_files:

            self.logger.info(f'Parsing Ubergraph for Ontological Hierarchy..')
            with tar_files.extractfile(f'{self.redundant_graph_path}/edges.tsv') as edges_file:
                for line in TextIOWrapper(edges_file):
                    record_counter += 1
                    if (self.test_mode and
                            (record_counter - skipped_non_subclass_record_counter - skipped_record_counter == 5000)):
                        break

                    subject_id, predicate_id, object_id = tuple(line.rstrip().split('\t'))
                    predicate_curie = ubergraph_tools.get_curie_for_edge_id(predicate_id)
                    if not predicate_curie or 'subClassOf' not in predicate_curie:
                        skipped_non_subclass_record_counter += 1
                        continue

                    subject_curie = ubergraph_tools.get_curie_for_node_id(subject_id)
                    object_curie = ubergraph_tools.get_curie_for_node_id(object_id)
                    if subject_curie and object_curie:
                        self.output_file_writer.write_node(node_id=subject_curie)
                        self.output_file_writer.write_node(node_id=object_curie)
                        self.output_file_writer.write_edge(subject_id=subject_curie,
                                                           object_id=object_curie,
                                                           predicate=predicate_curie,
                                                           primary_knowledge_source=self.provenance_id)
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
