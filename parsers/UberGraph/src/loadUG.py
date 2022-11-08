import os
import argparse
import io
import pyoxigraph
import tarfile

from Common.prefixes import HGNC, NCBIGENE
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader, SourceDataBrokenError


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
    parsing_version: str = '1.2'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        # this is the name of the archive file the source files will come from
        self.data_file = 'nonredundant-graph-table.tgz'
        self.data_url: str = 'https://ubergraph.apps.renci.org/downloads/current/'

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        latest_source_version = None
        archive_url = f'{self.data_url}{self.data_file}'
        gd = GetData(self.logger.level)
        gd.pull_via_http(archive_url,
                         self.data_path)
        tar_path = os.path.join(self.data_path, self.data_file)
        with tarfile.open(tar_path, 'r') as tar_files:
            with tar_files.extractfile('nonredundant-graph-table/build-metadata.nt') as metadata_file:
                for metadata_triple in pyoxigraph.parse(metadata_file, mime_type='application/n-triples'):
                    print(metadata_triple.predicate.value)
                    if metadata_triple.predicate.value == 'http://purl.org/dc/terms/created':
                        latest_source_version = metadata_triple.object.value.split("T")[0]
        os.remove(tar_path)
        if latest_source_version is None:
            raise SourceDataBrokenError('Metadata file not found for UberGraph. Latest source version unavailable.')
        else:
            return latest_source_version

    def get_data(self):
        """
        Gets the uberon graph data.

        """
        archive_url = f'{self.data_url}{self.data_file}'
        gd = GetData(self.logger.level)
        gd.pull_via_http(archive_url,
                         self.data_path)
        return True

    def parse_data(self):
        """
        Parses the data file for graph nodes/edges
        """

        def convert_node_iri_to_curie(iri):
            id_portion = iri.rsplit('/')[-1]
            id_portion.replace('#_', ':')
            id_portion.replace('_', ':')

            # HGNC must be handled differently that the others
            if 'hgnc' in iri:
                return f"{HGNC}:{id_portion}"

            # HGNC must be handled differently that the others
            if 'ncbigene' in iri:
                return f"{NCBIGENE}:{id_portion}"

            if id_portion.islower():
                print(id_portion)
                return None

            return id_portion

        predicate_prefix_conversion = {
            'rdf-schema': 'rdfs',
            'core': 'UBERON_CORE',
            'so': 'FMA'
        }

        def convert_edge_iri_to_curie(iri):
            id_portion = iri.rsplit('/')[-1]
            if '#' in id_portion:
                prefix, predicate = tuple(id_portion.split('#'))
            elif '_' in id_portion:
                prefix, predicate = tuple(id_portion.split('_', 1))
            if prefix in predicate_prefix_conversion:
                prefix = predicate_prefix_conversion[prefix]
            else:
                prefix = prefix.upper()
            return f'{prefix}:{predicate}'

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        tar_path = os.path.join(self.data_path, self.data_file)
        with tarfile.open(tar_path, 'r') as tar_files:

            node_curies = {}
            with tar_files.extractfile('nonredundant-graph-table/node-labels.tsv') as node_labels_file:
                for line in io.TextIOWrapper(node_labels_file):
                    node_id, node_iri = tuple(line.rstrip().split('\t'))
                    node_curies[node_id] = convert_node_iri_to_curie(node_iri)

            edge_curies = {}
            with tar_files.extractfile('nonredundant-graph-table/edge-labels.tsv') as edge_labels_file:
                for line in io.TextIOWrapper(edge_labels_file):
                    edge_id, edge_iri = tuple(line.rstrip().split('\t'))
                    edge_curies[edge_id] = convert_edge_iri_to_curie(edge_iri)

            with tar_files.extractfile('nonredundant-graph-table/edges.tsv') as edges_file:
                for line in io.TextIOWrapper(edges_file):
                    record_counter += 1

                    if self.test_mode and record_counter == 2000:
                        break

                    subject_id, predicate_id, object_id = tuple(line.rstrip().split('\t'))
                    subject_curie = node_curies[subject_id]
                    object_curie = node_curies[object_id]
                    predicate_curie = edge_curies[predicate_id]
                    self.output_file_writer.write_node(node_id=subject_curie)
                    self.output_file_writer.write_node(node_id=object_curie)
                    self.output_file_writer.write_edge(subject_id=subject_curie,
                                                       object_id=object_curie,
                                                       predicate=predicate_curie,
                                                       primary_knowledge_source=self.provenance_id)

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
