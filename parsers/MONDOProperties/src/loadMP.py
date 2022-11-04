import os
import argparse
import pyoxigraph

from collections import defaultdict
from zipfile import ZipFile
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge
from Common.prefixes import HGNC

#This cutoff defines which MONDO classes will be turned into properties.  To find it, I used yasgui to run
# the following query on ubergraph:
# PREFIX obo: <http://purl.obolibrary.org/obo/>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX reasoner: <http://reasoner.renci.org/vocab/>
# SELECT DISTINCT ?mondo ?mondo_label ?ic
# WHERE {
#   GRAPH <http://reasoner.renci.org/redundant> {
#     ?mondo rdfs:subClassOf* obo:MONDO_0000001
#   }
# ?mondo rdfs:isDefinedBy obo:mondo.owl .
# ?mondo rdfs:label ?mondo_label .
# ?mondo reasoner:normalizedInformationContent ?ic
# }
# From there, I looked at the results and tried to find a pretty good cutoff, and then made sure that the cutoff didn't
# include too many things.  This gives 400 properties, which is a lot, but small compared to the mondo ids.   Maybe
# it should be less?  It's configurable.
# There is an argument that this should not be a cutoff situation, but somebody should go through by hand and choose
# which nodes you want to turn into properties, because the MONDO hierarchy is of different depths and therefore
# the IC can vary from one region to another.   But this seems like a good first pass.
IC_CUTOFF=70.0


class MPLoader(SourceDataLoader):

    source_id: str = 'MONDOProperties'

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
        skipped_non_subclass_record_counter: int = 0
        skipped_non_mondo: int = 0

        property_mondos: set = set()
        mondo_labels: dict = {}
        mondo_superclasses: dict = defaultdict( list )

        archive_file_path = os.path.join(self.data_path, f'{self.data_file}')
        with ZipFile(archive_file_path) as zf:
            with zf.open(self.data_file.replace('.zip', '.ttl')) as ttl_file:

                # Loop through the triples
                # We're only intereseted in MONDO ids
                # For any mondo ID with a low Information Content, we want to make it a property
                # So we need to find those, and find their names, and find what other MONDOs are subclasses of that thing.
                # Then we can add those as properties to the subclasses and write everything out.
                for ttl_triple in pyoxigraph.parse(ttl_file, mime_type='text/turtle'):

                    record_counter += 1

                    if self.test_mode and record_counter == 2000:
                        break

                    subject_curie = convert_iri_to_curie(ttl_triple.subject.value)
                    if not subject_curie.startswith('MONDO'):
                        skipped_non_mondo += 1
                        continue

                    if 'normalizedInformationContent' in ttl_triple.predicate:
                        ic = ttl_triple.object.value
                        if ic < IC_CUTOFF:
                            property_mondos.add(subject_curie)

                    elif 'label' in ttl_triple.predicate:
                        label = ttl_triple.object.value
                        mondo_labels[subject_curie] = label

                    elif 'subClassOf' not in ttl_triple.predicate.value :
                        skipped_non_subclass_record_counter += 1
                        continue

                    object_curie = convert_iri_to_curie(ttl_triple.object.value)
                    if not object_curie.startswith('MONDO'):
                        skipped_non_mondo += 1
                        continue

                    #We want superclasses, so
                    mondo_superclasses[object_curie].append(subject_curie)

        nodes = []
        #Having now collected our properties and superclasses, we want to write nodes.
        for mondo_curie, scs in mondo_superclasses.items():
            props = { f"MONDO_SUPERCLASS:{'_'.join(mondo_labels[sc_mc].split())}":True for sc_mc in scs}
            node = kgxnode(mondo_curie,nodeprops=props)
            nodes.append(node)

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'non_mondo_source_lines': skipped_non_mondo,
            'non_subclass_source_lines': skipped_non_subclass_record_counter
            }

        # create the nodes and edges
        for node in nodes:
            self.output_file_writer.write_kgx_node(node)

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
    ldr = MPLoader()

    # load the data files and create KGX output
    ldr.load(data_dir + '/nodes.jsonl', data_dir + '/edges.jsonl')
