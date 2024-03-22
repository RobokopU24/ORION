
import os
import tarfile
import orjson

from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxedge
from Common.biolink_constants import PUBLICATIONS
from Common.utils import GetData


##############
# Class: Monarch KG source loader
#
# Desc: Class that loads/parses the Monarch KG data.
##############
class MonarchKGLoader(SourceDataLoader):

    source_id: str = 'MonarchKG'
    provenance_id: str = 'infores:monarchinitiative'
    parsing_version: str = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        # there is a /latest/ for this url, but without a valid get_latest_source_version function,
        # it could create a mismatch, pin to this version for now
        self.data_url = 'https://data.monarchinitiative.org/monarch-kg-dev/2024-03-18/'
        self.monarch_graph_archive = 'monarch-kg.jsonl.tar.gz'
        self.monarch_edge_file_archive_path = 'monarch-kg_edges.jsonl'
        self.data_files = [self.monarch_graph_archive]

        self.desired_predicates = {
            'biolink:causes',
            'biolink:contributes_to',
            'biolink:has_phenotype',
            'biolink:expressed_in'
        }

        self.knowledge_source_ignore_list = {
            'infores:ctd',
            'infores:reactome',
            'infores:goa',
            'infores:cafa',
            'infores:bhf-ucl',
            'infores:aruk-ucl',
            'infores:parkinsonsuk-ucl',
            'infores:alzheimers-university-of-toronto',
            'infores:agbase',
            'infores:dictybase',
            'infores:ntnu-sb',
            'infores:wb'
        }

        self.knowledge_source_mapping = {
            'infores:alliancegenome': 'infores:agrkb',
            'infores:hgnc-ucl': 'infores:hgnc',
            'infores:go-central': 'infores:go'
        }

    def get_latest_source_version(self) -> str:
        # possible to retrieve from /latest/index.html with beautifulsoup or some html parser but not ideal,
        # planning to try to set up a better method with owners
        latest_version = '2024-03-18'
        return latest_version

    def get_data(self) -> bool:
        source_data_url = f'{self.data_url}{self.monarch_graph_archive}'
        data_puller = GetData()
        data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        record_counter = 0
        skipped_bad_record_counter = 0
        skipped_ignore_knowledge_source = 0
        skipped_undesired_predicate = 0
        full_tar_path = os.path.join(self.data_path, self.monarch_graph_archive)
        with tarfile.open(full_tar_path, 'r') as tar_files:
            with tar_files.extractfile(self.monarch_edge_file_archive_path) as edges_file:
                for line in edges_file:
                    monarch_edge = orjson.loads(line)
                    subject_id = monarch_edge['subject']
                    object_id = monarch_edge['object']
                    predicate = monarch_edge['predicate']
                    if not (subject_id and object_id and predicate):
                        skipped_bad_record_counter += 1
                        print(line)
                        continue

                    if predicate not in self.desired_predicates:
                        skipped_undesired_predicate += 1
                        continue

                    # get the knowledge sources, map them to something else if needed,
                    # then check if edge should be ignored due to the knowledge source
                    primary_knowledge_source = self.knowledge_source_mapping.get(monarch_edge['primary_knowledge_source'],
                                                                                 monarch_edge['primary_knowledge_source'])
                    aggregator_knowledge_sources = [self.knowledge_source_mapping.get(ks, ks) for ks in monarch_edge['aggregator_knowledge_source']]
                    if primary_knowledge_source in self.knowledge_source_ignore_list or \
                            any([ks in self.knowledge_source_ignore_list for ks in aggregator_knowledge_sources]):
                        skipped_ignore_knowledge_source += 1
                        continue

                    edge_properties = {}
                    if monarch_edge['publications']:
                        edge_properties[PUBLICATIONS] = monarch_edge['publications']
                    for edge_attribute in monarch_edge:
                        if '_qualifier' in edge_attribute and monarch_edge[edge_attribute]:
                            edge_properties[edge_attribute] = monarch_edge[edge_attribute]
                    output_edge = kgxedge(
                        subject_id=subject_id,
                        predicate=predicate,
                        object_id=object_id,
                        primary_knowledge_source=primary_knowledge_source,
                        aggregator_knowledge_sources=aggregator_knowledge_sources
                    )
                    self.output_file_writer.write_node(object_id)
                    self.output_file_writer.write_node(subject_id)
                    self.output_file_writer.write_kgx_edge(output_edge)
                    record_counter += 1
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_bad_record_counter,
            'lines_skipped_due_to_undesired_predicate': skipped_undesired_predicate,
            'lines_skipped_due_to_knowledge_source_ignore_list': skipped_ignore_knowledge_source
        }
        return load_metadata
