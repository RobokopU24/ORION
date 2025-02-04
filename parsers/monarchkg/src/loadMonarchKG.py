
import os
import tarfile
import orjson
import requests

from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxedge
from Common.biolink_constants import *
from Common.utils import GetData, GetDataPullError


##############
# Class: Monarch KG source loader
#
# Desc: Class that loads/parses the Monarch KG data.
##############
class MonarchKGLoader(SourceDataLoader):

    source_id: str = 'MonarchKG'
    provenance_id: str = 'infores:monarchinitiative'
    parsing_version: str = '1.2'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        # there is a /latest/ for this url, but without a valid get_latest_source_version function,
        # it could create a mismatch, pin to this version for now
        self.data_url = 'https://data.monarchinitiative.org/monarch-kg-dev/latest/'
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
        """
        Gets the name of latest monarch kg version from metadata. 
        """
        latest_version = None
        try:
            metadata_yaml : requests.Response = requests.get("https://data.monarchinitiative.org/monarch-kg-dev/latest/metadata.yaml")
            for line in metadata_yaml.text.split('\n'):
                if("kg-version:" in line): latest_version = line.replace("kg-version:","").strip()
            if(latest_version==None):raise ValueError("Cannot find 'kg-version' in Monarch KG metadata yaml.")
        except Exception as e:
            raise GetDataPullError(error_message=f'Unable to determine latest version for Monarch KG: {e}')
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
        protected_edge_labels = [SUBJECT_ID, OBJECT_ID, PREDICATE,PRIMARY_KNOWLEDGE_SOURCE,
                                 AGGREGATOR_KNOWLEDGE_SOURCES, KNOWLEDGE_LEVEL, AGENT_TYPE,
                                 PUBLICATIONS, "biolink:primary_knowledge_source", "biolink:aggregator_knowledge_source"]

        with tarfile.open(full_tar_path, 'r') as tar_files:
            with tar_files.extractfile(self.monarch_edge_file_archive_path) as edges_file:
                for line in edges_file:
                    monarch_edge = orjson.loads(line)
                    # normally we wouldn't use constants to read FROM a source,
                    # but in this case monarch kg is biolink compliant, so they should be the same
                    subject_id = monarch_edge[SUBJECT_ID]
                    object_id = monarch_edge[OBJECT_ID]
                    predicate = monarch_edge[PREDICATE]
                    if not (subject_id and object_id and predicate):
                        skipped_bad_record_counter += 1
                        continue

                    if predicate not in self.desired_predicates:
                        skipped_undesired_predicate += 1
                        continue

                    # get the knowledge sources, map them to something else if needed,
                    # then check if edge should be ignored due to the knowledge source
                    primary_knowledge_source = self.knowledge_source_mapping.get(monarch_edge[PRIMARY_KNOWLEDGE_SOURCE],
                                                                                 monarch_edge[PRIMARY_KNOWLEDGE_SOURCE])
                    aggregator_knowledge_sources = [self.knowledge_source_mapping.get(ks, ks) for ks in monarch_edge[AGGREGATOR_KNOWLEDGE_SOURCES]]
                    if primary_knowledge_source in self.knowledge_source_ignore_list or \
                            any([ks in self.knowledge_source_ignore_list for ks in aggregator_knowledge_sources]):
                        skipped_ignore_knowledge_source += 1
                        continue

                    edge_properties = {
                        KNOWLEDGE_LEVEL: monarch_edge[KNOWLEDGE_LEVEL] if KNOWLEDGE_LEVEL in monarch_edge else NOT_PROVIDED,
                        AGENT_TYPE: monarch_edge[AGENT_TYPE] if AGENT_TYPE in monarch_edge else NOT_PROVIDED
                    }

                    if monarch_edge[PUBLICATIONS]:
                        edge_properties[PUBLICATIONS] = monarch_edge[PUBLICATIONS]

                    for edge_attribute in monarch_edge:
                        if edge_attribute not in protected_edge_labels and monarch_edge[edge_attribute]:
                            edge_properties[edge_attribute] = monarch_edge[edge_attribute]

                    output_edge = kgxedge(
                        subject_id=subject_id,
                        predicate=predicate,
                        object_id=object_id,
                        primary_knowledge_source=primary_knowledge_source,
                        aggregator_knowledge_sources=aggregator_knowledge_sources,
                        edgeprops=edge_properties
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
