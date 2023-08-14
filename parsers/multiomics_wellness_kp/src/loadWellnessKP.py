
import os
import enum
import gzip

from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from Common.node_types import PRIMARY_KNOWLEDGE_SOURCE
from Common.utils import GetData


class WELLNESS_EDGES_DATACOLS(enum.IntEnum):
    SUBJECT_ID = 0
    PREDICATE = 1
    OBJECT_ID = 2
    RELATION = 3
    SUBJECT_NAME = 4
    OBJECT_NAME = 5
    EDGE_CATEGORY = 6
    N = 7
    TYPE_OF_REL = 8
    STRENGTH_OF_REL = 9
    QUALIFIER_DOMAIN = 10
    QUALIFIERS = 11
    QUALIFIER_VALUE = 12
    PVAL = 13


CORRELATION_ATTRIBUTE_MAPPING = {
    # Regression Method: ENM:8000094
    # http://purl.obolibrary.org/obo/NCIT_C53237
    "Ridge regression coefficient": {"NCIT:C53237": "ENM:8000094"},
    # Correlation Test: Spearman Correlation Test
    # http://purl.obolibrary.org/obo/NCIT_C53236
    # http://purl.obolibrary.org/obo/NCIT_C53249
    "Spearman Correlation": {"NCIT:C53236": "NCIT:C53249"}
}

##############
# Class: Multiomics Wellness KP source loader
#
# Desc: Class that loads/parses the Multiomics Wellness KP data.
##############
class MWKPLoader(SourceDataLoader):

    source_id: str = 'MultiomicsWellnessKP'
    provenance_id: str = 'infores:biothings-multiomics-wellness'
    parsing_version: str = '1.3'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.wellness_kp_url = 'https://storage.googleapis.com/multiomics_provider_kp_data/wellness/'
        self.wellness_edges_file = 'wellness_kg_edges_v1.7.tsv'
        self.data_files = [self.wellness_edges_file]

    def get_latest_source_version(self) -> str:
        # if possible go to the source and retrieve a string that is the latest version of the source data
        latest_version = 'v1.7'
        return latest_version

    def get_data(self) -> bool:
        source_data_url = f'{self.wellness_kp_url}{self.wellness_edges_file}'
        data_puller = GetData()
        data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor(file_writer=self.output_file_writer)
        wellness_edges_file_path: str = os.path.join(self.data_path, self.wellness_edges_file)
        with open(wellness_edges_file_path, 'rt') as fp:
            extractor.csv_extract(fp,
                                  lambda line: self.parse_node_id(line[WELLNESS_EDGES_DATACOLS.SUBJECT_ID.value]),  # subject id
                                  lambda line: self.parse_node_id(line[WELLNESS_EDGES_DATACOLS.OBJECT_ID.value]),  # object id
                                  # here we use the relation column instead of predicate because the RO:xxxx curie
                                  # is preferred as a way to map to the best biolink predicate in normalization
                                  lambda line: self.get_edge_predicate(data_row=line),  # predicate extractor
                                  lambda line: {'name': line[WELLNESS_EDGES_DATACOLS.SUBJECT_NAME.value]},  # subject properties
                                  lambda line: {'name': line[WELLNESS_EDGES_DATACOLS.OBJECT_NAME.value]},  # object properties
                                  lambda line: self.get_edge_properties(data_row=line),  # edge properties
                                  comment_character='#',
                                  delim='\t',
                                  has_header_row=True)
        return extractor.load_metadata

    def parse_node_id(self, original_node_id):
        if original_node_id.startswith('HMDB'):
            return f'HMDB:HMDB00{original_node_id[9:]}'
        return original_node_id

    def get_edge_predicate(self, data_row):
        if data_row[WELLNESS_EDGES_DATACOLS.QUALIFIER_DOMAIN] and \
                data_row[WELLNESS_EDGES_DATACOLS.QUALIFIER_DOMAIN] != 'nan':
            return None
        else:
            return data_row[WELLNESS_EDGES_DATACOLS.PREDICATE.value]

    def get_edge_properties(self, data_row):
        edge_properties = {PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id}
        if data_row[WELLNESS_EDGES_DATACOLS.TYPE_OF_REL.value] in CORRELATION_ATTRIBUTE_MAPPING:
            edge_properties.update(CORRELATION_ATTRIBUTE_MAPPING[data_row[WELLNESS_EDGES_DATACOLS.TYPE_OF_REL.value]])
        else:
            self.logger.warning(f'Unexpected type_of_relationship encountered: {data_row[WELLNESS_EDGES_DATACOLS.TYPE_OF_REL.value]}')

        # http://purl.obolibrary.org/obo/STATO_0000085 (effect size estimate)
        edge_properties["STATO:0000085"] = data_row[WELLNESS_EDGES_DATACOLS.STRENGTH_OF_REL.value]

        # http://purl.obolibrary.org/obo/GECKO_0000106 (sample size)
        edge_properties["GECKO:0000106"] = int(data_row[WELLNESS_EDGES_DATACOLS.N.value])

        # NCIT:C61594 - bonferroni_pval
        edge_properties["NCIT:C61594"] = float(data_row[WELLNESS_EDGES_DATACOLS.PVAL.value])

        """
        qualifier = None if (not data_row[WELLNESS_EDGES_DATACOLS.QUALIFIERS.value] or
                             (data_row[WELLNESS_EDGES_DATACOLS.QUALIFIERS.value] == 'nan')) \
            else data_row[WELLNESS_EDGES_DATACOLS.QUALIFIERS.value]
        qualifier_value = None if (not data_row[WELLNESS_EDGES_DATACOLS.QUALIFIER_VALUE.value] or
                             (data_row[WELLNESS_EDGES_DATACOLS.QUALIFIER_VALUE.value] == 'nan')) \
            else data_row[WELLNESS_EDGES_DATACOLS.QUALIFIER_VALUE.value]
        if qualifier and qualifier_value:
            edge_properties[qualifier] = qualifier_value
        """
        return edge_properties
