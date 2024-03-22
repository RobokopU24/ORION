import os
import enum

# ct
from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from Common.prefixes import UMLS  # only an example, use existing curie prefixes or add your own to the prefixes file
from Common.utils import GetData
from Common.biolink_constants import PRIMARY_KNOWLEDGE_SOURCE, AGGREGATOR_KNOWLEDGE_SOURCES, KNOWLEDGE_LEVEL, AGENT_TYPE, SUPPORTING_DATA_SOURCE

class CLINICALTRIALS_EDGES_DATACOLS(enum.IntEnum):
    SUBJECT = 0
    PREDICATE = 1
    OBJECT = 2
    SUBJECT_NAME = 3
    OBJECT_NAME = 4
    CATEGORY = 5
    NCTID = 6
    NCTID_CURIE = 7

##############
# Class: Multiomics Clinical Trials KP Loader
# Desc: Class that loads/parses the Multiomics Clinical Trials data.
##############
class ClinicalTrialsLoader(SourceDataLoader):
    
    source_id: str = "MultiomicsClinicalTrials"
    provenance_id_primary: str = "infores:clinicaltrials"
    provenance_id_supporting: str = "infores:aact"
    provenance_id_aggregator: str = "infores:biothings-multiomics-clinicaltrials"
    knowledge_level: str = "biolink:knowledge_assertion"
    agent_type: str = "biolink:text_mining_agent"


    description = "Multiomics Clinical Trials hosts clinical trials study design and other related information from ClinicalTrials.gov via AACT (https://aact.ctti-clinicaltrials.org/)"
    source_data_url = "https://aact.ctti-clinicaltrials.org/" # in camkp, this
    license = "https://github.com/Hadlock-Lab/clinical_risk_kp/blob/master/LICENSE"
    attribution = "https://github.com/Hadlock-Lab/Multiomics_ClinicalTrials_KP"  # what is "attribution"?
    parsing_version = "1.0"
    
    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.clinicaltrials_kp_url: str = 'https://storage.googleapis.com/multiomics_provider_kp_data/clinical_trials/'
        self.clinicaltrials_edges_file: str = 'ClinTrials_KG_edges_v01_3.csv'
        self.data_files = [self.clinicaltrials_edges_file]
        

    def get_latest_source_version(self) -> str:
        # if possible go to the source and retrieve a string that is the latest version of the source data
        latest_version = 'v1.0'
        return latest_version

    def get_data(self) -> bool:
        source_data_url = f'{self.clinicaltrials_kp_url}{self.clinicaltrials_edges_file}'
        data_puller = GetData()
        data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor(file_writer=self.output_file_writer)
        clinicaltrials_edges_file_path: str = os.path.join(self.data_path, self.clinicaltrials_edges_file)
        with open(clinicaltrials_edges_file_path, 'rt') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[CLINICALTRIALS_EDGES_DATACOLS.SUBJECT.value],  # subject id
                                  lambda line: line[CLINICALTRIALS_EDGES_DATACOLS.OBJECT.value],  # object id
                                  lambda line: line[CLINICALTRIALS_EDGES_DATACOLS.PREDICATE.value],  # predicate
                                  lambda line: {'name': line[CLINICALTRIALS_EDGES_DATACOLS.SUBJECT_NAME.value]},  # subject properties
                                  lambda line: {'name': line[CLINICALTRIALS_EDGES_DATACOLS.OBJECT_NAME.value]},  # object properties
                                  lambda line: self.get_edge_properties(data_row=line),  # edge properties
                                  comment_character='#',
                                  delim='\t',
                                  has_header_row=True)
        return extractor.load_metadata

    def get_edge_properties(self, data_row):
        edge_properties = {PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id_primary}
        edge_properties[AGGREGATOR_KNOWLEDGE_SOURCES] = [self.provenance_id_aggregator]
        edge_properties[SUPPORTING_DATA_SOURCE] = self.provenance_id_supporting
        edge_properties[KNOWLEDGE_LEVEL] = self.knowledge_level
        edge_properties[AGENT_TYPE] = self.agent_type

        # NCT ID (the ID # of the Clinical Trial Study associated with the relationship)
        edge_properties["clinicaltrials_id"] = [data_row[CLINICALTRIALS_EDGES_DATACOLS.NCTID.value]]

        # NCT ID (the ID # of the Clinical Trial Study associated with the relationship, in CURIE form)
        edge_properties["nctid_curie"] = [data_row[CLINICALTRIALS_EDGES_DATACOLS.NCTID_CURIE.value]]
        return edge_properties