import os
import enum
import json
import re

from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from Common.biolink_constants import PRIMARY_KNOWLEDGE_SOURCE, SUPPORTING_DATA_SOURCE, KNOWLEDGE_LEVEL, AGENT_TYPE
from Common.utils import GetData


class OLD_EHRKP_EDGES_DATACOLS(enum.IntEnum):
    SUBJECT_ID = 0
    PREDICATE = 1
    OBJECT_ID = 2
    RELATION = 3
    PROVIDED_BY = 4
    PROVIDED_DATE = 5
    CATEGORY = 6
    CLASSIFIER = 7
    AUCROC = 8
    PVAL = 9
    FEATURE_IMPORTANCE = 10
    FEATURE_COEFFICIENT = 11
    PATIENT_COUNT_WITH_CONDITION = 12
    PATIENT_COUNT_WITHOUT_CONDITION = 13

class EHRKP_EDGES_DATACOLS(enum.IntEnum):
    SUBJECT_ID = 0
    SUBJECT_NAME = 1
    PREDICATE = 2
    OBJECT_ID = 3
    OBJECT_NAME = 4
    RELATION = 5
    PROVIDED_BY = 6
    PROVIDED_DATE = 7
    CATEGORY = 8
    CLASSIFIER = 9
    AUCROC = 10
    FEATURE_IMPORTANCE = 11
    FEATURE_COEFFICIENT = 12
    LOWER_95_CI = 13
    UPPER_95_CI = 14
    PVAL = 15
    READABLE_PVAL = 16
    PATIENT_COUNT_WITH_CONDITION = 17
    PATIENT_COUNT_WITHOUT_CONDITION = 18
    READABLE_PATIENT_COUNT_WITH_CONDITION = 19
    READABLE_PATIENT_COUNT_WITHOUT_CONDITION = 20
    

##############
# Class: Multiomics EHR KP Loader
#
# Desc: Class that loads/parses the Multiomics EHR KP data.
##############
class EHRKPLoader(SourceDataLoader):

    source_id: str = 'MultiomicsEHRKP'
    primary_knowledge_source: str = 'infores:multiomics-ehr-risk'
    supporting_data_source: str = 'infores:providence-st-joseph-ehr'
    knowledge_level: str = 'biolink:statistical_association'
    agent_type: str = 'biolink:computational_model'
    parsing_version: str = '1.0'
    

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.ehr_kp_url = 'https://storage.googleapis.com/multiomics_provider_kp_data/clinical_risk/'
        self.ehr_edges_file = 'ehr_risk_edges_data_2022_06_01.csv'
        self.data_files = [self.ehr_edges_file]

    def get_latest_source_version(self) -> str:
        # if possible go to the source and retrieve a string that is the latest version of the source data
        # version = we infer the date from the filename of the edges TSVs deployed 
        edges_file = self.ehr_edges_file
        try:
            date_digits = re.findall(r'\d+', edges_file)
            date_digits = ''.join(date_digits)
            latest_version = "TSV_Date_" + date_digits[:4] + '_' + date_digits[4:6] + '_' + date_digits[6:8]
        except IndexError:
            latest_version = "1.0"

        return latest_version

    def get_data(self) -> bool:
        source_data_url = f'{self.ehr_kp_url}{self.ehr_edges_file}'
        data_puller = GetData()
        data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor(file_writer=self.output_file_writer)
        ehr_edges_file_path: str = os.path.join(self.data_path, self.ehr_edges_file)
        with open(ehr_edges_file_path, 'rt') as fp:
            
            extractor.csv_extract(fp,
                                  lambda line: line[OLD_EHRKP_EDGES_DATACOLS.SUBJECT_ID.value],  # subject id
                                  lambda line: line[OLD_EHRKP_EDGES_DATACOLS.OBJECT_ID.value],  # object id
                                  # here we use the relation column instead of predicate because the RO:xxxx curie
                                  # is preferred as a way to map to the best biolink predicate in normalization
                                  lambda line: line[OLD_EHRKP_EDGES_DATACOLS.PREDICATE.value],  # predicate extractor
                                  lambda line: {}, # {'name': line[EHRKP_EDGES_DATACOLS.SUBJECT_NAME.value]}, # subject properties
                                  lambda line: {}, # {'name': line[EHRKP_EDGES_DATACOLS.OBJECT_NAME.value]},  # object properties
                                  lambda line: self.get_edge_properties(data_row=line),  # edge properties
                                  comment_character='#',
                                  delim=',',
                                  has_header_row=True)
            return extractor.load_metadata

    def get_edge_properties(self, data_row):
        edge_properties = {PRIMARY_KNOWLEDGE_SOURCE: self.primary_knowledge_source}  # Multiomics Team
        edge_properties[SUPPORTING_DATA_SOURCE] = self.supporting_data_source
        edge_properties[KNOWLEDGE_LEVEL] = self.knowledge_level
        edge_properties[AGENT_TYPE] = self.agent_type
        edge_properties["num_patients_with_condition"] = 10**(float(data_row[OLD_EHRKP_EDGES_DATACOLS.PATIENT_COUNT_WITH_CONDITION.value]))
        edge_properties["num_patients_without_condition"] = 10**(float(data_row[OLD_EHRKP_EDGES_DATACOLS.PATIENT_COUNT_WITHOUT_CONDITION.value]))

        edge_properties["biolink:supporting_study_method_type"] = "STATO:0000149"
        edge_properties["biolink:update_date"] = "2022-05-18"
        edge_properties["biolink:p_value"] = float(data_row[OLD_EHRKP_EDGES_DATACOLS.PVAL.value])
        edge_properties["STATO:0000209"] = float(data_row[OLD_EHRKP_EDGES_DATACOLS.AUCROC.value])
        edge_properties["biolink:log_odds_ratio"] = float(data_row[OLD_EHRKP_EDGES_DATACOLS.FEATURE_COEFFICIENT.value])
        edge_properties["biolink:supporting_study_date_range"] = "2022-2023"
        edge_properties["biolink:supporting_study_cohort"] = "age < 18 excluded"
        # edge_properties["biolink:log_odds_ratio_95_ci"] = [float(data_row[OLD_EHRKP_EDGES_DATACOLS.LOWER_95_CI.value]), float(data_row[OLD_EHRKP_EDGES_DATACOLS.UPPER_95_CI.value])]

        """
        edge_properties["biolink:has_supporting_study_result"] = json.dumps(
            {"value": "We train many multivariable, binary logistic regression models on EHR data for each specific condition/disease/outcome. Features include labs, medications, and phenotypes. Directed edges point from risk factors to specific outcomes (diseases, phenotype, or medication exposure).",
             "attributes": [
                {"attribute_type_id": "biolink:supporting_study_method_type"},
                {"value": "STATO:0000149",
                 "description": "Binomial logistic regression for analysis of dichotomous dependent variable (in this case, for having this particular condition/disease/outcome or not)"},
                {"attribute_type_id": "biolink:update_date",
                 "value": "2022-05-18"},
                {"attribute_type_id": "biolink:p_value",
                 "value": float(data_row[EHRKP_EDGES_DATACOLS.READABLE_PVAL.value]),
                 "description": "The p-value represents the probability of observing the estimated coefficient (or more extreme value) under the assumption of the null hypothesis (which assumes that there is no relationship between the independent variable and outcome variable). A low p-value suggests that the observed relationship between the independent variable and the outcome is unlikely to occur by chance alone."},
                {"attribute_type_id": "STATO:0000209",
                 "value": float(data_row[EHRKP_EDGES_DATACOLS.AUCROC.value]),
                 "description": "The AUROC provides a way to evaluate the model's ability to discriminate between the two classes (the presenece of absence of condition/disease/outcome). Values range between 0-1; the higher the AUROC, the better the model's ability to discriminate between clasess"},
                {"attribute_type_id": "biolink:log_odds_ratio",
                 "value": float(data_row[EHRKP_EDGES_DATACOLS.FEATURE_COEFFICIENT.value]),
                 "description": "The logarithm of the odds ratio (log odds ratio), or the ratio of the odds of event Y occurring in an exposed group versus the odds of event Y occurring in a non-exposed group."},
                {"attribute_type_id": "biolink:supporting_study_date_range",
                 "value": "2022-2023"},
                {"attribute_type_id": "biolink:supporting_study_cohort",
                 "value": "age < 18 excluded"},
                # {"attribute_type_id": "biolink:total_sample_size",
                #  "value": data_row[EHRKP_EDGES_DATACOLS.total_sample_size.value],
                 # "description": "The total number of patients or participants within a sample population."},
                {"attribute_type_id": "biolink:log_odds_ratio_95_ci",
                 "value": [float(data_row[EHRKP_EDGES_DATACOLS.LOWER_95_CI.value]), float(data_row[EHRKP_EDGES_DATACOLS.UPPER_95_CI.value])]}
        ]})
        """
        return edge_properties
