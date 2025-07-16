import os
import requests
import yaml
import csv
import json

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader


class EHRMayTreatLoader(SourceDataLoader):
    source_id: str = "EHRMayTreatKP"
    provenance_id: str = "infores:isb-EHRMLA-data"
    parsing_version = "1.0"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url: str = 'https://raw.githubusercontent.com/Hadlock-Lab/EHRMLA_KPs/main/May%20Treat/csvs/'
        self.edge_file_name: str = 'May_Treat_KP_edges_latest_deploy.csv'
        self.data_file = self.edge_file_name
        self.version_file = 'ehr-may-treat-kp.yaml'

        self.edge_file_property_ignore_list = ['subject_name', 'object_name', 'KG_type', 'category', '']
        self.edge_file_json_properties = ['log_odds_ratio_95_ci']
        self.edge_file_float_properties = ['auc_roc', 'log_odds_ratio', 'log_odds_ratio_95_ci_lower',
                                           'log_odds_ratio_95_ci_upper', 'adjusted_p_value', ]
        self.edge_file_int_properties = ['positive_patient_count', 'negative_patient_count', 'total_sample_size']

    def get_latest_source_version(self) -> str:
        version_file_url = f"{self.data_url}{self.version_file}"
        r = requests.get(version_file_url)
        version_yaml = yaml.full_load(r.text)
        build_version = str(version_yaml['build'])
        return build_version

    def get_data(self) -> int:
        data_puller = GetData()
        source_url = f"{self.data_url}{self.data_file}"
        data_puller.pull_via_http(source_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX files.

        :return: ret_val: record counts
        """
        record_counter = 0
        skipped_record_counter = 0

        edge_file_path: str = os.path.join(self.data_path, self.edge_file_name)
        with open(edge_file_path, 'r', newline='') as edge_file:

            csv_reader = csv.DictReader(edge_file, quotechar='"')
            for edge in csv_reader:
                try:
                    for prop in self.edge_file_property_ignore_list:
                        edge.pop(prop, None)
                    for edge_prop in edge:
                        if edge_prop in self.edge_file_json_properties:
                            edge[edge_prop] = json.loads(edge[edge_prop])
                        elif edge_prop in self.edge_file_float_properties:
                            edge[edge_prop] = float(edge[edge_prop])
                        elif edge_prop in self.edge_file_int_properties:
                            edge[edge_prop] = int(edge[edge_prop])
                    self.output_file_writer.write_node(edge['subject'])
                    self.output_file_writer.write_node(edge['object'])
                    self.output_file_writer.write_normalized_edge(edge)
                    record_counter += 1
                except (ValueError, KeyError) as e:
                    self.logger.error(str(e))
                    skipped_record_counter += 1

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter}
        return load_metadata


class EHRClinicalConnectionsLoader(EHRMayTreatLoader):
    source_id: str = "EHRClinicalConnectionsKP"
    provenance_id: str = "infores:isb-EHRMLA-clinicalconnections"
    description = "Multiomics EHRMLA Clinical Connections KP."
    source_data_url = "https://github.com/NCATSTranslator/Translator-All/wiki/Multiomics-EHRMLA-May-Treat-KP"
    license = "https://github.com/NCATSTranslator/Translator-All/wiki/Multiomics-EHRMLA-May-Treat-KP"
    attribution = "https://github.com/NCATSTranslator/Translator-All/wiki/Multiomics-EHRMLA-May-Treat-KP"
    parsing_version = "1.0"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url: str = 'https://raw.githubusercontent.com/Hadlock-Lab/EHRMLA_KPs/main/Clinical%20Connections/csvs/'
        self.edge_file_name: str = 'ClinicalConnections_KP_edges_latest_deploy.csv'
        self.data_file = self.edge_file_name
        self.version_file = 'ehr-clinical-connections-kp.yaml'
