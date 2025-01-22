
import os
import requests
import yaml
import enum
import orjson

from io import TextIOWrapper
from zipfile import ZipFile
from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from Common.biolink_constants import *
from Common.utils import GetData


class EDGESDATACOLS(enum.IntEnum):
    SUBJECT_ID = 0
    SUBJECT_NAME = 1
    OBJECT_ID = 2
    OBJECT_NAME = 3
    PREDICATE = 4
    CHI_SQUARED_P_VALUE = 5
    LOG_ODDS_RATIO = 6
    LOG_ODDS_RATIO_95_CI = 7
    SCORE = 8
    TOTAL_SAMPLE_SIZE = 9
    PRIMARY_KS = 10


##############
# Class: OHD source loader
#
# Desc: Class that loads/parses the Open Health Data @ Carolina data.
##############
class OHDLoader(SourceDataLoader):

    source_id: str = 'OHD-Carolina'
    provenance_id: str = 'infores:openhealthdata-carolina'
    parsing_version: str = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url = 'https://stars.renci.org/var/data_services/ohd/'
        self.version_file = 'ohd.yaml'
        self.ohd_archive_file = 'unc_omop_2018_2022_kg.zip'
        self.ohd_edges_file = 'unc_omop_2018_2022_kg.csv'
        self.data_files = [self.ohd_archive_file]

    def get_latest_source_version(self) -> str:
        version_file_url = f"{self.data_url}{self.version_file}"
        r = requests.get(version_file_url)
        if not r.ok:
            r.raise_for_status()
        version_yaml = yaml.full_load(r.text)
        build_version = str(version_yaml['build'])
        return build_version

    def get_data(self) -> bool:
        for data_file in self.data_files:
            source_data_url = f'{self.data_url}{data_file}'
            data_puller = GetData()
            data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        extractor = Extractor(file_writer=self.output_file_writer)

        ohd_archive_file_path: str = os.path.join(self.data_path, self.ohd_archive_file)
        with ZipFile(ohd_archive_file_path) as ohd_archive:
            with ohd_archive.open(self.ohd_edges_file, "r") as fp:
                extractor.csv_extract(TextIOWrapper(fp),
                                      lambda line: line[EDGESDATACOLS.SUBJECT_ID.value],  # subject id
                                      lambda line: line[EDGESDATACOLS.OBJECT_ID.value],  # object id
                                      lambda line: line[EDGESDATACOLS.PREDICATE],  # predicate extractor
                                      lambda line: {NAME: line[EDGESDATACOLS.SUBJECT_NAME.value]},  # subject props
                                      lambda line: {NAME: line[EDGESDATACOLS.OBJECT_NAME.value]},  # object props
                                      lambda line: self.get_edge_properties(line),  # edgeprops
                                      comment_character=None,
                                      delim=',',
                                      has_header_row=True)

        return extractor.load_metadata

    @staticmethod
    def get_edge_properties(line):
        return {
            AGENT_TYPE: DATA_PIPELINE,
            KNOWLEDGE_LEVEL: STATISTICAL_ASSOCIATION,
            'score': line[EDGESDATACOLS.SCORE.value],
            PRIMARY_KNOWLEDGE_SOURCE: line[EDGESDATACOLS.PRIMARY_KS.value],
            P_VALUE: float(line[EDGESDATACOLS.CHI_SQUARED_P_VALUE.value]),
            LOG_ODDS_RATIO: float(line[EDGESDATACOLS.LOG_ODDS_RATIO.value]),
            LOG_ODDS_RATIO_95_CI: orjson.loads(line[EDGESDATACOLS.LOG_ODDS_RATIO_95_CI.value]),
            TOTAL_SAMPLE_SIZE: int(line[EDGESDATACOLS.TOTAL_SAMPLE_SIZE.value])
        }

    """
    # this should probably be something like this instead to match COHD, 
    because merged edges wont be able to handle conflicting attributes across multiple supporting studies
    'attributes': [orjson.dumps({
                HAS_SUPPORTING_STUDY_RESULT: [{
                    P_VALUE: float(line[EDGESDATACOLS.CHI_SQUARED_P_VALUE.value]),
                    LOG_ODDS_RATIO: float(line[EDGESDATACOLS.LOG_ODDS_RATIO.value]),
                    LOG_ODDS_RATIO_95_CI: orjson.loads(line[EDGESDATACOLS.LOG_ODDS_RATIO_95_CI.value]),
                    TOTAL_SAMPLE_SIZE: int(line[EDGESDATACOLS.TOTAL_SAMPLE_SIZE.value])
                }]
            }).decode('utf-8')]
    """

