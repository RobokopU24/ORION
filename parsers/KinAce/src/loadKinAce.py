import os
import enum
import requests

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.biolink_constants import *


##############
# Class: Loading kinase-substrate phosphorylation reactions from KinAce
# By: Jon-Michael Beasley
# Date: 03/7/2024
##############

class DATACOLS(enum.IntEnum):
    kinase = 0
    substrate = 2
    p_site = 4
    primary_source = 5
    PUBLICATIONS = 7


class KinAceLoader(SourceDataLoader):

    source_id: str = 'KinAce'
    provenance_id: str = 'infores:kinace'
    parsing_version = '1.2'

    KINACE_INFORES_MAPPING = {
        'PhosphoSitePlus': 'infores:psite-plus',
        'EPSD': 'infores:epsd',
        'iPTMNet': 'infores:iptmnet'
    }

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        # self.kinace_version = "2023-10-30"
        # KinAce downloaded this data on the 30th of October 2023. However, they have made changes to the files since
        # I suggest using the last commit date of the file to version this data set
        self.kinace_data_url = f"https://github.com/GauravPandeyLab/KiNet/raw/master/data/ksi_source_full_dataset.csv"
        # Let's use the full source for completeness rather than the pruned list
        self.interactions_file_name = f"ksi_source_full_dataset.csv"
        self.data_files = [self.interactions_file_name]

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        url = (f"https://api.github.com/repos/GauravPandeyLab/KiNet/commits?"
               f"path=./data/{self.interactions_file_name}&per_page=1")
        response = requests.get(url)
        commits = response.json()
        last_commit_date = commits[0]['commit']['committer']['date']
        date_version = last_commit_date[:10]
        return f"{date_version}"

    def get_data(self) -> int:
        """
        Gets the KinAce data.

        """
        data_puller = GetData()
        source_url = f"{self.kinace_data_url}"
        data_puller.pull_via_http(source_url, self.data_path)

        return True

    def get_pmids(self, line):
        publication_list = []

        if line[DATACOLS.PUBLICATIONS.value] in ['', 'NA']:
            return publication_list

        ids = line[DATACOLS.PUBLICATIONS.value].split(';')
        publication_list = ['PMID:' + i.strip() for i in ids if i.strip()]

        return publication_list

    def get_KL_AT_assignments(self, line):
        knowledge_level = NOT_PROVIDED
        agent_type = NOT_PROVIDED
        if line[DATACOLS.primary_source.value] == 'PhosphoSitePlus':
            knowledge_level = KNOWLEDGE_ASSERTION
            agent_type = MANUAL_AGENT
        elif line[DATACOLS.primary_source.value] == 'EPSD':
            knowledge_level = NOT_PROVIDED
            agent_type = NOT_PROVIDED
        elif line[DATACOLS.primary_source.value] == 'iPTMNet':
            knowledge_level = NOT_PROVIDED
            agent_type = TEXT_MINING_AGENT
        return [knowledge_level, agent_type]

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges
        We are going to group by kinase-substrate pair and aggregate all phosphorylation sites and primary/secondary sources.

        :return: ret_val: load_metadata
        """

        extractor = Extractor(file_writer=self.output_file_writer)

        with open(os.path.join(self.data_path, self.interactions_file_name)) as csvfile:
            extractor.csv_extract(csvfile,
                                  subject_extractor=lambda line: f"UniProtKB:{line[DATACOLS.kinase.value]}",
                                  object_extractor=lambda line: f"UniProtKB:{line[DATACOLS.substrate.value]}",
                                  predicate_extractor=lambda line: "biolink:affects",  # predicate
                                  edge_property_extractor=lambda line:
                                  {QUALIFIED_PREDICATE: 'biolink:causes',
                                   OBJECT_DIRECTION_QUALIFIER: 'increased',
                                   OBJECT_ASPECT_QUALIFIER: 'phosphorylation',
                                   'phosphorylation_sites': [line[DATACOLS.p_site.value]],
                                   KNOWLEDGE_LEVEL: self.get_KL_AT_assignments(line)[0],
                                   AGENT_TYPE:  self.get_KL_AT_assignments(line)[1],
                                   PRIMARY_KNOWLEDGE_SOURCE:
                                       self.KINACE_INFORES_MAPPING.get(line[DATACOLS.primary_source.value], None),
                                   AGGREGATOR_KNOWLEDGE_SOURCES: [self.provenance_id],
                                   PUBLICATIONS: self.get_pmids(line)},
                                  has_header_row=True,
                                  delim=','
                                  )
        return extractor.load_metadata
