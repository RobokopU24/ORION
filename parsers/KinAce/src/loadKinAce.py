import os
import enum
import csv
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

class BD_EDGEUMAN(enum.IntEnum):
    KINASE = 1
    SUBSTRATE = 3
    P_SITE = 5
    PRIMARY_SOURCE = 6
    SECONDARY_SOURCE = 8

class KinAceLoader(SourceDataLoader):

    source_id: str = 'KinAce'
    provenance_id: str = 'infores:kinace'
    description = ("The KinAce web portal aggregates and visualizes the network of interactions between "
                   "protein-kinases and their substrates in the human genome.")
    source_data_url = "https://kinace.kinametrix.com/session/ff792906de38db0d1c9900ac5882497b/download/download0?w="
    license = "Creative Commons Attribution 4.0 International"
    attribution = 'https://kinace.kinametrix.com/#section-about'
    parsing_version = '1.1'

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
        self.data_path = "."
        self.kinace_version = self.get_latest_source_version()
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

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges
        We are going to group by kinase-substrate pair and aggregate all phosphorylation sites and primary/secondary sources.

        :return: ret_val: load_metadata
        """
        print('OK, parsing')

        extractor = Extractor(file_writer=self.output_file_writer)
        extractor = Extractor()

        with open(os.path.join(self.data_path, self.interactions_file_name), "r", newline="\n") as csvfile:
            reader = csv.reader(csvfile, delimiter=",")

            # Reading the header
            header = next(reader)

            # Reading each line in the CSVdata
            for row in reader:
                print(row)

                # Skip rows where all elements are empty strings
                if all(element == '' for element in row):
                    continue  # Skip this row

                extractor.parse_row(row,
                                    lambda line: f"UniProtKB:{line[0]}",
                                    lambda line: f"UniProtKB:{line[2]}",
                                    lambda line: "biolink:affects",  # predicate
                                    lambda line: {},  # Node 1 props
                                    lambda line: {},  # Node 2 props
                                    lambda line: {'qualified_predicate': 'biolink:causes',
                                                  'object_direction_qualifier': 'increased',
                                                  'object_aspect_qualifier': 'phosphorylation',
                                                  'phosphorylation_sites': line[4],
                                                  'primary_sources': line[5],
                                                  'secondary_sources': line[7],
                                                  'knowledge_level': KNOWLEDGE_ASSERTION if line[7] == 'PhosphoSitePlus' else
                                                                     NOT_PROVIDED if line[7] in ['EPSD', 'iPTMNet'] else
                                                                     NOT_PROVIDED,
                                                  'agent_type': MANUAL_AGENT if line[7] == 'PhosphoSitePlus' else
                                                                NOT_PROVIDED if line[7] == 'EPSD' else
                                                                TEXT_MINING_AGENT if line[7] == 'iPTMNet' else
                                                                NOT_PROVIDED,
                                                  'primary_knowledge_source': f"{line[7]}",
                                                  'aggregator_knowledge_sources': f"infores:kinace"
                                                  }  # Edge props
                                    )
        return extractor.load_metadata
