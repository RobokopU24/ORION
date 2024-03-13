import os
import enum
from zipfile import ZipFile as zipfile
import pandas as pd

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import PUBLICATIONS

# Full Kinase-Substrate Phosphorylation Data.

#make this reflect the column that the data is found in
class BD_EDGEUMAN(enum.IntEnum):
    KINASE = 1
    SUBSTRATE = 2
    P_SITE = 3
    PRIMARY_SOURCE = 4
    SECONDARY_SOURCE = 5

##############
# Class: Loading kinase-substrate phosphorylation reactions from KinAce
# By: Jon-Michael Beasley
# Date: 03/7/2024
##############
class KinAceLoader(SourceDataLoader):

    source_id: str = 'KinAce'
    provenance_id: str = 'infores:kinace'
    description = "The KinAce web portal aggregates and visualizes the network of interactions between protein-kinases and their substrates in the human genome."
    source_data_url = "https://kinace.kinametrix.com/session/ff792906de38db0d1c9900ac5882497b/download/download0?w="
    license = "All data and download files in bindingDB are freely available under a 'Creative Commons BY 3.0' license.'"
    attribution = 'https://kinace.kinametrix.com/#section-about'
    parsing_version = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.kinace_version = "2023-10-30"
        #self.kinace_version = self.get_latest_source_version()
        self.kinace_data_url = f"https://raw.githubusercontent.com/GauravPandeyLab/KinAce/master/data/{self.kinace_version}-kinace-dataset.zip"

        self.archive_file_name = f"{self.kinace_version}-kinace-dataset.zip"
        self.interactions_file_name = f"ksi_source.csv"
        self.data_files = [self.interactions_file_name]

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        if self.kinace_version:
            return self.kinace_version
        
        return f"{self.kinace_version}"

    def get_data(self) -> int:
        """
        Gets the KinAce data.

        """
        data_puller = GetData()
        source_url = f"{self.kinace_data_url}"
        data_puller.pull_via_http(source_url, self.data_path)
        with zipfile(os.path.join(self.data_path, self.archive_file_name), 'r') as zip_ref:
            zip_ref.extract(self.interactions_file_name, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges
        We are going to group by kinase-substrate pair and aggregate all phosphorylation sites and primary/secondary sources.

        :return: ret_val: load_metadata
        """
        print('ok parsing')
        # with zipfile(os.path.join(self.data_path, self.archive_file_name), 'r') as zip_ref:
        #     zip_ref.extract(self.interactions_file_name, self.data_path)
        data = pd.read_csv(os.path.join(self.data_path, self.interactions_file_name))
        data = data.groupby(["Kinase", "Substrate"]).agg({"Site": list, "PrimarySource": list, "SecondarySource": list}).reset_index()
        # Define a function to deduplicate lists
        def deduplicate_list(lst):
            lst = [x for x in lst if x == x]
            return list(set(lst))
        # Apply deduplication function to each aggregated list
        data['Site'] = data.apply(lambda row: list(set([x for x in row['Site'] if x==x])), axis=1)
        data['PrimarySource'] = data.apply(lambda row: list(set([x for x in row['PrimarySource'] if x==x])), axis=1)
        data['SecondarySource'] = data.apply(lambda row: list(set([x for x in row['SecondarySource'] if x==x])), axis=1)
        data.to_csv(os.path.join(self.data_path, self.interactions_file_name))
        extractor = Extractor(file_writer=self.output_file_writer)
        with open(os.path.join(self.data_path, self.interactions_file_name), 'rt') as fp:
            extractor.csv_extract(fp,
                                lambda line: f"UniProtKB:{line[1]}",  # subject id
                                lambda line: f"UniProtKB:{line[2]}",  # object id
                                lambda line: "biolink:phosphorylates",  # predicate
                                lambda line: {}, #Node 1 props
                                lambda line: {}, #Node 2 props
                                lambda line: {
                                                'phosphorylation_sites':line[3],
                                                'primary_sources':line[4],
                                                'secondary_sources':line[5]
                                            }, #Edge props
                                comment_character=None,
                                delim=",",
                                has_header_row=True
                            )
        return extractor.load_metadata