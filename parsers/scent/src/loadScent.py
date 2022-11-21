import os
import enum

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import PRIMARY_KNOWLEDGE_SOURCE, AGGREGATOR_KNOWLEDGE_SOURCES


# the scent odorant edge header columns:
class SOREDGEUMAN(enum.IntEnum):
    DRUG_ID = 0
    VERBAL_SCENT = 1
    PREDICATE = 2

#mmod_id,primary_ifa_vsd,relationship,cosine_distance
class SOREDGECOSDIST(enum.IntEnum):
    DRUG_ID = 0
    VERBAL_SCENT = 1
    PREDICATE = 2
    DISTANCE = 3

##############
# Class: Scent Data loader
#
# By: Daniel Korn
# Date: 12/1/2021
# Desc: Class that loads/parses the unique data for scentkop.
##############
class ScentLoader(SourceDataLoader):

    source_id: str = 'Scent'
    provenance_id: str = 'infores:Scent'
    parsing_version: str = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.cos_dist_threshold = 1.0
        self.scent_data_url = 'https://stars.renci.org/var/data_services/scent_data/'

        self.ifa_vsd_file_name = "primary_ifa_vsd_list.txt"
        self.human_vsd_list_file_name = "sor_dataset_human_generated_vsd_list.txt"
        self.sor_vsd_human_edges_file_name = "sor_dataset_mmod_sor_dataset_vsd_edges.csv"
        self.sor_vsd_cos_dist_edges_file_name = "sor_dataset_mmod_primary_ifa_vsd_cos_dist_weighted_edges.csv"
        self.sor_list_file_name = "sor_dataset_robokop_id_list.txt"

        self.data_files = [
            self.ifa_vsd_file_name,
            self.human_vsd_list_file_name,
            self.sor_vsd_human_edges_file_name,
            self.sor_vsd_cos_dist_edges_file_name,
            self.sor_list_file_name
        ]

        #primary_ifa_vsd_list.txt  sor_dataset_human_generated_vsd_list.txt  sor_dataset_mmod_sor_dataset_vsd_edges.csv  sor_dataset_robokop_id_list.txt

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return 'scent_v1'

    def get_data(self) -> int:
        """
        Gets the scent data.

        """
        data_puller = GetData()
        for source in self.data_files:
            source_url = f"{self.scent_data_url}{source}"
            data_puller.pull_via_http(source_url, self.data_path)

        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor()

        #This file is just a list of terms which consistitue our VSDs
        ifa_vsd_file: str = os.path.join(self.data_path, self.ifa_vsd_file_name)
        with open(ifa_vsd_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[0].replace(" ","_").strip(),  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'categories': ['verbal_scent_descriptor','ifa_vsd'],
                                                'notes': "", 
                                                PRIMARY_KNOWLEDGE_SOURCE: "ifa",
                                                "name":line[0].strip()},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {},#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=False)

        human_vsd_list_file: str = os.path.join(self.data_path, self.human_vsd_list_file_name)
        with open(human_vsd_list_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[0].replace(" ","_").strip(),  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'categories': ['verbal_scent_descriptor'],"name":line[0].strip()},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #This file is a list of pubchem ids for monomolecular odorant molecules
        sor_list_file: str = os.path.join(self.data_path,  self.sor_list_file_name)
        with open(sor_list_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[0], # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'categories': ['odorant','biolink:ChemicalEntity']},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=False)

        sor_vsd_human_edges_file: str = os.path.join(self.data_path, self.sor_vsd_human_edges_file_name)
        with open(sor_vsd_human_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[SOREDGEUMAN.DRUG_ID.value], #subject id
                                  lambda line: "SCENT:" + line[SOREDGEUMAN.VERBAL_SCENT.value].replace(' ','_'),  # object id
                                  lambda line: line[SOREDGEUMAN.PREDICATE.value],  # predicate extractor
                                  lambda line: {'categories': ['odorant','biolink:ChemicalEntity']},  # subject props
                                  lambda line: {'categories': ['verbal_scent_descriptor'],"name":line[SOREDGEUMAN.VERBAL_SCENT.value]},  # object props
                                  lambda line: {}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        
        #Goes through the file and only yields the rows in which the cosine distance is above a predefined threshold.
        def cos_dist_filter(infile):
            #Header
            yield next(infile)
            for line in infile:
               if(float(line.split(',')[SOREDGECOSDIST.DISTANCE.value])<=self.cos_dist_threshold): yield line

                 
        sor_vsd_cos_dist_edges_file: str = os.path.join(self.data_path, self.sor_vsd_cos_dist_edges_file_name)
        with open(sor_vsd_cos_dist_edges_file, 'r') as fp:
            extractor.csv_extract(cos_dist_filter(fp),
                                  lambda line: line[SOREDGECOSDIST.DRUG_ID.value], #subject id
                                  lambda line: "SCENT:" + line[SOREDGECOSDIST.VERBAL_SCENT.value].replace(' ','_'),  # object id
                                  lambda line: line[SOREDGECOSDIST.PREDICATE.value],  # predicate extractor
                                  lambda line: {'categories': ['odorant','biolink:ChemicalEntity']},  # subject props
                                  lambda line: {'categories': ['verbal_scent_descriptor'],"name":line[SOREDGECOSDIST.VERBAL_SCENT.value]},  # object props
                                  lambda line: {'cosine_distance':float(line[SOREDGECOSDIST.DISTANCE.value])}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata

