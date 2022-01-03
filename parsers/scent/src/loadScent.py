import os
import logging
import enum

from copy import deepcopy
from Common.utils import LoggingUtil, GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import AGGREGATOR_KNOWLEDGE_SOURCES, ORIGINAL_KNOWLEDGE_SOURCE

from Common.kgxmodel import kgxnode, kgxedge

#                if chemical_id not in self.previous_node_ids:
#                    chem_node = kgxnode(chemical_id, name=r['chem_label'])
#                    node_list.append(chem_node)
#                    self.previous_node_ids.add(chemical_id)
#
#                # save the gene node
#                if gene_id not in self.previous_node_ids:
#                    gene_node = kgxnode(gene_id, name=r['gene_label'], nodeprops={NCBITAXON: r['taxonID'].split(':')[1]})
#                    node_list.append(gene_node)
#                    self.previous_node_ids.add(gene_id)



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
    provenance_id: str = 'infores:cord19'

    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super(SourceDataLoader, self).__init__()

        # NOTE 1: The nodes files are not necessary, unless we decide we want the names from them.
        # Leaving them set up here just in case we do in the future..

        self.cos_dist_threshold = 0.7
        self.scent_data_url = 'https://stars.renci.org/var/data_services/scent_data/'

        self.ifa_vsd_file_name = "primary_ifa_vsd_list.txt"
        self.human_vsd_list_file_name = "sor_dataset_human_generated_vsd_list.txt"
        self.sor_vsd_human_edges_file_name = "sor_dataset_mmod_sor_dataset_vsd_edges.csv"
        self.sor_vsd_cos_dist_edges_file_name = "sor_dataset_mmod_primary_ifa_vsd_cos_dist_weighted_edges.csv"
        self.sor_list_file_name = "sor_dataset_robokop_id_list.txt"
        

#primary_ifa_vsd_list.txt  sor_dataset_human_generated_vsd_list.txt  sor_dataset_mmod_sor_dataset_vsd_edges.csv  sor_dataset_robokop_id_list.txt

        self.data_path: str = os.path.join(os.environ['DATA_SERVICES_STORAGE'], self.source_id, 'source')
        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)
        self.test_mode: bool = test_mode

        # the final output lists of nodes and edges
        self.final_node_list: list = []
        self.final_edge_list: list = []

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.cord19.Cord19Loader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return 'scent_v1_scigraph_v12'

    def get_data(self) -> int:
        """
        Gets the scent data.

        """
        sources_to_pull = [
            f'{self.covid_phenotypes_url}{self.covid_phenotypes_file_name}'#,
            # f'{self.scibite_url}{self.scibite_nodes_file_name}',
        #    f'{self.scibite_url}{self.scibite_edges_file_name}',
            # f'{self.scrigraph_url}{self.scigraph_nodes_file_name}',
        #    f'{self.scrigraph_url}{self.scigraph_edges_file_name}',
        #    f'{self.drug_bank_trials_url}{self.drug_bank_trials_file_name}'
        ]
        #data_puller = GetData()
        #for source_url in sources_to_pull:
        #    data_puller.pull_via_http(source_url, self.data_path)

        sources_to_pull = [self.ifa_vsd_file_name, 
        self.human_vsd_list_file_name,
        self.sor_vsd_human_edges_file_name,
        self.sor_vsd_cos_dist_edges_file_name,
        self.sor_list_file_name]

        data_puller = GetData()
        for source in sources_to_pull:
            source_url = f"{self.scent_data_url}{source}"
            print(source_url)
            data_puller.pull_via_http(source_url, self.data_path)
#        for source in sources_to_copy:
#            os.popen(f"cp /home/dkorn/SCENT_KOP/SCENT_DATA/{source} {self.data_path}")
        
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
                                                ORIGINAL_KNOWLEDGE_SOURCE: "ifa",
                                                AGGREGATOR_KNOWLEDGE_SOURCES: ["ifa"],
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
               if(float(line.split(',')[SOREDGECOSDIST.DISTANCE.value])>self.cos_dist_threshold): yield line

                 
        print(len(extractor.edges)) 
        sor_vsd_cos_dist_edges_file: str = os.path.join(self.data_path, self.sor_vsd_cos_dist_edges_file_name)
        with open(sor_vsd_cos_dist_edges_file, 'r') as fp:
            extractor.csv_extract(cos_dist_filter(fp),
                                  lambda line: line[SOREDGECOSDIST.DRUG_ID.value], #subject id
                                  lambda line: "SCENT:" + line[SOREDGECOSDIST.VERBAL_SCENT.value].replace(' ','_'),  # object id
                                  lambda line: line[SOREDGECOSDIST.PREDICATE.value],  # predicate extractor
                                  lambda line: {'categories': ['odorant','biolink:ChemicalEntity']},  # subject props
                                  lambda line: {'categories': ['verbal_scent_descriptor'],"name":line[SOREDGECOSDIST.VERBAL_SCENT.value]},  # object props
                                  lambda line: {'cosine_distance':line[SOREDGECOSDIST.DISTANCE.value]}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        print(len(extractor.edges)) 

        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges

        print("Nodes")
        
        #import time
        #for n in self.final_node_list[::1]:
        #    print(n.categories)
        #    print(n.name)
        #    print(n.identifier)
        #    time.sleep(0.1)
        print("Edges")
        #print(self.final_edge_list)
        return extractor.load_metadata

