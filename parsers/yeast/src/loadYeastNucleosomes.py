import os
import enum

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import AGGREGATOR_KNOWLEDGE_SOURCES, ORIGINAL_KNOWLEDGE_SOURCE


# Maps Experimental Condition affects Nucleosome edge.
class EXPNUC_EDGEUMAN(enum.IntEnum):
    EXPCONDITIONS = 0
    NUCLEOSOME = 1
    PREDICATE = 2
    OCCUPANCY = 3


# Maps Nucleosomes located_on Gene edge
class NUCGENE_EDGEUMAN(enum.IntEnum):
    NUCLEOSOME = 0
    GENE = 1
    PREDICATE = 2

#mmod_id,hypothetical_nucleosome,relationship,cosine_distance
"""
class NUCGENE_EDGECOSDIST(enum.IntEnum):
    DRUG_ID = 0
    VERBAL_SCENT = 1
    PREDICATE = 2
    DISTANCE = 3
"""


##############
# Class: Nucleosome Mapping to Gene Data loader
#
# By: Jon-Michael Beasley
# Date: 03/08/2022
# Desc: Class that loads/parses the unique data for mapping nucleosomes to genes.
##############
class YeastNucleosomeLoader(SourceDataLoader):

    source_id: str = 'YeastNucleosomes'
    provenance_id: str = 'yeast_nucleosomes'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.cos_dist_threshold = 1.0
        self.yeast_data_url = 'https://stars.renci.org/var/data_services/yeast/'

        self.experimental_conditions_list_file_name = "experimental_conditions_list.csv"
        self.hypothetical_nucleosome_list_file_name = "hypothetical_nucleosome_list.csv"
        self.sgd_gene_list_file_name = "sgd_gene_robokop_id_list.csv"
        self.experimental_conditions_to_nucleosomes_edges_file_name = "experimental_conditions_mapped_to_nucleosomes.csv"
        self.nucleosome_to_gene_edges_file_name = "Yeast_Nucleosomes_Mapped_to_Genes.csv"
        

        self.data_files = [
            self.experimental_conditions_list_file_name,
            self.hypothetical_nucleosome_list_file_name,
            self.sgd_gene_list_file_name,
            self.experimental_conditions_to_nucleosomes_edges_file_name,
            self.nucleosome_to_gene_edges_file_name,
        ]

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return 'yeast_v1'

    def get_data(self) -> int:
        """
        Gets the yeast data.

        """
        data_puller = GetData()
        for source in self.data_files:
            source_url = f"{self.yeast_data_url}{source}"
            data_puller.pull_via_http(source_url, self.data_path)

        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor()

        #This file is just a list of experimental conditions.
        experimental_conditions_file: str = os.path.join(self.data_path, self.experimental_conditions_list_file_name)
        with open(experimental_conditions_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[0].replace(" ","_").strip(),  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'categories': ['experimental_condition','biolink:Treatment'],
                                                'organism': line[1],
                                                'strain': line[2],
                                                'name': line[3],
                                                'observable': line[4],
                                                ORIGINAL_KNOWLEDGE_SOURCE: "GEO",
                                                AGGREGATOR_KNOWLEDGE_SOURCES: ["GEO"]}, #subject props
                                  lambda line: {},  # object props
                                  lambda line: {},#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=False)

        #This file is a list of hypothetical nucleosomes that may be present along the yeast genome.
        hypothetical_nucleosome_list_file: str = os.path.join(self.data_path,  self.hypothetical_nucleosome_list_file_name)
        with open(hypothetical_nucleosome_list_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[0].replace(" ","_").strip(), # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'categories': ['nucleosome','biolink:GenomicEntity'],
                                                'name':line[1]},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=False)

        #This file is just a list of SGD IDs which consistitute our yeast genes.
        sgd_gene_file: str = os.path.join(self.data_path, self.sgd_gene_list_file_name)
        with open(sgd_gene_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[0].replace(" ","_").strip(),  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'categories': ['saccharomyces_cerevisiae_gene','yeast_gene','biolink:Gene'],
                                                'secondaryID': line[1],
                                                'name': line[2] if line[2] != "_?_" else line[1],
                                                'protein': line[3],
                                                'organism': line[4],
                                                'featureType': line[5],
                                                'chromosomeLocation': line[6],
                                                ORIGINAL_KNOWLEDGE_SOURCE: "SGD",
                                                AGGREGATOR_KNOWLEDGE_SOURCES: ["SGD"]}, # subject props
                                  lambda line: {},  # object props
                                  lambda line: {},#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=False)

        #Experimental conditions to nucleosomes edges. Edges contain nucleosome occupancy as a property
        experimental_conditions_to_nucleosome_edges_file: str = os.path.join(self.data_path, self.experimental_conditions_to_nucleosomes_edges_file_name)
        with open(experimental_conditions_to_nucleosome_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[EXPNUC_EDGEUMAN.EXPCONDITIONS.value], #subject id
                                  lambda line: line[EXPNUC_EDGEUMAN.NUCLEOSOME.value].replace(' ','_'),  # object id
                                  lambda line: line[EXPNUC_EDGEUMAN.PREDICATE.value],  # predicate extractor
                                  lambda line: None,  # subject props
                                  lambda line: None,  # object props
                                  lambda line: {'occupancy':float(line[EXPNUC_EDGEUMAN.OCCUPANCY.value])}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        nucleosome_to_gene_edges_file: str = os.path.join(self.data_path, self.nucleosome_to_gene_edges_file_name)
        with open(nucleosome_to_gene_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[NUCGENE_EDGEUMAN.NUCLEOSOME.value], #subject id
                                  lambda line: line[NUCGENE_EDGEUMAN.GENE.value].replace(' ','_'),  # object id
                                  lambda line: line[NUCGENE_EDGEUMAN.PREDICATE.value],  # predicate extractor
                                  lambda line: None,  # subject props
                                  lambda line: None,  # object props
                                  lambda line: {}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        
        #Goes through the file and only yields the rows in which the cosine distance is above a predefined threshold.
        """
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
        """
        
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata

