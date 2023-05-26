import os
import logging
import enum

from copy import deepcopy

import numpy
from parsers.yeast.src.collectCostanza2016Data import main
from Common.utils import LoggingUtil, GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import AGGREGATOR_KNOWLEDGE_SOURCES, PRIMARY_KNOWLEDGE_SOURCE

from Common.kgxmodel import kgxnode, kgxedge

# Costanza 2016 Yeast Genetic Interactions
class COSTANZA_GENEINTERACTIONS(enum.IntEnum):
    GENE1 = 0
    GENE2 = 21
    EVIDENCEPMID = 8
    PREDICATE = 14
    PVALUE = 17
    SGASCORE = 18
    GENE1ALLELE = 19
    GENE2ALLELE = 20

##############
# Class: Mapping Costanza 2016 Genetic Interaction Data to Phenotypes
#
# By: Jon-Michael Beasley
# Date: 05/08/2023
##############
class Costanza2016Loader(SourceDataLoader):

    source_id: str = 'Costanza2016'
    provenance_id: str = 'infores:CostanzaGeneticInteractions'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        #self.yeast_data_url = 'https://stars.renci.org/var/data_services/yeast/'

        self.costanza_genetic_interactions_file_name = "Costanza2016GeneticInteractions.csv"
        
        self.data_files = [
            self.costanza_genetic_interactions_file_name
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
        main(self.data_path)

        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor()

        # Costanza Genetic Interactions Parser. Add edges between "fitness" and the yeast genotype.
        costanza_genetic_interactions: str = os.path.join(self.data_path, self.costanza_genetic_interactions_file_name)
        with open(costanza_genetic_interactions, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE1.value]+"-"+line[COSTANZA_GENEINTERACTIONS.GENE2.value].replace("SGD:",""),  # subject id
                                  lambda line: "APO:0000216",  # object id # In this case, APO:0000216 is "fitness"
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.PREDICATE.value],  # predicate extractor
                                  lambda line: {
                                                    'name': line[COSTANZA_GENEINTERACTIONS.GENE1ALLELE.value]+"-"+line[COSTANZA_GENEINTERACTIONS.GENE2ALLELE.value],
                                                    'categories': ['biolink:Genotype'],
                                                    'gene1_allele': line[COSTANZA_GENEINTERACTIONS.GENE1ALLELE.value],
                                                    'gene2_allele': line[COSTANZA_GENEINTERACTIONS.GENE2ALLELE.value]
                                                }, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {
                                                    'p-value': line[COSTANZA_GENEINTERACTIONS.PVALUE.value],
                                                    'sgaScore': line[COSTANZA_GENEINTERACTIONS.SGASCORE.value],
                                                    'evidencePMIDs': line[COSTANZA_GENEINTERACTIONS.EVIDENCEPMID.value],
                                                    PRIMARY_KNOWLEDGE_SOURCE: "CostanzaGeneticInteractions"
                                                }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        # Costanza Genetic Interactions Parser. Genotype to Gene 1 edge.
        costanza_genetic_interactions: str = os.path.join(self.data_path, self.costanza_genetic_interactions_file_name)
        with open(costanza_genetic_interactions, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE1.value]+"-"+line[COSTANZA_GENEINTERACTIONS.GENE2.value].replace("SGD:",""),  # subject id
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE1.value],  # object id
                                  lambda line: "biolink:has_part",  # predicate extractor
                                  lambda line: {}, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {
                                                    'gene1_allele': line[COSTANZA_GENEINTERACTIONS.GENE1ALLELE.value],
                                                    'evidencePMIDs': line[COSTANZA_GENEINTERACTIONS.EVIDENCEPMID.value],
                                                    PRIMARY_KNOWLEDGE_SOURCE: "CostanzaGeneticInteractions"

                                  }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
            
        # Costanza Genetic Interactions Parser. Genotype to Gene 2 edge.
        costanza_genetic_interactions: str = os.path.join(self.data_path, self.costanza_genetic_interactions_file_name)
        with open(costanza_genetic_interactions, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE1.value]+"-"+line[COSTANZA_GENEINTERACTIONS.GENE2.value].replace("SGD:",""),  # subject id
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE2.value],  # object id
                                  lambda line: "biolink:has_part",  # predicate extractor
                                  lambda line: {}, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {
                                                    'gene1_allele': line[COSTANZA_GENEINTERACTIONS.GENE2ALLELE.value],
                                                    'evidencePMIDs': line[COSTANZA_GENEINTERACTIONS.EVIDENCEPMID.value],
                                                    PRIMARY_KNOWLEDGE_SOURCE: "CostanzaGeneticInteractions"

                                  }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)        
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata