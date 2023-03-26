import gzip
import os
import enum
import gzip
import pandas as pd
import numpy as np

from parsers.yeast.src.collectSGDdata import createLociWindows
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import AGGREGATOR_KNOWLEDGE_SOURCES, PRIMARY_KNOWLEDGE_SOURCE


# Maps Experimental Condition affects Nucleosome edge.
class EXPGENE_EDGEUMAN(enum.IntEnum):
    YORF = 0
    NAME = 1
    GWEIGHT = 2
    MINUTE5 = 3
    MINUTE10 = 4
    MINUTE20 = 5
    MINUTE30 = 6
    MINUTE40 = 7
    MINUTE50 = 8
    MINUTE60 = 9
    MINUTE90 = 10
    MEANEXP = 11
    PRIMARYID = 12


# Maps Nucleosomes located_on Gene edge
class NUCGENE_EDGEUMAN(enum.IntEnum):
    NUCLEOSOME = 1
    GENE = 6

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
# Date: 07/31/2022
# Desc: Class that loads/parses the unique data for handling GSE61888 nucleosomes.
##############
class YeastGaschDiamideLoader(SourceDataLoader):

    source_id: str = 'YeastGaschDiamideGeneExpression'
    provenance_id: str = 'infores:YeastGasch'


    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.yeast_data_url = 'https://stars.renci.org/var/data_services/yeast/'

        self.genes_file = f"{self.data_path}/../../../YeastSGDInfo/yeast_v1/source/SGDAllGenes.csv"
        self.GASCH_diamide_gene_expression = "Gasch_Diamide_Gene_Expression.csv"
        
        self.data_files = [
            self.GASCH_diamide_gene_expression
        ]

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return 'yeast_v1_5'

    def int_to_Roman(self, num):
        val = [
            1000, 900, 500, 400,
            100, 90, 50, 40,
            10, 9, 5, 4,
            1
            ]
        syb = [
            "M", "CM", "D", "CD",
            "C", "XC", "L", "XL",
            "X", "IX", "V", "IV",
            "I"
            ]
        roman_num = ''
        i = 0
        while  num > 0:
            for _ in range(num // val[i]):
                roman_num += syb[i]
                num -= val[i]
            i += 1
        return roman_num

    def get_data(self) -> int:
        """
        Gets the GEO datasets.
        """
        all_genes = pd.read_csv(self.genes_file)
        
        data_puller = GetData()
        for source in self.data_files:
            source_url = f"{self.yeast_data_url}{source}"
            data_puller.pull_via_http(source_url, self.data_path)
            dataset = pd.read_csv(source_url)
            means = []
            for row,idx in dataset.iterrows():
                means = means + [dataset.iloc[row,3:11].to_numpy().mean()]
            dataset['Mean Expression'] = means
            mergedf = dataset.merge(all_genes, how='inner',left_on='YORF',right_on='secondaryIdentifier')
            print(f"Diamide Gene Expression Mapping Complete!")
            csv_f3name = "GaschDiamideGeneExpression2Genes.csv"
            mergedf.to_csv(os.path.join(self.data_path,csv_f3name), encoding="utf-8-sig", index=False)
            print(os.path.join(self.data_path,csv_f3name))
    
        return True
    
    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor()

        #Experimental conditions to Gene Expression edges. Edges contain expression fold change as propoerties.
        #In this case, handles data from "Genomic expression programs in the response of yeast cells to environmental changes" (Gasch, 2010)
        gene_expresion_file: str = os.path.join(self.data_path, "GaschDiamideGeneExpression2Genes.csv")
        with open(gene_expresion_file, 'r') as fp:

            extractor.csv_extract(fp,
                                  lambda line: "PUBCHEM.COMPOUND:5353800", #subject id #In this case, it is set as "Diamide", the stressor used in GSE61888
                                  lambda line: line[EXPGENE_EDGEUMAN.PRIMARYID.value],  # object id (In this case, the primary gene ID, SGD:xxxxx)
                                  lambda line: "biolink:increases_expression_of" if float(line[EXPGENE_EDGEUMAN.MEANEXP.value]) > 0 else "biolink:decreases_expression_of", # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {
                                            'dataset':'Gasch, 2010',
                                            'dataComment':"Genomic expression programs in the response of yeast cells to environmental changes (Diamide exposure). Measurements taken at 5, 10, 20, 30, 40, 50, 60, and 90 minutes with values reported as log2FC.",
                                            'MeanExpression': float(line[EXPGENE_EDGEUMAN.MEANEXP.value]),
                                            'ExpressionTimeSeries':[float(line[EXPGENE_EDGEUMAN.MINUTE5.value]),float(line[EXPGENE_EDGEUMAN.MINUTE10.value]),
                                                                    float(line[EXPGENE_EDGEUMAN.MINUTE20.value]),float(line[EXPGENE_EDGEUMAN.MINUTE30.value]),
                                                                    float(line[EXPGENE_EDGEUMAN.MINUTE40.value]),float(line[EXPGENE_EDGEUMAN.MINUTE50.value]),
                                                                    float(line[EXPGENE_EDGEUMAN.MINUTE60.value]),float(line[EXPGENE_EDGEUMAN.MINUTE90.value])], # This is what we need to fill in from dataset.
                                            PRIMARY_KNOWLEDGE_SOURCE: "GaschGeneExpression"
                                  }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
       
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata

