import os
import logging
import enum
import gzip
import pandas as pd
import requests as rq
from bs4 import BeautifulSoup

from copy import deepcopy

import numpy
from parsers.yeast.src.collectSGDdata import main
from Common.utils import LoggingUtil, GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import AGGREGATOR_KNOWLEDGE_SOURCES, PRIMARY_KNOWLEDGE_SOURCE

from Common.kgxmodel import kgxnode, kgxedge

# Full PPI Data.
class PPI_EDGEUMAN(enum.IntEnum):
    PROTEIN1 = 0
    PROTEIN2 = 1
    NEIGHBORHOOD = 2
    NEIGHBORHOOD_TRANSFERRED = 3
    FUSION = 4
    COOCCURANCE = 5
    HOMOLOGY = 6
    COEXPRESSION = 7
    COEXPRESSION_TRANSFERRED = 8
    EXPERIMENTS = 9
    EXPERIMENTS_TRANSFERRED	= 10
    DATABASE = 11
    DATABASE_TRANSFERRED = 12
    TEXTMINING = 13
    TEXTMINING_TRANSFERRED = 14
    COMBINED_SCORE = 15

# Physical Subnetwork PPI Data.
class PPI_PHYSICAL_EDGEUMAN(enum.IntEnum):
    PROTEIN1 = 0
    PROTEIN2 = 1
    HOMOLOGY = 2
    EXPERIMENTS = 3
    EXPERIMENTS_TRANSFERRED	= 4
    DATABASE = 5
    DATABASE_TRANSFERRED = 6
    TEXTMINING = 7
    TEXTMINING_TRANSFERRED = 8
    COMBINED_SCORE = 9

#     PREDICATE = 9
#     EVIDENCECODE = 8
#     EVIDENCECODETEXT = 10
#     ANNOTATIONTYPE = 12
#     EVIDENCEPMID = 13

# # Maps Genes to Pathways
# class GENEPATHWAYS_EDGEUMAN(enum.IntEnum):
#     GENE = 0
#     PATHWAY = 2

# # Maps Genes to Phenotypes
# class GENEPHENOTYPES_EDGEUMAN(enum.IntEnum):
#     GENE = 0
#     PHENOTYPE = 18
#     QUALIFIER = 8
#     EXPERIMENTTYPE = 5
#     MUTANTTYPE = 6
#     ALLELE = 9
#     ALLELEDESCRIPTION = 10
#     STRAINBACKGROUND = 11
#     CHEMICAL = 12
#     CONDITION = 13
#     DETAILS = 14
#     EVIDENCEPMID = 15

# # Maps Genes to Complexes
# class GENECOMPLEXES_EDGEUMAN(enum.IntEnum):
#     GENE = 11
#     COMPLEX = 10
#     ROLE = 5
#     STOICHIOMETRY = 6
#     TYPE = 7


# class COMPLEXEGO_EDGEUMAN(enum.IntEnum):
#     GOTERM = 1
#     COMPLEX = 0
#     PREDICATE = 3
#     COMPLEXNAME = 2
     
##############
# Class: Mapping Protein-Protein Interactions from STRING-DB
#
# By: Jon-Michael Beasley
# Date: 09/09/2022
# Desc: Class that loads/parses human protein-protein interaction data.
##############
class STRINGDBLoader(SourceDataLoader):

    source_id: str = 'STRING-DB'
    provenance_id: str = 'infores:STRING'
    taxon_id: str = '9606' #Human taxon

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.taxon_id = '9606'
        self.cos_dist_threshold = 1.0
        self.version_index = rq.get('https://string-db.org/').text.index('string_database_version_dotted:')+33
        self.string_db_version = rq.get('https://string-db.org/').text[self.version_index:self.version_index+4]
        
        self.stringdb_data_url = [f"https://stringdb-static.org/download/protein.links.full.v{self.string_db_version}/",f"https://stringdb-static.org/download/protein.physical.links.full.v{self.string_db_version}/"]

        self.ppi_full_file_name = self.taxon_id+f".protein.links.full.v{self.string_db_version}.txt.gz"
        self.ppi_physical_subnetwork_file_name = self.taxon_id+f".protein.physical.links.full.v{self.string_db_version}.txt.gz"
        #self.pathway_list_file_name = "yeast_pathways_list.csv"
        #self.phenotype_list_file_name = "yeast_phenotype_list.csv"

        # self.genes_to_go_term_edges_file_name = "SGDGene2GOTerm.csv"
        # self.genes_to_pathway_edges_file_name = "SGDGene2Pathway.csv"
        # self.genes_to_phenotype_edges_file_name = "SGDGene2Phenotype.csv"
        # self.genes_to_complex_edges_file_name = "SGDGene2Complex.csv"
        # self.complex_to_go_term_edges_file_name = "SGDComplex2GOTerm.csv"
        
        self.data_files = [self.ppi_full_file_name, self.ppi_physical_subnetwork_file_name]
            #self.go_term_list_file_name,
            #self.pathway_list_file_name,
            #self.phenotype_list_file_name,
            # self.genes_to_go_term_edges_file_name,
            # self.genes_to_pathway_edges_file_name,
            # self.genes_to_phenotype_edges_file_name,
            # self.genes_to_complex_edges_file_name,
            # self.complex_to_go_term_edges_file_name]

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return f"STRING_v{self.string_db_version}"

    def get_data(self) -> int:
        """
        Gets the yeast data.

        """
        data_puller = GetData()
        i=0
        for source in self.data_files:
            source_url = f"{self.stringdb_data_url[i]}{source}"
            data_puller.pull_via_http(source_url, self.data_path)
            i+=1

        return True
    
    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor()
        
        #This file contains full STRING PPI data for the Human proteome.
        ppi_full_file: str = os.path.join(self.data_path, self.ppi_full_file_name)
        with gzip.open(ppi_full_file, 'r') as fp:
            textdatafile = pd.read_csv(fp,delimiter=" ")
            textdatafile.to_csv(ppi_full_file+'.csv', index = None)
        with open(ppi_full_file+'.csv', 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: "ENSEMBL:"+line[PPI_EDGEUMAN.PROTEIN1.value].replace(self.taxon_id+".",""),  # subject id
                                  lambda line: "ENSEMBL:"+line[PPI_EDGEUMAN.PROTEIN2.value].replace(self.taxon_id+".",""),  # object id
                                  lambda line: 'biolink:coexpressed_with' if int(line[PPI_EDGEUMAN.COEXPRESSION.value]) or int(line[PPI_EDGEUMAN.COEXPRESSION_TRANSFERRED.value]) > 0 else "", # predicate
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {
                                    "Coexpression":line[PPI_EDGEUMAN.COEXPRESSION.value],
                                    "Coexpression_transferred":line[PPI_EDGEUMAN.COEXPRESSION_TRANSFERRED.value],
                                    "Experiments":line[PPI_EDGEUMAN.EXPERIMENTS.value],
                                    "Experiments_transferred":line[PPI_EDGEUMAN.EXPERIMENTS_TRANSFERRED.value],
                                    "Database":line[PPI_EDGEUMAN.DATABASE.value],
                                    "Database_transferred":line[PPI_EDGEUMAN.DATABASE_TRANSFERRED.value],
                                    "Textmining":line[PPI_EDGEUMAN.TEXTMINING.value],
                                    "Textmining_transferred":line[PPI_EDGEUMAN.TEXTMINING_TRANSFERRED.value],
                                    "Cooccurance":line[PPI_EDGEUMAN.COOCCURANCE.value],
                                    "Combined_score":line[PPI_EDGEUMAN.COMBINED_SCORE.value]
                                  }, # edge props
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
            extractor.csv_extract(fp,
                                  lambda line: "ENSEMBL:"+line[PPI_EDGEUMAN.PROTEIN1.value].replace(self.taxon_id+".",""),  # subject id
                                  lambda line: "ENSEMBL:"+line[PPI_EDGEUMAN.PROTEIN2.value].replace(self.taxon_id+".",""),  # object id
                                  lambda line: 'biolink:homologous_to' if int(line[PPI_EDGEUMAN.HOMOLOGY.value]) > 0 else "", # predicate
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {
                                    "Homology":line[PPI_EDGEUMAN.HOMOLOGY.value],
                                    "Experiments":line[PPI_EDGEUMAN.EXPERIMENTS.value],
                                    "Experiments_transferred":line[PPI_EDGEUMAN.EXPERIMENTS_TRANSFERRED.value],
                                    "Database":line[PPI_EDGEUMAN.DATABASE.value],
                                    "Database_transferred":line[PPI_EDGEUMAN.DATABASE_TRANSFERRED.value],
                                    "Textmining":line[PPI_EDGEUMAN.TEXTMINING.value],
                                    "Textmining_transferred":line[PPI_EDGEUMAN.TEXTMINING_TRANSFERRED.value],
                                    "Cooccurance":line[PPI_EDGEUMAN.COOCCURANCE.value],
                                    "Combined_score":line[PPI_EDGEUMAN.COMBINED_SCORE.value]
                                  }, #edge props
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
            extractor.csv_extract(fp,
                                  lambda line: "ENSEMBL:"+line[PPI_EDGEUMAN.PROTEIN1.value].replace(self.taxon_id+".",""),  # subject id
                                  lambda line: "ENSEMBL:"+line[PPI_EDGEUMAN.PROTEIN2.value].replace(self.taxon_id+".",""),  # object id
                                  lambda line: 'biolink:gene_fusion_with' if int(line[PPI_EDGEUMAN.FUSION.value]) > 0 else "", # predicate
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {
                                    "Fusion":line[PPI_EDGEUMAN.FUSION.value],
                                    "Experiments":line[PPI_EDGEUMAN.EXPERIMENTS.value],
                                    "Experiments_transferred":line[PPI_EDGEUMAN.EXPERIMENTS_TRANSFERRED.value],
                                    "Database":line[PPI_EDGEUMAN.DATABASE.value],
                                    "Database_transferred":line[PPI_EDGEUMAN.DATABASE_TRANSFERRED.value],
                                    "Textmining":line[PPI_EDGEUMAN.TEXTMINING.value],
                                    "Textmining_transferred":line[PPI_EDGEUMAN.TEXTMINING_TRANSFERRED.value],
                                    "Cooccurance":line[PPI_EDGEUMAN.COOCCURANCE.value],
                                    "Combined_score":line[PPI_EDGEUMAN.COMBINED_SCORE.value]
                                  }, #edge props
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
            extractor.csv_extract(fp,
                                  lambda line: "ENSEMBL:"+line[PPI_EDGEUMAN.PROTEIN1.value].replace(self.taxon_id+".",""),  # subject id
                                  lambda line: "ENSEMBL:"+line[PPI_EDGEUMAN.PROTEIN2.value].replace(self.taxon_id+".",""),  # object id
                                  lambda line: 'biolink:genetic_neighborhood_of' if int(line[PPI_EDGEUMAN.NEIGHBORHOOD.value]) > 0 or int(line[PPI_EDGEUMAN.NEIGHBORHOOD_TRANSFERRED.value]) > 0 else "", # predicate
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {
                                    "Neighborhood":line[PPI_EDGEUMAN.NEIGHBORHOOD.value],
                                    "Neighborhood_transferred":line[PPI_EDGEUMAN.NEIGHBORHOOD_TRANSFERRED.value],
                                    "Experiments":line[PPI_EDGEUMAN.EXPERIMENTS.value],
                                    "Experiments_transferred":line[PPI_EDGEUMAN.EXPERIMENTS_TRANSFERRED.value],
                                    "Database":line[PPI_EDGEUMAN.DATABASE.value],
                                    "Database_transferred":line[PPI_EDGEUMAN.DATABASE_TRANSFERRED.value],
                                    "Textmining":line[PPI_EDGEUMAN.TEXTMINING.value],
                                    "Textmining_transferred":line[PPI_EDGEUMAN.TEXTMINING_TRANSFERRED.value],
                                    "Cooccurance":line[PPI_EDGEUMAN.COOCCURANCE.value],
                                    "Combined_score":line[PPI_EDGEUMAN.COMBINED_SCORE.value]
                                  }, # edge props
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
      
        #This file contains physical subnetwork STRING PPI data for the Human proteome.
        ppi_physical_file: str = os.path.join(self.data_path, self.ppi_physical_subnetwork_file_name)
        with gzip.open(ppi_physical_file, 'r') as fp:
            textdatafile = pd.read_csv(fp,delimiter=" ")
            textdatafile.to_csv(ppi_physical_file+'.csv', index = None)
        with open(ppi_physical_file+'.csv', 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: "ENSEMBL:"+line[PPI_PHYSICAL_EDGEUMAN.PROTEIN1.value].replace(self.taxon_id+".",""),  # subject id
                                  lambda line: "ENSEMBL:"+line[PPI_PHYSICAL_EDGEUMAN.PROTEIN2.value].replace(self.taxon_id+".",""),  # object id
                                  lambda line: 'biolink:physically_interacts_with' if int(line[PPI_PHYSICAL_EDGEUMAN.COMBINED_SCORE.value]) > 0 else "",  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {
                                    "Homology":line[PPI_PHYSICAL_EDGEUMAN.HOMOLOGY.value],
                                    "Experiments":line[PPI_PHYSICAL_EDGEUMAN.EXPERIMENTS.value],
                                    "Experiments_transferred":line[PPI_PHYSICAL_EDGEUMAN.EXPERIMENTS_TRANSFERRED.value],
                                    "Database":line[PPI_PHYSICAL_EDGEUMAN.DATABASE.value],
                                    "Database_transferred":line[PPI_PHYSICAL_EDGEUMAN.DATABASE_TRANSFERRED.value],
                                    "Textmining":line[PPI_PHYSICAL_EDGEUMAN.TEXTMINING.value],
                                    "Textmining_transferred":line[PPI_PHYSICAL_EDGEUMAN.TEXTMINING_TRANSFERRED.value],
                                    "Combined_score":line[PPI_PHYSICAL_EDGEUMAN.COMBINED_SCORE.value],
                                    }, # edge props
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata