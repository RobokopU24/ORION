import os
import logging
import enum

from copy import deepcopy

import numpy
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

# Maps Experimental Condition Histone and Expression edges to Genes.
class EXPGENE_EDGEUMAN(enum.IntEnum):
    EXPCONDITIONS = 0
    GENE = 1
    RNAPREDICATE = 2
    CHIPPREDICATE = 3
    RNAFC = 4
    RNAFDR = 5
    CHIPFC = 6
    CHIPPVAL = 7


##############
# Class: Experiments affecting histones and expression on Genes Loader
#
# By: Jon-Michael Beasley
# Date: 03/15/2022
# Desc: Class that loads/parses the unique data for mapping experiments to genes where edges contain information about histone and expression changes.
##############
class YeastHistoneAndExpressionLoader(SourceDataLoader):

    source_id: str = 'YeastHistoneAndExpression'
    provenance_id: str = 'infores:Yeast'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.cos_dist_threshold = 1.0
        self.yeast_data_url = 'https://stars.renci.org/var/data_services/yeast/'

        self.experimental_conditions_list_file_name = "experimental_conditions_list_GSE178161.csv"
        self.sgd_gene_list_file_name = "sgd_gene_robokop_id_list.csv"

        self.experimental_conditions_to_gene_edges_file_name = "experiment_mapped_to_genes_GSE178161.csv"
        
        self.data_files = [
            self.experimental_conditions_list_file_name,
            self.sgd_gene_list_file_name,
            self.experimental_conditions_to_gene_edges_file_name,
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

        #Experimental conditions to genes edges. Edges contain RNA expression changes as a property
        experimental_conditions_to_gene_edges_file: str = os.path.join(self.data_path, self.experimental_conditions_to_gene_edges_file_name)
        with open(experimental_conditions_to_gene_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[EXPGENE_EDGEUMAN.EXPCONDITIONS.value], #subject id
                                  lambda line: line[EXPGENE_EDGEUMAN.GENE.value].replace(' ','_'),  # object id
                                  lambda line: line[EXPGENE_EDGEUMAN.RNAPREDICATE.value],  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {'ExpressionFoldChange':float(line[EXPGENE_EDGEUMAN.RNAFC.value]),
                                                'ExpressionFalseDiscoveryRate':float(line[EXPGENE_EDGEUMAN.RNAFDR.value])}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #Experimental conditions to genes edges. Edges contain histone modification changes as a property
        experimental_conditions_to_gene_edges_file: str = os.path.join(self.data_path, self.experimental_conditions_to_gene_edges_file_name)
        with open(experimental_conditions_to_gene_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[EXPGENE_EDGEUMAN.EXPCONDITIONS.value], #subject id
                                  lambda line: line[EXPGENE_EDGEUMAN.GENE.value].replace(' ','_'),  # object id
                                  lambda line: line[EXPGENE_EDGEUMAN.CHIPPREDICATE.value],  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {'HistoneModicationFoldChange':float(line[EXPGENE_EDGEUMAN.CHIPFC.value]),
                                                'HistoneModificationPValue':float(line[EXPGENE_EDGEUMAN.CHIPPVAL.value])}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata

