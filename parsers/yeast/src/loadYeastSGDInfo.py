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

# Maps Genes to GO Terms.
class GENEGOTERMS_EDGEUMAN(enum.IntEnum):
    GENE = 0
    GOTERM = 1
    PREDICATE = 2
    EVIDENCECODE = 3
    ANNOTATIONTYPE = 4
    EVIDENCEPMID = 5

# Maps Genes to Pathways
class GENEPATHWAYS_EDGEUMAN(enum.IntEnum):
    GENE = 0
    PATHWAY = 1
    PREDICATE = 2

# # Maps Genes to Phenotypes
class GENEPHENOTYPES_EDGEUMAN(enum.IntEnum):
    GENE = 0
    PHENOTYPE = 1
    PREDICATE = 2

##############
# Class: Mapping SGD Genes to SGD Associations
#
# By: Jon-Michael Beasley
# Date: 03/14/2022
# Desc: Class that loads/parses the unique data for yeast genes to SGD associations.
##############
class YeastSGDLoader(SourceDataLoader):

    source_id: str = 'YeastSGDAssociations'
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

        self.sgd_gene_list_file_name = "sgd_gene_robokop_id_list.csv"
        self.go_term_list_file_name = "yeast_GO_term_list.csv"
        self.pathway_list_file_name = "yeast_pathways_list.csv"
        self.phenotype_list_file_name = "yeast_phenotype_list.csv"

        self.genes_to_go_term_edges_file_name = "yeast_gene_GO_term_association_edges.csv"
        self.genes_to_pathway_edges_file_name = "genes_pathways_association_edges.csv"
        self.genes_to_phenotype_edges_file_name = "yeast_gene_phenotype_association_edges.csv"
        
        self.data_files = [
            self.sgd_gene_list_file_name,
            self.go_term_list_file_name,
            self.pathway_list_file_name,
            self.phenotype_list_file_name,
            self.genes_to_go_term_edges_file_name,
            self.genes_to_pathway_edges_file_name,
            self.genes_to_phenotype_edges_file_name
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

        #This file is just a list of GO terms.
        go_term_file: str = os.path.join(self.data_path, self.go_term_list_file_name)
        with open(go_term_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[0].replace(" ","_").strip(),  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'name': line[1],
                                                'categories': [line[2]],
                                                ORIGINAL_KNOWLEDGE_SOURCE: "SGD",
                                                AGGREGATOR_KNOWLEDGE_SOURCES: ["SGD"]}, #subject props
                                  lambda line: {},  # object props
                                  lambda line: {},#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=False)

        #This file is a list of pathways in yeast.
        yeast_pathway_list_file: str = os.path.join(self.data_path, self.pathway_list_file_name)
        with open(yeast_pathway_list_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[0].replace(" ","_").strip(), # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'name':line[1],
                                                'categories': ['biolink:Pathway'],
                                                ORIGINAL_KNOWLEDGE_SOURCE: "SGD",
                                                AGGREGATOR_KNOWLEDGE_SOURCES: ["SGD"]},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=False)

        #This file is a list of yeast phenotypes.
        yeast_phenotype_file: str = os.path.join(self.data_path, self.phenotype_list_file_name)
        with open(yeast_phenotype_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[0].replace(" ","_").strip(),  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'name': line[1] if line[1] != "_?_" else "unknown",
                                                'details': line[2] if line[2] != "_?_" else "unknown",
                                                'strain': line[3] if line[3] != "_?_" else "unknown",
                                                'allele': line[8] if line[8] != "_?_" else "unknown",
                                                'allele_description': line[8] if line[8] != "_?_" else "unknown",
                                                'mutant_type': line[4] if line[4] != "_?_" else "unknown",
                                                'experiment_type': line[5] if line[5] != "_?_" else "unknown",
                                                'experiment_chemical': line[6] if line[6] != "_?_" else "unknown",
                                                'experiment_condition': line[7] if line[7] != "_?_" else "unknown",
                                                'experiment_reporter': line[8] if line[8] != "_?_" else "unknown",
                                                'categories': ['yeast_phenotype','biolink:PhenotypicFeature'],
                                                ORIGINAL_KNOWLEDGE_SOURCE: "SGD",
                                                AGGREGATOR_KNOWLEDGE_SOURCES: ["SGD"]}, # subject props
                                  lambda line: {},  # object props
                                  lambda line: {},#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=False)

        #Genes to GO Annotations. Evidence and annotation type as edge properties.
        gene_to_go_term_edges_file: str = os.path.join(self.data_path, self.genes_to_go_term_edges_file_name)
        with open(gene_to_go_term_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[GENEGOTERMS_EDGEUMAN.GENE.value], #subject id
                                  lambda line: line[GENEGOTERMS_EDGEUMAN.GOTERM.value].replace(' ','_'),  # object id
                                  lambda line: line[GENEGOTERMS_EDGEUMAN.PREDICATE.value],  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {'evidence_code': line[GENEGOTERMS_EDGEUMAN.EVIDENCECODE.value],
                                             'annotation_type': line[GENEGOTERMS_EDGEUMAN.ANNOTATIONTYPE.value],
                                             'evidence_PMIDs': line[GENEGOTERMS_EDGEUMAN.EVIDENCEPMID.value]
                                             }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        
        #Genes to Pathways.
        gene_to_pathway_edges_file: str = os.path.join(self.data_path, self.genes_to_pathway_edges_file_name)
        with open(gene_to_pathway_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[GENEPATHWAYS_EDGEUMAN.GENE.value], #subject id
                                  lambda line: line[GENEPATHWAYS_EDGEUMAN.PATHWAY.value].replace(' ','_'),  # object id
                                  lambda line: line[GENEPATHWAYS_EDGEUMAN.PREDICATE.value],  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #Genes to Phenotypes.
        gene_to_phenotype_edges_file: str = os.path.join(self.data_path, self.genes_to_phenotype_edges_file_name)
        with open(gene_to_phenotype_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                   lambda line: line[GENEPHENOTYPES_EDGEUMAN.GENE.value], #subject id
                                   lambda line: line[GENEPHENOTYPES_EDGEUMAN.PHENOTYPE.value].replace(' ','_'),  # object id
                                   lambda line: line[GENEPHENOTYPES_EDGEUMAN.PREDICATE.value].replace(' ','_'),  # predicate extractor
                                   lambda line: {},  # subject props
                                   lambda line: {},  # object props
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