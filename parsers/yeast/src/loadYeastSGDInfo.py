import os
import logging
import enum

from copy import deepcopy

import numpy
from parsers.yeast.src.collectSGDdata import main
from Common.utils import LoggingUtil, GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import AGGREGATOR_KNOWLEDGE_SOURCES, PRIMARY_KNOWLEDGE_SOURCE

from Common.kgxmodel import kgxnode, kgxedge

# Maps Genes to GO Terms.
class GENEGOTERMS_EDGEUMAN(enum.IntEnum):
    GENE = 0
    GOTERM = 5
    PREDICATE = 9
    EVIDENCECODE = 8
    EVIDENCECODETEXT = 10
    ANNOTATIONTYPE = 12
    EVIDENCEPMID = 13

# Maps Genes to Pathways
class GENEPATHWAYS_EDGEUMAN(enum.IntEnum):
    GENE = 0
    PATHWAY = 2

# Maps Genes to Phenotypes
class GENEPHENOTYPES_EDGEUMAN(enum.IntEnum):
    GENE = 0
    PHENOTYPE = 18
    QUALIFIER = 8
    EXPERIMENTTYPE = 5
    MUTANTTYPE = 6
    ALLELE = 9
    ALLELEDESCRIPTION = 10
    STRAINBACKGROUND = 11
    CHEMICAL = 12
    CONDITION = 13
    DETAILS = 14
    EVIDENCEPMID = 15

# Maps Genes to Complexes
class GENECOMPLEXES_EDGEUMAN(enum.IntEnum):
    GENE = 11
    COMPLEX = 10
    ROLE = 5
    STOICHIOMETRY = 6
    TYPE = 7

# Maps Complexes to GO Terms
class COMPLEXEGO_EDGEUMAN(enum.IntEnum):
    GOTERM = 1
    COMPLEX = 0
    PREDICATE = 3
    COMPLEXNAME = 2

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

        #self.yeast_data_url = 'https://stars.renci.org/var/data_services/yeast/'

        self.sgd_gene_list_file_name = "SGDAllGenes.csv"
        self.genes_to_go_term_edges_file_name = "SGDGene2GOTerm.csv"
        self.genes_to_pathway_edges_file_name = "SGDGene2Pathway.csv"
        self.genes_to_phenotype_edges_file_name = "SGDGene2Phenotype.csv"
        self.genes_to_complex_edges_file_name = "SGDGene2Complex.csv"
        self.complex_to_go_term_edges_file_name = "SGDComplex2GOTerm.csv"
        
        self.data_files = [
            self.sgd_gene_list_file_name,
            self.genes_to_go_term_edges_file_name,
            self.genes_to_pathway_edges_file_name,
            self.genes_to_phenotype_edges_file_name,
            self.genes_to_complex_edges_file_name,
            self.complex_to_go_term_edges_file_name
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

        #This file contains yeast genes and properties to define gene nodes.
        sgd_gene_file: str = os.path.join(self.data_path, self.sgd_gene_list_file_name)
        with open(sgd_gene_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[0].replace(" ","_").strip(),  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'categories': ['biolink:Gene'],
                                  'secondaryID': line[1],
                                  'name': line[2] if line[2] != "?" else line[1],
                                  'namesake': line[3],
                                  'protein': line[4],
                                  'description': line[5],
                                  'organism': line[10],
                                  'featureType': line[11],
                                  'chromosomeLocation': f"{line[6]}:{line[7]}-{line[8]}, strand: {line[9]}",
                                  'referenceLink': line[12]}, # subject props
                                  lambda line: {},  # object props
                                  lambda line: {},#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #This file contains GO Term annotations for the genes.
        go_term_file: str = os.path.join(self.data_path, self.genes_to_go_term_edges_file_name)
        with open(go_term_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[5].replace(" ","_").strip(),  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'name': line[6],
                                                'categories': [line[7]]
                                                }, #subject props
                                  lambda line: {},  # object props
                                  lambda line: {},#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #This file is a list of pathways in yeast.
        yeast_pathway_list_file: str = os.path.join(self.data_path, self.genes_to_pathway_edges_file_name)
        with open(yeast_pathway_list_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[2].replace(" ","_").strip(), # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'name': line[3],
                                                'categories': ['biolink:Pathway'],
                                                'taxon': 'NCBI_Taxon:559292',
                                                'organism': line[1],
                                                'referenceLink': line[4]},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #This file is a list of yeast phenotypes.
        yeast_phenotype_file: str = os.path.join(self.data_path, self.genes_to_phenotype_edges_file_name)
        with open(yeast_phenotype_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[18].replace(" ","_").strip(),  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'name': line[7],
                                                'categories': ['biolink:PhenotypicFeature'],
                                                'taxon': 'NCBITaxon:559292',
                                                'organism': "S. cerevisiae",
                                                'referenceLink': line[19]}, # subject props
                                  lambda line: {},  # object props
                                  lambda line: {},#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #This file is a list of yeast protein complexes.
        yeast_complex_file: str = os.path.join(self.data_path, self.genes_to_complex_edges_file_name)
        with open(yeast_complex_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: "CPX:" + line[10],  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'name': line[0],
                                                'categories': ['biolink:MacromolecularComplexMixin'],
                                                'function': line[1],
                                                'systematicName': line[2],
                                                'properties': line[9],
                                                'SGDAccessionID': line[10],
                                                'taxon': 'NCBITaxon:559292',
                                                'organism': "S. cerevisiae",
                                                'referenceLink': line[12]}, # subject props
                                  lambda line: {},  # object props
                                  lambda line: {},#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #This file is a list of histone modification genomic loci.
        histone_modification_file: str = os.path.join(self.data_path, self.histone_mod_list_file_name)
        with open(histone_modification_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[HISTONEMODBINS_EDGEUMAN.ID.value],  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'name': f"{line[HISTONEMODBINS_EDGEUMAN.MODIFICATION.value]} ({line[HISTONEMODBINS_EDGEUMAN.CHROMOSOME.value]}:{line[HISTONEMODBINS_EDGEUMAN.STARTLOCATION.value]}-{line[HISTONEMODBINS_EDGEUMAN.ENDLOCATION.value]})",
                                                'categories': ['biolink:NucleosomeModification','biolink:PosttranslationalModification'],
                                                'histoneModification': line[HISTONEMODBINS_EDGEUMAN.MODIFICATION.value],
                                                'chromosomeLocation': line[HISTONEMODBINS_EDGEUMAN.LOCI.value]}, # subject props
                                  lambda line: {},  # object props
                                  lambda line: {},#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #Genes to GO Annotations. Evidence and annotation type as edge properties.
        gene_to_go_term_edges_file: str = os.path.join(self.data_path, self.genes_to_go_term_edges_file_name)
        with open(gene_to_go_term_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[GENEGOTERMS_EDGEUMAN.GENE.value], #subject id
                                  lambda line: line[GENEGOTERMS_EDGEUMAN.GOTERM.value].replace(' ','_'),  # object id
                                  lambda line: line[GENEGOTERMS_EDGEUMAN.PREDICATE.value] if line[GENEGOTERMS_EDGEUMAN.PREDICATE.value] != "involved in" else "biolink:actively_involved_in",  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {'evidenceCode': line[GENEGOTERMS_EDGEUMAN.EVIDENCECODE.value],
                                                'evidenceCodeText': line[GENEGOTERMS_EDGEUMAN.EVIDENCECODETEXT.value],
                                                'annotationType': line[GENEGOTERMS_EDGEUMAN.ANNOTATIONTYPE.value],
                                                'evidencePMIDs': line[GENEGOTERMS_EDGEUMAN.EVIDENCEPMID.value],
                                                PRIMARY_KNOWLEDGE_SOURCE: "SGD"
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
                                  lambda line: "biolink:participates_in",  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {
                                                PRIMARY_KNOWLEDGE_SOURCE: "SGD"
                                                }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #Genes to Phenotypes.
        gene_to_phenotype_edges_file: str = os.path.join(self.data_path, self.genes_to_phenotype_edges_file_name)
        with open(gene_to_phenotype_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[GENEPHENOTYPES_EDGEUMAN.GENE.value], #subject id
                                  lambda line: line[GENEPHENOTYPES_EDGEUMAN.PHENOTYPE.value].replace(' ','_'),  # object id
                                  lambda line: "biolink:genetic_association",  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {'effectOnPhenotype': line[GENEPHENOTYPES_EDGEUMAN.QUALIFIER.value],
                                                'phenotypeDetails': line[GENEPHENOTYPES_EDGEUMAN.DETAILS.value],
                                                'experimentType': line[GENEPHENOTYPES_EDGEUMAN.EXPERIMENTTYPE.value],
                                                'mutantType': line[GENEPHENOTYPES_EDGEUMAN.MUTANTTYPE.value],
                                                'geneAllele': line[GENEPHENOTYPES_EDGEUMAN.ALLELE.value],
                                                'alleleDescription': line[GENEPHENOTYPES_EDGEUMAN.ALLELEDESCRIPTION.value],
                                                'yeastStrainBackground': line[GENEPHENOTYPES_EDGEUMAN.STRAINBACKGROUND.value],
                                                'chemicalExposure': line[GENEPHENOTYPES_EDGEUMAN.CHEMICAL.value],
                                                'experimentalCondition': line[GENEPHENOTYPES_EDGEUMAN.CONDITION.value],
                                                'evidencePMIDs': line[GENEPHENOTYPES_EDGEUMAN.EVIDENCEPMID.value],
                                                PRIMARY_KNOWLEDGE_SOURCE: "SGD"}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #Genes to Complexes.
        gene_to_complex_edges_file: str = os.path.join(self.data_path, self.genes_to_complex_edges_file_name)
        with open(gene_to_complex_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[GENECOMPLEXES_EDGEUMAN.GENE.value], #subject id
                                  lambda line: line[GENECOMPLEXES_EDGEUMAN.COMPLEX.value],  # object id
                                  lambda line: "biolink:in_complex_with",  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {'geneBiologicalRole': line[GENECOMPLEXES_EDGEUMAN.ROLE.value],
                                                'geneStoichiometry': line[GENECOMPLEXES_EDGEUMAN.STOICHIOMETRY.value],
                                                'interactorType': line[GENECOMPLEXES_EDGEUMAN.TYPE.value],
                                                PRIMARY_KNOWLEDGE_SOURCE: "SGD"}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #Complex to GO Annotations. Evidence and annotation type as edge properties.
        complex_to_go_term_edges_file: str = os.path.join(self.data_path, self.complex_to_go_term_edges_file_name)
        with open(complex_to_go_term_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[COMPLEXEGO_EDGEUMAN.COMPLEX.value],#.split(':')[1], #subject id
                                  lambda line: line[COMPLEXEGO_EDGEUMAN.GOTERM.value],#.replace(' ','_'),  # object id
                                  lambda line: line[COMPLEXEGO_EDGEUMAN.PREDICATE.value] if line[COMPLEXEGO_EDGEUMAN.PREDICATE.value] != "involved in" else "biolink:actively_involved_in",  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {
                                                PRIMARY_KNOWLEDGE_SOURCE: "SGD"
                                                }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata