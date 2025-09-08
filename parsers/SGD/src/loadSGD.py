import os
import enum
import requests

from parsers.SGD.src.sgd_source_retriever import retrieve_sgd_files
from orion.loader_interface import SourceDataLoader
from orion.extractor import Extractor
from orion.prefixes import PUBMED
from orion.biolink_constants import PRIMARY_KNOWLEDGE_SOURCE, NODE_TYPES, PUBLICATIONS
from parsers.yeast.src.yeast_constants import SGD_ALL_GENES_FILE

# Maps Genes to GO Terms.
class GENEGOTERMS_EDGEUMAN(enum.IntEnum):
    GENE = 0
    GOTERM = 5
    GOTERM_NAME = 6
    PREDICATE = 9
    EVIDENCECODE = 8
    EVIDENCECODETEXT = 10
    ANNOTATIONTYPE = 12
    EVIDENCEPMID = 15

# Maps Genes to Pathways
class GENEPATHWAYS_EDGEUMAN(enum.IntEnum):
    GENE = 0
    ORGANISM_NAME = 1
    PATHWAY = 2
    PATHWAY_NAME = 3
    PATHWAY_LINK = 4

# Maps Genes to Phenotypes
class GENEPHENOTYPES_EDGEUMAN(enum.IntEnum):
    GENE = 0
    EXPERIMENTTYPE = 5
    MUTANTTYPE = 6
    PHENOTYPE_NAME = 7
    QUALIFIER = 8
    ALLELE = 9
    ALLELEDESCRIPTION = 10
    STRAINBACKGROUND = 11
    CHEMICAL = 12
    CONDITION = 13
    DETAILS = 14
    EVIDENCEPMID = 15
    PHENOTYPE = 18
    PHENOTYPE_LINK = 19

# Maps Genes to Complexes
class GENECOMPLEXES_EDGEUMAN(enum.IntEnum):
    NAME = 0
    FUNCTION = 1
    SYSTEMATIC_NAME = 2
    ROLE = 5
    STOICHIOMETRY = 6
    TYPE = 7
    PROPERTIES = 9
    COMPLEX = 10
    GENE = 11

# Maps Complexes to GO Terms
class COMPLEXEGO_EDGEUMAN(enum.IntEnum):
    GOTERM = 1
    COMPLEX = 0
    PREDICATE = 3
    COMPLEXNAME = 2


def convert_go_qualifier_to_predicate(go_qualifier):
    # NOTE that there are a lot more values that go_qualifier could be, these are two currently identified as needing
    # mapping before normalization (the rest will be converted succesfully with biolink lookup in edge normalization)
    # better long term solution would be to add these two mappings to biolink or to map all the options from GO
    if go_qualifier == 'involved in':
        return 'biolink:actively_involved_in'
    elif go_qualifier == 'is active in':
        return 'biolink:active_in'
    else:
        return go_qualifier

##############
# Class: Mapping SGD Genes to SGD Associations
#
# By: Jon-Michael Beasley
# Date: 03/14/2022
# Desc: Class that loads/parses the unique data for yeast genes to SGD associations.
##############
class SGDLoader(SourceDataLoader):

    source_id: str = 'SGD'
    provenance_id: str = 'infores:sgd'
    parsing_version = '1.2'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.sgd_gene_list_file_name = SGD_ALL_GENES_FILE
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

        self.yeast_complex_base_url = "https://www.yeastgenome.org/complex/"

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        # not exactly right because we also get data from http://ontologies.berkeleybop.org/apo.obo for this currently
        yeastmine_release_response = requests.get('https://yeastmine.yeastgenome.org/yeastmine/service/version/release')
        release_text = yeastmine_release_response.text
        release_version = release_text.split('Data Updated on:')[1].split('; GO-Release')[0].strip()
        return release_version

    def get_data(self) -> int:
        """
        Gets the yeast data.

        """
        retrieve_sgd_files(download_destination_path=self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor(self.output_file_writer)

        #This file contains yeast genes and properties to define gene nodes.
        sgd_gene_file: str = os.path.join(self.data_path, self.sgd_gene_list_file_name)
        with open(sgd_gene_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[0].replace(" ","_").strip(),  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {NODE_TYPES: ['biolink:Gene'],
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
                                  lambda line: {PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id},  #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #Genes to GO Annotations. Evidence and annotation type as edge properties.
        gene_to_go_term_edges_file: str = os.path.join(self.data_path, self.genes_to_go_term_edges_file_name)
        with open(gene_to_go_term_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[GENEGOTERMS_EDGEUMAN.GENE.value], #subject id
                                  lambda line: line[GENEGOTERMS_EDGEUMAN.GOTERM.value],  # object id
                                  lambda line: convert_go_qualifier_to_predicate(line[GENEGOTERMS_EDGEUMAN.PREDICATE.value]),  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {'name': line[GENEGOTERMS_EDGEUMAN.GOTERM_NAME.value]},  # object props
                                  lambda line: {'evidenceCode': line[GENEGOTERMS_EDGEUMAN.EVIDENCECODE.value],
                                                'evidenceCodeText': line[GENEGOTERMS_EDGEUMAN.EVIDENCECODETEXT.value],
                                                'annotationType': line[GENEGOTERMS_EDGEUMAN.ANNOTATIONTYPE.value],
                                                PUBLICATIONS: [f'{PUBMED}:{line[GENEGOTERMS_EDGEUMAN.EVIDENCEPMID.value]}']
                                                if line[GENEGOTERMS_EDGEUMAN.EVIDENCEPMID.value] != "?" else [],
                                                PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id
                                             }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        
        #Genes to Pathways.
        gene_to_pathway_edges_file: str = os.path.join(self.data_path, self.genes_to_pathway_edges_file_name)
        with open(gene_to_pathway_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[GENEPATHWAYS_EDGEUMAN.GENE.value], #subject id
                                  lambda line: line[GENEPATHWAYS_EDGEUMAN.PATHWAY.value], # object id
                                  lambda line: "biolink:participates_in",  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {'name': line[GENEPATHWAYS_EDGEUMAN.PATHWAY_NAME.value],
                                                NODE_TYPES: ['biolink:Pathway'],
                                                'taxon': 'NCBI_Taxon:559292',
                                                'organism': line[GENEPATHWAYS_EDGEUMAN.ORGANISM_NAME.value],
                                                'referenceLink': line[GENEPATHWAYS_EDGEUMAN.PATHWAY_LINK.value]},  # object props
                                  lambda line: {PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id}, #edgeprops
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
                                  lambda line: {'name': line[GENEPHENOTYPES_EDGEUMAN.PHENOTYPE_NAME.value],
                                                NODE_TYPES: ['biolink:PhenotypicFeature'],
                                                'taxon': 'NCBITaxon:559292',
                                                'organism': "S. cerevisiae",
                                                'referenceLink': line[GENEPHENOTYPES_EDGEUMAN.PHENOTYPE_LINK.value]},  # object props
                                  lambda line: {'effectOnPhenotype': line[GENEPHENOTYPES_EDGEUMAN.QUALIFIER.value],
                                                'phenotypeDetails': line[GENEPHENOTYPES_EDGEUMAN.DETAILS.value],
                                                'experimentType': line[GENEPHENOTYPES_EDGEUMAN.EXPERIMENTTYPE.value],
                                                'mutantType': line[GENEPHENOTYPES_EDGEUMAN.MUTANTTYPE.value],
                                                'geneAllele': line[GENEPHENOTYPES_EDGEUMAN.ALLELE.value],
                                                'alleleDescription': line[GENEPHENOTYPES_EDGEUMAN.ALLELEDESCRIPTION.value],
                                                'yeastStrainBackground': line[GENEPHENOTYPES_EDGEUMAN.STRAINBACKGROUND.value],
                                                'chemicalExposure': line[GENEPHENOTYPES_EDGEUMAN.CHEMICAL.value],
                                                'experimentalCondition': line[GENEPHENOTYPES_EDGEUMAN.CONDITION.value],
                                                PUBLICATIONS: [f'{PUBMED}:{line[GENEPHENOTYPES_EDGEUMAN.EVIDENCEPMID.value]}']
                                                if line[GENEPHENOTYPES_EDGEUMAN.EVIDENCEPMID.value] != "?" else [],
                                                PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #Genes to Complexes.
        gene_to_complex_edges_file: str = os.path.join(self.data_path, self.genes_to_complex_edges_file_name)
        with open(gene_to_complex_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[GENECOMPLEXES_EDGEUMAN.GENE.value], #subject id
                                  lambda line: f"CPX:{line[GENECOMPLEXES_EDGEUMAN.COMPLEX.value]}",  # object id
                                  lambda line: "biolink:in_complex_with",  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {'name': line[GENECOMPLEXES_EDGEUMAN.NAME.value],
                                                NODE_TYPES: ['biolink:MacromolecularComplexMixin'],
                                                'function': line[GENECOMPLEXES_EDGEUMAN.FUNCTION.value],
                                                'systematicName': line[GENECOMPLEXES_EDGEUMAN.SYSTEMATIC_NAME.value],
                                                'properties': line[GENECOMPLEXES_EDGEUMAN.PROPERTIES.value],
                                                'SGDAccessionID': line[GENECOMPLEXES_EDGEUMAN.COMPLEX.value],
                                                'taxon': 'NCBITaxon:559292',
                                                'organism': "S. cerevisiae",
                                                'referenceLink': f'{self.yeast_complex_base_url}{line[GENECOMPLEXES_EDGEUMAN.COMPLEX.value]}'},  # object props
                                  lambda line: {'geneBiologicalRole': line[GENECOMPLEXES_EDGEUMAN.ROLE.value],
                                                'geneStoichiometry': line[GENECOMPLEXES_EDGEUMAN.STOICHIOMETRY.value],
                                                'interactorType': line[GENECOMPLEXES_EDGEUMAN.TYPE.value],
                                                PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #Complex to GO Annotations. Evidence and annotation type as edge properties.
        complex_to_go_term_edges_file: str = os.path.join(self.data_path, self.complex_to_go_term_edges_file_name)
        with open(complex_to_go_term_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f"CPX:{line[COMPLEXEGO_EDGEUMAN.COMPLEX.value]}", #subject id
                                  lambda line: line[COMPLEXEGO_EDGEUMAN.GOTERM.value],  # object id
                                  lambda line: line[COMPLEXEGO_EDGEUMAN.PREDICATE.value],  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {
                                                PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id
                                                }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        return extractor.load_metadata
