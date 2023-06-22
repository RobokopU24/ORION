import os
import logging
import enum

from copy import deepcopy

import numpy
from parsers.yeast.src.collectHistoneMapData import main
from Common.utils import LoggingUtil, GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import AGGREGATOR_KNOWLEDGE_SOURCES, PRIMARY_KNOWLEDGE_SOURCE

from Common.kgxmodel import kgxnode, kgxedge

#List of Binned Histone Modifications
class HISTONEMODBINS_EDGEUMAN(enum.IntEnum):
    ID = 0
    CHROMOSOME = 1
    STARTLOCATION = 2
    ENDLOCATION = 3
    LOCI = 4
    MODIFICATION = 5

# Maps Histone Modifications to Genes
class HISTONEMODGENE_EDGEUMAN(enum.IntEnum):
    ID = 0
    CHROMOSOME = 1
    STARTLOCATION = 2
    ENDLOCATION = 3
    LOCI = 4
    MODIFICATION = 5
    GENE = 6

# Maps Histone Modifications to GO Terms
class HISTONEMODGOTERMS_EDGEUMAN(enum.IntEnum):
    ID = 0
    PRED = 1
    GOID = 2
    GONAME = 3

##############
# Class: Loading all histone nodes and histone modifications to GO Terms
#
# By: Jon-Michael Beasley
# Date: 05/08/2023
##############
class YeastHistoneMapLoader(SourceDataLoader):

    source_id: str = 'YeastHistoneMap'
    provenance_id: str = 'infores:SGD'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.genome_resolution = 150
        #self.yeast_data_url = 'https://stars.renci.org/var/data_services/yeast/'
        self.histone_mod_list_file_name = f"Res{self.genome_resolution}HistoneModLoci.csv"
        self.histone_mod_to_gene_file_name = "HistoneMod2Gene.csv"
        self.histone_mod_to_go_term_file_name = "HistonePTM2GO.csv"
        
        self.data_files = [
            self.histone_mod_list_file_name,
            self.histone_mod_to_gene_file_name,
            self.histone_mod_to_go_term_file_name,
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
        genome_resolution = 150
        main(genome_resolution, self.data_path)

        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor()

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

        #Genes to BinnedHistonePTMs.
        gene_to_histone_mod_edges_file: str = os.path.join(self.data_path, self.histone_mod_to_gene_file_name)
        with open(gene_to_histone_mod_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[HISTONEMODGENE_EDGEUMAN.ID.value], #subject id
                                  lambda line: line[HISTONEMODGENE_EDGEUMAN.GENE.value],  # object id
                                  lambda line: "biolink:located_in",  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {
                                                PRIMARY_KNOWLEDGE_SOURCE: "Epigenomics"
                                  }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        
        #Binned Histone PTMS to general PTMs.
        histone_modification_file: str = os.path.join(self.data_path, self.histone_mod_list_file_name)
        with open(histone_modification_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[HISTONEMODBINS_EDGEUMAN.ID.value],  # subject id
                                  lambda line: "HisPTM:"+line[HISTONEMODBINS_EDGEUMAN.MODIFICATION.value],  # object id
                                  lambda line: "biolink:subclass_of",  # predicate extractor
                                  lambda line: {}, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {
                                                PRIMARY_KNOWLEDGE_SOURCE: "Epigenomics"
                                                },#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        
        #General PTMs to GO Terms.
        histone_mod2go_term_file: str = os.path.join(self.data_path, self.histone_mod_to_go_term_file_name)
        with open(histone_mod2go_term_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[HISTONEMODGOTERMS_EDGEUMAN.ID.value],  # subject id
                                  lambda line: line[HISTONEMODGOTERMS_EDGEUMAN.GOID.value],  # object id
                                  lambda line: line[HISTONEMODGOTERMS_EDGEUMAN.PRED.value],  # predicate extractor
                                  lambda line: {}, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {
                                                PRIMARY_KNOWLEDGE_SOURCE: "Epigenomics"
                                                }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)      
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata