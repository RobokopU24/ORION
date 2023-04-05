import os
import enum
import gzip
import requests as rq

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.prefixes import ENSEMBL

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
    description = "The Search Tool for the Retrieval of Interacting Genes/Proteins (STRING) database provides information on known and predicted protein-protein interactions (both direct and indirect) derived from genomic context predictions, high-throughput laboratory experiments, conserved co-expression, automated text mining, and aggregated knowledge from primary data sources."
    source_data_url = "https://string-db.org"
    license = "All data and download files in STRING are freely available under a 'Creative Commons BY 4.0' license."
    attribution = "https://string-db.org/cgi/about?footer_active_subpage=references"
    parsing_version = '1.0'

    taxon_id: str = '9606' # Human taxon

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.taxon_id = '9606'
        self.cos_dist_threshold = 1.0
        self.coexpression_score_threshold = 1
        self.homologous_score_threshold = 1
        self.gene_fusion_score_threshold = 1
        self.genetic_neighborhood_score_threshold = 1
        self.physical_interaction_score_threshold = 1

        self.coexpression_predicate = 'biolink:coexpressed_with'
        self.homologous_predicate = 'biolink:homologous_to'
        self.gene_fusion_predicate = 'biolink:gene_fusion_with'
        self.genetic_neighborhood_predicate = 'biolink:genetic_neighborhood_of'
        self.physically_interacts_with_predicate = 'biolink:physically_interacts_with'

        self.stringdb_version = None
        self.stringdb_version = self.get_latest_source_version()
        self.stringdb_data_url = [f"https://stringdb-static.org/download/protein.links.full.{self.stringdb_version}/",
                                  f"https://stringdb-static.org/download/protein.physical.links.full.{self.stringdb_version}/"]

        self.ppi_full_file_name = self.taxon_id+f".protein.links.full.{self.stringdb_version}.txt.gz"
        self.ppi_physical_subnetwork_file_name = self.taxon_id+f".protein.physical.links.full.{self.stringdb_version}.txt.gz"

        self.data_files = [self.ppi_full_file_name,
                           self.ppi_physical_subnetwork_file_name]

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        if self.stringdb_version:
            return self.stringdb_version

        version_index = rq.get('https://string-db.org/').text.index('string_database_version_dotted:') + 33
        stringdb_version = rq.get('https://string-db.org/').text[version_index:version_index + 4]
        return f"v{stringdb_version}"

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

        extractor = Extractor(file_writer=self.output_file_writer)
        
        #This file contains full STRING PPI data for the Human proteome.
        ppi_full_file: str = os.path.join(self.data_path, self.ppi_full_file_name)
        with gzip.open(ppi_full_file, 'rt') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f'{ENSEMBL}:{line[PPI_EDGEUMAN.PROTEIN1.value].replace(self.taxon_id+".","")}',  # subject id
                                  lambda line: f'{ENSEMBL}:{line[PPI_EDGEUMAN.PROTEIN2.value].replace(self.taxon_id+".","")}',  # object id
                                  lambda line: self.coexpression_predicate if int(line[PPI_EDGEUMAN.COEXPRESSION.value]) or int(line[PPI_EDGEUMAN.COEXPRESSION_TRANSFERRED.value]) >= self.coexpression_score_threshold else None, # predicate
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
                                  },
                                  comment_character=None,
                                  delim=" ",
                                  has_header_row=True)

        with gzip.open(ppi_full_file, 'rt') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f'{ENSEMBL}:{line[PPI_EDGEUMAN.PROTEIN1.value].replace(self.taxon_id+".","")}',  # subject id
                                  lambda line: f'{ENSEMBL}:{line[PPI_EDGEUMAN.PROTEIN2.value].replace(self.taxon_id+".","")}',  # object id
                                  lambda line: self.homologous_predicate if int(line[PPI_EDGEUMAN.HOMOLOGY.value]) >= self.homologous_score_threshold else None, # predicate
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
                                  },
                                  comment_character=None,
                                  delim=" ",
                                  has_header_row=True)

        with gzip.open(ppi_full_file, 'rt') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f'{ENSEMBL}:{line[PPI_EDGEUMAN.PROTEIN1.value].replace(self.taxon_id+".","")}',  # subject id
                                  lambda line: f'{ENSEMBL}:{line[PPI_EDGEUMAN.PROTEIN2.value].replace(self.taxon_id+".","")}',  # object id
                                  lambda line: self.gene_fusion_predicate if int(line[PPI_EDGEUMAN.FUSION.value]) >= self.gene_fusion_score_threshold else None, # predicate
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
                                  },
                                  comment_character=None,
                                  delim=" ",
                                  has_header_row=True)

        with gzip.open(ppi_full_file, 'rt') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f'{ENSEMBL}:{line[PPI_EDGEUMAN.PROTEIN1.value].replace(self.taxon_id+".","")}',  # subject id
                                  lambda line: f'{ENSEMBL}:{line[PPI_EDGEUMAN.PROTEIN2.value].replace(self.taxon_id+".","")}',  # object id
                                  lambda line: self.genetic_neighborhood_predicate if int(line[PPI_EDGEUMAN.NEIGHBORHOOD.value]) >= self.genetic_neighborhood_score_threshold else None, # predicate
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
                                  },
                                  comment_character=None,
                                  delim=" ",
                                  has_header_row=True)
      
        # This file contains physical subnetwork STRING PPI data for the Human proteome.
        ppi_physical_file: str = os.path.join(self.data_path, self.ppi_physical_subnetwork_file_name)
        with gzip.open(ppi_physical_file, 'rt') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f'{ENSEMBL}:{line[PPI_PHYSICAL_EDGEUMAN.PROTEIN1.value].replace(self.taxon_id+".","")}',  # subject id
                                  lambda line: f'{ENSEMBL}:{line[PPI_PHYSICAL_EDGEUMAN.PROTEIN2.value].replace(self.taxon_id+".","")}',  # object id
                                  lambda line: self.physically_interacts_with_predicate if int(line[PPI_PHYSICAL_EDGEUMAN.COMBINED_SCORE.value]) >= self.physical_interaction_score_threshold else None,  # predicate extractor
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
                                  delim=" ",
                                  has_header_row=True)

        return extractor.load_metadata
