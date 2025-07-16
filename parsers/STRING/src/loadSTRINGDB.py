import os
import enum
import gzip
import requests as rq

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.prefixes import ENSEMBL, NCBITAXON
from Common.biolink_constants import *


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
    EXPERIMENTS_TRANSFERRED = 10
    DATABASE = 11
    DATABASE_TRANSFERRED = 12
    TEXTMINING = 13
    TEXTMINING_TRANSFERRED = 14
    COMBINED_SCORE = 15

##############
# Class: Mapping Protein-Protein Interactions from STRING-DB
#
# By: Jon-Michael Beasley
# Date: 09/09/2022
# Desc: Class that loads/parses human protein-protein interaction data.
##############
class STRINGDBLoader(SourceDataLoader):

    source_id: str = 'STRING-DB'
    provenance_id: str = 'infores:string'
    taxon_id = None  # this is overwritten by classes that inherit from this one

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        # We do not have a predicate defined for the evidence channel HOMOLOGY. Why? Because in STRING DB a high score in the
        # HOMOLOGY channel does not mean A is homologous to B but instead
        # “The interaction between A and B is inferred via homologous proteins in another species”
        # Therefore we default to physical_interacts_with for pairs of proteins with a high score in the HOMOLOGY channel
        # HOMOLOGY is used only for KL/AT assignment, never for predicate assignment.

        self.EVIDENCE_CHANNELS = {
            "NEIGHBORHOOD": "biolink:genetic_neighborhood_of",
            "FUSION": "biolink:gene_fusion_with",
            "COOCCURANCE": "biolink:genetically_interacts_with",
            "COEXPRESSION": "biolink:coexpressed_with",
            "EXPERIMENTS": "biolink:physically_interacts_with",
            "TEXTMINING": "biolink:interacts_with"
        }
        self.EVIDENCE_CHANNEL_QUALIFIERS = {
            "NEIGHBORHOOD": (PREDICTION, DATA_PIPELINE),
            "FUSION": (PREDICTION, DATA_PIPELINE),
            "COOCCURANCE": (STATISTICAL_ASSOCIATION, DATA_PIPELINE),
            "HOMOLOGY": (PREDICTION, COMPUTATIONAL_MODEL),
            "COEXPRESSION": (STATISTICAL_ASSOCIATION, DATA_PIPELINE),
            "EXPERIMENTS": (KNOWLEDGE_ASSERTION, MANUAL_AGENT),
            "DATABASE": (KNOWLEDGE_ASSERTION, MANUAL_AGENT),
            "TEXTMINING": (NOT_PROVIDED, TEXT_MINING_AGENT)
        }

        self.stringdb_version = None
        self.stringdb_version = self.get_latest_source_version()
        self.string_db_full_file_url = f"https://stringdb-downloads.org/download/protein.links.full.{self.stringdb_version}/"

        self.ppi_full_file_name = self.taxon_id+f".protein.links.full.{self.stringdb_version}.txt.gz"
        self.data_files = [self.ppi_full_file_name]

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
        data_puller = GetData()

        full_file_url = f"{self.string_db_full_file_url}{self.ppi_full_file_name}"
        data_puller.pull_via_http(full_file_url, self.data_path)

        return True

    def predicate_extractor_factory(self, score_threshold=500, high_conf_threshold=750):
        def extractor(line):
            # Skip the row if the combined score is below threshold
            if int(line[PPI_EDGEUMAN.COMBINED_SCORE.value]) <= score_threshold:
                return None

            high_conf_predicates = []
            for channel, predicate in self.EVIDENCE_CHANNELS.items():
                score = int(line[getattr(PPI_EDGEUMAN, channel).value])
                if score > high_conf_threshold:
                    high_conf_predicates.append(predicate)

            # Return high-confidence predicates, or fallback
            return high_conf_predicates if high_conf_predicates else ["biolink:physically_interacts_with"]

        return extractor

    def edge_property_extractor_factory(self):
        def extractor(line):
            max_score = -1
            selected_channel = None
            high_conf_channels = []

            for channel in self.EVIDENCE_CHANNEL_QUALIFIERS:
                score = int(line[getattr(PPI_EDGEUMAN, channel).value])
                if score > max_score:
                    max_score = score
                    selected_channel = channel
                if score > 750:
                    high_conf_channels.append(channel)

            # Default assignments
            if selected_channel:
                knowledge_level, agent_type = self.EVIDENCE_CHANNEL_QUALIFIERS[selected_channel]
            else:
                knowledge_level, agent_type = NOT_PROVIDED, NOT_PROVIDED

            # Override if multiple high-confidence channels exist
            if len(high_conf_channels) > 1:
                knowledge_level = KNOWLEDGE_ASSERTION
                if any(
                        self.EVIDENCE_CHANNEL_QUALIFIERS[channel][1] == MANUAL_AGENT
                        for channel in high_conf_channels
                ):
                    agent_type = MANUAL_AGENT
                else:
                    agent_type = DATA_PIPELINE

            return {
                SPECIES_CONTEXT_QUALIFIER: f"{NCBITAXON}:{self.taxon_id}",
                PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id,
                KNOWLEDGE_LEVEL: knowledge_level,
                AGENT_TYPE: agent_type
            }

        return extractor

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor(file_writer=self.output_file_writer)

        # in the files the protein ids have the taxon prepended to them - we can use this length to remove that part
        taxon_string_length = len(self.taxon_id) + 1

        # This file contains full STRING PPI data.
        ppi_full_file: str = os.path.join(self.data_path, self.ppi_full_file_name)
        with gzip.open(ppi_full_file, 'rt') as fp:
            extractor.csv_extract(
                infile=fp,
                subject_extractor=lambda line: f"{ENSEMBL}:{line[PPI_EDGEUMAN.PROTEIN1.value][taxon_string_length:]}",
                object_extractor=lambda line: f"{ENSEMBL}:{line[PPI_EDGEUMAN.PROTEIN2.value][taxon_string_length:]}",
                predicate_extractor=self.predicate_extractor_factory(score_threshold=500, high_conf_threshold=750),
                subject_property_extractor=lambda line: {},
                object_property_extractor=lambda line: {},
                edge_property_extractor=self.edge_property_extractor_factory(),
                comment_character=None,
                delim=" ",
                has_header_row=True
            )

        return extractor.load_metadata


class HumanSTRINGDBLoader(STRINGDBLoader):
    source_id: str = 'STRING-DB-Human'
    parsing_version = '1.3'
    taxon_id: str = '9606'  # Human taxon


class YeastSTRINGDBLoader(STRINGDBLoader):
    source_id: str = 'STRING-DB-Yeast'
    parsing_version = '1.3'
    taxon_id: str = '4932'  # Saccharomyces cerevisiae taxon
