import os
import enum
from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from Common.utils import GetData
from datetime import date


# Parsing the columns in the csv file downloaded from clingen source ( https://search.clinicalgenome.org/kb/gene-validity/download )
class ClinGenGeneDiseaseValidityCOLS(enum.IntEnum):
    GENE_SYMBOL = 0
    GENE_ID = 1
    DISEASE_LABEL = 2
    DISEASE_ID = 3
    MOI = 4
    SOP = 5
    CLASSIFICATION = 6
    ONLINE_REPORT = 7
    CLASSIFICATION_DATE = 8
    GCEP = 9


LINES_TO_SKIP = 6  # Number of initial metadata lines to skip in the source file


##############
# Class: ClinGenGeneDiseaseValidity  source loader
#
# Desc: Class that loads/parses the ClinGenGeneDiseaseValidity data.
##############
class ClinGenGeneDiseaseValidityLoader(SourceDataLoader):
    source_id: str = "ClinGenGeneDiseaseValidity"
    # this should be a valid infores curie from the biolink infores catalog
    provenance_id: str = "infores:clingen"
    # increment parsing_version whenever changes are made to the parser that would result in changes to parsing output
    parsing_version: str = "1.0"
    # source data
    source_data_url: str = "https://search.clinicalgenome.org/kb/gene-validity/download"
    attribution: str = (
        "https://clinicalgenome.org/curation-activities/gene-disease-validity/"
    )
    description: str = (
        "The ClinGen Gene-Disease Clinical Validity curation process involves evaluating the strength of evidence supporting or refuting a claim that variation in a particular gene causes a particular monogenic disease."
    )
    license: str = "https://creativecommons.org/publicdomain/zero/1.0/"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_file = "clingen_gene_disease_validity.tsv"

    def get_latest_source_version(self) -> str:
        # No version is available at the source, using the year_month when the code was run as versioning proxy
        latest_version = date.today().strftime("%Y%m")
        return latest_version

    def get_data(self) -> bool:
        GetData().pull_via_http(
            self.source_data_url, self.data_path, saved_file_name=self.data_file
        )
        return True

    # Normalizing MODE_OF_INHERITANCE of the disease using the moi_lookup dictionary

    moi_lookup = {
        "AD": {
            "CLINGEN_MODE_OF_INHERITANCE": "AD",
            "NORMALIZED_MODE_OF_INHERITANCE": "Autosomal Dominant",
            "HPO_FOR_NORMALIZED_MODE_OF_INHERITANCE": "0000006",
        },
        "AR": {
            "CLINGEN_MODE_OF_INHERITANCE": "AR",
            "NORMALIZED_MODE_OF_INHERITANCE": "Autosomal Recessive",
            "HPO_FOR_NORMALIZED_MODE_OF_INHERITANCE": "0000007",
        },
        "MT": {
            "CLINGEN_MODE_OF_INHERITANCE": "MT",
            "NORMALIZED_MODE_OF_INHERITANCE": "Mitochondrial",
            "HPO_FOR_NORMALIZED_MODE_OF_INHERITANCE": "0001427",
        },
        "SD": {
            "CLINGEN_MODE_OF_INHERITANCE": "SD",
            "NORMALIZED_MODE_OF_INHERITANCE": "Semidominant",
            "HPO_FOR_NORMALIZED_MODE_OF_INHERITANCE": "0032113",
        },
        "XL": {
            "CLINGEN_MODE_OF_INHERITANCE": "XL",
            "NORMALIZED_MODE_OF_INHERITANCE": "X-linked",
            "HPO_FOR_NORMALIZED_MODE_OF_INHERITANCE": "0001417",
        },
        "UD": {
            "CLINGEN_MODE_OF_INHERITANCE": "UD",
            "NORMALIZED_MODE_OF_INHERITANCE": "Undetermined Mode of Inheritance",
            "HPO_FOR_NORMALIZED_MODE_OF_INHERITANCE": None,
        },
    }

    def moi_normalizer(self, moi, gene, disease):
        try:
            normalized_moi = self.moi_lookup[moi]
        except KeyError:
            normalized_moi = {
                "NORMALIZED_MODE_OF_INHERITANCE": None,
                "HPO_FOR_NORMALIZED_MODE_OF_INHERITANCE": None,
            }
            self.logger.info(
                f"No mapping available for {moi} in the moi lookup dictionary for the gene {gene} - disease {disease} pair"
            )
        return normalized_moi

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        extractor = Extractor(file_writer=self.output_file_writer)
        gene_disease_data_file: str = os.path.join(self.data_path, self.data_file)

        # Need to incorporate the logic to skip the initial metadata rows
        # either skipped record counter or using loop, rn due the ORION normalization it's working fine
        with open(gene_disease_data_file, "rt") as fp:
            # WARNING: this is brittle to potential changes in the source file format
            # the first 6 lines are metadata and should be skipped
            for _ in range(LINES_TO_SKIP):
                next(fp)
            extractor.csv_extract(
                fp,
                lambda line: f"{line[ClinGenGeneDiseaseValidityCOLS.GENE_ID.value]}",  # subject id
                lambda line: f"{line[ClinGenGeneDiseaseValidityCOLS.DISEASE_ID.value]}",  # object id
                lambda line: "gene_associated_with_condition",  # predicate extractor
                lambda line: {},  # subject properties
                lambda line: {},  # object properties
                lambda line: {
                    "CLINGEN_VALIDITY_CLASSIFICATION": line[
                        ClinGenGeneDiseaseValidityCOLS.CLASSIFICATION.value
                    ],
                    "CLINGEN_CLASSIFICATION_DATE": line[
                        ClinGenGeneDiseaseValidityCOLS.CLASSIFICATION_DATE.value
                    ],
                    "CLINGEN_CLASSIFICATION_REPORT": line[
                        ClinGenGeneDiseaseValidityCOLS.ONLINE_REPORT.value
                    ],
                    **self.moi_normalizer(
                        line[ClinGenGeneDiseaseValidityCOLS.MOI.value],
                        line[ClinGenGeneDiseaseValidityCOLS.GENE_ID.value],
                        line[ClinGenGeneDiseaseValidityCOLS.DISEASE_ID.value],
                    ),
                },  # edge properties
                comment_character="#",
                delim=",",
                has_header_row=True,
            )
        return extractor.load_metadata
