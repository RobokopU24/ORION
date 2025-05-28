import os
from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from biolink_constants import PRIMARY_KNOWLEDGE_SOURCE, NODE_TYPES, SEQUENCE_VARIANT
from Common.utils import GetData
from datetime import date
import csv


# Constants for data processing
HUMAN_DISEASE = "MONDO:0700096"
SKIP_ROWS = 5

BASE_URL = "http://ftp.clinicalgenome.org/"
GENES_FILE = "ClinGen_gene_curation_list_GRCh38.tsv"
REGIONS_FILE = "ClinGen_region_curation_list_GRCh38.tsv"


##############
# Class: ClinGenDosageSensitivity  data loader
# Desc: Class that loads/parses the ClinGen Dosage Sensitivity  data.
###########
class ClinGenDosageSensitivityLoader(SourceDataLoader):
    source_id: str = "ClinGenDosageSensitivity"
    provenance_id: str = "infores:clingen"
    # increment parsing_version whenever changes are made to the parser that would result in changes to parsing output
    parsing_version: str = "v1.0"
    # source data url
    source_data_url = {
        "genes": BASE_URL + GENES_FILE,
        "regions": BASE_URL + REGIONS_FILE,
    }
    attribution: str = (
        "https://clinicalgenome.org/curation-activities/dosage-sensitivity/"
    )
    description: str = (
        "The ClinGen Dosage Sensitivity curation process collects evidence supporting/refuting the haploinsufficiency and triplosensitivity of genes and genomic regions."
    )
    license: str = "https://creativecommons.org/publicdomain/zero/1.0/"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_files = [GENES_FILE, REGIONS_FILE]

    def get_latest_source_version(self) -> str:
        # No version is available at the source, using the year_month when the code was run as versioning proxy
        latest_version = date.today().strftime("%Y%m")
        return latest_version

    def get_data(self) -> bool:
        # get_data is responsible for fetching the files in self.data_files and saving them to self.data_path
        data_puller = GetData()
        for url in self.source_data_url.values():
            data_puller.pull_via_http(url, self.data_path)
        return True

    def dosage_sensitivity_edge_generator(
        self,
        data_file: str,
        subject_extractor: str,
        predicate: str,
    ):
        """
        Generator function to yield edges from the dosage sensitivity data file.

        :param data_file: Path to the data file.
        :param subject_extractor: Function that takes a row and returns the subject identifier.
        :param predicate: predicate associated with the relationship between subject and object.
        :return: Generator yielding edges as dictionaries.
        """
        with open(data_file, "rt") as fp:
            # gnarly, but the ClinGen dosage sensitivity files have a header that is not in the first row,
            # so we have to skip extra metadata rows. This is obviously brittle to changes in their file format.
            for _ in range(SKIP_ROWS):
                next(fp)
            data = csv.DictReader(fp, dialect="excel-tab")
            for line in data:
                record = {
                    "subject": subject_extractor(line),
                    "object": line["Haploinsufficiency Disease ID"] or HUMAN_DISEASE,
                    "predicate": predicate,
                    "subject_properties": {},
                    "object_properties": {},
                    "edge_properties": {
                        PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id,
                        "HAPLOINSUFFICIENCY_DESCRIPTION": line[
                            "Haploinsufficiency Description"
                        ],
                        "HAPLOINSUFFICIENCY_SCORE": line["Haploinsufficiency Score"],
                        **get_edge_properties(
                            line["Haploinsufficiency Score"],
                            line["Haploinsufficiency Disease ID"],
                        ),
                    },
                }
                if line["Haploinsufficiency Score"] != "Not yet evaluated":
                    yield record

                record["object"] = line["Triplosensitivity Disease ID"] or HUMAN_DISEASE
                record["edge_properties"] = {
                    PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id,
                    "TRIPLOSENSITIVITY_DESCRIPTION": line[
                        "Triplosensitivity Description"
                    ],
                    "TRIPLOSENSITIVITY_SCORE": line["Triplosensitivity Score"],
                    **get_edge_properties(
                        line["Triplosensitivity Score"],
                        line["Triplosensitivity Disease ID"],
                    ),
                }
                if line["Triplosensitivity Score"] != "Not yet evaluated":
                    yield record

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        extractor = Extractor(file_writer=self.output_file_writer)
        # Gene file processing
        dosage_sensitivity_gene_file: str = os.path.join(self.data_path, GENES_FILE)
        extractor.json_extract(
            self.dosage_sensitivity_edge_generator(
                dosage_sensitivity_gene_file,
                subject_extractor=lambda row: f'NCBIGene:{row["Gene ID"]}',
                predicate="gene associated with condition",
            )
        )
        # Region file processing - NOTE: these regions will not normalize.
        dosage_sensitivity_region_file: str = os.path.join(self.data_path, REGIONS_FILE)
        extractor.json_extract(
            self.dosage_sensitivity_edge_generator(
                dosage_sensitivity_region_file,
                subject_extractor=lambda row: row["#ISCA ID"].replace("-", ":"),
                predicate="region associated with condition",
            )
        )

        return extractor.load_metadata


# Created a common function to take the score value and return attributes,
# this may have problems with TS and HI score where the mode of inheritance is captured, followed ClinGen criteria for converting scores
# Numeric scores are as describe in the
# [ClinGen Dosage Sensitivity Curation Guidelines](https://clinicalgenome.org/docs/dosage-standard-operating-procedure-scoring-guide)
# 0: No evidence available
# 1: Little evidence for dosage pathogenicity
# 2: Some evidence for dosage pathogenicity
# 3: Sufficient evidence for dosage pathogenicity
# 30: Gene associated with autosomal recessive phenotype (so haploinsufficiency not applicable)
# 40: Dosage sensitivity unlikely
def get_edge_properties(score, disease_id):
    if disease_id != "":
        try:
            score = int(score)
        except ValueError:
            return {"Status": "Not yet evaluated"}
        if score in (2, 3):
            return {"negated": False}
        elif score == 30:
            return {"negated": False}
        elif score in (0, 1, 40):  #  condition for negation
            return {"negated": True}
        elif score == -1:
            return {"Status": "Not planned to be evaluated", "negated": True}
    else:  # Negate only if no HI disease and TS disease is available for the gene
        return {"negated": True}
