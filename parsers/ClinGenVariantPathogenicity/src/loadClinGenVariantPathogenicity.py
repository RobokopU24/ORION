import os
import csv
from Common.biolink_constants import (
    PRIMARY_KNOWLEDGE_SOURCE,
    NODE_TYPES,
    SEQUENCE_VARIANT,
    PUBLICATIONS,
    NEGATED,
)
from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from Common.prefixes import CLINGEN_ALLELE_REGISTRY, PUBMED
from Common.utils import GetData
from datetime import date


##############
# Class: ClinGenVariantPathogenicity  source loader
# Desc: Class that loads/parses the ClinGenVariantPathogenicity data.
##############
class ClinGenVariantPathogenicityLoader(SourceDataLoader):
    source_id: str = "ClinGenVariantPathogenicity"
    provenance_id: str = "infores:clingen"
    # increment parsing_version whenever changes are made to the parser that would result in changes to parsing output
    parsing_version: str = "1.0"
    # source_data
    source_data_url: str = (
        "http://erepo.clinicalgenome.org/evrepo/api/classifications/all"
    )
    attribution: str = (
        "https://clinicalgenome.org/curation-activities/variant-pathogenicity/"
    )
    license: str = "https://creativecommons.org/publicdomain/zero/1.0/"
    description: str = (
        "ClinGen variant curation utilizes the 2015 American College of Medical Genetics and Genomics (ACMG) guideline for sequence variant interpretation, which provides an evidence-based framework to classify variants. The results of these analyses will be deposited in ClinVar for community access."
    )
    has_sequence_variants = (
        True  # Flag to use robokop_genetics server to tackle sequence variant data
    )

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_file = "clingen_variant_pathogenicity.tsv"

    def get_latest_source_version(self) -> str:
        # No version is available at the source, using the year_month when the code was run as versioning proxy
        latest_version = date.today().strftime("%Y%m")
        return latest_version

    def get_data(self) -> bool:
        GetData().pull_via_http(
            self.source_data_url, self.data_path, saved_file_name=self.data_file
        )
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        extractor = Extractor(file_writer=self.output_file_writer)
        clingen_variant_pathogenicity_file: str = os.path.join(
            self.data_path, self.data_file
        )

        with open(clingen_variant_pathogenicity_file, "rt") as fp:
            reader = csv.DictReader(fp, dialect="excel-tab")
            extractor.json_extract(
                reader,
                lambda line: f"CAID:{line['Allele Registry Id']}",  # subject id
                lambda line: line["Mondo Id"],  # object id
                lambda line: (
                    "causes" if line["Retracted"] == "false" else None
                ),  # predicate extractor
                lambda line: {
                    NODE_TYPES: SEQUENCE_VARIANT,
                    "VARIATION": line["#Variation"],
                    "HGNC_GENE_SYMBOL": line["HGNC Gene Symbol"],
                },  # subject properties
                lambda line: {},  # object properties
                lambda line: {
                    PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id,
                    "ASSERTION": line["Assertion"],
                    "APPLIED_EVIDENCE_CODES_MET": line["Applied Evidence Codes (Met)"],
                    "APPLIED_EVIDENCE_CODES_NOT_MET": line[
                        "Applied Evidence Codes (Not Met)"
                    ],
                    "SUMMARY": line["Summary of interpretation"],
                    PUBLICATIONS: [
                        f"{PUBMED}:{pub.strip()}"
                        for pub in line["PubMed Articles"].split(",")
                    ],
                    "EXPERT_PANEL": line["Expert Panel"],
                    "EVIDENCE_REPO_LINK": line["Evidence Repo Link"],
                    "GUIDELINE": line["Guideline"],
                    "APPROVAL_DATA": line["Approval Date"],
                    "PUBLISHED_DATE": line["Published Date"],
                    **self.moi_normalizer(
                        line["Mode of Inheritance"],
                        line["Evidence Repo Link"],
                    ),
                    **self.get_edge_properties(line["Assertion"]),
                },  # edge properties
                exclude_unconnected_nodes=True,
            )
        return extractor.load_metadata

    moi_lookup = {
        "Autosomal dominant inheritance": "HP:0000006",
        "Autosomal dominant inheritance (with paternal imprinting (HP:0012274))": "HP:0012274",
        "Autosomal dominant inheritance (mosaic)": ["HP:0000006", "HP:0001442"],
        "Autosomal recessive inheritance": "HP:0000007",
        "Autosomal recessive inheritance (with genetic anticipation)": "HP:0000007",
        "X-linked inheritance": "HP:0001417",
        "X-linked inheritance (dominant (HP:0001423))": "HP:0001423",
        "X-linked inheritance (recessive (HP:0001419))": "HP:0001419",
        "Semidominant inheritance": "HP:0032113",
        "Mitochondrial inheritance": "HP:0001427",
        "Mitochondrial inheritance (primarily or exclusively heteroplasmic)": "HP:0001427",
        # No HPO term for heteroplasmic type
    }

    def moi_normalizer(self, MOI, EREPO_LINK):
        MOI = str(MOI)
        try:
            HPO = self.moi_lookup[MOI]
        except KeyError:
            self.logger.warning(
                f"We do not have a mapping for {MOI=} in the moi_lookup dictionary at source {EREPO_LINK}"
            )
            HPO = ""
        # TODO: Check the second HPO for "Autosomal recessive inheritance (with genetic anticipation)" and "Mitochondrial inheritance (primarily or exclusively heteroplasmic)"
        return {"MODE_OF_INHERITANCE": MOI, "HPO_FOR_MODE_OF_INHERITANCE": HPO}

    def get_edge_properties(self, assertion):
        if assertion == "Benign" or assertion == "Likely Benign":
            return {"DIRECTION": "Contradicts", NEGATED: True}
        elif assertion == "Likely Pathogenic" or assertion == "Pathogenic":
            return {"DIRECTION": "Supports", NEGATED: False}
        elif assertion == "Uncertain Significance":
            return {"DIRECTION": "Inconclusive", NEGATED: True}
        else:
            return {
                "STATUS": "Not evaluated",
                "DIRECTION": "Inconclusive",
                NEGATED: True,
            }
