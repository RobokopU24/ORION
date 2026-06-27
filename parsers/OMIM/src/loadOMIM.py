import csv
import os
from datetime import date
from email.utils import parsedate_to_datetime

import requests

from orion.biolink_constants import (
    AGENT_TYPE,
    DATA_PIPELINE,
    KNOWLEDGE_ASSERTION,
    KNOWLEDGE_LEVEL,
    SUPPORTING_DATA_SOURCE,
)
from orion.kgxmodel import kgxedge
from orion.loader_interface import SourceDataLoader
from orion.utils import GetData


OMIM_INFORES = "infores:omim"
MEDGEN_INFORES = "infores:medgen"
MIM2GENE_MEDGEN_URL = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/mim2gene_medgen"
MIM2GENE_MEDGEN_FILE = "mim2gene_medgen"
GENE_ASSOCIATED_WITH_CONDITION = "biolink:gene_associated_with_condition"


class OMIMLoader(SourceDataLoader):
    source_id = "OMIM"
    provenance_id = OMIM_INFORES
    parsing_version = "1.0"
    source_data_url = MIM2GENE_MEDGEN_URL
    attribution = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/"
    description = (
        "NCBI mim2gene_medgen mappings from OMIM phenotype MIM numbers to NCBI Gene IDs "
        "and MedGen identifiers."
    )
    license = "https://www.ncbi.nlm.nih.gov/home/about/policies/"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_file = MIM2GENE_MEDGEN_FILE

    def get_latest_source_version(self) -> str:
        try:
            response = requests.head(MIM2GENE_MEDGEN_URL, allow_redirects=True, timeout=30)
            response.raise_for_status()
            last_modified = response.headers.get("Last-Modified")
            if last_modified:
                return parsedate_to_datetime(last_modified).strftime("%Y%m%d")
        except Exception:
            pass
        return date.today().strftime("%Y%m%d")

    def get_data(self) -> bool:
        GetData().pull_via_http(
            MIM2GENE_MEDGEN_URL,
            self.data_path,
            saved_file_name=self.data_file,
        )
        return True

    def parse_data(self) -> dict:
        source_lines = 0
        skipped_rows = 0
        edges_written = 0
        data_path = os.path.join(self.data_path, self.data_file)

        with open(data_path, "rt", encoding="utf-8") as fp:
            reader = csv.DictReader(fp, delimiter="\t")
            for row in reader:
                source_lines += 1
                row = normalize_mim2gene_row(row)
                if row["type"] != "phenotype" or row["GeneID"] == "-":
                    skipped_rows += 1
                    continue

                subject_id = f"NCBIGene:{row['GeneID']}"
                object_id = f"OMIM:{row['MIM number']}"
                self.output_file_writer.write_node(subject_id)
                self.output_file_writer.write_node(object_id)
                self.output_file_writer.write_kgx_edge(
                    kgxedge(
                        subject_id=subject_id,
                        object_id=object_id,
                        predicate=GENE_ASSOCIATED_WITH_CONDITION,
                        primary_knowledge_source=self.provenance_id,
                        edgeprops=omim_edge_properties(row),
                    )
                )
                edges_written += 1

        return {
            "num_source_lines": source_lines,
            "unusable_source_lines": skipped_rows,
            "source_edges": edges_written,
        }


def normalize_mim2gene_row(row: dict) -> dict:
    normalized = {}
    for key, value in row.items():
        normalized[key.lstrip("#")] = value.strip() if value else ""
    return normalized


def omim_edge_properties(row: dict) -> dict:
    return {
        KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
        AGENT_TYPE: DATA_PIPELINE,
        SUPPORTING_DATA_SOURCE: MEDGEN_INFORES,
        "omim_mim_number": row.get("MIM number", ""),
        "omim_type": row.get("type", ""),
        "omim_source": row.get("Source", ""),
        "medgen_cui": row.get("MedGenCUI", ""),
        "omim_comment": row.get("Comment", ""),
    }
