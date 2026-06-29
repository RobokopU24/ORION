import os
import re

import requests

from orion.biolink_constants import (
    AGENT_TYPE,
    DATA_PIPELINE,
    DISEASE_CONTEXT_QUALIFIER,
    KNOWLEDGE_ASSERTION,
    KNOWLEDGE_LEVEL,
    MANUAL_AGENT,
    PUBLICATIONS,
    SUPPORTING_DATA_SOURCE,
)
from orion.kgxmodel import kgxedge
from orion.loader_interface import SourceDataLoader
from orion.utils import GetData, GetDataPullError


HPOA_INFORES = "infores:hpo-annotations"
HPOA_DISEASE_PHENOTYPE_FILE = "phenotype.hpoa"
HPOA_GENE_PHENOTYPE_FILE = "genes_to_phenotype.txt"
HPOA_DISEASE_PHENOTYPE_URL = f"https://purl.obolibrary.org/obo/hp/hpoa/{HPOA_DISEASE_PHENOTYPE_FILE}"
HPOA_GENE_PHENOTYPE_URL = f"https://purl.obolibrary.org/obo/hp/hpoa/{HPOA_GENE_PHENOTYPE_FILE}"

HPOA_DISEASE_PHENOTYPE_COLUMNS = [
    "database_id",
    "disease_name",
    "qualifier",
    "hpo_id",
    "reference",
    "evidence",
    "onset",
    "frequency",
    "sex",
    "modifier",
    "aspect",
    "biocuration",
]

HPOA_GENE_PHENOTYPE_COLUMNS = [
    "ncbi_gene_id",
    "gene_symbol",
    "hpo_id",
    "hpo_name",
    "frequency",
    "disease_id",
]

DISEASE_PREFIX_TO_SUPPORTING_SOURCE = {
    "DECIPHER": "infores:decipher",
    "MONDO": "infores:mondo",
    "OMIM": "infores:omim",
    "ORPHA": "infores:orphanet",
}

HAS_PHENOTYPE = "biolink:has_phenotype"


def disease_supporting_source(disease_id: str) -> str | None:
    prefix = disease_id.split(":", maxsplit=1)[0].upper()
    return DISEASE_PREFIX_TO_SUPPORTING_SOURCE.get(prefix)


def is_zero_frequency(frequency: str) -> bool:
    frequency = frequency.strip()
    if not frequency:
        return False
    if re.fullmatch(r"0+(\.0+)?", frequency):
        return True
    if re.fullmatch(r"0+(\.0+)?\s*%", frequency):
        return True
    if re.fullmatch(r"0+\s*/\s*\d+", frequency):
        return True
    return False


def pmids_from_text(value: str) -> list[str]:
    return [f"PMID:{pmid}" for pmid in re.findall(r"PMID:(\d+)", value or "")]


def hpoa_row_is_positive_phenotype(row: dict) -> bool:
    disease_id = row.get("database_id", "").strip()
    hpo_id = row.get("hpo_id", "").strip()
    aspect = row.get("aspect", "").strip()
    qualifier = row.get("qualifier", "").strip()
    frequency = row.get("frequency", "").strip()
    return bool(
        disease_id
        and hpo_id
        and aspect == "P"
        and not qualifier
        and not is_zero_frequency(frequency)
    )


def iter_hpoa_tsv(path: str, default_columns: list[str]) -> dict:
    header = None
    with open(path, "rt", encoding="utf-8") as source_file:
        for raw_line in source_file:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            if line.startswith("#"):
                candidate_header = line[1:].split("\t")
                if candidate_header == default_columns:
                    header = candidate_header
                continue

            values = line.split("\t")
            if header is None:
                if values == default_columns:
                    header = values
                    continue
                header = default_columns

            if len(values) < len(header):
                values.extend([""] * (len(header) - len(values)))
            yield dict(zip(header, values))


def disease_phenotype_edge_properties(row: dict) -> dict:
    edge_properties = {
        KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
        AGENT_TYPE: MANUAL_AGENT,
        SUPPORTING_DATA_SOURCE: disease_supporting_source(row["database_id"]),
        "hpoa_database_id": row.get("database_id", ""),
        "hpoa_disease_name": row.get("disease_name", ""),
        "hpoa_qualifier": row.get("qualifier", ""),
        "hpoa_reference": row.get("reference", ""),
        "hpoa_evidence": row.get("evidence", ""),
        "hpoa_onset": row.get("onset", ""),
        "hpoa_frequency": row.get("frequency", ""),
        "hpoa_sex": row.get("sex", ""),
        "hpoa_modifier": row.get("modifier", ""),
        "hpoa_aspect": row.get("aspect", ""),
        "hpoa_biocuration": row.get("biocuration", ""),
    }
    publications = pmids_from_text(row.get("reference", ""))
    if publications:
        edge_properties[PUBLICATIONS] = publications
    return edge_properties


def gene_phenotype_edge_properties(row: dict) -> dict:
    disease_id = row["disease_id"]
    return {
        KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
        AGENT_TYPE: DATA_PIPELINE,
        SUPPORTING_DATA_SOURCE: disease_supporting_source(disease_id),
        DISEASE_CONTEXT_QUALIFIER: disease_id,
        "hpoa_disease_id": disease_id,
        "hpoa_gene_symbol": row.get("gene_symbol", ""),
        "hpoa_hpo_name": row.get("hpo_name", ""),
        "hpoa_frequency": row.get("frequency", ""),
    }


class HPOALoader(SourceDataLoader):
    source_id = "HPOA"
    provenance_id = HPOA_INFORES
    parsing_version = "1.0"
    source_data_url = HPOA_DISEASE_PHENOTYPE_URL
    attribution = "https://hpo.jax.org/data/annotations"
    description = (
        "The Human Phenotype Ontology annotations provide curated disease-phenotype "
        "annotations and derived disease-conditioned gene-phenotype annotations."
    )
    license = "https://hpo.jax.org/app/license"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_files = [HPOA_DISEASE_PHENOTYPE_FILE, HPOA_GENE_PHENOTYPE_FILE]

    def get_latest_source_version(self) -> str:
        try:
            response = requests.get(HPOA_DISEASE_PHENOTYPE_URL, stream=True, timeout=30)
            response.raise_for_status()
            for line in response.iter_lines(decode_unicode=True):
                if not line:
                    continue
                if line.startswith("#version:"):
                    return line.split(":", maxsplit=1)[1].strip()
                if not line.startswith("#"):
                    break
        except Exception as e:
            raise GetDataPullError(f"Unable to determine latest HPOA version: {e}")
        raise GetDataPullError("Unable to determine latest HPOA version: #version line not found")

    def get_data(self) -> bool:
        data_puller = GetData()
        data_puller.pull_via_http(
            HPOA_DISEASE_PHENOTYPE_URL,
            self.data_path,
            saved_file_name=HPOA_DISEASE_PHENOTYPE_FILE,
        )
        data_puller.pull_via_http(
            HPOA_GENE_PHENOTYPE_URL,
            self.data_path,
            saved_file_name=HPOA_GENE_PHENOTYPE_FILE,
        )
        return True

    def parse_data(self) -> dict:
        phenotype_path = os.path.join(self.data_path, HPOA_DISEASE_PHENOTYPE_FILE)
        gene_phenotype_path = os.path.join(self.data_path, HPOA_GENE_PHENOTYPE_FILE)

        disease_phenotype_metadata, kept_disease_phenotype_pairs = self.parse_disease_phenotypes(
            phenotype_path
        )
        gene_phenotype_metadata = self.parse_gene_phenotypes(
            gene_phenotype_path,
            kept_disease_phenotype_pairs,
        )

        metadata = {}
        metadata.update(disease_phenotype_metadata)
        metadata.update(gene_phenotype_metadata)
        metadata["num_source_lines"] = (
            disease_phenotype_metadata["disease_phenotype_source_lines"]
            + gene_phenotype_metadata["gene_phenotype_source_lines"]
        )
        metadata["unusable_source_lines"] = (
            disease_phenotype_metadata["disease_phenotype_rows_skipped"]
            + gene_phenotype_metadata["gene_phenotype_rows_skipped"]
        )
        return metadata

    def parse_disease_phenotypes(self, phenotype_path: str) -> tuple[dict, set[tuple[str, str]]]:
        source_lines = 0
        rows_skipped = 0
        duplicate_positive_rows = 0
        kept_pairs = set()

        for row in iter_hpoa_tsv(phenotype_path, HPOA_DISEASE_PHENOTYPE_COLUMNS):
            source_lines += 1
            if not hpoa_row_is_positive_phenotype(row):
                rows_skipped += 1
                continue

            disease_id = row["database_id"]
            hpo_id = row["hpo_id"]
            pair = (disease_id, hpo_id)
            if pair in kept_pairs:
                duplicate_positive_rows += 1
                continue

            kept_pairs.add(pair)
            self.write_edge(
                subject_id=disease_id,
                object_id=hpo_id,
                predicate=HAS_PHENOTYPE,
                edge_properties=disease_phenotype_edge_properties(row),
            )

        return (
            {
                "disease_phenotype_source_lines": source_lines,
                "disease_phenotype_rows_skipped": rows_skipped,
                "disease_phenotype_duplicate_positive_rows": duplicate_positive_rows,
                "disease_phenotype_edges_written": len(kept_pairs),
            },
            kept_pairs,
        )

    def parse_gene_phenotypes(
        self,
        gene_phenotype_path: str,
        kept_disease_phenotype_pairs: set[tuple[str, str]],
    ) -> dict:
        source_lines = 0
        rows_skipped = 0
        edges_written = 0

        for row in iter_hpoa_tsv(gene_phenotype_path, HPOA_GENE_PHENOTYPE_COLUMNS):
            source_lines += 1
            disease_id = row.get("disease_id", "").strip()
            hpo_id = row.get("hpo_id", "").strip()
            ncbi_gene_id = row.get("ncbi_gene_id", "").strip()
            if not (
                disease_id
                and hpo_id
                and ncbi_gene_id
                and (disease_id, hpo_id) in kept_disease_phenotype_pairs
            ):
                rows_skipped += 1
                continue

            self.write_edge(
                subject_id=f"NCBIGene:{ncbi_gene_id}",
                object_id=hpo_id,
                predicate=HAS_PHENOTYPE,
                edge_properties=gene_phenotype_edge_properties(row),
            )
            edges_written += 1

        return {
            "gene_phenotype_source_lines": source_lines,
            "gene_phenotype_rows_skipped": rows_skipped,
            "gene_phenotype_edges_written": edges_written,
        }

    def write_edge(
        self,
        subject_id: str,
        object_id: str,
        predicate: str,
        edge_properties: dict,
    ) -> None:
        self.output_file_writer.write_node(subject_id)
        self.output_file_writer.write_node(object_id)
        self.output_file_writer.write_kgx_edge(
            kgxedge(
                subject_id=subject_id,
                object_id=object_id,
                predicate=predicate,
                primary_knowledge_source=self.provenance_id,
                edgeprops=edge_properties,
            )
        )
