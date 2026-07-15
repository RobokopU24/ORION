import csv
import gzip
import os
from pathlib import Path

import requests

from orion.biolink_constants import (
    AGENT_TYPE,
    ANATOMICAL_ENTITY,
    DISEASE,
    GENE,
    KNOWLEDGE_ASSERTION,
    KNOWLEDGE_LEVEL,
    MANUAL_AGENT,
    PHENOTYPIC_FEATURE,
    PUBLICATIONS,
    TAXON,
)
from orion.loader_interface import SourceDataLoader, SourceDataBrokenError
from orion.prefixes import NCBIGENE, NCBITAXON
from orion.utils import GetData, GetDataPullError


MGI_REPORT_BASE_URL = "https://www.informatics.jax.org/downloads/reports/"
MGI_INFORES = "infores:mgi"
MOUSE_TAXON_ID = f"{NCBITAXON}:10090"


MARKER_ID = "MGI Accession ID"
MARKER_SYMBOL = "Marker Symbol"
MARKER_NAME = "Marker Name"
MARKER_TYPE = "Marker Type"
GENE_MARKER_TYPE = "Gene"


def _report_url(report_name: str) -> str:
    return f"{MGI_REPORT_BASE_URL}{report_name}"


def _source_version_for_reports(report_names: tuple[str, ...]) -> str:
    data_puller = GetData()
    report_versions = []
    for report_name in report_names:
        modified_date = data_puller.get_http_file_modified_date(_report_url(report_name))
        report_name_for_version = report_name
        if report_name_for_version.endswith(".gz"):
            report_name_for_version = Path(report_name_for_version).stem
        if report_name_for_version.endswith(".rpt"):
            report_name_for_version = Path(report_name_for_version).stem
        report_versions.append(f"{report_name_for_version}_{modified_date}")
    return "-".join(report_versions)


def _download_reports(report_names: tuple[str, ...], data_path: str) -> bool:
    for report_name in report_names:
        _download_report(report_name, data_path)
    return True


def _download_report(report_name: str, data_path: str, max_attempts: int = 20) -> None:
    os.makedirs(data_path, exist_ok=True)
    url = _report_url(report_name)
    output_path = os.path.join(data_path, report_name)
    part_path = f"{output_path}.part"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ORION MGI ingest)"}

    try:
        head_response = requests.head(url, headers=headers, timeout=30)
        head_response.raise_for_status()
        expected_size = int(head_response.headers.get("content-length") or 0)
    except Exception as e:
        raise GetDataPullError(f"Error checking MGI report size for {url}: {repr(e)}-{e}")

    existing_path = output_path if os.path.exists(output_path) else part_path
    if os.path.exists(existing_path):
        existing_size = os.path.getsize(existing_path)
        if expected_size and existing_size == expected_size:
            os.replace(existing_path, output_path)
            return
        if not expected_size:
            if existing_path == part_path:
                os.replace(part_path, output_path)
            return
        if existing_size > expected_size:
            os.remove(existing_path)

    last_error = None
    for attempt in range(1, max_attempts + 1):
        existing_part_size = os.path.getsize(part_path) if os.path.exists(part_path) else 0
        request_headers = dict(headers)
        file_mode = "wb"
        if existing_part_size:
            request_headers["Range"] = f"bytes={existing_part_size}-"
            file_mode = "ab"
        try:
            with requests.get(url, headers=request_headers, stream=True, timeout=(10, 120)) as response:
                response.raise_for_status()
                if existing_part_size and response.status_code == 200:
                    existing_part_size = 0
                    file_mode = "wb"
                with open(part_path, file_mode) as output_file:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            output_file.write(chunk)
            current_size = os.path.getsize(part_path)
            if expected_size and current_size != expected_size:
                last_error = (
                    f"Downloaded {current_size} bytes for {url}, expected {expected_size} "
                    f"(attempt {attempt}/{max_attempts})"
                )
                continue
            os.replace(part_path, output_path)
            return
        except Exception as e:
            current_size = os.path.getsize(part_path) if os.path.exists(part_path) else 0
            last_error = f"{repr(e)}-{e}; retained {current_size} bytes (attempt {attempt}/{max_attempts})"

    if os.path.exists(part_path):
        os.remove(part_path)
    raise GetDataPullError(f"Failed to download complete MGI report {url}: {last_error}")


def _pubmed_ids_to_curies(pubmed_ids: str) -> list[str]:
    publications = []
    for pubmed_id in pubmed_ids.replace(",", "|").split("|"):
        pubmed_id = pubmed_id.strip()
        if not pubmed_id:
            continue
        if pubmed_id.startswith("PMID:"):
            publications.append(pubmed_id)
        elif pubmed_id.isdigit():
            publications.append(f"PMID:{pubmed_id}")
    return publications


def _split_pipe_values(value: str) -> list[str]:
    return [item.strip() for item in value.split("|") if item.strip()]


def _shared_edge_properties() -> dict:
    return {
        KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
        AGENT_TYPE: MANUAL_AGENT,
    }


def _open_text_report(report_path: str):
    if report_path.endswith(".gz"):
        return gzip.open(report_path, mode="rt", newline="")
    return open(report_path, newline="")


def _load_marker_lookup(marker_file_path: str) -> dict[str, dict[str, str]]:
    marker_lookup = {}
    with _open_text_report(marker_file_path) as marker_file:
        marker_reader = csv.DictReader(marker_file, delimiter="\t")
        required_columns = {MARKER_ID, MARKER_SYMBOL, MARKER_NAME, MARKER_TYPE}
        missing_columns = required_columns - set(marker_reader.fieldnames or [])
        if missing_columns:
            raise SourceDataBrokenError(
                f"MRK_List2.rpt is missing required columns: {sorted(missing_columns)}"
            )
        for row in marker_reader:
            marker_lookup[row[MARKER_ID]] = {
                MARKER_SYMBOL: row[MARKER_SYMBOL],
                MARKER_NAME: row[MARKER_NAME],
                MARKER_TYPE: row[MARKER_TYPE],
            }
    return marker_lookup


def _is_gene_marker(marker_lookup: dict[str, dict[str, str]], marker_id: str) -> bool:
    marker = marker_lookup.get(marker_id)
    return bool(marker and marker[MARKER_TYPE] == GENE_MARKER_TYPE)


class MGILoader(SourceDataLoader):
    provenance_id = MGI_INFORES
    source_data_url = MGI_REPORT_BASE_URL
    license = "CC BY 4.0"
    attribution = "Mouse Genome Informatics"

    source_reports: tuple[str, ...] = ()

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_files = list(self.source_reports)

    def get_latest_source_version(self) -> str:
        return _source_version_for_reports(self.source_reports)

    def get_data(self) -> bool:
        return _download_reports(self.source_reports, self.data_path)

    def _report_path(self, report_name: str) -> str:
        return os.path.join(self.data_path, report_name)


class MGIGenePhenotypesLoader(MGILoader):
    source_id = "MGIGenePhenotypes"
    parsing_version = "1.0"
    description = "MGI genotype-to-MP phenotype annotations filtered to mouse gene markers."
    source_reports = ("MGI_GenePheno.rpt", "MRK_List2.rpt.gz")

    def parse_data(self) -> dict:
        marker_lookup = _load_marker_lookup(self._report_path("MRK_List2.rpt.gz"))
        gene_pheno_path = self._report_path("MGI_GenePheno.rpt")

        metadata = {
            "rows_read": 0,
            "gene_marker_edges": 0,
            "skipped_non_gene_marker_ids": 0,
            "skipped_missing_marker_ids": 0,
            "errors": [],
        }

        with open(gene_pheno_path, newline="") as gene_pheno_file:
            reader = csv.reader(gene_pheno_file, delimiter="\t")
            for line_number, row in enumerate(reader, start=1):
                metadata["rows_read"] += 1
                if len(row) != 8:
                    metadata["errors"].append(
                        f"line {line_number}: expected 8 columns, found {len(row)}"
                    )
                    continue

                allelic_composition = row[0]
                allele_symbols = row[1]
                allele_ids = row[2]
                genetic_background = row[3]
                mp_id = row[4]
                pubmed_ids = row[5]
                marker_ids = _split_pipe_values(row[6])
                genotype_id = row[7]

                if not mp_id:
                    metadata["errors"].append(f"line {line_number}: missing MP identifier")
                    continue

                self.output_file_writer.write_node(
                    mp_id,
                    node_types=[PHENOTYPIC_FEATURE],
                )

                for marker_id in marker_ids:
                    marker = marker_lookup.get(marker_id)
                    if not marker:
                        metadata["skipped_missing_marker_ids"] += 1
                        continue
                    if marker[MARKER_TYPE] != GENE_MARKER_TYPE:
                        metadata["skipped_non_gene_marker_ids"] += 1
                        continue

                    self.output_file_writer.write_node(
                        marker_id,
                        node_name=marker[MARKER_SYMBOL],
                        node_types=[GENE],
                        node_properties={TAXON: MOUSE_TAXON_ID},
                    )

                    edge_properties = _shared_edge_properties()
                    edge_properties.update(
                        {
                            "mgi_allelic_composition": allelic_composition,
                            "mgi_allele_symbols": allele_symbols,
                            "mgi_allele_ids": allele_ids,
                            "mgi_genetic_background": genetic_background,
                            "mgi_genotype_id": genotype_id,
                            "mgi_marker_id": marker_id,
                        }
                    )
                    publications = _pubmed_ids_to_curies(pubmed_ids)
                    if publications:
                        edge_properties[PUBLICATIONS] = publications

                    self.output_file_writer.write_edge(
                        subject_id=marker_id,
                        predicate="biolink:has_phenotype",
                        object_id=mp_id,
                        primary_knowledge_source=self.provenance_id,
                        edge_properties=edge_properties,
                    )
                    metadata["gene_marker_edges"] += 1

        return metadata


class MGIGeneDiseaseLoader(MGILoader):
    source_id = "MGIGeneDisease"
    parsing_version = "1.0"
    description = "MGI mouse gene to human disease model annotations from MGI_DO.rpt."
    source_reports = ("MGI_DO.rpt", "MRK_List2.rpt.gz")

    def parse_data(self) -> dict:
        marker_lookup = _load_marker_lookup(self._report_path("MRK_List2.rpt.gz"))
        disease_path = self._report_path("MGI_DO.rpt")
        metadata = {
            "rows_read": 0,
            "mouse_gene_rows": 0,
            "skipped_rows": 0,
            "errors": [],
        }

        with open(disease_path, newline="") as disease_file:
            disease_reader = csv.DictReader(disease_file, delimiter="\t")
            required_columns = {
                "DO Disease ID",
                "DO Disease Name",
                "NCBI Taxon ID",
                "Symbol",
                "EntrezGene ID",
                "Mouse MGI ID",
            }
            missing_columns = required_columns - set(disease_reader.fieldnames or [])
            if missing_columns:
                raise SourceDataBrokenError(
                    f"MGI_DO.rpt is missing required columns: {sorted(missing_columns)}"
                )

            for line_number, row in enumerate(disease_reader, start=2):
                metadata["rows_read"] += 1
                taxon_id = row["NCBI Taxon ID"]
                mgi_marker_id = row["Mouse MGI ID"]
                entrez_gene_id = row["EntrezGene ID"]
                disease_id = row["DO Disease ID"]

                if taxon_id != "10090" or not mgi_marker_id or not entrez_gene_id:
                    metadata["skipped_rows"] += 1
                    continue
                if not _is_gene_marker(marker_lookup, mgi_marker_id):
                    metadata["skipped_rows"] += 1
                    continue
                if not disease_id:
                    metadata["errors"].append(f"line {line_number}: missing DO Disease ID")
                    continue

                subject_id = f"{NCBIGENE}:{entrez_gene_id}"
                self.output_file_writer.write_node(
                    subject_id,
                    node_name=row["Symbol"],
                    node_types=[GENE],
                    node_properties={TAXON: MOUSE_TAXON_ID},
                )
                self.output_file_writer.write_node(
                    disease_id,
                    node_name=row["DO Disease Name"],
                    node_types=[DISEASE],
                )

                edge_properties = _shared_edge_properties()
                edge_properties["mgi_marker_id"] = mgi_marker_id

                self.output_file_writer.write_edge(
                    subject_id=subject_id,
                    predicate="biolink:model_of",
                    object_id=disease_id,
                    primary_knowledge_source=self.provenance_id,
                    edge_properties=edge_properties,
                )
                metadata["mouse_gene_rows"] += 1

        return metadata


class MGIPhenotypeAnatomyLoader(MGILoader):
    source_id = "MGIPhenotypeAnatomy"
    parsing_version = "1.0"
    description = "MGI MP to EMAPA phenotype anatomy associations."
    source_reports = ("MP_EMAPA.rpt",)

    def parse_data(self) -> dict:
        phenotype_anatomy_path = self._report_path("MP_EMAPA.rpt")
        metadata = {
            "rows_read": 0,
            "phenotype_anatomy_edges": 0,
            "errors": [],
        }

        with open(phenotype_anatomy_path, newline="") as phenotype_anatomy_file:
            reader = csv.reader(phenotype_anatomy_file, delimiter="\t")
            for line_number, row in enumerate(reader, start=1):
                metadata["rows_read"] += 1
                if len(row) != 4:
                    metadata["errors"].append(
                        f"line {line_number}: expected 4 columns, found {len(row)}"
                    )
                    continue

                mp_id, mp_label, emapa_id, emapa_label = row
                if not mp_id or not emapa_id:
                    metadata["errors"].append(f"line {line_number}: missing MP or EMAPA identifier")
                    continue

                self.output_file_writer.write_node(
                    mp_id,
                    node_name=mp_label,
                    node_types=[PHENOTYPIC_FEATURE],
                )
                self.output_file_writer.write_node(
                    emapa_id,
                    node_name=emapa_label,
                    node_types=[ANATOMICAL_ENTITY],
                )

                edge_properties = _shared_edge_properties()
                edge_properties.update(
                    {
                        "mgi_mp_label": mp_label,
                        "mgi_emapa_label": emapa_label,
                    }
                )

                self.output_file_writer.write_edge(
                    subject_id=mp_id,
                    predicate="biolink:affects",
                    object_id=emapa_id,
                    primary_knowledge_source=self.provenance_id,
                    edge_properties=edge_properties,
                )
                metadata["phenotype_anatomy_edges"] += 1

        return metadata
