import os
import re
import xml.etree.ElementTree as ET

import requests

from orion.biolink_constants import (
    AGENT_TYPE,
    DATA_PIPELINE,
    KNOWLEDGE_ASSERTION,
    KNOWLEDGE_LEVEL,
    PUBLICATIONS,
)
from orion.kgxmodel import kgxedge
from orion.loader_interface import SourceDataLoader
from orion.utils import GetData, GetDataPullError


ORPHANET_INFORES = "infores:orphanet"
ORPHADATA_PRODUCT6_URL = "http://www.orphadata.org/data/xml/en_product6.xml"
ORPHADATA_PRODUCT6_FILE = "en_product6.xml"
GENE_ASSOCIATED_WITH_CONDITION = "biolink:gene_associated_with_condition"

ASSESSED_STATUS = "Assessed"
SKIPPED_ASSOCIATION_TYPES = {
    "Biomarker tested in",
    "Candidate gene tested in",
}


class OrphanetLoader(SourceDataLoader):
    source_id = "Orphanet"
    provenance_id = ORPHANET_INFORES
    parsing_version = "1.0"
    source_data_url = ORPHADATA_PRODUCT6_URL
    attribution = "https://www.orphadata.com/"
    description = (
        "Orphadata disease-gene associations, limited to assessed associations with HGNC "
        "gene identifiers."
    )
    license = "https://www.orphadata.com/data/xml/en_product6.xml"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_file = ORPHADATA_PRODUCT6_FILE

    def get_latest_source_version(self) -> str:
        try:
            response = requests.get(ORPHADATA_PRODUCT6_URL, stream=True, timeout=30)
            response.raise_for_status()
            root_chunk = response.raw.read(4096).decode("utf-8", errors="ignore")
            date_match = re.search(r'date="([^"]+)"', root_chunk)
            version_match = re.search(r'version="([^"]+)"', root_chunk)
            if date_match:
                version = date_match.group(1)
                if version_match:
                    version = f"{version}_{version_match.group(1)}"
                return re.sub(r"[^A-Za-z0-9_.-]+", "_", version).strip("_")
        except Exception as e:
            raise GetDataPullError(f"Unable to determine latest Orphanet version: {e}")
        raise GetDataPullError("Unable to determine latest Orphanet version from XML root")

    def get_data(self) -> bool:
        GetData().pull_via_http(
            ORPHADATA_PRODUCT6_URL,
            self.data_path,
            saved_file_name=self.data_file,
        )
        return True

    def parse_data(self) -> dict:
        source_lines = 0
        skipped_rows = 0
        skipped_no_hgnc = 0
        edges_written = 0
        data_path = os.path.join(self.data_path, self.data_file)

        for disorder in iter_disorders(data_path):
            orpha_code = text(disorder, "OrphaCode")
            if not orpha_code:
                continue
            object_id = f"ORPHA:{orpha_code}"
            disorder_metadata = {
                "orphanet_disorder_name": text(disorder, "Name"),
                "orphanet_disorder_type": text(disorder, "DisorderType/Name"),
                "orphanet_disorder_group": text(disorder, "DisorderGroup/Name"),
            }

            associations = disorder.findall(
                "DisorderGeneAssociationList/DisorderGeneAssociation"
            )
            for association in associations:
                source_lines += 1
                association_type = text(association, "DisorderGeneAssociationType/Name")
                association_status = text(association, "DisorderGeneAssociationStatus/Name")
                if (
                    association_status != ASSESSED_STATUS
                    or association_type in SKIPPED_ASSOCIATION_TYPES
                ):
                    skipped_rows += 1
                    continue

                gene = association.find("Gene")
                hgnc_id = hgnc_identifier(gene)
                if not hgnc_id:
                    skipped_no_hgnc += 1
                    continue

                edge_properties = orphanet_edge_properties(
                    association,
                    association_type,
                    association_status,
                    disorder_metadata,
                )
                self.output_file_writer.write_node(hgnc_id)
                self.output_file_writer.write_node(object_id)
                self.output_file_writer.write_kgx_edge(
                    kgxedge(
                        subject_id=hgnc_id,
                        object_id=object_id,
                        predicate=GENE_ASSOCIATED_WITH_CONDITION,
                        primary_knowledge_source=self.provenance_id,
                        edgeprops=edge_properties,
                    )
                )
                edges_written += 1

        return {
            "num_source_lines": source_lines,
            "unusable_source_lines": skipped_rows + skipped_no_hgnc,
            "orphanet_rows_skipped_by_status_or_type": skipped_rows,
            "orphanet_rows_skipped_no_hgnc": skipped_no_hgnc,
            "source_edges": edges_written,
        }


def iter_disorders(path: str):
    for _event, element in ET.iterparse(path, events=("end",)):
        if element.tag == "Disorder":
            yield element
            element.clear()


def text(element: ET.Element | None, path: str) -> str:
    if element is None:
        return ""
    value = element.findtext(path)
    return value.strip() if value else ""


def hgnc_identifier(gene: ET.Element | None) -> str | None:
    if gene is None:
        return None
    for external_reference in gene.findall("ExternalReferenceList/ExternalReference"):
        if text(external_reference, "Source") != "HGNC":
            continue
        reference = text(external_reference, "Reference")
        if not reference:
            return None
        return reference if reference.startswith("HGNC:") else f"HGNC:{reference}"
    return None


def pmids_from_validation(source_of_validation: str) -> list[str]:
    pmids = [f"PMID:{pmid}" for pmid in re.findall(r"(\d+)\[PMID\]", source_of_validation)]
    pmids.extend(f"PMID:{pmid}" for pmid in re.findall(r"PMID:(\d+)", source_of_validation))
    return sorted(set(pmids))


def orphanet_edge_properties(
    association: ET.Element,
    association_type: str,
    association_status: str,
    disorder_metadata: dict,
) -> dict:
    source_of_validation = text(association, "SourceOfValidation")
    edge_properties = {
        KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
        AGENT_TYPE: DATA_PIPELINE,
        "orphanet_association_type": association_type,
        "orphanet_association_status": association_status,
        "orphanet_source_of_validation": source_of_validation,
        "orphanet_gene_symbol": text(association, "Gene/Symbol"),
        **disorder_metadata,
    }
    publications = pmids_from_validation(source_of_validation)
    if publications:
        edge_properties[PUBLICATIONS] = publications
    return edge_properties
