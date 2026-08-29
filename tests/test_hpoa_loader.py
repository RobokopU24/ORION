import json
from pathlib import Path

import pytest

from orion.utils import GetDataPullError
from parsers.HPOA.src.loadHPOA import (
    HPOA_DISEASE_PHENOTYPE_COLUMNS,
    HPOA_GENE_PHENOTYPE_COLUMNS,
    HPOALoader,
    get_disease_curie,
    is_zero_frequency,
    publications_from_reference,
)


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def write_hpoa_files(loader: HPOALoader, phenotype_rows: list[list[str]], gene_rows: list[list[str]]) -> None:
    phenotype_path = Path(loader.data_path) / "phenotype.hpoa"
    phenotype_path.write_text(
        "#version: test\n"
        + "\t".join(HPOA_DISEASE_PHENOTYPE_COLUMNS) + "\n"
        + "\n".join("\t".join(row) for row in phenotype_rows) + "\n"
    )
    gene_path = Path(loader.data_path) / "genes_to_phenotype.txt"
    gene_path.write_text(
        "\t".join(HPOA_GENE_PHENOTYPE_COLUMNS) + "\n"
        + "\n".join("\t".join(row) for row in gene_rows) + "\n"
    )


def test_hpoa_loader_disease_phenotype_edges(tmp_path):
    loader = HPOALoader(source_data_dir=str(tmp_path))
    write_hpoa_files(
        loader,
        phenotype_rows=[
            ["OMIM:1", "disease one", "", "HP:0001", "PMID:1", "TAS", "HP:0003581", "50%", "MALE", "HP:0012825", "P", "HPO:x"],
            ["OMIM:1", "disease one", "NOT", "HP:0002", "PMID:2", "TAS", "", "", "", "", "P", ""],
            ["OMIM:1", "disease one", "", "HP:0003", "PMID:3", "TAS", "", "", "", "", "C", ""],
            ["OMIM:1", "disease one", "", "HP:0004", "PMID:4", "TAS", "", "0%", "", "", "P", ""],
            ["OMIM:1", "disease one", "", "HP:0001", "PMID:5", "TAS", "", "", "", "", "P", ""],
            ["ORPHA:2", "disease two", "", "HP:0005", "ORPHA:2", "TAS", "", "", "", "", "P", ""],
            ["OMIM:3", "disease three", "", "HP:0006", "OMIM:3;PMID:6", "TAS", "", "", "", "", "P", ""],
            ["OMIM:4", "disease four", "", "HP:0007", "ISBN-13:978-0-12-345", "TAS", "", "", "", "", "P", ""],
            ["OMIM:5", "disease five", "", "HP:0008", "http://example.org/book", "TAS", "", "", "", "", "P", ""],
        ],
        gene_rows=[],
    )

    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))
    edges_by_object = {edge["object"]: edge for edge in read_jsonl(edges_path)}

    assert metadata["disease_phenotype_source_lines"] == 9
    assert metadata["disease_phenotype_rows_skipped"] == 3
    assert metadata["disease_phenotype_duplicate_positive_rows"] == 1
    assert metadata["disease_phenotype_edges_written"] == 5
    assert set(edges_by_object) == {"HP:0001", "HP:0005", "HP:0006", "HP:0007", "HP:0008"}

    assert edges_by_object["HP:0001"] == {
        "subject": "OMIM:1",
        "predicate": "biolink:has_phenotype",
        "object": "HP:0001",
        "primary_knowledge_source": "infores:hpo-annotations",
        "knowledge_level": "knowledge_assertion",
        "agent_type": "manual_agent",
        "hpoa_disease_name": "disease one",
        "hpoa_evidence": "TAS",
        "hpoa_modifier": "HP:0012825",
        "hpoa_biocuration": "HPO:x",
        "onset_qualifier": "HP:0003581",
        "frequency_qualifier": "50%",
        "sex_qualifier": "MALE",
        "supporting_data_source": ["infores:omim"],
        "publications": ["PMID:1"],
    }

    orphanet_edge = edges_by_object["HP:0005"]
    assert orphanet_edge["subject"] == "Orphanet:2"
    assert orphanet_edge["supporting_data_source"] == ["infores:orphanet"]
    assert "publications" not in orphanet_edge

    assert edges_by_object["HP:0006"]["publications"] == ["PMID:6"]
    assert edges_by_object["HP:0007"]["publications"] == ["isbn:978-0-12-345"]
    assert edges_by_object["HP:0008"]["publications"] == ["http://example.org/book"]


def test_hpoa_loader_gene_phenotype_edges_require_a_kept_disease_phenotype_pair(tmp_path):
    loader = HPOALoader(source_data_dir=str(tmp_path))
    write_hpoa_files(
        loader,
        phenotype_rows=[
            ["OMIM:1", "disease one", "", "HP:0001", "PMID:1", "TAS", "", "", "", "", "P", ""],
            ["OMIM:1", "disease one", "", "HP:0003", "PMID:2", "TAS", "", "", "", "", "C", ""],
            ["ORPHA:2", "disease two", "", "HP:0005", "", "TAS", "", "", "", "", "P", ""],
        ],
        gene_rows=[
            ["10", "GENE1", "HP:0001", "phenotype one", "50%", "OMIM:1"],
            ["11", "GENE2", "HP:0003", "phenotype three", "", "OMIM:1"],
            ["12", "GENE3", "HP:0005", "phenotype five", "", "ORPHA:2"],
        ],
    )

    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))
    edges_by_subject = {
        edge["subject"]: edge for edge in read_jsonl(edges_path) if edge["subject"].startswith("NCBIGene")
    }

    assert metadata["gene_phenotype_source_lines"] == 3
    assert metadata["gene_phenotype_rows_skipped"] == 1
    assert metadata["gene_phenotype_edges_written"] == 2
    assert set(edges_by_subject) == {"NCBIGene:10", "NCBIGene:12"}

    assert edges_by_subject["NCBIGene:10"] == {
        "subject": "NCBIGene:10",
        "predicate": "biolink:has_phenotype",
        "object": "HP:0001",
        "primary_knowledge_source": "infores:hpo-annotations",
        "knowledge_level": "knowledge_assertion",
        "agent_type": "data_analysis_pipeline",
        "disease_context_qualifier": "OMIM:1",
        "hpoa_hpo_name": "phenotype one",
        "frequency_qualifier": "50%",
        "supporting_data_source": ["infores:omim"],
    }

    orphanet_gene_edge = edges_by_subject["NCBIGene:12"]
    assert orphanet_gene_edge["disease_context_qualifier"] == "Orphanet:2"
    assert orphanet_gene_edge["supporting_data_source"] == ["infores:orphanet"]
    assert "frequency_qualifier" not in orphanet_gene_edge


@pytest.mark.parametrize("frequency", ["0", "0.0", "0%", "0.0%", "0/3"])
def test_is_zero_frequency_recognizes_all_zero_forms(frequency):
    assert is_zero_frequency(frequency)


@pytest.mark.parametrize("frequency", ["", "50%", "1/3", "5.0%"])
def test_is_zero_frequency_rejects_nonzero_forms(frequency):
    assert not is_zero_frequency(frequency)


def test_get_disease_curie_remaps_orpha_to_orphanet_prefix():
    assert get_disease_curie("ORPHA:558") == "Orphanet:558"
    assert get_disease_curie("orpha:558") == "Orphanet:558"
    assert get_disease_curie("OMIM:154700") == "OMIM:154700"
    assert get_disease_curie("") == ""


def test_publications_from_reference_ignores_self_citations_and_unrecognized_types():
    assert publications_from_reference("OMIM:609153") == []
    assert publications_from_reference("OMIM:186579") == []
    assert publications_from_reference("") == []


def test_hpoa_get_latest_source_version_handles_undecoded_bytes(tmp_path, monkeypatch):
    loader = HPOALoader(source_data_dir=str(tmp_path))

    class MockResponse:
        def raise_for_status(self):
            pass

        def iter_lines(self):
            return iter([
                b'#description: "HPO annotations for rare diseases"',
                b"#version: 2026-06-23",
                b"#tracker: https://github.com/obophenotype/human-phenotype-ontology/issues",
            ])

    monkeypatch.setattr("parsers.HPOA.src.loadHPOA.requests.get", lambda *args, **kwargs: MockResponse())

    assert loader.get_latest_source_version() == "2026-06-23"


def test_hpoa_get_latest_source_version_raises_when_version_line_missing(tmp_path, monkeypatch):
    loader = HPOALoader(source_data_dir=str(tmp_path))

    class MockResponse:
        def raise_for_status(self):
            pass

        def iter_lines(self):
            return iter([b"#description: no version line here"])

    monkeypatch.setattr("parsers.HPOA.src.loadHPOA.requests.get", lambda *args, **kwargs: MockResponse())

    with pytest.raises(GetDataPullError):
        loader.get_latest_source_version()
