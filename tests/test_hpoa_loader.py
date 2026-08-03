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
    # phenotype.hpoa's real header line has no leading '#' (unlike the description/version/tracker
    # lines above it) - mirrored here since iter_hpoa_tsv's header-fallback detection exists
    # specifically to handle that.
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
            # kept: exercises every qualifier/property this row shape can produce
            ["OMIM:1", "disease one", "", "HP:0001", "PMID:1", "TAS", "HP:0003581", "50%", "MALE", "HP:0012825", "P", "HPO:x"],
            # excluded: qualifier is non-empty ("NOT")
            ["OMIM:1", "disease one", "NOT", "HP:0002", "PMID:2", "TAS", "", "", "", "", "P", ""],
            # excluded: aspect isn't "P" (course, not phenotypic abnormality)
            ["OMIM:1", "disease one", "", "HP:0003", "PMID:3", "TAS", "", "", "", "", "C", ""],
            # excluded: zero frequency
            ["OMIM:1", "disease one", "", "HP:0004", "PMID:4", "TAS", "", "0%", "", "", "P", ""],
            # duplicate of the first row's (disease, hpo) pair - must not produce a second edge
            ["OMIM:1", "disease one", "", "HP:0001", "PMID:5", "TAS", "", "", "", "", "P", ""],
            # kept: ORPHA prefix remapped to Orphanet, reference is a pure self-citation (dropped)
            ["ORPHA:2", "disease two", "", "HP:0005", "ORPHA:2", "TAS", "", "", "", "", "P", ""],
            # kept: reference mixes a self-citation with a real PMID - only the PMID survives
            ["OMIM:3", "disease three", "", "HP:0006", "OMIM:3;PMID:6", "TAS", "", "", "", "", "P", ""],
            # kept: ISBN reference
            ["OMIM:4", "disease four", "", "HP:0007", "ISBN-13:978-0-12-345", "TAS", "", "", "", "", "P", ""],
            # kept: bare URL reference (e.g. NCBI Bookshelf/GeneReviews)
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
            # aspect isn't "P", so (OMIM:1, HP:0003) never enters kept_pairs
            ["OMIM:1", "disease one", "", "HP:0003", "PMID:2", "TAS", "", "", "", "", "C", ""],
            ["ORPHA:2", "disease two", "", "HP:0005", "", "TAS", "", "", "", "", "P", ""],
        ],
        gene_rows=[
            # kept: (OMIM:1, HP:0001) was kept above
            ["10", "GENE1", "HP:0001", "phenotype one", "50%", "OMIM:1"],
            # excluded: (OMIM:1, HP:0003) was never kept above
            ["11", "GENE2", "HP:0003", "phenotype three", "", "OMIM:1"],
            # kept: disease id must be remapped the same way on both sides to match
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
    # The node normalizer's registered prefix is "Orphanet:" - "ORPHA:" (what HPOA/Orphadata's
    # own files use) fails to normalize entirely, silently dropping that disease's edges.
    assert get_disease_curie("ORPHA:558") == "Orphanet:558"
    assert get_disease_curie("orpha:558") == "Orphanet:558"
    assert get_disease_curie("OMIM:154700") == "OMIM:154700"
    assert get_disease_curie("") == ""


def test_publications_from_reference_extracts_pmid():
    assert publications_from_reference("PMID:19890111") == ["PMID:19890111"]


def test_publications_from_reference_extracts_pmid_alongside_self_citation():
    # a database self-citation combined with a real PMID via ';' - only the PMID is a publication
    assert publications_from_reference("OMIM:107480;PMID:11478532") == ["PMID:11478532"]


def test_publications_from_reference_extracts_isbn():
    assert publications_from_reference("ISBN-10:0-19-262896-8") == ["isbn:0-19-262896-8"]
    assert publications_from_reference("ISBN-13:978-0721606156") == ["isbn:978-0721606156"]
    assert publications_from_reference("ISBN:3642035590") == ["isbn:3642035590"]


def test_publications_from_reference_extracts_multiple_isbns():
    assert publications_from_reference("ISBN-13:978-0-12-383834-6;ISBN-13:978-3-7945-2657-4") == [
        "isbn:978-0-12-383834-6",
        "isbn:978-3-7945-2657-4",
    ]


def test_publications_from_reference_extracts_bare_url():
    # e.g. an NCBI Bookshelf/GeneReviews chapter link - biolink's own
    # GeneAffectsChemicalAssociation example uses a bare URL in `publications`.
    url = "http://www.ncbi.nlm.nih.gov/bookshelf/br.fcgi?book=gene&part=whs"
    assert publications_from_reference(url) == [url]


def test_publications_from_reference_extracts_url_alongside_isbn():
    url = "http://www.ncbi.nlm.nih.gov/bookshelf/br.fcgi?book=gene&part=ofd1"
    assert publications_from_reference(f"{url};ISBN-13:978-0721606156") == [
        url,
        "isbn:978-0721606156",
    ]


def test_publications_from_reference_ignores_self_citations_and_unrecognized_types():
    # a disease database citing itself carries no information not already on the edge
    assert publications_from_reference("OMIM:609153") == []
    # a cross-reference to a different (unrelated) OMIM entry isn't a publication
    assert publications_from_reference("OMIM:186579") == []
    assert publications_from_reference("") == []


def test_hpoa_get_latest_source_version_handles_undecoded_bytes(tmp_path, monkeypatch):
    # phenotype.hpoa is served with no charset in Content-Type (application/octet-stream),
    # so requests' iter_lines(decode_unicode=True) silently yields raw bytes instead of str.
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
