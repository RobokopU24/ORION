import json
from pathlib import Path

import pytest

from orion.utils import GetDataPullError
from parsers.OMIM.src.loadOMIM import OMIMLoader


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_omim_keeps_phenotype_rows_with_gene_ids(tmp_path):
    loader = OMIMLoader(source_data_dir=str(tmp_path))
    data_path = Path(loader.data_path) / loader.data_file
    data_path.write_text(
        "#MIM number\tGeneID\ttype\tSource\tMedGenCUI\tComment\n"
        "100100\t123\tphenotype\tOMIM\tC0000001\tkept\n"
        "100200\t456\tgene\tOMIM\tC0000002\tskipped type\n"
        "100300\t-\tphenotype\tOMIM\tC0000003\tskipped missing gene\n"
    )
    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"

    metadata = loader.load(str(nodes_path), str(edges_path))
    edges = read_jsonl(edges_path)

    assert metadata["source_edges"] == 1
    assert edges[0]["subject"] == "NCBIGene:123"
    assert edges[0]["predicate"] == "biolink:gene_associated_with_condition"
    assert edges[0]["object"] == "OMIM:100100"
    assert edges[0]["primary_knowledge_source"] == "infores:omim"
    assert edges[0]["supporting_data_source"] == ["infores:medgen"]
    assert edges[0]["medgen_cui"] == "C0000001"
    assert edges[0]["omim_comment"] == "kept"
    # omim_type is always "phenotype" (already filtered upstream) and omim_mim_number always
    # duplicates the numeric suffix of the object id - both dead weight, dropped.
    assert "omim_type" not in edges[0]
    assert "omim_mim_number" not in edges[0]


def test_omim_get_latest_source_version_raises_when_last_modified_missing(tmp_path, monkeypatch):
    loader = OMIMLoader(source_data_dir=str(tmp_path))

    class MockResponse:
        headers = {}

        def raise_for_status(self):
            pass

    monkeypatch.setattr("parsers.OMIM.src.loadOMIM.requests.head", lambda *args, **kwargs: MockResponse())

    with pytest.raises(GetDataPullError):
        loader.get_latest_source_version()


def test_omim_get_latest_source_version_raises_on_request_failure(tmp_path, monkeypatch):
    loader = OMIMLoader(source_data_dir=str(tmp_path))

    def raise_error(*args, **kwargs):
        raise ConnectionError("network down")

    monkeypatch.setattr("parsers.OMIM.src.loadOMIM.requests.head", raise_error)

    with pytest.raises(GetDataPullError):
        loader.get_latest_source_version()
