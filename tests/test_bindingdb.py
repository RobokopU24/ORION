import json
import zipfile

import pytest

from orion.biolink_constants import AFFINITY, AFFINITY_PARAMETER
from parsers.BINDING.src.loadBINDINGDB import BINDINGDBLoader, negative_log, parse_affinity_nm


def make_bindingdb_row(values):
    row = [""] * 46
    for index, value in values.items():
        row[index] = value
    return "\t".join(row)


@pytest.mark.parametrize(
    ("raw_value", "expected_nm"),
    [
        (".10,000", 0.1),
        ("10,000", 10000.0),
        ("<10,000", 10000.0),
        (" 1,234.5 ", 1234.5),
    ],
)
def test_parse_affinity_nm_removes_bindingdb_commas(raw_value, expected_nm):
    assert parse_affinity_nm(raw_value) == expected_nm


def test_parse_affinity_nm_skips_zero_values():
    assert parse_affinity_nm("0") is None


def test_parse_affinity_nm_fails_on_invalid_values():
    with pytest.raises(ValueError):
        parse_affinity_nm("not-a-number")


def test_bindingdb_get_data_uses_shared_session_gate(tmp_path, monkeypatch):
    monkeypatch.setattr(BINDINGDBLoader, "get_latest_source_version", lambda self: "test")

    calls = []

    def fake_pull_via_http_session_gate(self, url, data_dir, gate_url, **kwargs):
        calls.append((url, data_dir, gate_url, kwargs))
        return True

    monkeypatch.setattr("parsers.BINDING.src.loadBINDINGDB.GetData.pull_via_http_session_gate",
                        fake_pull_via_http_session_gate)

    loader = BINDINGDBLoader(test_mode=True, source_data_dir=str(tmp_path))
    assert loader.get_data() is True

    assert len(calls) == 1
    url, data_dir, gate_url, kwargs = calls[0]
    assert url == f"https://www.bindingdb.org/rwd/bind/downloads/{loader.archive_file}"
    assert data_dir == str(tmp_path / "source")
    assert gate_url.endswith("SDFdownload.jsp")
    assert kwargs["gate_params"]["download_file"] == (
        f"/rwd/bind/downloads/{loader.archive_file}"
    )
    assert kwargs["expected_content_type"] == "application/zip"


def test_bindingdb_loader_outputs_expected_affinities_for_comma_values(tmp_path, monkeypatch):
    monkeypatch.setattr(BINDINGDBLoader, "get_latest_source_version", lambda self: "test")

    loader = BINDINGDBLoader(test_mode=True, source_data_dir=str(tmp_path))
    zip_path = tmp_path / "source" / loader.archive_file
    header = "\t".join(f"column_{index}" for index in range(46))
    rows = [
        make_bindingdb_row({
            8: ".10,000",
            19: "12345",
            20: "678",
            31: "111",
            44: "P11111",
            45: "tail",
        }),
        make_bindingdb_row({
            10: "<10,000",
            21: "US123",
            31: "222",
            44: "P22222",
            45: "tail",
        }),
        make_bindingdb_row({
            8: ">10,000",
            31: "333",
            44: "P33333",
            45: "tail",
        }),
        make_bindingdb_row({
            8: "0",
            31: "444",
            44: "P44444",
            45: "tail",
        }),
    ]
    with zipfile.ZipFile(zip_path, "w") as zip_file:
        zip_file.writestr(loader.bd_file_name, header + "\n" + "\n".join(rows) + "\n")

    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))

    edges = [json.loads(line) for line in edges_path.read_text().splitlines()]
    assert metadata["source_edges"] == 2
    assert len(edges) == 2

    pki_edge = next(edge for edge in edges if edge["subject"] == "PUBCHEM.COMPOUND:111")
    assert pki_edge[AFFINITY_PARAMETER] == "pKi"
    assert pki_edge[AFFINITY] == round(negative_log(0.1), 2)
    assert pki_edge["supporting_affinities"] == [round(negative_log(0.1), 2)]
    assert pki_edge["publications"] == ["PMID:12345"]
    assert pki_edge["pubchem_assay_ids"] == ["PUBCHEM.AID:678"]
    assert "ligand" not in pki_edge
    assert "protein" not in pki_edge

    pkd_edge = next(edge for edge in edges if edge["subject"] == "PUBCHEM.COMPOUND:222")
    assert pkd_edge[AFFINITY_PARAMETER] == "pKd"
    assert pkd_edge[AFFINITY] == round(negative_log(10000.0), 2)
    assert pkd_edge["supporting_affinities"] == [round(negative_log(10000.0), 2)]
    assert pkd_edge["patent_ids"] == ["PATENT:US123"]

    skipped_subjects = {edge["subject"] for edge in edges}
    assert "PUBCHEM.COMPOUND:333" not in skipped_subjects
    assert "PUBCHEM.COMPOUND:444" not in skipped_subjects
