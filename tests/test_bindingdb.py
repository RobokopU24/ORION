import json
import zipfile

import pytest

from orion.biolink_constants import AFFINITY, AFFINITY_PARAMETER
from parsers.BINDING.src import loadBINDINGDB
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


def test_bindingdb_get_data_uses_download_gate(tmp_path, monkeypatch):
    monkeypatch.setattr(BINDINGDBLoader, "get_latest_source_version", lambda self: "test")

    class FakeResponse:
        def __init__(self, content=b"", content_type="application/zip"):
            self.content = content
            self.headers = {"Content-Type": content_type}

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size):
            yield self.content

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, **kwargs):
            self.calls.append((url, kwargs))
            if "SDFdownload.jsp" in url:
                return FakeResponse()
            return FakeResponse(content=b"zip-content")

    fake_session = FakeSession()
    monkeypatch.setattr(loadBINDINGDB.requests, "Session", lambda: fake_session)

    loader = BINDINGDBLoader(test_mode=True, source_data_dir=str(tmp_path))
    assert loader.get_data() is True

    output_path = tmp_path / "source" / loader.archive_file
    assert output_path.read_bytes() == b"zip-content"
    assert fake_session.calls[0][0].endswith("SDFdownload.jsp")
    assert fake_session.calls[0][1]["params"]["download_file"] == (
        f"/rwd/bind/downloads/{loader.archive_file}"
    )
    assert fake_session.calls[1][0] == (
        f"https://www.bindingdb.org/rwd/bind/downloads/{loader.archive_file}"
    )


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
