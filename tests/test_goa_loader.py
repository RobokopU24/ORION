import gzip
import json
from pathlib import Path

from parsers.GOA.src.loadGOA import DATACOLS, MouseGOALoader, get_goa_subject_id


def make_gaf_row(**overrides):
    row = [
        "UniProtKB",
        "P12345",
        "GENE1",
        "enables",
        "GO:0003674",
        "PMID:1",
        "IDA",
        "",
        "F",
        "gene product 1",
        "",
        "protein",
        "taxon:9606",
        "20260101",
        "UniProt",
        "",
        "",
    ]
    for column, value in overrides.items():
        row[DATACOLS[column].value] = value
    return row


def test_goa_subject_id_preserves_prefixed_ids():
    row = make_gaf_row(DB="MGI", DB_Object_ID="MGI:101757")

    assert get_goa_subject_id(row) == "MGI:101757"


def test_goa_subject_id_prefixes_unprefixed_ids():
    row = make_gaf_row(DB="UniProtKB", DB_Object_ID="P12345")

    assert get_goa_subject_id(row) == "UniProtKB:P12345"


def test_mouse_goa_loader_uses_mgi_ids_and_mouse_taxon(tmp_path):
    loader = MouseGOALoader(source_data_dir=str(tmp_path))
    data_path = Path(loader.data_path)
    gaf_path = data_path / loader.goa_data_file
    row = make_gaf_row(
        DB="MGI",
        DB_Object_ID="MGI:101757",
        DB_Object_Symbol="Cfl1",
        GO_ID="GO:0000281",
        Taxon_Interacting_taxon="taxon:10090",
        Gene_Product_Form_ID="MGI:MGI:101757",
    )
    with gzip.open(gaf_path, "wt", encoding="utf-8") as gaf:
        gaf.write("!gaf-version: 2.2\n")
        gaf.write("\t".join(row) + "\n")

    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))

    nodes = [json.loads(line) for line in nodes_path.read_text().splitlines()]
    edges = [json.loads(line) for line in edges_path.read_text().splitlines()]
    nodes_by_id = {node["id"]: node for node in nodes}

    assert metadata["source_edges"] == 1
    assert "MGI:101757" in nodes_by_id
    assert "MGI:MGI:101757" not in nodes_by_id
    assert nodes_by_id["MGI:101757"]["taxon"] == "NCBITaxon:10090"
    assert edges == [
        {
            "subject": "MGI:101757",
            "predicate": "RO:0002327",
            "object": "GO:0000281",
            "primary_knowledge_source": "infores:goa",
            "knowledge_level": "knowledge_assertion",
            "agent_type": "manual_agent",
            "publications": ["PMID:1"],
        }
    ]
