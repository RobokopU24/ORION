import json

from parsers.drugmechdb.src.loadDrugMechDB import DrugMechDBLoader, iter_json_array
from orion.biolink_constants import QUALIFIED_PREDICATE, OBJECT_DIRECTION_QUALIFIER, OBJECT_ASPECT_QUALIFIER


def write_drugmechdb_source(tmp_path, entries):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "indication_paths.json").write_text(json.dumps(entries))


def make_direct_target_entry(dmdb_id):
    return {
        "directed": True,
        "graph": {
            "_id": dmdb_id,
            "disease": "CML (ph+)",
            "disease_mesh": "MESH:D015464",
            "drug": "imatinib",
            "drug_mesh": "MESH:D000068877",
            "drugbank": "DB:DB00619",
        },
        "links": [
            {"key": "decreases activity of", "source": "MESH:D000068877", "target": "UniProt:P00519"},
            {"key": "causes", "source": "UniProt:P00519", "target": "MESH:D015464"},
        ],
        "multigraph": True,
        "nodes": [
            {"id": "MESH:D000068877", "label": "Drug", "name": "imatinib"},
            {"id": "UniProt:P00519", "label": "Protein", "name": "BCR/ABL"},
            {"id": "MESH:D015464", "label": "Disease", "name": "CML (ph+)"},
        ],
    }


def make_metabolite_target_entry():
    return {
        "directed": True,
        "graph": {
            "_id": "DB00000_MESH_D012345_1",
            "disease": "test disease",
            "disease_mesh": "MESH:D012345",
            "drug": "test drug",
            "drug_mesh": "MESH:D000000",
            "drugbank": "DB:DB00000",
        },
        "links": [
            {"key": "has metabolite", "source": "MESH:D000000", "target": "DB:DBMET02573"},
            {"key": "decreases activity of", "source": "DB:DBMET02573", "target": "UniProt:P3535"},
            {"key": "causes", "source": "UniProt:P3535", "target": "MESH:D012345"},
        ],
        "multigraph": True,
        "nodes": [
            {"id": "MESH:D000000", "label": "Drug", "name": "test drug"},
            {"id": "DB:DBMET02573", "label": "ChemicalSubstance", "name": "metabolite"},
            {"id": "UniProt:P3535", "label": "Protein", "name": "COX2"},
            {"id": "MESH:D012345", "label": "Disease", "name": "test disease"},
        ],
    }


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_iter_json_array_streams_array_items(tmp_path):
    source_file = tmp_path / "data.json"
    source_file.write_text(json.dumps([{"id": 1}, {"id": 2}, {"id": 3}]))

    assert list(iter_json_array(str(source_file), chunk_size=5)) == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_drugmechdb_loader_outputs_merged_mechanism_and_target_edges(tmp_path, monkeypatch):
    monkeypatch.setattr(DrugMechDBLoader, "get_latest_source_version", lambda self: "test")
    entries = [
        make_direct_target_entry("DB00619_MESH_D015464_1"),
        make_direct_target_entry("DB00619_MESH_D015464_2"),
        make_metabolite_target_entry(),
    ]
    write_drugmechdb_source(tmp_path, entries)

    loader = DrugMechDBLoader(test_mode=True, source_data_dir=str(tmp_path))
    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))

    edges = read_jsonl(edges_path)
    assert metadata["record_counter"] == 3
    assert metadata["source_edges"] == 7
    assert not (tmp_path / "source" / "indication_paths.csv").exists()

    direct_mechanism = next(
        edge for edge in edges
        if edge["subject"] == "MESH:D000068877"
        and edge["object"] == "UniProtKB:P00519"
    )
    assert direct_mechanism["predicate"] == "biolink:affects"
    assert direct_mechanism["drugmechdb_path_id"] == [
        "DB00619_MESH_D015464_1",
        "DB00619_MESH_D015464_2",
    ]
    assert direct_mechanism[QUALIFIED_PREDICATE] == "biolink:causes"
    assert direct_mechanism[OBJECT_DIRECTION_QUALIFIER] == "decreased"
    assert direct_mechanism[OBJECT_ASPECT_QUALIFIER] == "activity"

    direct_target_for = next(
        edge for edge in edges
        if edge["predicate"] == "biolink:target_for"
        and edge["subject"] == "UniProtKB:P00519"
    )
    assert direct_target_for["object"] == "MESH:D015464"
    assert direct_target_for["primary_knowledge_source"] == "infores:drugmechdb"
    assert direct_target_for["drugmechdb_path_id"] == [
        "DB00619_MESH_D015464_1",
        "DB00619_MESH_D015464_2",
    ]

    metabolite_edge = next(
        edge for edge in edges
        if edge["subject"] == "MESH:D000000"
        and edge["object"] == "PUBCHEM.COMPOUND:631051"
    )
    assert metabolite_edge["predicate"] == "biolink:has_metabolite"

    metabolite_target_for = next(
        edge for edge in edges
        if edge["predicate"] == "biolink:target_for"
        and edge["subject"] == "UniProtKB:P35354"
    )
    assert metabolite_target_for["object"] == "MESH:D012345"
    assert metabolite_target_for["drugmechdb_path_id"] == ["DB00000_MESH_D012345_1"]
