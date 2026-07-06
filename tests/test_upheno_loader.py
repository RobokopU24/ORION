import json
from pathlib import Path

from parsers.UPheno.src.loadUPheno import UPhenoHumanMousePhenotypeHomologyLoader


def write_test_upheno_obo(obo_path: Path) -> None:
    obo_path.write_text(
        "\n".join(
            [
                "format-version: 1.2",
                "",
                "[Term]",
                "id: UPHENO:0000001",
                "name: generic phenotype one",
                "",
                "[Term]",
                "id: UPHENO:0000002",
                "name: generic phenotype two",
                "",
                "[Term]",
                "id: UPHENO:0000003",
                "name: generic phenotype three",
                "",
                "[Term]",
                "id: HP:0000001",
                "name: human phenotype one",
                "is_a: UPHENO:0000001 ! generic phenotype one",
                "",
                "[Term]",
                "id: MP:0000001",
                "name: mouse phenotype one",
                "is_a: UPHENO:0000001 ! generic phenotype one",
                "",
                "[Term]",
                "id: HP:0000002",
                "name: human phenotype two",
                "is_a: UPHENO:0000001 ! generic phenotype one",
                "is_a: UPHENO:0000002 ! generic phenotype two",
                "",
                "[Term]",
                "id: MP:0000002",
                "name: mouse phenotype two",
                "is_a: UPHENO:0000001 ! generic phenotype one",
                "is_a: UPHENO:0000002 ! generic phenotype two",
                "",
                "[Term]",
                "id: HP:0000003",
                "name: human phenotype three",
                "is_a: UPHENO:0000003 ! generic phenotype three",
                "relationship: biolink:homologous_to MP:0000003",
                "",
                "[Term]",
                "id: MP:0000003",
                "name: mouse phenotype three",
                "is_a: UPHENO:0000003 ! generic phenotype three",
                "",
            ]
        )
        + "\n"
    )


def test_upheno_loader_infers_human_mouse_homology_edges(tmp_path):
    loader = UPhenoHumanMousePhenotypeHomologyLoader(source_data_dir=str(tmp_path))
    data_path = Path(loader.data_path)
    write_test_upheno_obo(data_path / loader.data_file)

    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))

    nodes = [json.loads(line) for line in nodes_path.read_text().splitlines()]
    edges = [json.loads(line) for line in edges_path.read_text().splitlines()]
    edges_by_subject_object = {
        (edge["subject"], edge["object"]): edge
        for edge in edges
    }

    assert metadata["source_edges"] == 4
    assert metadata["inferred_homology_edges"] == 4
    assert metadata["candidate_homology_edges"] == 6
    assert metadata["duplicate_candidate_edges"] == 1
    assert metadata["existing_homology_edges"] == 1
    assert metadata["skipped_existing_homology_edges"] == 1
    assert set(edges_by_subject_object) == {
        ("HP:0000001", "MP:0000001"),
        ("HP:0000001", "MP:0000002"),
        ("HP:0000002", "MP:0000001"),
        ("HP:0000002", "MP:0000002"),
    }
    assert edges_by_subject_object[("HP:0000001", "MP:0000001")] == {
        "subject": "HP:0000001",
        "predicate": "biolink:homologous_to",
        "object": "MP:0000001",
        "primary_knowledge_source": "infores:upheno",
        "knowledge_level": "logical_entailment",
        "agent_type": "data_analysis_pipeline",
        "supporting_data_source": "infores:upheno",
        "upheno_generic_parent": ["UPHENO:0000001"],
    }
    assert edges_by_subject_object[("HP:0000002", "MP:0000002")]["upheno_generic_parent"] == [
        "UPHENO:0000001",
        "UPHENO:0000002",
    ]
    assert {node["id"] for node in nodes} == {
        "HP:0000001",
        "MP:0000001",
        "HP:0000002",
        "MP:0000002",
    }
    assert all(node["category"] == ["biolink:PhenotypicFeature"] for node in nodes)
