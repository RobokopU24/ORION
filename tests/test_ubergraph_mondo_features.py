import json
import tarfile

from parsers.UberGraph.src.loadUG import (
    DISEASE_FEATURE_QUALIFIER,
    MONDO_DISEASE_HAS_MAJOR_FEATURE,
    RO_DISEASE_HAS_FEATURE,
    RO_HAS_MODIFIER,
    UGLoader,
)


class StubCurieConverter:
    def __init__(self):
        self.mapping = {
            "http://purl.obolibrary.org/obo/MONDO_0000001": "MONDO:0000001",
            "http://purl.obolibrary.org/obo/HP_0000001": "HP:0000001",
            "http://purl.obolibrary.org/obo/PATO_0000001": "PATO:0000001",
            "http://purl.obolibrary.org/obo/RO_0002573": RO_HAS_MODIFIER,
            "http://purl.obolibrary.org/obo/RO_0004029": RO_DISEASE_HAS_FEATURE,
        }

    def compress(self, iri):
        return self.mapping.get(iri)


def write_ubergraph_archive(source_dir):
    archive_path = source_dir / "nonredundant-graph-table.tgz"
    table_dir = source_dir / "nonredundant-graph-table"
    table_dir.mkdir()
    (table_dir / "node-labels.tsv").write_text(
        "\n".join(
            [
                "1\thttp://purl.obolibrary.org/obo/MONDO_0000001",
                "2\thttp://purl.obolibrary.org/obo/HP_0000001",
                "3\thttp://purl.obolibrary.org/obo/PATO_0000001",
            ]
        )
        + "\n"
    )
    (table_dir / "edge-labels.tsv").write_text(
        "\n".join(
            [
                "10\thttp://purl.obolibrary.org/obo/mondo#disease_has_major_feature",
                "11\thttp://purl.obolibrary.org/obo/RO_0002573",
                "12\thttp://purl.obolibrary.org/obo/RO_0004029",
            ]
        )
        + "\n"
    )
    (table_dir / "edges.tsv").write_text(
        "\n".join(
            [
                "1\t10\t2",
                "1\t11\t2",
                "3\t11\t2",
                "1\t12\t2",
            ]
        )
        + "\n"
    )
    with tarfile.open(archive_path, "w:gz") as archive:
        for file_path in table_dir.iterdir():
            archive.add(file_path, arcname=f"nonredundant-graph-table/{file_path.name}")
    return archive_path


def read_jsonl(path):
    with open(path) as lines:
        return [json.loads(line) for line in lines]


def test_ubergraph_maps_major_feature_and_scopes_modifier_filter(monkeypatch, tmp_path):
    from parsers.UberGraph.src.ubergraph import UberGraphTools

    monkeypatch.setattr(UberGraphTools, "init_curie_converter", lambda self: StubCurieConverter())

    loader = UGLoader(test_mode=True, source_data_dir=str(tmp_path))
    write_ubergraph_archive(tmp_path / "source")
    edges_out = tmp_path / "edges.jsonl"
    nodes_out = tmp_path / "nodes.jsonl"

    metadata = loader.load(str(nodes_out), str(edges_out))
    edges = read_jsonl(edges_out)

    assert metadata["num_source_lines"] == 4
    assert metadata["unusable_source_lines"] == 0
    assert metadata["lines_skipped_due_to_filtering"] == 1

    major_feature_edges = [
        edge
        for edge in edges
        if edge["subject"] == "MONDO:0000001"
        and edge["object"] == "HP:0000001"
        and edge.get("original_predicate") == MONDO_DISEASE_HAS_MAJOR_FEATURE
    ]
    assert len(major_feature_edges) == 1
    assert major_feature_edges[0]["predicate"] == RO_DISEASE_HAS_FEATURE
    assert major_feature_edges[0][DISEASE_FEATURE_QUALIFIER] == "major"

    modifier_edges = [edge for edge in edges if edge["predicate"] == RO_HAS_MODIFIER]
    assert modifier_edges == [
        {
            "subject": "PATO:0000001",
            "predicate": RO_HAS_MODIFIER,
            "object": "HP:0000001",
            "primary_knowledge_source": "infores:ubergraph",
            "knowledge_level": "knowledge_assertion",
            "agent_type": "manual_agent",
        }
    ]

    ordinary_feature_edges = [
        edge
        for edge in edges
        if edge["predicate"] == RO_DISEASE_HAS_FEATURE
        and edge.get("original_predicate") != MONDO_DISEASE_HAS_MAJOR_FEATURE
    ]
    assert len(ordinary_feature_edges) == 1
    assert DISEASE_FEATURE_QUALIFIER not in ordinary_feature_edges[0]
