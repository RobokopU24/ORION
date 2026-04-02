import json
import shutil
from pathlib import Path
from zipfile import ZipFile

import yaml

from orion.croissant_resolver import CroissantResolver
from orion.metadata_driven_loader import MetadataDrivenLoader
from parsers.BINDING.src.loadBINDINGDB import BINDINGDBLoader
from parsers.hgnc.src.loadHGNC import HGNCLoader
from parsers.metadata_driven.src.loadMetadataDriven import BINDINGDBCroissantLoader, HGNCCroissantLoader
from orion.parser_spec import load_parser_spec


TEST_RESOURCE_DIR = Path(__file__).parent / "resources" / "metadata_parser" / "hgnc"
BINDINGDB_RESOURCE_DIR = Path(__file__).parent / "resources" / "metadata_parser" / "bindingdb"


class HGNCTestMetadataLoader(MetadataDrivenLoader):
    pass


class BindingDBTestMetadataLoader(MetadataDrivenLoader):
    pass


def _read_jsonl(path: Path) -> list[dict]:
    with path.open("r") as handle:
        return [json.loads(line) for line in handle]


def _sorted_records(path: Path) -> list[dict]:
    return sorted(_read_jsonl(path), key=lambda record: json.dumps(record, sort_keys=True))


def _bindingdb_legacy_archive(path: Path) -> None:
    header = [f"col{i}" for i in range(46)]
    header[8] = "Ki (nM)"
    header[9] = "IC50 (nM)"
    header[10] = "Kd (nM)"
    header[11] = "EC50 (nM)"
    header[19] = "PMID"
    header[20] = "PubChem AID"
    header[21] = "Patent Number"
    header[31] = "PubChem CID"
    header[44] = "UniProt (SwissProt) Primary ID of Target Chain 1"

    def row(pubchem_cid, protein, ki="", ic50="", kd="", ec50="", pmid="", aid="", patent=""):
        values = ["" for _ in range(46)]
        values[8] = ki
        values[9] = ic50
        values[10] = kd
        values[11] = ec50
        values[19] = pmid
        values[20] = aid
        values[21] = patent
        values[31] = pubchem_cid
        values[44] = protein
        return values

    rows = [
        header,
        row("111", "P11111", ki="100", pmid="12345", aid="7001", patent="PAT-1"),
        row("111", "P11111", ki="10", pmid="23456", aid="7002", patent="PAT-1"),
        row("111", "P11111", ic50="200", pmid="12345", aid="7001"),
        row("222", "P22222", ec50="50", pmid="34567", aid="8001", patent="PAT-2"),
        row("", "P99999", ki="25", pmid="99999", aid="9999", patent="PAT-X"),
    ]

    tsv_content = "\n".join("\t".join(row_values) for row_values in rows) + "\n"
    with ZipFile(path, "w") as zip_file:
        zip_file.writestr("BindingDB_All.tsv", tsv_content)


def test_croissant_resolver_hgnc_fixture():
    resolver = CroissantResolver.from_path(str(TEST_RESOURCE_DIR / "hgnc_croissant.json"))
    assert resolver.dataset_id == "hgnc"
    assert resolver.dataset_version == "2026-03-06"

    distribution = resolver.get_distribution("hgnc/hgnc_complete_set_tsv")
    assert distribution.content_url == "https://example.org/hgnc_complete_set.txt"

    column_map = resolver.get_field_column_map("hgnc/hgnc_complete_set")
    assert column_map["hgnc_id"] == "hgnc_id"
    assert column_map["gene_group_id"] == "gene_group_id"


def test_load_parser_spec_hgnc_fixture():
    spec = load_parser_spec(str(TEST_RESOURCE_DIR / "parser.yaml"))
    assert spec.source_id == "HGNC"
    assert spec.provenance_id == "infores:hgnc"
    assert spec.input.record_set == "hgnc/hgnc_complete_set"
    assert spec.croissant.path == str(TEST_RESOURCE_DIR / "hgnc_croissant.json")


def test_metadata_driven_loader_emits_expected_hgnc_graph(tmp_path):
    source_root = tmp_path / "source_root"
    source_dir = source_root / "source"
    source_dir.mkdir(parents=True)
    shutil.copyfile(TEST_RESOURCE_DIR / "hgnc_complete_set.txt", source_dir / "hgnc_complete_set.txt")

    loader = HGNCTestMetadataLoader(
        parser_spec_path=str(TEST_RESOURCE_DIR / "parser.yaml"),
        source_data_dir=str(source_root),
    )

    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))

    assert metadata["num_source_lines"] == 3
    assert metadata["unusable_source_lines"] == 1
    assert metadata["source_nodes"] == 5
    assert metadata["source_edges"] == 3

    nodes = _read_jsonl(nodes_path)
    edges = _read_jsonl(edges_path)

    node_ids = {node["id"] for node in nodes}
    assert node_ids == {"HGNC:1", "HGNC:3", "HGNC.FAMILY:5", "HGNC.FAMILY:7", "HGNC.FAMILY:9"}

    gene_node = next(node for node in nodes if node["id"] == "HGNC:1")
    assert gene_node["name"] == "alpha-1-B glycoprotein"
    assert gene_node["category"] == ["biolink:Gene"]
    assert gene_node["symbol"] == "A1BG"

    family_node = next(node for node in nodes if node["id"] == "HGNC.FAMILY:5")
    assert family_node["name"] == "Signal family"
    assert family_node["category"] == ["biolink:GeneFamily"]

    assert {edge["predicate"] for edge in edges} == {"RO:0002350"}
    assert {edge["subject"] for edge in edges} == {"HGNC:1", "HGNC:3"}
    assert {edge["object"] for edge in edges} == {"HGNC.FAMILY:5", "HGNC.FAMILY:7", "HGNC.FAMILY:9"}

    publication_edge = next(edge for edge in edges if edge["object"] == "HGNC.FAMILY:5")
    assert publication_edge["primary_knowledge_source"] == "infores:hgnc"
    assert publication_edge["publications"] == ["PMID:12345", "PMID:23456"]


def test_metadata_driven_loader_supports_fileset_in_zip(tmp_path):
    zipped_source_root = tmp_path / "zipped_source_root"
    source_dir = zipped_source_root / "source"
    source_dir.mkdir(parents=True)

    archive_path = source_dir / "hgnc_bundle.zip"
    with ZipFile(archive_path, "w") as zip_file:
        zip_file.write(TEST_RESOURCE_DIR / "hgnc_complete_set.txt", arcname="exports/hgnc_complete_set.txt")

    croissant_path = tmp_path / "hgnc_zip_croissant.json"
    croissant_path.write_text(
        json.dumps(
            {
                "@context": {
                    "@language": "en",
                    "@vocab": "https://schema.org/",
                    "sc": "https://schema.org/",
                    "cr": "http://mlcommons.org/croissant/",
                },
                "@type": "https://schema.org/Dataset",
                "@id": "hgnc",
                "version": "2026-03-06",
                "distribution": [
                    {
                        "@type": "http://mlcommons.org/croissant/FileObject",
                        "@id": "hgnc/hgnc_zip",
                        "contentUrl": "https://example.org/hgnc_bundle.zip",
                        "encodingFormat": "application/zip",
                    },
                    {
                        "@type": "http://mlcommons.org/croissant/FileSet",
                        "@id": "hgnc/hgnc_complete_set_fileset",
                        "containedIn": [{"@id": "hgnc/hgnc_zip"}],
                        "includes": "*hgnc_complete_set.txt",
                        "encodingFormat": "text/tab-separated-values",
                    },
                ],
                "recordSet": [
                    {
                        "@type": "http://mlcommons.org/croissant/RecordSet",
                        "@id": "hgnc/hgnc_complete_set",
                        "name": "hgnc_complete_set",
                        "field": [
                            {
                                "@type": "http://mlcommons.org/croissant/Field",
                                "@id": "field/hgnc_id",
                                "name": "hgnc_id",
                                "source": {
                                    "fileSet": {"@id": "hgnc/hgnc_complete_set_fileset"},
                                    "extract": {"column": "hgnc_id"},
                                },
                            },
                            {
                                "@type": "http://mlcommons.org/croissant/Field",
                                "@id": "field/name",
                                "name": "name",
                                "source": {
                                    "fileSet": {"@id": "hgnc/hgnc_complete_set_fileset"},
                                    "extract": {"column": "name"},
                                },
                            },
                            {
                                "@type": "http://mlcommons.org/croissant/Field",
                                "@id": "field/symbol",
                                "name": "symbol",
                                "source": {
                                    "fileSet": {"@id": "hgnc/hgnc_complete_set_fileset"},
                                    "extract": {"column": "symbol"},
                                },
                            },
                            {
                                "@type": "http://mlcommons.org/croissant/Field",
                                "@id": "field/locus_group",
                                "name": "locus_group",
                                "source": {
                                    "fileSet": {"@id": "hgnc/hgnc_complete_set_fileset"},
                                    "extract": {"column": "locus_group"},
                                },
                            },
                            {
                                "@type": "http://mlcommons.org/croissant/Field",
                                "@id": "field/location",
                                "name": "location",
                                "source": {
                                    "fileSet": {"@id": "hgnc/hgnc_complete_set_fileset"},
                                    "extract": {"column": "location"},
                                },
                            },
                            {
                                "@type": "http://mlcommons.org/croissant/Field",
                                "@id": "field/gene_group_id",
                                "name": "gene_group_id",
                                "source": {
                                    "fileSet": {"@id": "hgnc/hgnc_complete_set_fileset"},
                                    "extract": {"column": "gene_group_id"},
                                },
                            },
                            {
                                "@type": "http://mlcommons.org/croissant/Field",
                                "@id": "field/gene_group",
                                "name": "gene_group",
                                "source": {
                                    "fileSet": {"@id": "hgnc/hgnc_complete_set_fileset"},
                                    "extract": {"column": "gene_group"},
                                },
                            },
                            {
                                "@type": "http://mlcommons.org/croissant/Field",
                                "@id": "field/pubmed_id",
                                "name": "pubmed_id",
                                "source": {
                                    "fileSet": {"@id": "hgnc/hgnc_complete_set_fileset"},
                                    "extract": {"column": "pubmed_id"},
                                },
                            },
                        ],
                    }
                ],
            }
        )
    )

    parser_spec_path = tmp_path / "parser.yaml"
    parser_spec_path.write_text(
        yaml.safe_dump(
            {
                "source_id": "HGNC",
                "provenance_id": "infores:hgnc",
                "parsing_version": "1.0",
                "croissant": {
                    "path": str(croissant_path),
                    "dataset_id": "hgnc",
                    "version_from": "dataset.version",
                },
                "input": {
                    "distribution": "hgnc/hgnc_complete_set_fileset",
                    "record_set": "hgnc/hgnc_complete_set",
                    "format": "tsv",
                    "header": True,
                    "delimiter": "\t",
                    "member_pattern": "*hgnc_complete_set.txt",
                },
                "row_filters": [{"exists": "gene_group_id"}],
                "emit": {
                    "nodes": [
                        {
                            "id": {"op": "field", "name": "hgnc_id"},
                            "name": {"op": "field", "name": "name"},
                            "categories": ["biolink:Gene"],
                        }
                    ],
                    "edges": [
                        {
                            "foreach": {
                                "op": "explode_zip",
                                "fields": ["gene_group_id", "gene_group"],
                                "separator": "|",
                            },
                            "subject": {"op": "field", "name": "hgnc_id"},
                            "predicate": "RO:0002350",
                            "object": {"op": "template", "value": "HGNC.FAMILY:{item.0}"},
                        }
                    ],
                },
            }
        )
    )

    loader = HGNCTestMetadataLoader(
        parser_spec_path=str(parser_spec_path),
        source_data_dir=str(zipped_source_root),
    )

    nodes_path = tmp_path / "zip_nodes.jsonl"
    edges_path = tmp_path / "zip_edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))

    assert metadata["num_source_lines"] == 3
    assert metadata["unusable_source_lines"] == 1
    assert metadata["source_nodes"] == 2
    assert metadata["source_edges"] == 3


def test_load_parser_spec_bindingdb_fixture():
    spec = load_parser_spec(str(BINDINGDB_RESOURCE_DIR / "parser.yaml"))
    assert spec.source_id == "BINDING-DB"
    assert spec.aggregate is not None
    assert spec.input.distribution == "bindingdb/all_tsv_fileset"


def test_metadata_driven_loader_bindingdb_aggregation(tmp_path):
    source_root = tmp_path / "bindingdb_source_root"
    source_dir = source_root / "source"
    source_dir.mkdir(parents=True)

    archive_path = source_dir / "BindingDB_All_202603_tsv.zip"
    with ZipFile(archive_path, "w") as zip_file:
        zip_file.write(BINDINGDB_RESOURCE_DIR / "BindingDB_All.tsv", arcname="BindingDB_All.tsv")

    loader = BindingDBTestMetadataLoader(
        parser_spec_path=str(BINDINGDB_RESOURCE_DIR / "parser.yaml"),
        source_data_dir=str(source_root),
    )

    nodes_path = tmp_path / "bindingdb_nodes.jsonl"
    edges_path = tmp_path / "bindingdb_edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))

    assert metadata["num_source_lines"] == 5
    assert metadata["unusable_source_lines"] == 1
    assert metadata["source_nodes"] == 4
    assert metadata["source_edges"] == 3

    nodes = _read_jsonl(nodes_path)
    edges = _read_jsonl(edges_path)

    assert {node["id"] for node in nodes} == {
        "PUBCHEM.COMPOUND:111",
        "UniProtKB:P11111",
        "PUBCHEM.COMPOUND:222",
        "UniProtKB:P22222",
    }

    edge_lookup = {
        (edge["subject"], edge["predicate"], edge["object"]): edge
        for edge in edges
    }

    pki_edge = edge_lookup[("PUBCHEM.COMPOUND:111", "biolink:inhibits", "UniProtKB:P11111")]
    assert pki_edge["affinity_parameter"] == "pKi"
    assert pki_edge["average_affinity_nm"] == 55.0
    assert pki_edge["affinity"] == 7.26
    assert pki_edge["supporting_affinities"] == [100.0, 10.0]
    assert pki_edge["publications"] == ["PMID:12345", "PMID:23456"]
    assert pki_edge["pubchem_assay_ids"] == ["PUBCHEM.AID:7001", "PUBCHEM.AID:7002"]
    assert pki_edge["patent_ids"] == ["PATENT:PAT-1"]
    assert pki_edge["primary_knowledge_source"] == "infores:bindingdb"

    pic50_edge = edge_lookup[("PUBCHEM.COMPOUND:111", "CTD:decreases_activity_of", "UniProtKB:P11111")]
    assert pic50_edge["affinity_parameter"] == "pIC50"
    assert pic50_edge["average_affinity_nm"] == 200.0
    assert pic50_edge["affinity"] == 6.7

    pec50_edge = edge_lookup[("PUBCHEM.COMPOUND:222", "CTD:increases_activity_of", "UniProtKB:P22222")]
    assert pec50_edge["affinity_parameter"] == "pEC50"
    assert pec50_edge["average_affinity_nm"] == 50.0
    assert pec50_edge["affinity"] == 7.3


def test_hgnc_croissant_loader_matches_legacy_loader(tmp_path):
    source_root = tmp_path / "hgnc_parity_source"
    source_dir = source_root / "source"
    source_dir.mkdir(parents=True)
    shutil.copyfile(TEST_RESOURCE_DIR / "hgnc_complete_set.txt", source_dir / "hgnc_complete_set.txt")

    legacy_nodes = tmp_path / "legacy_hgnc_nodes.jsonl"
    legacy_edges = tmp_path / "legacy_hgnc_edges.jsonl"
    legacy_loader = HGNCLoader(source_data_dir=str(source_root))
    legacy_metadata = legacy_loader.load(str(legacy_nodes), str(legacy_edges))

    croissant_nodes = tmp_path / "croissant_hgnc_nodes.jsonl"
    croissant_edges = tmp_path / "croissant_hgnc_edges.jsonl"
    croissant_loader = HGNCCroissantLoader(source_data_dir=str(source_root))
    croissant_metadata = croissant_loader.load(str(croissant_nodes), str(croissant_edges))

    assert legacy_metadata == croissant_metadata
    assert _sorted_records(legacy_nodes) == _sorted_records(croissant_nodes)
    assert _sorted_records(legacy_edges) == _sorted_records(croissant_edges)


def test_bindingdb_croissant_loader_matches_legacy_loader_on_compatible_fixture(tmp_path):
    source_root = tmp_path / "bindingdb_parity_source"
    source_dir = source_root / "source"
    source_dir.mkdir(parents=True)
    archive_path = source_dir / "BindingDB_All_202603_tsv.zip"
    _bindingdb_legacy_archive(archive_path)

    original_get_latest = BINDINGDBLoader.get_latest_source_version
    BINDINGDBLoader.get_latest_source_version = lambda self: "202603"
    try:
        legacy_nodes = tmp_path / "legacy_binding_nodes.jsonl"
        legacy_edges = tmp_path / "legacy_binding_edges.jsonl"
        legacy_loader = BINDINGDBLoader(source_data_dir=str(source_root))
        legacy_metadata = legacy_loader.load(str(legacy_nodes), str(legacy_edges))
    finally:
        BINDINGDBLoader.get_latest_source_version = original_get_latest

    croissant_nodes = tmp_path / "croissant_binding_nodes.jsonl"
    croissant_edges = tmp_path / "croissant_binding_edges.jsonl"
    croissant_loader = BINDINGDBCroissantLoader(source_data_dir=str(source_root))
    croissant_metadata = croissant_loader.load(str(croissant_nodes), str(croissant_edges))

    assert legacy_metadata["source_nodes"] == croissant_metadata["source_nodes"]
    assert legacy_metadata["source_edges"] == croissant_metadata["source_edges"]

    legacy_nodes_json = _sorted_records(legacy_nodes)
    croissant_nodes_json = _sorted_records(croissant_nodes)
    assert legacy_nodes_json == croissant_nodes_json

    legacy_edges_json = _sorted_records(legacy_edges)
    croissant_edges_json = _sorted_records(croissant_edges)
    assert legacy_edges_json == croissant_edges_json
