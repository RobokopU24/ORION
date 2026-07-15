import gzip
import json
from pathlib import Path

from parsers.MGI.src.loadMGI import (
    MGIGeneDiseaseLoader,
    MGIGenePhenotypesLoader,
    MGIPhenotypeAnatomyLoader,
)


MRK_LIST2_HEADER = "\t".join(
    [
        "MGI Accession ID",
        "Chr",
        "cM Position",
        "genome coordinate start",
        "genome coordinate end",
        "strand",
        "Marker Symbol",
        "Status",
        "Marker Name",
        "Marker Type",
        "Feature Type",
        "Marker Synonyms (pipe-separated)",
    ]
)


def write_marker_file(path: Path):
    marker_text = (
        "\n".join(
            [
                MRK_LIST2_HEADER,
                "MGI:1\t1\t\t1\t2\t+\tGeneA\tO\tgene A\tGene\tprotein coding gene\t",
                "MGI:2\t1\t\t3\t4\t+\tRegionA\tO\tregion A\tComplex/Cluster/Region\tcomplex\t",
                "MGI:3\t2\t\t5\t6\t-\tGeneB\tO\tgene B\tGene\tprotein coding gene\t",
            ]
        )
        + "\n"
    )
    with gzip.open(path, "wt", encoding="utf-8") as marker_file:
        marker_file.write(marker_text)


def read_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_mgi_gene_phenotypes_filters_to_gene_markers_and_preserves_properties(tmp_path):
    loader = MGIGenePhenotypesLoader(source_data_dir=str(tmp_path))
    data_path = Path(loader.data_path)
    write_marker_file(data_path / "MRK_List2.rpt.gz")
    (data_path / "MGI_GenePheno.rpt").write_text(
        "\n".join(
            [
                "a/a\tAllele A\tMGI:allele1\tinvolves: C57BL/6J\tMP:0000001\t12345\tMGI:1\tMGI:geno1",
                "b/b\tAllele B\tMGI:allele2\tinvolves: BALB/cJ\tMP:0000002\t\tMGI:2\tMGI:geno2",
                "c/c\tAllele C\tMGI:allele3\tinvolves: 129S\tMP:0000003\tPMID:67890\tMGI:1|MGI:3\tMGI:geno3",
            ]
        )
        + "\n"
    )

    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))

    nodes = read_jsonl(nodes_path)
    edges = read_jsonl(edges_path)
    nodes_by_id = {node["id"]: node for node in nodes}

    assert metadata["source_edges"] == 3
    assert metadata["gene_marker_edges"] == 3
    assert metadata["skipped_non_gene_marker_ids"] == 1
    assert "MGI:2" not in nodes_by_id
    assert nodes_by_id["MGI:1"]["category"] == ["biolink:Gene"]
    assert nodes_by_id["MGI:1"]["taxon"] == "NCBITaxon:10090"

    first_edge = edges[0]
    assert first_edge["subject"] == "MGI:1"
    assert first_edge["predicate"] == "biolink:has_phenotype"
    assert first_edge["object"] == "MP:0000001"
    assert first_edge["primary_knowledge_source"] == "infores:mgi"
    assert first_edge["publications"] == ["PMID:12345"]
    assert first_edge["mgi_allelic_composition"] == "a/a"
    assert first_edge["mgi_allele_symbols"] == "Allele A"
    assert first_edge["mgi_allele_ids"] == "MGI:allele1"
    assert first_edge["mgi_genetic_background"] == "involves: C57BL/6J"
    assert first_edge["mgi_genotype_id"] == "MGI:geno1"
    assert first_edge["mgi_marker_id"] == "MGI:1"


def test_mgi_gene_disease_uses_mouse_gene_rows_only_and_omits_omim(tmp_path):
    loader = MGIGeneDiseaseLoader(source_data_dir=str(tmp_path))
    data_path = Path(loader.data_path)
    write_marker_file(data_path / "MRK_List2.rpt.gz")
    (data_path / "MGI_DO.rpt").write_text(
        "\n".join(
            [
                "DO Disease ID\tDO Disease Name\tOMIM IDs\tCommon Organism Name\tNCBI Taxon ID\tSymbol\tEntrezGene ID\tMouse MGI ID",
                "DOID:1\tDisease one\tOMIM:1\tmouse, laboratory\t10090\tGeneA\t101\tMGI:1",
                "DOID:1\tDisease one\tOMIM:1\thuman\t9606\tGENEA\t201\t",
                "DOID:2\tDisease two\tOMIM:2\tmouse, laboratory\t10090\tRegionA\t102\tMGI:2",
            ]
        )
        + "\n"
    )

    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))

    nodes = read_jsonl(nodes_path)
    edges = read_jsonl(edges_path)
    nodes_by_id = {node["id"]: node for node in nodes}

    assert metadata["source_edges"] == 1
    assert metadata["mouse_gene_rows"] == 1
    assert nodes_by_id["NCBIGene:101"]["category"] == ["biolink:Gene"]
    assert nodes_by_id["NCBIGene:101"]["taxon"] == "NCBITaxon:10090"
    assert nodes_by_id["DOID:1"]["category"] == ["biolink:Disease"]
    assert edges == [
        {
            "subject": "NCBIGene:101",
            "predicate": "biolink:model_of",
            "object": "DOID:1",
            "primary_knowledge_source": "infores:mgi",
            "knowledge_level": "knowledge_assertion",
            "agent_type": "manual_agent",
            "mgi_marker_id": "MGI:1",
        }
    ]
    assert "OMIM:1" not in json.dumps(edges[0])


def test_mgi_phenotype_anatomy_maps_mp_to_emapa_with_labels(tmp_path):
    loader = MGIPhenotypeAnatomyLoader(source_data_dir=str(tmp_path))
    data_path = Path(loader.data_path)
    (data_path / "MP_EMAPA.rpt").write_text(
        "MP:0000003\tabnormal adipose tissue morphology\tEMAPA:35112\tadipose tissue\n"
    )

    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"
    metadata = loader.load(str(nodes_path), str(edges_path))

    nodes = read_jsonl(nodes_path)
    edges = read_jsonl(edges_path)
    nodes_by_id = {node["id"]: node for node in nodes}

    assert metadata["source_edges"] == 1
    assert nodes_by_id["MP:0000003"]["category"] == ["biolink:PhenotypicFeature"]
    assert nodes_by_id["EMAPA:35112"]["category"] == ["biolink:AnatomicalEntity"]
    assert edges == [
        {
            "subject": "MP:0000003",
            "predicate": "biolink:affects",
            "object": "EMAPA:35112",
            "primary_knowledge_source": "infores:mgi",
            "knowledge_level": "knowledge_assertion",
            "agent_type": "manual_agent",
            "mgi_mp_label": "abnormal adipose tissue morphology",
            "mgi_emapa_label": "adipose tissue",
        }
    ]
