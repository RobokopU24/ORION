import json
from pathlib import Path

from parsers.HPOA.src.loadHPOA import (
    HPOA_DISEASE_PHENOTYPE_COLUMNS,
    HPOA_GENE_PHENOTYPE_COLUMNS,
    HPOALoader,
)
from parsers.OMIM.src.loadOMIM import OMIMLoader
from parsers.Orphanet.src.loadOrphanet import OrphanetLoader


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def write_hpoa_inputs(loader: HPOALoader) -> None:
    phenotype_rows = [
        ["OMIM:1", "kept disease", "", "HP:0000001", "PMID:1", "TAS", "", "", "", "", "P", "HPO:probinson"],
        ["OMIM:1", "course row", "", "HP:0000002", "PMID:2", "TAS", "", "", "", "", "C", "HPO:probinson"],
        ["OMIM:1", "inheritance row", "", "HP:0000003", "PMID:3", "TAS", "", "", "", "", "I", "HPO:probinson"],
        ["OMIM:1", "modifier row", "", "HP:0000004", "PMID:4", "TAS", "", "", "", "", "M", "HPO:probinson"],
        ["OMIM:1", "history row", "", "HP:0000005", "PMID:5", "TAS", "", "", "", "", "H", "HPO:probinson"],
        ["OMIM:1", "not row", "NOT", "HP:0000006", "PMID:6", "TAS", "", "", "", "", "P", "HPO:probinson"],
        ["OMIM:1", "zero fraction", "", "HP:0000007", "PMID:7", "TAS", "", "0/3", "", "", "P", "HPO:probinson"],
        ["OMIM:1", "zero percent", "", "HP:0000008", "PMID:8", "TAS", "", "0%", "", "", "P", "HPO:probinson"],
        ["DECIPHER:2", "kept decipher", "", "HP:0000009", "PMID:9", "TAS", "", "50%", "", "", "P", "HPO:probinson"],
    ]
    gene_rows = [
        ["1", "GENE1", "HP:0000001", "kept phenotype", "", "OMIM:1"],
        ["1", "GENE1", "HP:0000002", "course phenotype", "", "OMIM:1"],
        ["2", "GENE2", "HP:0000009", "kept decipher phenotype", "50%", "DECIPHER:2"],
        ["2", "GENE2", "HP:9999999", "missing disease phenotype", "", "DECIPHER:2"],
    ]

    phenotype_path = Path(loader.data_path) / "phenotype.hpoa"
    gene_path = Path(loader.data_path) / "genes_to_phenotype.txt"
    phenotype_path.write_text(
        "#version: test\n"
        + "#"
        + "\t".join(HPOA_DISEASE_PHENOTYPE_COLUMNS)
        + "\n"
        + "\n".join("\t".join(row) for row in phenotype_rows)
        + "\n"
    )
    gene_path.write_text(
        "\t".join(HPOA_GENE_PHENOTYPE_COLUMNS)
        + "\n"
        + "\n".join("\t".join(row) for row in gene_rows)
        + "\n"
    )


def test_hpoa_filters_disease_phenotypes_and_keeps_conditioned_gene_phenotypes(tmp_path):
    loader = HPOALoader(source_data_dir=str(tmp_path))
    write_hpoa_inputs(loader)
    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"

    metadata = loader.load(str(nodes_path), str(edges_path))
    edges = read_jsonl(edges_path)
    edge_keys = {(edge["subject"], edge["predicate"], edge["object"]) for edge in edges}

    assert metadata["source_edges"] == 4
    assert edge_keys == {
        ("OMIM:1", "biolink:has_phenotype", "HP:0000001"),
        ("DECIPHER:2", "biolink:has_phenotype", "HP:0000009"),
        ("NCBIGene:1", "biolink:has_phenotype", "HP:0000001"),
        ("NCBIGene:2", "biolink:has_phenotype", "HP:0000009"),
    }

    gene_edge = next(edge for edge in edges if edge["subject"] == "NCBIGene:1")
    assert gene_edge["disease_context_qualifier"] == "OMIM:1"
    assert gene_edge["hpoa_disease_id"] == "OMIM:1"
    assert gene_edge["supporting_data_source"] == "infores:omim"

    decipher_edge = next(edge for edge in edges if edge["subject"] == "DECIPHER:2")
    assert decipher_edge["supporting_data_source"] == "infores:decipher"
    assert decipher_edge["publications"] == ["PMID:9"]


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
    assert edges[0]["supporting_data_source"] == "infores:medgen"
    assert edges[0]["medgen_cui"] == "C0000001"
    assert edges[0]["omim_comment"] == "kept"


def test_orphanet_keeps_assessed_supported_gene_disease_associations(tmp_path):
    loader = OrphanetLoader(source_data_dir=str(tmp_path))
    data_path = Path(loader.data_path) / loader.data_file
    data_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<JDBOR date="2026-01-01" version="test">
  <DisorderList>
    <Disorder>
      <OrphaCode>123</OrphaCode>
      <Name>kept disorder</Name>
      <DisorderGeneAssociationList>
        <DisorderGeneAssociation>
          <SourceOfValidation>111[PMID]</SourceOfValidation>
          <Gene>
            <Symbol>GENE1</Symbol>
            <ExternalReferenceList>
              <ExternalReference><Source>HGNC</Source><Reference>HGNC:1</Reference></ExternalReference>
            </ExternalReferenceList>
          </Gene>
          <DisorderGeneAssociationType><Name>Disease-causing germline mutation(s) in</Name></DisorderGeneAssociationType>
          <DisorderGeneAssociationStatus><Name>Assessed</Name></DisorderGeneAssociationStatus>
        </DisorderGeneAssociation>
        <DisorderGeneAssociation>
          <SourceOfValidation>222[PMID]</SourceOfValidation>
          <Gene>
            <Symbol>GENE2</Symbol>
            <ExternalReferenceList>
              <ExternalReference><Source>HGNC</Source><Reference>2</Reference></ExternalReference>
            </ExternalReferenceList>
          </Gene>
          <DisorderGeneAssociationType><Name>Candidate gene tested in</Name></DisorderGeneAssociationType>
          <DisorderGeneAssociationStatus><Name>Assessed</Name></DisorderGeneAssociationStatus>
        </DisorderGeneAssociation>
        <DisorderGeneAssociation>
          <SourceOfValidation>333[PMID]</SourceOfValidation>
          <Gene>
            <Symbol>GENE3</Symbol>
            <ExternalReferenceList>
              <ExternalReference><Source>HGNC</Source><Reference>3</Reference></ExternalReference>
            </ExternalReferenceList>
          </Gene>
          <DisorderGeneAssociationType><Name>Disease-causing somatic mutation(s) in</Name></DisorderGeneAssociationType>
          <DisorderGeneAssociationStatus><Name>Not yet assessed</Name></DisorderGeneAssociationStatus>
        </DisorderGeneAssociation>
        <DisorderGeneAssociation>
          <SourceOfValidation>444[PMID]</SourceOfValidation>
          <Gene><Symbol>GENE4</Symbol><ExternalReferenceList /></Gene>
          <DisorderGeneAssociationType><Name>Major susceptibility factor in</Name></DisorderGeneAssociationType>
          <DisorderGeneAssociationStatus><Name>Assessed</Name></DisorderGeneAssociationStatus>
        </DisorderGeneAssociation>
        <DisorderGeneAssociation>
          <SourceOfValidation>555[PMID]</SourceOfValidation>
          <Gene>
            <Symbol>GENE5</Symbol>
            <ExternalReferenceList>
              <ExternalReference><Source>HGNC</Source><Reference>5</Reference></ExternalReference>
            </ExternalReferenceList>
          </Gene>
          <DisorderGeneAssociationType><Name>Biomarker tested in</Name></DisorderGeneAssociationType>
          <DisorderGeneAssociationStatus><Name>Assessed</Name></DisorderGeneAssociationStatus>
        </DisorderGeneAssociation>
      </DisorderGeneAssociationList>
    </Disorder>
  </DisorderList>
</JDBOR>
"""
    )
    nodes_path = tmp_path / "nodes.jsonl"
    edges_path = tmp_path / "edges.jsonl"

    metadata = loader.load(str(nodes_path), str(edges_path))
    edges = read_jsonl(edges_path)

    assert metadata["source_edges"] == 1
    assert edges[0]["subject"] == "HGNC:1"
    assert edges[0]["predicate"] == "biolink:gene_associated_with_condition"
    assert edges[0]["object"] == "ORPHA:123"
    assert edges[0]["primary_knowledge_source"] == "infores:orphanet"
    assert edges[0]["orphanet_association_type"] == "Disease-causing germline mutation(s) in"
    assert edges[0]["orphanet_association_status"] == "Assessed"
    assert edges[0]["orphanet_source_of_validation"] == "111[PMID]"
    assert edges[0]["orphanet_gene_symbol"] == "GENE1"
    assert edges[0]["publications"] == ["PMID:111"]
