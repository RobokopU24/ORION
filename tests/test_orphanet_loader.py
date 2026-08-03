import json
from pathlib import Path

from parsers.Orphanet.src.loadOrphanet import OrphanetLoader


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


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
    assert edges[0]["object"] == "Orphanet:123"
    assert edges[0]["primary_knowledge_source"] == "infores:orphanet"
    assert edges[0]["orphanet_association_type"] == "Disease-causing germline mutation(s) in"
    assert edges[0]["orphanet_association_status"] == "Assessed"
    assert edges[0]["orphanet_source_of_validation"] == "111[PMID]"
    assert edges[0]["orphanet_gene_symbol"] == "GENE1"
    assert edges[0]["publications"] == ["PMID:111"]


def test_orphanet_get_latest_source_version_decompresses_gzip_content(tmp_path, monkeypatch):
    # The orphadata server gzip-compresses the response; response.raw.read() bypasses requests'
    # automatic decompression unless decode_content=True is passed explicitly.
    loader = OrphanetLoader(source_data_dir=str(tmp_path))
    xml_root = b'<?xml version="1.0" encoding="UTF-8"?>\n<JDBOR date="2026-06-23 07:57:31" version="1.3.42">'

    class MockRaw:
        def read(self, size, decode_content=False):
            assert decode_content is True
            return xml_root

    class MockResponse:
        raw = MockRaw()

        def raise_for_status(self):
            pass

    monkeypatch.setattr("parsers.Orphanet.src.loadOrphanet.requests.get", lambda *args, **kwargs: MockResponse())

    version = loader.get_latest_source_version()
    assert "2026-06-23" in version
    assert "1.3.42" in version
