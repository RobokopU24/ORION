import gzip
import json
from pathlib import Path

import pytest

from parsers.textminingkp.src.loadTMKP import TMKPLoader, TREATS_DROPPED_FILE, cleanup_file

TREATS = 'biolink:treats_or_applied_or_studied_to_treat'
CHEM_GENE = 'biolink:ChemicalToGeneAssociation'
GENE_GENE = 'biolink:GeneRegulatoryRelationship'
CHEM_DISEASE = 'biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation'


def sentence(sentence_id: str, text: str, paper: str) -> dict:
    """A TMKP supporting study result attribute."""
    return {'attribute_type_id': 'biolink:has_supporting_study_result', 'value': sentence_id,
            'value_type_id': 'biolink:TextMiningResult',
            'attributes': [{'attribute_type_id': 'biolink:supporting_text', 'value': text},
                           {'attribute_type_id': 'biolink:publications', 'value': paper}]}


def edge_row(subject: str, predicate: str, obj: str, edge_id: str, relation: str, direction: str = '',
             sentences: tuple = (('tmkp:s1', 'text one', 'PMID:1'),)) -> str:
    """One row of TMKP's edges.tsv layout (19 columns, no header)."""
    columns = [''] * 19
    columns[0], columns[1], columns[2] = subject, predicate, obj
    if direction:
        columns[3], columns[8], columns[9] = 'biolink:causes', 'activity_or_abundance', direction
    columns[13], columns[14], columns[15] = edge_id, relation, '0.9'
    columns[16] = '|'.join(s[0] for s in sentences)
    columns[17] = '|'.join(s[2] for s in sentences)
    columns[18] = json.dumps([sentence(*s) for s in sentences])
    return '\t'.join(columns)


def write_gz(path: Path, lines: list[str]) -> None:
    with gzip.open(path, 'wt') as f:
        f.write(''.join(line + '\n' for line in lines))


@pytest.fixture
def loaded(tmp_path):
    """Load a small TMKP release with cleanup lists; return (metadata, edges by id)."""
    loader = TMKPLoader(source_data_dir=str(tmp_path))
    data = Path(loader.data_path)
    write_gz(data / 'edges.tsv.gz', [
        edge_row('CHEBI:1', TREATS, 'MONDO:1', 'treats-dropped', CHEM_DISEASE),
        edge_row('CHEBI:2', TREATS, 'MONDO:2', 'treats-kept', CHEM_DISEASE),
        # shares the dropped treats edge's id, but the treats list doesn't judge contributes_to edges
        edge_row('CHEBI:1', 'biolink:contributes_to', 'MONDO:1', 'treats-dropped', CHEM_DISEASE),
        edge_row('CHEBI:3', 'biolink:affects', 'UniProtKB:P1', 'chem-1', CHEM_GENE, 'increased'),
        # same id, other direction: not on the list
        edge_row('CHEBI:3', 'biolink:affects', 'UniProtKB:P1', 'chem-1', CHEM_GENE, 'decreased'),
        edge_row('UniProtKB:P2', 'biolink:affects', 'UniProtKB:P3', 'gene-1', GENE_GENE, 'increased',
                 (('tmkp:keep', 'supports', 'PMID:10'), ('tmkp:prune', 'does not', 'PMID:11'))),
        edge_row('CHEBI:7', 'biolink:contributes_to', 'MONDO:7', 'cd-1', CHEM_DISEASE),
        # shares the dropped contributes_to edge's id, but the contributes_to list doesn't judge treats edges
        edge_row('CHEBI:7', TREATS, 'MONDO:7', 'cd-1', CHEM_DISEASE),
        edge_row('CHEBI:8', 'biolink:contributes_to', 'MONDO:8', 'cd-2', CHEM_DISEASE, '',
                 (('tmkp:keep', 'supports', 'PMID:20'), ('tmkp:prune', 'does not', 'PMID:21'))),
    ])
    write_gz(data / TREATS_DROPPED_FILE, ['treats-dropped'])
    write_gz(data / cleanup_file('chem_gene', 'dropped_edges'), ['chem-1\tincreased'])
    write_gz(data / cleanup_file('chem_gene', 'pruned_sentences'), [])
    write_gz(data / cleanup_file('chem_gene', 'repaired'), [
        edge_row('NCBIGene:5', 'biolink:regulates', 'UniProtKB:P4', 'tmkp-repair:a', GENE_GENE, 'decreased')])
    write_gz(data / cleanup_file('gene_gene', 'dropped_edges'), [])
    write_gz(data / cleanup_file('gene_gene', 'pruned_sentences'), ['gene-1\tincreased\ttmkp:prune'])
    write_gz(data / cleanup_file('gene_gene', 'repaired'), [])
    write_gz(data / cleanup_file('chem_disease', 'dropped_edges'), ['cd-1\t'])
    write_gz(data / cleanup_file('chem_disease', 'pruned_sentences'), ['cd-2\t\ttmkp:prune'])
    write_gz(data / cleanup_file('chem_disease', 'repaired'), [
        edge_row('CHEBI:9', 'biolink:contributes_to', 'MONDO:9', 'tmkp-repair:b', CHEM_DISEASE)])

    nodes_path, edges_path = tmp_path / 'nodes.jsonl', tmp_path / 'edges.jsonl'
    metadata = loader.load(str(nodes_path), str(edges_path))
    edges = [json.loads(line) for line in edges_path.read_text().splitlines()]
    return metadata, edges


def test_cleanup_counts(loaded):
    metadata, edges = loaded
    assert metadata['treats_edges_dropped_by_cleanup'] == 1
    assert metadata['sentence_filtered_edges_dropped_by_cleanup'] == 2
    assert metadata['sentences_pruned_by_cleanup'] == 2
    assert metadata['repaired_edges_added_by_cleanup'] == 2
    assert len(edges) == 8


def test_treats_list_drops_only_treats_edges(loaded):
    _, edges = loaded
    kept = {(e['subject'], e['predicate'], e['object']) for e in edges}
    assert ('CHEBI:1', TREATS, 'MONDO:1') not in kept
    assert ('CHEBI:2', TREATS, 'MONDO:2') in kept
    assert ('CHEBI:1', 'biolink:contributes_to', 'MONDO:1') in kept


def test_regulation_list_is_keyed_on_direction(loaded):
    _, edges = loaded
    directions = [e['object_direction_qualifier'] for e in edges if e['subject'] == 'CHEBI:3']
    assert directions == ['decreased']


def test_gene_regulation_is_regulates_and_pruned(loaded):
    _, edges = loaded
    [edge] = [e for e in edges if e['subject'] == 'UniProtKB:P2']
    assert edge['predicate'] == 'biolink:regulates'
    assert edge['tmkp_ids'] == ['tmkp:keep']
    assert edge['publications'] == ['PMID:10']
    assert edge['sentences'] == 'supports|PMID:10'


def test_repaired_edges_are_added(loaded):
    _, edges = loaded
    [edge] = [e for e in edges if e['subject'] == 'NCBIGene:5']
    assert edge['predicate'] == 'biolink:regulates'
    assert edge['object_direction_qualifier'] == 'decreased'


def test_contributes_to_lists_use_an_empty_direction(loaded):
    _, edges = loaded
    kept = {(e['subject'], e['predicate']) for e in edges}
    assert ('CHEBI:7', 'biolink:contributes_to') not in kept
    assert ('CHEBI:7', TREATS) in kept
    [edge] = [e for e in edges if e['subject'] == 'CHEBI:8']
    assert edge['predicate'] == 'biolink:contributes_to'
    assert edge['tmkp_ids'] == ['tmkp:keep']
    assert edge['publications'] == ['PMID:20']
    assert ('CHEBI:9', 'biolink:contributes_to') in kept
