import os
from urllib.parse import parse_qs, urlparse

import jsonlines
import pytest

from orion.biolink_constants import NAMED_THING, GENE, NODE_TYPES, SUBJECT_ID, OBJECT_ID, PREDICATE
from orion.kgx_file_normalizer import KGXFileNormalizer


NODE_NORM_RESPONSE = {
    'HGNC:7432': {
        'id': {
            'identifier': 'NCBIGene:4522',
            'label': 'MTHFD1',
            'description': 'methylenetetrahydrofolate dehydrogenase, cyclohydrolase and '
                           'formyltetrahydrofolate synthetase 1',
        },
        'type': [
            'biolink:Gene',
            'biolink:GeneOrGeneProduct',
            'biolink:GenomicEntity',
            'biolink:ChemicalEntityOrGeneOrGeneProduct',
            'biolink:PhysicalEssence',
            'biolink:OntologyClass',
            'biolink:BiologicalEntity',
            'biolink:ThingWithTaxon',
            'biolink:NamedThing',
            'biolink:PhysicalEssenceOrOccurrent',
            'biolink:MacromolecularMachineMixin',
        ],
        'equivalent_identifiers': [
            {'identifier': 'NCBIGene:4522', 'label': 'MTHFD1'},
            {'identifier': 'ENSEMBL:ENSG00000100714'},
            {'identifier': 'HGNC:7432', 'label': 'MTHFD1'},
            {'identifier': 'OMIM:172460'},
            {'identifier': 'UMLS:C1417420', 'label': 'MTHFD1 gene'},
        ],
        'information_content': 84.8,
    },
    'CHEBI:33551': {
        'id': {
            'identifier': 'CHEBI:33551',
            'label': 'organosulfonic acid',
            'description': 'An organic derivative of sulfonic acid in which the sulfo group '
                           'is linked directly to carbon.',
        },
        'type': [
            'biolink:SmallMolecule',
            'biolink:MolecularEntity',
            'biolink:ChemicalEntity',
            'biolink:PhysicalEssence',
            'biolink:ChemicalOrDrugOrTreatment',
            'biolink:ChemicalEntityOrGeneOrGeneProduct',
            'biolink:ChemicalEntityOrProteinOrPolypeptide',
            'biolink:NamedThing',
            'biolink:PhysicalEssenceOrOccurrent',
        ],
        'equivalent_identifiers': [
            {'identifier': 'CHEBI:33551', 'label': 'organosulfonic acid'},
        ],
        'information_content': 55.7,
    },
    'CHEBI:15377': {
        'id': {'identifier': 'CHEBI:15377', 'label': 'water'},
        'type': ['biolink:SmallMolecule', 'biolink:ChemicalEntity', 'biolink:NamedThing'],
        'equivalent_identifiers': [{'identifier': 'CHEBI:15377', 'label': 'water'}],
        'information_content': 40.0,
    },
}

PREDICATE_MAP = {
    'SEMMEDDB:CAUSES': {'predicate': 'biolink:causes', 'identifier': 'biolink:causes'},
    # biolink:affected_by normalizes to biolink:affects with the edge direction inverted
    'biolink:affected_by': {'predicate': 'biolink:affects', 'identifier': 'biolink:affects',
                            'inverted': True},
}


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f'status {self.status_code}')


@pytest.fixture
def mock_normalization(monkeypatch):
    """Stub every HTTP touchpoint in orion.normalization and track edge-norm calls."""
    edge_norm_calls = []

    monkeypatch.setattr('orion.normalization.get_current_node_norm_version', lambda: '2.4.1')
    monkeypatch.setattr('orion.normalization.get_current_babel_version', lambda: '2025sep1')

    def fake_hit_node_norm(self, curies, retries=0):
        return {curie: NODE_NORM_RESPONSE.get(curie) for curie in curies}
    monkeypatch.setattr('orion.normalization.NodeNormalizer.hit_node_norm_service', fake_hit_node_norm)

    monkeypatch.setattr('orion.normalization.EdgeNormalizer.get_available_versions',
                        lambda self: ['v4.3.7', 'v4.3.6', 'v4.2.6-rc5', 'latest'])

    def fake_requests_get(url, *args, **kwargs):
        if '/resolve_predicate' in url:
            edge_norm_calls.append(url)
            query = parse_qs(urlparse(url).query)
            predicates = query.get('predicate', [])
            result = {p: PREDICATE_MAP.get(p, {'predicate': 'biolink:related_to'})
                      for p in predicates}
            return _FakeResponse(200, result)
        raise RuntimeError(f'Unexpected requests.get call in test: {url}')
    monkeypatch.setattr('orion.normalization.requests.get', fake_requests_get)

    return {'edge_norm_calls': edge_norm_calls}


def _write_jsonl(path, items):
    with jsonlines.open(path, mode='w') as writer:
        for item in items:
            writer.write(item)


def _make_paths(tmp_path):
    return {
        'source_nodes_file_path': str(tmp_path / 'source_nodes.jsonl'),
        'nodes_output_file_path': str(tmp_path / 'nodes.jsonl'),
        'node_norm_map_file_path': str(tmp_path / 'node_norm_map.json'),
        'node_norm_failures_file_path': str(tmp_path / 'node_norm_failures.txt'),
        'source_edges_file_path': str(tmp_path / 'source_edges.jsonl'),
        'edges_output_file_path': str(tmp_path / 'edges.jsonl'),
        'edge_norm_predicate_map_file_path': str(tmp_path / 'edge_norm_predicate_map.json'),
    }


def test_kgx_file_normalizer_basic(tmp_path, mock_normalization):
    paths = _make_paths(tmp_path)
    nodes = [
        {'id': 'HGNC:7432', 'name': 'MTHFD1', NODE_TYPES: [GENE]},
        {'id': 'CHEBI:33551', 'name': '', NODE_TYPES: [NAMED_THING]},
    ]
    edges = [
        {SUBJECT_ID: 'HGNC:7432', OBJECT_ID: 'CHEBI:33551', PREDICATE: 'SEMMEDDB:CAUSES'},
    ]
    _write_jsonl(paths['source_nodes_file_path'], nodes)
    _write_jsonl(paths['source_edges_file_path'], edges)

    normalizer = KGXFileNormalizer(**paths, default_provenance='infores:testing')
    metadata = normalizer.normalize_kgx_files()

    assert metadata['final_normalized_nodes'] > 0
    assert metadata['final_normalized_edges'] == 1

    output_edges = list(jsonlines.open(paths['edges_output_file_path']))
    assert len(output_edges) == 1
    assert output_edges[0][PREDICATE] == 'biolink:causes'
    assert output_edges[0][SUBJECT_ID] == 'NCBIGene:4522'
    assert output_edges[0][OBJECT_ID] == 'CHEBI:33551'

    assert os.path.exists(paths['edge_norm_predicate_map_file_path'])
    assert mock_normalization['edge_norm_calls'], 'expected at least one bl_lookup call'


def test_predicates_pre_normalized_preserves_predicates(tmp_path, mock_normalization):
    paths = _make_paths(tmp_path)
    nodes = [
        {'id': 'HGNC:7432', 'name': 'MTHFD1', NODE_TYPES: [GENE]},
        {'id': 'CHEBI:33551', 'name': '', NODE_TYPES: [NAMED_THING]},
    ]
    # A predicate that bl_lookup would otherwise rewrite to biolink:causes — keeping it verbatim
    # proves the edge normalization path was skipped.
    preserved_predicate = 'SEMMEDDB:CAUSES'
    edges = [
        {SUBJECT_ID: 'HGNC:7432', OBJECT_ID: 'CHEBI:33551', PREDICATE: preserved_predicate},
    ]
    _write_jsonl(paths['source_nodes_file_path'], nodes)
    _write_jsonl(paths['source_edges_file_path'], edges)

    normalizer = KGXFileNormalizer(**paths,
                                   default_provenance='infores:testing',
                                   predicates_pre_normalized=True)

    # no EdgeNormalizer instance — guarantees no bl_lookup calls were made during construction
    assert normalizer.edge_normalizer is None

    normalizer.normalize_kgx_files()

    output_edges = list(jsonlines.open(paths['edges_output_file_path']))
    assert len(output_edges) == 1
    assert output_edges[0][PREDICATE] == preserved_predicate

    # no predicate map file should be written when predicates are pre-normalized
    assert not os.path.exists(paths['edge_norm_predicate_map_file_path'])

    # and no bl_lookup HTTP calls should have happened at any point
    assert mock_normalization['edge_norm_calls'] == []


def test_unconnected_nodes_are_removed(tmp_path, mock_normalization):
    paths = _make_paths(tmp_path)
    nodes = [
        {'id': 'HGNC:7432', 'name': 'MTHFD1', NODE_TYPES: [GENE]},
        {'id': 'CHEBI:33551', 'name': '', NODE_TYPES: [NAMED_THING]},
        # no edge references this node — should be dropped from the output by default
        {'id': 'CHEBI:15377', 'name': 'water', NODE_TYPES: [NAMED_THING]},
    ]
    edges = [
        {SUBJECT_ID: 'HGNC:7432', OBJECT_ID: 'CHEBI:33551', PREDICATE: 'SEMMEDDB:CAUSES'},
    ]
    _write_jsonl(paths['source_nodes_file_path'], nodes)
    _write_jsonl(paths['source_edges_file_path'], edges)

    normalizer = KGXFileNormalizer(**paths, default_provenance='infores:testing')
    metadata = normalizer.normalize_kgx_files()

    output_node_ids = {n['id'] for n in jsonlines.open(paths['nodes_output_file_path'])}
    assert 'CHEBI:15377' not in output_node_ids
    assert {'NCBIGene:4522', 'CHEBI:33551'} <= output_node_ids
    assert metadata['unconnected_nodes_removed'] == 1


def test_unconnected_nodes_preserved_when_flag_set(tmp_path, mock_normalization):
    paths = _make_paths(tmp_path)
    nodes = [
        {'id': 'HGNC:7432', 'name': 'MTHFD1', NODE_TYPES: [GENE]},
        {'id': 'CHEBI:33551', 'name': '', NODE_TYPES: [NAMED_THING]},
        {'id': 'CHEBI:15377', 'name': 'water', NODE_TYPES: [NAMED_THING]},
    ]
    edges = [
        {SUBJECT_ID: 'HGNC:7432', OBJECT_ID: 'CHEBI:33551', PREDICATE: 'SEMMEDDB:CAUSES'},
    ]
    _write_jsonl(paths['source_nodes_file_path'], nodes)
    _write_jsonl(paths['source_edges_file_path'], edges)

    normalizer = KGXFileNormalizer(**paths,
                                   default_provenance='infores:testing',
                                   preserve_unconnected_nodes=True)
    metadata = normalizer.normalize_kgx_files()

    output_node_ids = {n['id'] for n in jsonlines.open(paths['nodes_output_file_path'])}
    assert 'CHEBI:15377' in output_node_ids
    assert metadata['unconnected_nodes_removed'] == 0


def test_edge_inversion_swaps_subject_and_object(tmp_path, mock_normalization):
    paths = _make_paths(tmp_path)
    nodes = [
        {'id': 'HGNC:7432', 'name': 'MTHFD1', NODE_TYPES: [GENE]},
        {'id': 'CHEBI:33551', 'name': '', NODE_TYPES: [NAMED_THING]},
    ]
    # biolink:affected_by normalizes to biolink:affects with inverted=True,
    # so subject and object should swap in the output.
    edges = [
        {SUBJECT_ID: 'HGNC:7432', OBJECT_ID: 'CHEBI:33551', PREDICATE: 'biolink:affected_by'},
    ]
    _write_jsonl(paths['source_nodes_file_path'], nodes)
    _write_jsonl(paths['source_edges_file_path'], edges)

    normalizer = KGXFileNormalizer(**paths, default_provenance='infores:testing')
    normalizer.normalize_kgx_files()

    output_edges = list(jsonlines.open(paths['edges_output_file_path']))
    assert len(output_edges) == 1
    edge = output_edges[0]
    assert edge[PREDICATE] == 'biolink:affects'
    # original edge was HGNC:7432 -affected_by-> CHEBI:33551
    # after inversion: CHEBI:33551 -affects-> NCBIGene:4522
    assert edge[SUBJECT_ID] == 'CHEBI:33551'
    assert edge[OBJECT_ID] == 'NCBIGene:4522'