import pytest
from Common.normalization import NodeNormalizer, EdgeNormalizer, EdgeNormalizationResult
from Common.node_types import ROOT_ENTITY, GENE, SEQUENCE_VARIANT, FALLBACK_EDGE_PREDICATE, CUSTOM_NODE_TYPES

INVALID_NODE_TYPE = "testing:Type1"


def get_node_from_list(node_id: str, nodes: list):
    for node in nodes:
        if node['id'] == node_id:
            return node
    return None


@pytest.fixture
def test_nodes():
    nodes = [
        {"id": "HGNC:7432", "name": "MTHFD1", "category": [GENE, INVALID_NODE_TYPE], "test_prop": 1},
        {"id": "HGNC:15301", "name": "OR6A2-test", "category": [GENE, INVALID_NODE_TYPE]},
        {"id": "ENSEMBL:ENSG00000184933", "name": "OR6A2_", "category": [GENE, INVALID_NODE_TYPE]},
        {"id": "ENSEMBL:testing_id", "name": "broken gene", "category": [GENE, INVALID_NODE_TYPE]},
        {"id": "TESTING:testing_id", "name": "broken gene 2", "category": [INVALID_NODE_TYPE]},
        {"id": "TESTING:nameless", "name": "", "category": [INVALID_NODE_TYPE], "test_prop": 1}

    ]
    return nodes


def test_node_norm(test_nodes):

    node_normalizer = NodeNormalizer()
    node_normalizer.normalize_node_data(test_nodes)

    correct_normalized_id = 'NCBIGene:4522'
    normalized_id = node_normalizer.node_normalization_lookup['HGNC:7432'][0]
    assert normalized_id == correct_normalized_id
    normalized_node = get_node_from_list(normalized_id, test_nodes)
    assert normalized_node is not None
    assert GENE in normalized_node['category']
    assert ROOT_ENTITY in normalized_node['category']
    assert CUSTOM_NODE_TYPES not in normalized_node
    assert normalized_node['test_prop'] == 1

    normalized_id = node_normalizer.node_normalization_lookup['HGNC:15301'][0]
    normalized_id_2 = node_normalizer.node_normalization_lookup['ENSEMBL:ENSG00000184933'][0]
    assert normalized_id == normalized_id_2
    normalized_node = get_node_from_list(normalized_id, test_nodes)
    assert INVALID_NODE_TYPE not in normalized_node['category']
    assert get_node_from_list('ENSEMBL:testing_id', test_nodes) is None


def test_node_norm_lenient(test_nodes):
    node_normalizer = NodeNormalizer(strict_normalization=False)
    node_normalizer.normalize_node_data(test_nodes)

    correct_normalized_id = 'NCBIGene:4522'
    normalized_id = node_normalizer.node_normalization_lookup['HGNC:7432'][0]
    assert normalized_id == correct_normalized_id
    normalized_node = get_node_from_list(normalized_id, test_nodes)
    assert normalized_node is not None
    assert ROOT_ENTITY in normalized_node['category']
    assert INVALID_NODE_TYPE in normalized_node[CUSTOM_NODE_TYPES]
    assert normalized_node['test_prop'] == 1

    normalized_id = node_normalizer.node_normalization_lookup['ENSEMBL:testing_id'][0]
    normalized_node = get_node_from_list(normalized_id, test_nodes)
    assert INVALID_NODE_TYPE in normalized_node[CUSTOM_NODE_TYPES]
    assert ROOT_ENTITY in normalized_node['category']

    normalized_id = node_normalizer.node_normalization_lookup['TESTING:nameless'][0]
    normalized_node = get_node_from_list(normalized_id, test_nodes)
    assert INVALID_NODE_TYPE in normalized_node[CUSTOM_NODE_TYPES]
    assert ROOT_ENTITY in normalized_node['category']
    assert normalized_node['name'] == 'nameless'


def test_variant_node_norm():

    variant_nodes =[
        {"id": "DBSNP:rs12602172", "name": "", "category": ["biolink:SequenceVariant"]},
        {"id": "DBSNP:rs34762051", "name": "", "category": ["biolink:SequenceVariant"]},
        {"id": "DBSNP:rs146890554", "name": "", "category": ["biolink:SequenceVariant"]},
        {"id": "HGVS:NC_000011.10:g.68032291C>G", "name": "", "category": ["biolink:SequenceVariant"]},
        {"id": "HGVS:NC_000023.9:g.32317682G>A", "name": "", "category": ["biolink:SequenceVariant"]},
        {"id": "HGVS:NC_000017.10:g.43009127delG", "name": "", "category": ["biolink:SequenceVariant"]},
        {"id": "HGVS:NC_000001.40:fakehgvs.1231234A>C", "name": "", "category": ["biolink:SequenceVariant"]},
        {"id": "CLINVARVARIANT:18390", "name": "", "category": ["biolink:SequenceVariant"]},
        {"id": "BOGUS:rs999999999999", "name": "", "category": ["biolink:SequenceVariant"]},
    ]
    variant_nodes_2 = variant_nodes.copy()

    node_normalizer = NodeNormalizer(strict_normalization=True)
    node_normalizer.normalize_sequence_variants(variant_nodes)
    assert len(variant_nodes) >= 10
    assert len(node_normalizer.variant_node_splits) == 1

    assert not node_normalizer.node_normalization_lookup['BOGUS:rs999999999999']
    assert 'BOGUS:rs999999999999' in node_normalizer.failed_to_normalize_variant_ids
    assert node_normalizer.failed_to_normalize_variant_ids['BOGUS:rs999999999999']

    assert node_normalizer.node_normalization_lookup['HGVS:NC_000011.10:g.68032291C>G'] == ['CAID:CA6146346']

    it_worked = False
    for node in variant_nodes:
        if node['id'] == 'CAID:CA6146346':
            if node['name'] == 'rs369602258':
                if ROOT_ENTITY in node['category']:
                    if SEQUENCE_VARIANT in node['category']:
                        it_worked = True
    assert it_worked

    node_normalizer = NodeNormalizer(strict_normalization=False)
    node_normalizer.normalize_sequence_variants(variant_nodes_2)

    assert len(variant_nodes_2) >= 12

    it_worked = False
    for node in variant_nodes_2:
        print(node)
        if node['id'] == 'BOGUS:rs999999999999':
            if node['name'] == 'BOGUS:rs999999999999':
                if ROOT_ENTITY in node['category']:
                    if SEQUENCE_VARIANT in node['category']:
                        it_worked = True
    assert it_worked


def test_edge_normalization():

    edge_list = [{'predicate': 'SEMMEDDB:CAUSES'},
                 {'predicate': 'RO:0000052'},
                 {'predicate': 'RO:0002200'},
                 {'predicate': 'BADPREFIX:123456'}]
    edge_normalizer = EdgeNormalizer()
    edge_normalizer.normalize_edge_data(edge_list)

    edge_norm_result: EdgeNormalizationResult = edge_normalizer.edge_normalization_lookup['SEMMEDDB:CAUSES']
    assert edge_norm_result.predicate == 'biolink:causes'
    assert edge_norm_result.inverted is False

    edge_norm_result: EdgeNormalizationResult = edge_normalizer.edge_normalization_lookup['RO:0002200']
    assert edge_norm_result.predicate == 'biolink:has_phenotype'

    edge_norm_result: EdgeNormalizationResult = edge_normalizer.edge_normalization_lookup['BADPREFIX:123456']
    assert edge_norm_result.predicate == FALLBACK_EDGE_PREDICATE
