import pytest
from Common.biolink_constants import *
from Common.normalization import NodeNormalizer, EdgeNormalizer, EdgeNormalizationResult, \
    FALLBACK_EDGE_PREDICATE, CUSTOM_NODE_TYPES
from Common.kgx_file_normalizer import invert_edge

INVALID_NODE_TYPE = "testing:Type1"


def get_node_from_list(node_id: str, nodes: list):
    for node in nodes:
        if node['id'] == node_id:
            return node
    return None


@pytest.fixture
def test_nodes():
    nodes = [
        {"id": "HGNC:7432", "name": "MTHFD1", NODE_TYPES: [GENE, INVALID_NODE_TYPE], "test_prop": 1},
        {"id": "HGNC:15301", "name": "OR6A2-test", NODE_TYPES: [GENE, INVALID_NODE_TYPE]},
        {"id": "ENSEMBL:ENSG00000184933", "name": "OR6A2_", NODE_TYPES: [GENE, INVALID_NODE_TYPE]},
        {"id": "ENSEMBL:testing_id", "name": "broken gene", NODE_TYPES: [GENE, GENE, INVALID_NODE_TYPE]},
        {"id": "TESTING:testing_id", "name": "broken gene 2", NODE_TYPES: [INVALID_NODE_TYPE]},
        {"id": "TESTING:nameless", "name": "", NODE_TYPES: [INVALID_NODE_TYPE], "test_prop": 1},
        {"id": "CHEBI:33551", NODE_TYPES: [NAMED_THING]}
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
    assert GENE in normalized_node[NODE_TYPES]
    assert NAMED_THING in normalized_node[NODE_TYPES]
    assert CUSTOM_NODE_TYPES not in normalized_node
    assert normalized_node['test_prop'] == 1

    correct_normalized_id = 'CHEBI:33551'
    normalized_id = node_normalizer.node_normalization_lookup['CHEBI:33551'][0]
    assert normalized_id == correct_normalized_id
    normalized_node = get_node_from_list(normalized_id, test_nodes)
    assert INFORMATION_CONTENT in normalized_node and normalized_node[INFORMATION_CONTENT] > 0

    normalized_id = node_normalizer.node_normalization_lookup['HGNC:15301'][0]
    normalized_id_2 = node_normalizer.node_normalization_lookup['ENSEMBL:ENSG00000184933'][0]
    assert normalized_id == normalized_id_2
    normalized_node = get_node_from_list(normalized_id, test_nodes)
    assert INVALID_NODE_TYPE not in normalized_node[NODE_TYPES]
    assert get_node_from_list('ENSEMBL:testing_id', test_nodes) is None


def test_node_norm_lenient(test_nodes):
    node_normalizer = NodeNormalizer(strict_normalization=False)
    node_normalizer.normalize_node_data(test_nodes)

    correct_normalized_id = 'NCBIGene:4522'
    normalized_id = node_normalizer.node_normalization_lookup['HGNC:7432'][0]
    assert normalized_id == correct_normalized_id
    normalized_node = get_node_from_list(normalized_id, test_nodes)
    assert normalized_node is not None
    assert NAMED_THING in normalized_node[NODE_TYPES]
    assert INVALID_NODE_TYPE in normalized_node[CUSTOM_NODE_TYPES]
    assert normalized_node['test_prop'] == 1

    normalized_id = node_normalizer.node_normalization_lookup['ENSEMBL:testing_id'][0]
    normalized_node = get_node_from_list(normalized_id, test_nodes)
    assert INVALID_NODE_TYPE in normalized_node[CUSTOM_NODE_TYPES]
    assert NAMED_THING in normalized_node[NODE_TYPES]
    assert len(normalized_node[NODE_TYPES]) == 2  # should be GENE and NAMED_THING

    normalized_id = node_normalizer.node_normalization_lookup['TESTING:nameless'][0]
    normalized_node = get_node_from_list(normalized_id, test_nodes)
    assert INVALID_NODE_TYPE in normalized_node[CUSTOM_NODE_TYPES]
    assert NAMED_THING in normalized_node[NODE_TYPES]
    assert normalized_node['name'] == 'nameless'


def test_variant_node_norm():

    variant_nodes = [
        # should split into CA771890008 and CA14401342
        {"id": "DBSNP:rs12602172", "name": "", NODE_TYPES: ["biolink:SequenceVariant"]},
        # should split into CA290493185, CA625954562, CA983647756, CA625954561
        {"id": "DBSNP:rs34762051", "name": "", NODE_TYPES: ["biolink:SequenceVariant"]},
        {"id": "DBSNP:rs146890554", "name": "", NODE_TYPES: ["biolink:SequenceVariant"]},  # CA290466079
        {"id": "HGVS:NC_000011.10:g.68032291C>G", "name": "", NODE_TYPES: ["biolink:SequenceVariant"]},  # CA6146346 / rs369602258
        {"id": "HGVS:NC_000023.9:g.32317682G>A", "name": "", NODE_TYPES: ["biolink:SequenceVariant"]},  # CA267021 / rs398123953
        {"id": "HGVS:NC_000017.10:g.43009127delG", "name": "", NODE_TYPES: ["biolink:SequenceVariant"]},  # CA8609461 / rs775219016
        {"id": "HGVS:NC_000001.40:fakehgvs.1231234A>C", "name": "", NODE_TYPES: ["biolink:SequenceVariant"]}, # nothing
        {"id": "CLINVARVARIANT:18390", "name": "", NODE_TYPES: ["biolink:SequenceVariant"]},  # CA128085 / rs671
        {"id": "BOGUS:rs999999999999", "name": "", NODE_TYPES: ["biolink:SequenceVariant"]},  # none
    ]
    variant_nodes_2 = variant_nodes.copy()

    node_normalizer = NodeNormalizer(strict_normalization=True)
    node_normalizer.normalize_sequence_variants(variant_nodes)

    assert len(variant_nodes) == 11
    assert len(node_normalizer.variant_node_splits) == 2

    # these should be removed from the list
    assert not get_node_from_list('BOGUS:rs999999999999', variant_nodes)
    assert not get_node_from_list('HGVS:NC_000001.40:fakehgvs.1231234A>C', variant_nodes)

    # the lookup and failed list should reflect failure to normalize
    assert not node_normalizer.node_normalization_lookup['BOGUS:rs999999999999']
    assert 'BOGUS:rs999999999999' in node_normalizer.failed_to_normalize_variant_ids
    assert 'HGVS:NC_000001.40:fakehgvs.1231234A>C' in node_normalizer.failed_to_normalize_variant_ids

    # check some lookup mappings
    assert node_normalizer.node_normalization_lookup['HGVS:NC_000011.10:g.68032291C>G'] == ['CAID:CA6146346']
    assert node_normalizer.node_normalization_lookup['DBSNP:rs12602172'] == ['CAID:CA771890008', 'CAID:CA14401342']
    assert len(node_normalizer.node_normalization_lookup['DBSNP:rs34762051']) == 4

    # check name uses dbSNP
    node = get_node_from_list('CAID:CA6146346', variant_nodes)
    assert node['name'] == 'rs369602258'

    # make sure nodes aren't thrown out with strict normalization off
    node_normalizer = NodeNormalizer(strict_normalization=False)
    node_normalizer.normalize_sequence_variants(variant_nodes_2)
    assert len(variant_nodes_2) == 13
    bogus_node_after_normalization = get_node_from_list('BOGUS:rs999999999999', variant_nodes_2)
    assert bogus_node_after_normalization['name'] == 'BOGUS:rs999999999999'
    assert NAMED_THING in bogus_node_after_normalization[NODE_TYPES]
    assert SEQUENCE_VARIANT in bogus_node_after_normalization[NODE_TYPES]


def test_edge_normalization():

    edge_list = [{'predicate': 'SEMMEDDB:CAUSES'},
                 {'predicate': 'RO:0000052'},
                 {'predicate': 'RO:0002200'},
                 {'predicate': 'BADPREFIX:123456'},
                 {'predicate': 'biolink:affected_by'}]
    edge_normalizer = EdgeNormalizer()
    edge_normalizer.normalize_edge_data(edge_list)

    edge_norm_result: EdgeNormalizationResult = edge_normalizer.edge_normalization_lookup['SEMMEDDB:CAUSES']
    assert edge_norm_result.predicate == 'biolink:causes'
    assert edge_norm_result.inverted is False

    edge_norm_result: EdgeNormalizationResult = edge_normalizer.edge_normalization_lookup['RO:0002200']
    assert edge_norm_result.predicate == 'biolink:has_phenotype'

    edge_norm_result: EdgeNormalizationResult = edge_normalizer.edge_normalization_lookup['BADPREFIX:123456']
    assert edge_norm_result.predicate == FALLBACK_EDGE_PREDICATE

    edge_norm_result: EdgeNormalizationResult = edge_normalizer.edge_normalization_lookup['biolink:affected_by']
    assert edge_norm_result.predicate == 'biolink:affects'
    assert edge_norm_result.inverted is True


def test_edge_inversion():
    edge_1 = {
        SUBJECT_ID: 'hgnc:1',
        OBJECT_ID: 'hgnc:2',
        ORIGINAL_SUBJECT: 'orig:1',
        ORIGINAL_OBJECT: 'orig:2',
        SUBJECT_ASPECT_QUALIFIER: 'some_aspect',
        SUBJECT_DIRECTION_QUALIFIER: 'up',
        f'{OBJECT_ID}_fake_qualifier': 'test_value',
        f'test_{OBJECT_ID}_middle': 'test_value_middle',
        f'test_end_{OBJECT_ID}': 'test_value_end',
    }
    inverted_edge = invert_edge(edge_1)
    assert inverted_edge == {
        OBJECT_ID: 'hgnc:1',
        SUBJECT_ID: 'hgnc:2',
        ORIGINAL_OBJECT: 'orig:1',
        ORIGINAL_SUBJECT: 'orig:2',
        OBJECT_ASPECT_QUALIFIER: 'some_aspect',
        OBJECT_DIRECTION_QUALIFIER: 'up',
        f'{SUBJECT_ID}_fake_qualifier': 'test_value',
        f'test_{SUBJECT_ID}_middle': 'test_value_middle',
        f'test_end_{SUBJECT_ID}': 'test_value_end',
    }
