import json
import pytest
from orion.biolink_constants import *
from orion.config import Config, config
from orion.normalization import NodeNormalizer, EdgeNormalizer, EdgeNormalizationResult, \
    FALLBACK_EDGE_PREDICATE, CUSTOM_NODE_TYPES, NormalizationScheme
from orion.kgx_file_normalizer import invert_edge
from orion.variant_norm_cache import VariantNormalizationCache, VariantNormalizationCacheError, \
    CACHED_NORMALIZATION_FAILURE, NORM_NODE_MAP_FILE_NAME, NORMALIZED_NODES_FILE_NAME

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


def test_node_norm_adds_top_level_taxa(monkeypatch):
    node_norm_response = {
        'HGNC:7432': {
            'id': {'identifier': 'NCBIGene:4522', 'label': 'MTHFD1'},
            'type': [GENE, NAMED_THING],
            'equivalent_identifiers': [{'identifier': 'NCBIGene:4522', 'label': 'MTHFD1'}],
            'taxa': ['NCBITaxon:9606'],
        },
    }

    def fake_hit_node_norm(self, curies, retries=0):
        return {curie: node_norm_response.get(curie) for curie in curies}

    monkeypatch.setattr(NodeNormalizer, 'hit_node_norm_service', fake_hit_node_norm)

    nodes = [{"id": "HGNC:7432", "name": "MTHFD1", NODE_TYPES: [GENE]}]
    node_normalizer = NodeNormalizer()
    assert node_normalizer.include_taxa is True

    node_normalizer.normalize_node_data(nodes)

    normalized_node = get_node_from_list('NCBIGene:4522', nodes)
    assert normalized_node[TAXON] == 'NCBITaxon:9606'


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

    print(node_normalizer.node_normalization_lookup)

    # assert len(variant_nodes) == 11
    assert len(node_normalizer.variant_node_splits) == 3

    # these should be removed from the list
    assert not get_node_from_list('BOGUS:rs999999999999', variant_nodes)
    assert not get_node_from_list('HGVS:NC_000001.40:fakehgvs.1231234A>C', variant_nodes)

    # the lookup and failed list should reflect failure to normalize
    assert not node_normalizer.node_normalization_lookup['BOGUS:rs999999999999']
    assert 'BOGUS:rs999999999999' in node_normalizer.failed_to_normalize_variant_ids
    assert 'HGVS:NC_000001.40:fakehgvs.1231234A>C' in node_normalizer.failed_to_normalize_variant_ids

    # check some lookup mappings
    assert node_normalizer.node_normalization_lookup['HGVS:NC_000011.10:g.68032291C>G'] == ['CAID:CA6146346']
    assert node_normalizer.node_normalization_lookup['DBSNP:rs12602172'] == ['CAID:CA771890008', 'CAID:CA14401342', 'CAID:CA2259351209']
    assert node_normalizer.node_normalization_lookup['DBSNP:rs146890554'] == ['CAID:CA290466079', 'CAID:CA2259356418']
    assert node_normalizer.node_normalization_lookup['DBSNP:rs34762051'] == ['CAID:CA290493185', 'CAID:CA625954562', 'CAID:CA983647756', 'CAID:CA625954561', 'CAID:CA983647750']

    # check name uses dbSNP
    node = get_node_from_list('CAID:CA6146346', variant_nodes)
    assert node['name'] == 'rs369602258'

    # make sure nodes aren't thrown out with strict normalization off
    node_normalizer = NodeNormalizer(strict_normalization=False)
    node_normalizer.normalize_sequence_variants(variant_nodes_2)
    assert len(variant_nodes_2) == 16
    bogus_node_after_normalization = get_node_from_list('BOGUS:rs999999999999', variant_nodes_2)
    assert bogus_node_after_normalization['name'] == 'BOGUS:rs999999999999'
    assert NAMED_THING in bogus_node_after_normalization[NODE_TYPES]
    assert SEQUENCE_VARIANT in bogus_node_after_normalization[NODE_TYPES]


TEST_VARIANT_NODE_TYPES = [SEQUENCE_VARIANT, NAMED_THING]


def make_cached_variant_node(node_id: str, name: str):
    return {'id': node_id,
            'name': name,
            NODE_TYPES: TEST_VARIANT_NODE_TYPES,
            SYNONYMS: [f'DBSNP:{name}'],
            'hgvs': [f'HGVS:{node_id}'],
            'robokop_variant_id': f'ROBO_VARIANT:{node_id}'}


@pytest.fixture
def variant_norm_cache_dir(tmp_path):
    normalized_nodes = [
        make_cached_variant_node('CAID:CA1', 'rs1'),
        # rs2 split into two nodes
        make_cached_variant_node('CAID:CA2', 'rs2'),
        make_cached_variant_node('CAID:CA3', 'rs2'),
        # rs3 also split into two, but one of them is missing from the nodes file
        make_cached_variant_node('CAID:CA4', 'rs3'),
        # a regular node, it should be ignored by the variant cache
        {'id': 'MONDO:0005374', 'name': 'bone marrow neoplasm', NODE_TYPES: [DISEASE, NAMED_THING]},
    ]
    with open(tmp_path / NORMALIZED_NODES_FILE_NAME, 'w') as normalized_nodes_file:
        for node in normalized_nodes:
            normalized_nodes_file.write(f'{json.dumps(node)}\n')

    normalization_map = {
        'DBSNP:rs1': ['CAID:CA1'],
        'DBSNP:rs2': ['CAID:CA2', 'CAID:CA3'],
        'DBSNP:rs3': ['CAID:CA4', 'CAID:CA5'],  # CAID:CA5 is not in the nodes file
        'DBSNP:rs4': None,  # failed to normalize
        'EFO:0004251': ['MONDO:0005374'],
    }
    with open(tmp_path / NORM_NODE_MAP_FILE_NAME, 'w') as norm_map_file:
        json.dump({'normalization_map': normalization_map}, norm_map_file)

    return str(tmp_path)


class FakeGeneticsNormalizer:
    """Stands in for the genetics normalizer so tests can tell what was actually sent to it."""

    def __init__(self, *args, **kwargs):
        self.variant_ids_normalized = []

    def get_sequence_variant_node_types(self):
        return TEST_VARIANT_NODE_TYPES

    def normalize_variants(self, variant_ids):
        self.variant_ids_normalized.extend(variant_ids)
        return {variant_id: [{'id': f'CAID:CA_{variant_id.split(":")[-1]}',
                              'name': variant_id.split(':')[-1],
                              'equivalent_identifiers': [variant_id],
                              'hgvs': [],
                              'robokop_variant_id': 'ROBO_VARIANT:testing'}]
                for variant_id in variant_ids}


@pytest.fixture
def fake_genetics_normalizer(monkeypatch):
    fake_normalizer = FakeGeneticsNormalizer()
    monkeypatch.setattr('orion.normalization.GeneticsNormalizer', lambda *args, **kwargs: fake_normalizer)
    return fake_normalizer


def test_variant_norm_cache_contents(variant_norm_cache_dir):
    variant_norm_cache = VariantNormalizationCache(variant_norm_cache_dir)

    # rs1, rs2 and the rs4 failure, the other two entries should not be cached
    assert len(variant_norm_cache) == 3

    cached_nodes = variant_norm_cache.get_normalized_variant_nodes('DBSNP:rs1', TEST_VARIANT_NODE_TYPES)
    assert cached_nodes == [make_cached_variant_node('CAID:CA1', 'rs1')]

    # a split variant returns all of its nodes
    cached_nodes = variant_norm_cache.get_normalized_variant_nodes('DBSNP:rs2', TEST_VARIANT_NODE_TYPES)
    assert [node['id'] for node in cached_nodes] == ['CAID:CA2', 'CAID:CA3']

    # a normalization failure is a hit, and an empty one, so that it doesn't get normalized again
    assert variant_norm_cache.get_normalized_variant_nodes('DBSNP:rs4', TEST_VARIANT_NODE_TYPES) == []

    # a variant missing any of its normalized nodes is a miss, serving it would drop part of the split
    assert variant_norm_cache.get_normalized_variant_nodes('DBSNP:rs3', TEST_VARIANT_NODE_TYPES) is None

    # regular nodes and anything else in the map are not in the variant cache
    assert variant_norm_cache.get_normalized_variant_nodes('EFO:0004251', TEST_VARIANT_NODE_TYPES) is None
    assert variant_norm_cache.get_normalized_variant_nodes('DBSNP:rs9999', TEST_VARIANT_NODE_TYPES) is None


# cached nodes are handed off to the caller, which writes and mutates them,
# so a second look up of the same variant must not see the first one's changes
def test_variant_norm_cache_returns_copies(variant_norm_cache_dir):
    variant_norm_cache = VariantNormalizationCache(variant_norm_cache_dir)
    cached_node = variant_norm_cache.get_normalized_variant_nodes('DBSNP:rs1', TEST_VARIANT_NODE_TYPES)[0]
    cached_node['name'] = 'mutated'
    cached_node_again = variant_norm_cache.get_normalized_variant_nodes('DBSNP:rs1', TEST_VARIANT_NODE_TYPES)[0]
    assert cached_node_again['name'] == 'rs1'


def test_variant_norm_cache_missing_files(tmp_path):
    with pytest.raises(VariantNormalizationCacheError):
        VariantNormalizationCache(str(tmp_path))


def test_variant_norm_with_cache(monkeypatch, variant_norm_cache_dir, fake_genetics_normalizer):
    monkeypatch.setattr(config, 'ORION_VARIANT_NORM_CACHE', variant_norm_cache_dir)

    variant_nodes = [{"id": variant_id, "name": "", NODE_TYPES: [SEQUENCE_VARIANT]}
                     for variant_id in ['DBSNP:rs1', 'DBSNP:rs2', 'DBSNP:rs3', 'DBSNP:rs4', 'DBSNP:rs5']]

    node_normalizer = NodeNormalizer(strict_normalization=True)
    node_normalizer.normalize_sequence_variants(variant_nodes)

    # only the variants that weren't in the cache should have been normalized
    assert fake_genetics_normalizer.variant_ids_normalized == ['DBSNP:rs3', 'DBSNP:rs5']

    assert node_normalizer.node_normalization_lookup['DBSNP:rs1'] == ['CAID:CA1']
    assert node_normalizer.node_normalization_lookup['DBSNP:rs2'] == ['CAID:CA2', 'CAID:CA3']
    assert node_normalizer.variant_node_splits == {'DBSNP:rs2': ['CAID:CA2', 'CAID:CA3']}

    # the cached failure is treated the same way a fresh one would be
    assert node_normalizer.node_normalization_lookup['DBSNP:rs4'] is None
    assert node_normalizer.failed_to_normalize_variant_ids['DBSNP:rs4'] == CACHED_NORMALIZATION_FAILURE

    # cached and freshly normalized nodes end up in the same list
    assert [node['id'] for node in variant_nodes] == ['CAID:CA1', 'CAID:CA2', 'CAID:CA3',
                                                      'CAID:CA_rs3', 'CAID:CA_rs5']
    cached_node = get_node_from_list('CAID:CA1', variant_nodes)
    assert cached_node[NODE_TYPES] == TEST_VARIANT_NODE_TYPES
    assert cached_node['name'] == 'rs1'
    assert cached_node['hgvs'] == ['HGVS:CAID:CA1']
    assert cached_node['robokop_variant_id'] == 'ROBO_VARIANT:CAID:CA1'


def test_variant_norm_with_cache_lenient(monkeypatch, variant_norm_cache_dir, fake_genetics_normalizer):
    monkeypatch.setattr(config, 'ORION_VARIANT_NORM_CACHE', variant_norm_cache_dir)

    variant_nodes = [{"id": "DBSNP:rs4", "name": "", NODE_TYPES: [SEQUENCE_VARIANT]}]

    node_normalizer = NodeNormalizer(strict_normalization=False)
    node_normalizer.normalize_sequence_variants(variant_nodes)

    # with strict normalization off a cached failure is kept, just like a fresh one
    assert not fake_genetics_normalizer.variant_ids_normalized
    assert node_normalizer.node_normalization_lookup['DBSNP:rs4'] == ['DBSNP:rs4']
    assert variant_nodes == [{'id': 'DBSNP:rs4',
                              'name': 'DBSNP:rs4',
                              NODE_TYPES: TEST_VARIANT_NODE_TYPES,
                              SYNONYMS: [],
                              'hgvs': []}]


# The biolink-model GitHub tags and the bl-lookup /versions endpoint are both 'v'-prefixed, so a
# version requested without the prefix has to be standardized before it reaches either of them.
@pytest.mark.parametrize('requested_version', ['4.4.2', 'v4.4.2'])
def test_bl_version_standardized_from_config(requested_version):
    assert Config(BL_VERSION=requested_version).BL_VERSION == 'v4.4.2'


@pytest.mark.parametrize('requested_version', ['4.4.2', 'v4.4.2'])
def test_normalization_scheme_standardizes_edge_norm_version(requested_version):
    normalization_scheme = NormalizationScheme(node_normalization_version='2.3.0',
                                               babel_version='2024jan',
                                               edge_normalization_version=requested_version)
    assert normalization_scheme.edge_normalization_version == 'v4.4.2'


# Both spellings must produce the same composite version, otherwise identical content would
# hash to two different build versions depending on how the version happened to be written.
def test_edge_norm_version_spellings_produce_same_composite_version():
    def composite_for(edge_normalization_version):
        return NormalizationScheme(node_normalization_version='2.3.0',
                                   babel_version='2024jan',
                                   edge_normalization_version=edge_normalization_version
                                   ).get_composite_normalization_version()
    assert composite_for('4.4.2') == composite_for('v4.4.2')


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
