import pytest
from Common.utils import NodeNormUtils, EdgeNormUtils
from Common.node_types import ROOT_ENTITY, SEQUENCE_VARIANT


def test_variant_norm():

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

    node_normalizer = NodeNormUtils(strict_normalization=True)
    node_normalizer.normalize_sequence_variants(variant_nodes)

    assert len(variant_nodes) == 9
    assert len(node_normalizer.variant_node_splits) == 1

    assert not node_normalizer.node_normalization_lookup['BOGUS:rs999999999999']

    assert node_normalizer.node_normalization_lookup['HGVS:NC_000011.10:g.68032291C>G'] == ['CAID:CA6146346']

    it_worked = False
    for node in variant_nodes:
        if node['id'] == 'CAID:CA6146346':
            if node['name'] == 'rs369602258':
                if ROOT_ENTITY in node['category']:
                    if SEQUENCE_VARIANT in node['category']:
                        it_worked = True
    assert it_worked

    node_normalizer = NodeNormUtils(strict_normalization=False)
    node_normalizer.normalize_sequence_variants(variant_nodes_2)

    assert len(variant_nodes_2) == 11

    it_worked = False
    for node in variant_nodes_2:
        print(node)
        if node['id'] == 'BOGUS:rs999999999999':
            if node['name'] == 'BOGUS:rs999999999999':
                if ROOT_ENTITY in node['category']:
                    if SEQUENCE_VARIANT in node['category']:
                        it_worked = True
    assert it_worked
