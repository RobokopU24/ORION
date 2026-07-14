import gzip
import json
from pathlib import Path

import pytest

from parsers.Bgee.src.loadBgee import BgeeHumanLoader
from parsers.monarchkg.src.loadMonarchKG import MonarchKGLoader


BGEE_HEADER = [
    'Gene ID',
    'Gene name',
    'Anatomical entity ID',
    'Anatomical entity name',
    'Expression',
    'Call quality',
    'FDR',
    'Expression score',
    'Expression rank',
]


def write_bgee_rows(loader, rows):
    data_path = Path(loader.data_path)
    file_path = data_path / loader.data_file
    with gzip.open(file_path, 'wt', encoding='utf-8') as source_file:
        source_file.write('\t'.join(BGEE_HEADER) + '\n')
        for row in rows:
            source_file.write('\t'.join(row) + '\n')


def load_edges_and_nodes(tmp_path, loader):
    nodes_path = tmp_path / 'nodes.jsonl'
    edges_path = tmp_path / 'edges.jsonl'
    metadata = loader.load(str(nodes_path), str(edges_path))
    nodes = [json.loads(line) for line in nodes_path.read_text().splitlines()]
    edges = [json.loads(line) for line in edges_path.read_text().splitlines()]
    return metadata, nodes, edges


def test_bgee_loader_filters_by_expression_and_fdr(tmp_path):
    loader = BgeeHumanLoader(source_data_dir=str(tmp_path), fdr_threshold=0.0001)
    write_bgee_rows(
        loader,
        [
            [
                'ENSG00000000001',
                'GENE1',
                'UBERON:0002107',
                'liver',
                'present',
                'gold quality',
                '0.0001',
                '95.5',
                '120',
            ],
            [
                'ENSG00000000002',
                'GENE2',
                'UBERON:0002107',
                'liver',
                'present',
                'gold quality',
                '0.0001001',
                '95.5',
                '120',
            ],
            [
                'ENSG00000000003',
                'GENE3',
                'UBERON:0002107',
                'liver',
                'absent',
                'gold quality',
                '0.0000001',
                '95.5',
                '120',
            ],
            [
                'ENSG00000000004',
                'GENE4',
                'UBERON:0002107',
                'liver',
                'present',
                'gold quality',
                '0.0000001',
                '89.999',
                '120',
            ],
        ],
    )

    metadata, nodes, edges = load_edges_and_nodes(tmp_path, loader)

    assert metadata['num_source_lines'] == 4
    assert metadata['record_counter'] == 1
    assert metadata['lines_skipped_due_to_fdr_threshold'] == 1
    assert metadata['lines_skipped_due_to_absent_expression'] == 1
    assert metadata['lines_skipped_due_to_expression_score_threshold'] == 1
    assert metadata['fdr_threshold'] == 0.0001
    assert metadata['expression_score_threshold'] == 90.0
    assert edges == [
        {
            'subject': 'ENSEMBL:ENSG00000000001',
            'predicate': 'biolink:expressed_in',
            'object': 'UBERON:0002107',
            'primary_knowledge_source': 'infores:bgee',
            'knowledge_level': 'observation',
            'agent_type': 'data_analysis_pipeline',
            'adjusted_p_value': 0.0001,
            'has_confidence_level': 'gold quality',
            'has_confidence_score': 95.5,
            'has_quantitative_value': 120.0,
            'original_subject': 'ENSG00000000001',
            'original_object': 'UBERON:0002107',
        }
    ]
    nodes_by_id = {node['id']: node for node in nodes}
    assert nodes_by_id['ENSEMBL:ENSG00000000001']['taxon'] == 'NCBITaxon:9606'
    assert nodes_by_id['UBERON:0002107']['name'] == 'liver'


def test_bgee_loader_represents_cell_anatomy_intersections_as_context_qualifiers(tmp_path):
    loader = BgeeHumanLoader(source_data_dir=str(tmp_path), fdr_threshold=0.0001)
    write_bgee_rows(
        loader,
        [
            [
                'ENSG00000000001',
                'GENE1',
                'CL:0000089 ∩ UBERON:0000473',
                'male germ line stem cell (sensu Vertebrata) in testis',
                'present',
                'gold quality',
                '0.0000001',
                '99.0',
                '42',
            ],
        ],
    )

    metadata, nodes, edges = load_edges_and_nodes(tmp_path, loader)

    assert metadata['intersection_expression_lines'] == 1
    assert edges[0]['object'] == 'CL:0000089'
    assert edges[0]['anatomical_context_qualifier'] == 'UBERON:0000473'
    assert edges[0]['original_object'] == 'CL:0000089 ∩ UBERON:0000473'
    nodes_by_id = {node['id']: node for node in nodes}
    assert nodes_by_id['CL:0000089']['category'] == ['biolink:Cell']


def test_bgee_loader_fails_fast_on_unexpected_intersection_shape():
    with pytest.raises(ValueError, match='Unexpected Bgee anatomical entity intersection'):
        BgeeHumanLoader.parse_anatomical_entity('UBERON:0000473 ∩ CL:0000089')


def test_bgee_loader_threshold_can_be_configured_with_environment(tmp_path, monkeypatch):
    monkeypatch.setenv('BGEE_FDR_THRESHOLD', '0.001')

    loader = BgeeHumanLoader(source_data_dir=str(tmp_path))

    assert loader.fdr_threshold == 0.001


def test_bgee_loader_score_threshold_is_inclusive(tmp_path):
    loader = BgeeHumanLoader(source_data_dir=str(tmp_path), fdr_threshold=0.0001)
    write_bgee_rows(
        loader,
        [
            [
                'ENSG00000000001',
                'GENE1',
                'UBERON:0002107',
                'liver',
                'present',
                'gold quality',
                '0.0000001',
                '90',
                '120',
            ],
        ],
    )

    metadata, nodes, edges = load_edges_and_nodes(tmp_path, loader)

    assert metadata['record_counter'] == 1
    assert edges[0]['has_confidence_score'] == 90.0


def test_monarch_loader_filters_bgee_edges_from_curated_subset(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert loader.filter_edge(
        subject_id='ENSEMBL:ENSG00000000001',
        object_id='UBERON:0002107',
        predicate='biolink:expressed_in',
        primary_knowledge_source='infores:bgee',
        aggregator_knowledge_sources=['infores:monarchinitiative'],
    )
