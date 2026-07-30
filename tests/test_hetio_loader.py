import bz2
import json
from pathlib import Path

from parsers.hetio.src.loadHetio import HetioLoader


def write_hetio_json(loader, edges):
    data_path = Path(loader.data_path)
    file_path = data_path / loader.data_file
    hetnet_json = {
        'kind_to_abbrev': {
            'Anatomy': 'A',
            'Gene': 'G',
            'expresses': 'e',
        },
        'edges': edges,
    }
    with bz2.open(file_path, 'wt', encoding='utf-8') as source_file:
        json.dump(hetnet_json, source_file)


def load_edges_and_nodes(tmp_path, loader):
    nodes_path = tmp_path / 'nodes.jsonl'
    edges_path = tmp_path / 'edges.jsonl'
    metadata = loader.load(str(nodes_path), str(edges_path))
    nodes = [json.loads(line) for line in nodes_path.read_text().splitlines()]
    edges = [json.loads(line) for line in edges_path.read_text().splitlines()]
    return metadata, nodes, edges


def test_hetio_loader_filters_bgee_edges_before_writing_nodes_or_edges(tmp_path):
    loader = HetioLoader(source_data_dir=str(tmp_path))
    write_hetio_json(
        loader,
        [
            {
                'source_id': ['Anatomy', 'UBERON:0002107'],
                'target_id': ['Gene', '1'],
                'kind': 'expresses',
                'data': {'source': 'Bgee'},
            },
            {
                'source_id': ['Anatomy', 'UBERON:0000948'],
                'target_id': ['Gene', '2'],
                'kind': 'expresses',
                'data': {'source': 'TISSUES'},
            },
        ],
    )

    metadata, nodes, edges = load_edges_and_nodes(tmp_path, loader)

    assert metadata['lines_skipped_due_to_filtering'] == 1
    assert metadata['source_edges'] == 1
    assert edges == [
        {
            'subject': 'UBERON:0000948',
            'predicate': 'RO:0002292',
            'object': 'NCBIGene:2',
            'primary_knowledge_source': 'infores:tissues-expression-db',
            'aggregator_knowledge_source': ['infores:hetionet'],
            'knowledge_level': 'not_provided',
            'agent_type': 'not_provided',
        }
    ]
    assert {node['id'] for node in nodes} == {'UBERON:0000948', 'NCBIGene:2'}
