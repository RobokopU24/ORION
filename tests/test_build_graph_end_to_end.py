"""End-to-end smoke test for GraphBuilder.build_graph.

Exercises the full happy path of a single-source graph build:

- spec parsing → version determination → dependency resolution → ingest-pipeline
  parser-output materialization → KGXFileMerger.merge() → metadata files → file
  compression → build_results recording.

Mocks the IngestPipeline (no network, no actual data fetching) and the heavy
post-merge steps (QC, KGX schema generation, meta KG generation).
"""

import gzip
import json
from unittest.mock import MagicMock

import pytest

from orion.graph_pipeline import GraphBuilder
from orion.metadata import Metadata


FIXTURE_NODES = [
    {"id": "HGNC:1", "name": "GeneA", "category": ["biolink:Gene"]},
    {"id": "HGNC:2", "name": "GeneB", "category": ["biolink:Gene"]},
    {"id": "HGNC:3", "name": "GeneC", "category": ["biolink:Gene"]},
]

FIXTURE_EDGES = [
    {"subject": "HGNC:1", "object": "HGNC:2", "predicate": "biolink:related_to",
     "primary_knowledge_source": "infores:test"},
    {"subject": "HGNC:2", "object": "HGNC:3", "predicate": "biolink:related_to",
     "primary_knowledge_source": "infores:test"},
]

# Per-source fixtures used by the multi-source test. CTD overlaps HGNC:2 so the
# merger has both unique-add and dedup work to do.
PER_SOURCE_FIXTURES = {
    'HGNC': {
        'nodes': FIXTURE_NODES,
        'edges': FIXTURE_EDGES,
    },
    'CTD': {
        'nodes': [
            {"id": "CTD:1", "name": "ChemA", "category": ["biolink:ChemicalEntity"]},
            {"id": "HGNC:2", "name": "GeneB", "category": ["biolink:Gene"]},
        ],
        'edges': [
            {"subject": "CTD:1", "object": "HGNC:2", "predicate": "biolink:affects",
             "primary_knowledge_source": "infores:ctd"},
        ],
    },
}


def _write_jsonl(path, rows):
    with open(path, 'w') as f:
        for row in rows:
            f.write(json.dumps(row) + '\n')


def _build_mock_ingest_pipeline(nodes_path, edges_path):
    mock = MagicMock()
    mock.get_latest_source_version.side_effect = lambda source_id: f'{source_id}_sv1'
    mock.get_latest_parsing_version.side_effect = lambda source_id: f'{source_id}_pv1'
    # get_build_info returning a non-None dict short-circuits run_pipeline, so the
    # ingest stage is treated as already-complete and _run_ingest_pipeline jumps
    # straight to get_final_file_paths.
    mock_source_metadata = MagicMock()
    mock_source_metadata.get_build_info.return_value = {'parsing_info': {'note': 'fixture'}}
    mock.get_source_metadata.return_value = mock_source_metadata
    mock.get_final_file_paths.return_value = [str(nodes_path), str(edges_path)]
    return mock


def _build_per_source_mock_ingest_pipeline(source_paths: dict[str, tuple[str, str]]):
    """Mock pipeline whose get_final_file_paths picks files based on source_id."""
    mock = MagicMock()
    mock.get_latest_source_version.side_effect = lambda source_id: f'{source_id}_sv1'
    mock.get_latest_parsing_version.side_effect = lambda source_id: f'{source_id}_pv1'
    mock_source_metadata = MagicMock()
    mock_source_metadata.get_build_info.return_value = {'parsing_info': {'note': 'fixture'}}
    mock.get_source_metadata.return_value = mock_source_metadata
    mock.get_final_file_paths.side_effect = (
        lambda source_id, *_args, **_kwargs: list(source_paths[source_id])
    )
    return mock


def _patch_post_merge_heavy_steps(monkeypatch):
    """Patch QC, KGX schema generation, and meta KG generation to no-ops.

    These steps drag in biolink lookups and large-graph machinery that aren't
    the focus of build-orchestration tests. They get their own focused tests.
    """
    monkeypatch.setattr('orion.graph_pipeline.validate_graph',
                        lambda **kwargs: {'pass': True})
    monkeypatch.setattr('orion.graph_pipeline.generate_kgx_schema_file',
                        lambda **kwargs: None)
    monkeypatch.setattr(GraphBuilder, 'has_meta_kg', staticmethod(lambda graph_directory: True))
    monkeypatch.setattr(GraphBuilder, 'has_test_data', staticmethod(lambda graph_directory: True))


def test_build_graph_end_to_end(tmp_path, monkeypatch):
    # --- Fixture parser output that the mocked ingest pipeline will hand back ---
    fixture_dir = tmp_path / 'parser_output'
    fixture_dir.mkdir()
    fixture_nodes = fixture_dir / 'normalized_nodes.jsonl'
    fixture_edges = fixture_dir / 'normalized_edges.jsonl'
    _write_jsonl(fixture_nodes, FIXTURE_NODES)
    _write_jsonl(fixture_edges, FIXTURE_EDGES)

    _patch_post_merge_heavy_steps(monkeypatch)

    # --- Inline spec for a single-source graph (graph_id == source_id so the
    #     single-source predicate routes us through _resolve_parser_output). ---
    inline_spec = {
        'graphs': [{
            'graph_id': 'HGNC',
            'graph_name': 'HGNC Test Graph',
            'output_format': 'jsonl',
            'sources': [{'source_id': 'HGNC'}],
        }]
    }

    graphs_dir = tmp_path / 'graphs'
    graphs_dir.mkdir()
    empty_spec_dir = tmp_path / 'specs'
    empty_spec_dir.mkdir()

    builder = GraphBuilder(graph_specs_dir=str(empty_spec_dir),
                           inline_graph_spec=inline_spec,
                           graph_output_dir=str(graphs_dir))
    builder.ingest_pipeline = _build_mock_ingest_pipeline(fixture_nodes, fixture_edges)

    # --- Build ---
    graph_spec = builder.graph_specs['HGNC']
    assert builder.build_graph(graph_spec) is True

    # --- Assert versioning ---
    release_version = graph_spec.release_version
    assert release_version is not None
    assert graph_spec.build_version is not None

    # --- Assert on-disk artifacts ---
    output_dir = graphs_dir / 'HGNC' / release_version
    assert output_dir.is_dir()
    # nodes/edges get gzipped at the end of build_graph
    nodes_gz = output_dir / 'nodes.jsonl.gz'
    edges_gz = output_dir / 'edges.jsonl.gz'
    assert nodes_gz.exists(), f'merged nodes file missing; dir contents: {list(output_dir.iterdir())}'
    assert edges_gz.exists(), f'merged edges file missing; dir contents: {list(output_dir.iterdir())}'

    # confirm the merged files actually contain our fixture rows
    with gzip.open(nodes_gz, 'rt') as f:
        merged_nodes = [json.loads(line) for line in f]
    assert {n['id'] for n in merged_nodes} == {n['id'] for n in FIXTURE_NODES}
    with gzip.open(edges_gz, 'rt') as f:
        merged_edges = [json.loads(line) for line in f]
    assert len(merged_edges) == len(FIXTURE_EDGES)

    # --- Assert ORION metadata ---
    meta_file = output_dir / 'HGNC.meta.json'
    assert meta_file.exists()
    with open(meta_file) as f:
        meta = json.load(f)
    assert meta['graph_id'] == 'HGNC'
    assert meta['release_version'] == release_version
    assert meta['build_version'] == graph_spec.build_version
    assert meta['build_status'] == Metadata.STABLE
    assert meta['build_time'] is not None

    # --- Assert KGX graph-metadata.json was generated ---
    graph_metadata_file = output_dir / 'graph-metadata.json'
    assert graph_metadata_file.exists()
    with open(graph_metadata_file) as f:
        kgx_meta = json.load(f)
    assert kgx_meta['name'] == 'HGNC Test Graph'
    assert kgx_meta['version'] == release_version

    # --- Assert build_results was populated for the deployment-record file ---
    assert 'HGNC' in builder.build_results
    result = builder.build_results['HGNC']
    assert result == {
        'graph_id': 'HGNC',
        'release_version': release_version,
        'build_version': graph_spec.build_version,
        'graph_dir': str(output_dir),
        'build_status': Metadata.STABLE,
        'build_time': meta['build_time'],
    }


def test_build_graph_end_to_end_multi_source(tmp_path, monkeypatch):
    """This exercises the recursive _build_single_source_graph_dependency path:
    each data source is built as its own single-source artifact under
    graphs_dir/<source_id>/<release_version>/ first, then the parent build
    re-merges those artifacts under graphs_dir/<graph_id>/<release_version>/.
    """
    # --- One fixture nodes/edges pair per source ---
    fixture_paths_by_source = {}
    for source_id, fixture in PER_SOURCE_FIXTURES.items():
        src_dir = tmp_path / 'parser_output' / source_id
        src_dir.mkdir(parents=True)
        nodes_path = src_dir / 'normalized_nodes.jsonl'
        edges_path = src_dir / 'normalized_edges.jsonl'
        _write_jsonl(nodes_path, fixture['nodes'])
        _write_jsonl(edges_path, fixture['edges'])
        fixture_paths_by_source[source_id] = (str(nodes_path), str(edges_path))

    _patch_post_merge_heavy_steps(monkeypatch)

    inline_spec = {
        'graphs': [{
            'graph_id': 'Multi_Source_Test',
            'graph_name': 'Multi Source Test Graph',
            'output_format': 'jsonl',
            'sources': [
                {'source_id': 'HGNC'},
                {'source_id': 'CTD'},
            ],
        }]
    }

    graphs_dir = tmp_path / 'graphs'
    graphs_dir.mkdir()
    empty_spec_dir = tmp_path / 'specs'
    empty_spec_dir.mkdir()

    builder = GraphBuilder(graph_specs_dir=str(empty_spec_dir),
                           inline_graph_spec=inline_spec,
                           graph_output_dir=str(graphs_dir))
    builder.ingest_pipeline = _build_per_source_mock_ingest_pipeline(fixture_paths_by_source)

    graph_spec = builder.graph_specs['Multi_Source_Test']
    assert builder.build_graph(graph_spec) is True

    release_version = graph_spec.release_version
    assert release_version is not None
    assert graph_spec.build_version is not None

    # --- The parent graph artifact ---
    parent_dir = graphs_dir / 'Multi_Source_Test' / release_version
    assert parent_dir.is_dir()
    parent_nodes_gz = parent_dir / 'nodes.jsonl.gz'
    parent_edges_gz = parent_dir / 'edges.jsonl.gz'
    assert parent_nodes_gz.exists()
    assert parent_edges_gz.exists()

    # The merger dedupes HGNC:2 across sources, so we expect 4 unique node ids.
    with gzip.open(parent_nodes_gz, 'rt') as f:
        merged_node_ids = {json.loads(line)['id'] for line in f}
    assert merged_node_ids == {'HGNC:1', 'HGNC:2', 'HGNC:3', 'CTD:1'}

    with gzip.open(parent_edges_gz, 'rt') as f:
        merged_edges = [json.loads(line) for line in f]
    assert len(merged_edges) == 3  # two HGNC + one CTD

    # --- The two single-source artifacts that the parent build produced as deps ---
    for source_id in ('HGNC', 'CTD'):
        assert source_id in builder.build_results, (
            f'Recursive single-source build for {source_id} did not record a build result. '
            f'build_results keys: {list(builder.build_results)}'
        )
        single_source_result = builder.build_results[source_id]
        single_source_dir = graphs_dir / source_id / single_source_result['release_version']
        assert single_source_dir.is_dir()
        assert (single_source_dir / 'nodes.jsonl.gz').exists()
        assert (single_source_dir / 'edges.jsonl.gz').exists()
        assert (single_source_dir / f'{source_id}.meta.json').exists()

    # --- Parent metadata records both sources ---
    parent_meta_file = parent_dir / 'Multi_Source_Test.meta.json'
    assert parent_meta_file.exists()
    with open(parent_meta_file) as f:
        parent_meta = json.load(f)
    assert parent_meta['build_status'] == Metadata.STABLE
    assert parent_meta['release_version'] == release_version
    recorded_source_ids = {s.get('source_id') for s in parent_meta.get('sources', [])}
    assert recorded_source_ids == {'HGNC', 'CTD'}

    # --- build_results has three entries: parent + two dependencies ---
    assert set(builder.build_results) == {'Multi_Source_Test', 'HGNC', 'CTD'}
    parent_result = builder.build_results['Multi_Source_Test']
    assert parent_result['graph_dir'] == str(parent_dir)
    assert parent_result['build_status'] == Metadata.STABLE


def test_build_graph_end_to_end_with_subgraph_dependency(tmp_path, monkeypatch):
    """Parent graph declares another graph as a subgraph dependency.

    Exercises SubgraphBuildResolver: when the subgraph isn't already built on
    disk (and registry is disabled), the resolver falls through to building it
    via the graph_pipeline. Confirms the parent merge composes a built-subgraph
    contribution with a fresh single-source-from-parser-output contribution.
    """
    fixture_paths_by_source = {}
    for source_id, fixture in PER_SOURCE_FIXTURES.items():
        src_dir = tmp_path / 'parser_output' / source_id
        src_dir.mkdir(parents=True)
        nodes_path = src_dir / 'normalized_nodes.jsonl'
        edges_path = src_dir / 'normalized_edges.jsonl'
        _write_jsonl(nodes_path, fixture['nodes'])
        _write_jsonl(edges_path, fixture['edges'])
        fixture_paths_by_source[source_id] = (str(nodes_path), str(edges_path))

    _patch_post_merge_heavy_steps(monkeypatch)

    # Two graphs declared together. Parent_Graph depends on My_Subgraph (a graph
    # built from HGNC) and adds CTD as a direct data source. My_Subgraph must
    # already be in graph_specs for SubgraphBuildResolver to find it.
    inline_spec = {
        'graphs': [
            {
                'graph_id': 'My_Subgraph',
                'graph_name': 'My Subgraph',
                'output_format': 'jsonl',
                'sources': [{'source_id': 'HGNC'}],
            },
            {
                'graph_id': 'Parent_Graph',
                'graph_name': 'Parent Graph',
                'output_format': 'jsonl',
                'sources': [{'source_id': 'CTD'}],
                'subgraphs': [{'graph_id': 'My_Subgraph'}],
            },
        ]
    }

    graphs_dir = tmp_path / 'graphs'
    graphs_dir.mkdir()
    empty_spec_dir = tmp_path / 'specs'
    empty_spec_dir.mkdir()

    builder = GraphBuilder(graph_specs_dir=str(empty_spec_dir),
                           inline_graph_spec=inline_spec,
                           graph_output_dir=str(graphs_dir))
    builder.ingest_pipeline = _build_per_source_mock_ingest_pipeline(fixture_paths_by_source)

    parent_spec = builder.graph_specs['Parent_Graph']
    assert builder.build_graph(parent_spec) is True

    parent_release_version = parent_spec.release_version
    subgraph_spec = builder.graph_specs['My_Subgraph']
    subgraph_release_version = subgraph_spec.release_version
    assert parent_release_version is not None
    assert subgraph_release_version is not None

    parent_dir = graphs_dir / 'Parent_Graph' / parent_release_version
    subgraph_dir = graphs_dir / 'My_Subgraph' / subgraph_release_version
    assert parent_dir.is_dir()
    assert subgraph_dir.is_dir()
    assert (subgraph_dir / 'nodes.jsonl.gz').exists()
    assert (subgraph_dir / 'edges.jsonl.gz').exists()
    assert (subgraph_dir / 'My_Subgraph.meta.json').exists()

    # Parent merge should combine the subgraph's HGNC nodes with CTD's contribution.
    # HGNC:2 appears in both, so dedup leaves 4 unique node ids.
    parent_nodes_gz = parent_dir / 'nodes.jsonl.gz'
    parent_edges_gz = parent_dir / 'edges.jsonl.gz'
    assert parent_nodes_gz.exists()
    assert parent_edges_gz.exists()
    with gzip.open(parent_nodes_gz, 'rt') as f:
        merged_node_ids = {json.loads(line)['id'] for line in f}
    assert merged_node_ids == {'HGNC:1', 'HGNC:2', 'HGNC:3', 'CTD:1'}
    with gzip.open(parent_edges_gz, 'rt') as f:
        merged_edges = [json.loads(line) for line in f]
    assert len(merged_edges) == 3

    # Parent metadata lists CTD as a source and My_Subgraph as a subgraph.
    with open(parent_dir / 'Parent_Graph.meta.json') as f:
        parent_meta = json.load(f)
    assert parent_meta['build_status'] == Metadata.STABLE
    recorded_source_ids = {s.get('source_id') for s in parent_meta.get('sources', [])}
    recorded_subgraph_ids = {s.get('graph_id') for s in parent_meta.get('subgraphs', [])}
    assert recorded_source_ids == {'CTD'}
    assert recorded_subgraph_ids == {'My_Subgraph'}

    # All four builds recorded: parent, subgraph, and the two single-source
    # artifacts that backed them (HGNC for the subgraph, CTD for the parent).
    assert set(builder.build_results) == {'Parent_Graph', 'My_Subgraph', 'HGNC', 'CTD'}
    assert builder.build_results['Parent_Graph']['graph_dir'] == str(parent_dir)
    assert builder.build_results['My_Subgraph']['graph_dir'] == str(subgraph_dir)