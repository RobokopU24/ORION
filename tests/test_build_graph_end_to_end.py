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
from orion.ingest_pipeline import IngestPipeline
from orion.kgx_metadata import source_ids_from_graph_metadata
from orion.metadata import Metadata, get_source_build_version


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


def _build_mock_ingest_pipeline(storage_dir, nodes_path, edges_path):
    """A real IngestPipeline with heavy methods (network ingest, parsing, normalization)
    stubbed out and parser-output file paths pre-populated. Real implementations of
    storage_dir / get_source_build_directory / etc. so the source builds cache layout
    works in tests.
    """
    return _build_per_source_mock_ingest_pipeline(
        storage_dir, {None: (str(nodes_path), str(edges_path))})


def _build_per_source_mock_ingest_pipeline(storage_dir,
                                            source_paths: dict[str | None, tuple[str, str] | None]):
    """As above, but selects parser-output paths by source_id (None key = same fixture for all).

    The heavy ingest stages (fetch/parse/normalize/supplement/QC) are stubbed so run_pipeline runs
    through to the real finalize_source_build, which merges the fixture parser output into the
    content-addressed source build. run_qc returns the real build_version hash so the finalized
    build lands at the same key the GraphBuilder computes from the spec. A source whose fixture
    paths are None simulates an ingest that produced no parser output (finalization fails).
    """
    pipeline = IngestPipeline(storage_dir=str(storage_dir))
    pipeline.get_latest_source_version = MagicMock(side_effect=lambda source_id: f'{source_id}_sv1')
    pipeline.get_latest_parsing_version = MagicMock(side_effect=lambda source_id: f'{source_id}_pv1')

    pipeline.run_fetch_stage = MagicMock(return_value=True)
    pipeline.run_parsing_stage = MagicMock(return_value=True)
    pipeline.run_normalization_stage = MagicMock(return_value=True)
    pipeline.run_supplementation_stage = MagicMock(return_value=True)

    def _qc(source_id, source_version, parsing_version=None, normalization_scheme=None,
            supplementation_version=None):
        return get_source_build_version(source_id, source_version, parsing_version,
                                        normalization_scheme.get_composite_normalization_version(),
                                        supplementation_version)
    pipeline.run_qc_and_metadata_stage = MagicMock(side_effect=_qc)

    def _pick_paths(source_id, *_args, **_kwargs):
        paths = source_paths.get(source_id, source_paths.get(None))
        return list(paths) if paths is not None else None
    pipeline.get_final_file_paths = MagicMock(side_effect=_pick_paths)

    # Wrap run_pipeline in a call-through spy so tests can assert whether an ingest was triggered
    # (e.g. a second graph reusing a cached source build should not call it) while still running the
    # real stage-stubbed finalization.
    pipeline.run_pipeline = MagicMock(side_effect=pipeline.run_pipeline)
    return pipeline


def _patch_post_merge_heavy_steps(monkeypatch):
    """Patch QC, KGX schema generation, and meta KG generation to no-ops.

    These steps drag in biolink lookups and large-graph machinery that aren't
    the focus of build-orchestration tests. They get their own focused tests.
    """
    monkeypatch.setattr('orion.graph_pipeline.validate_graph',
                        lambda **kwargs: {'pass': True})
    monkeypatch.setattr('orion.graph_pipeline.generate_kgx_schema_file',
                        lambda **kwargs: None)
    monkeypatch.setattr('orion.ingest_pipeline.validate_graph',
                        lambda **kwargs: {'pass': True})
    monkeypatch.setattr('orion.ingest_pipeline.generate_kgx_schema_file',
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
    storage_dir = tmp_path / 'storage'
    storage_dir.mkdir()
    empty_spec_dir = tmp_path / 'specs'
    empty_spec_dir.mkdir()

    builder = GraphBuilder(graph_specs_dir=str(empty_spec_dir),
                           inline_graph_spec=inline_spec,
                           graph_output_dir=str(graphs_dir),
                           ingest_pipeline=_build_mock_ingest_pipeline(storage_dir,
                                                                         fixture_nodes,
                                                                         fixture_edges))

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

    # --- graph-metadata.json is the single source of truth for the built graph ---
    graph_metadata_file = output_dir / 'graph-metadata.json'
    assert graph_metadata_file.exists()
    with open(graph_metadata_file) as f:
        kgx_meta = json.load(f)
    assert kgx_meta['name'] == 'HGNC Test Graph'
    assert kgx_meta['version'] == release_version
    assert kgx_meta['orion:buildVersion'] == graph_spec.build_version
    assert kgx_meta['dateCreated']
    # There's no longer a separate .meta.json file.
    assert not (output_dir / 'HGNC.meta.json').exists()

    # --- Assert build_results was populated for the deployment-record file ---
    assert 'HGNC' in builder.build_results
    result = builder.build_results['HGNC']
    assert result == {
        'graph_id': 'HGNC',
        'release_version': release_version,
        'build_version': graph_spec.build_version,
        'graph_dir': str(output_dir),
        'build_status': Metadata.STABLE,
        'build_time': kgx_meta['dateCreated'],
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
    storage_dir = tmp_path / 'storage'
    storage_dir.mkdir()
    empty_spec_dir = tmp_path / 'specs'
    empty_spec_dir.mkdir()

    builder = GraphBuilder(graph_specs_dir=str(empty_spec_dir),
                           inline_graph_spec=inline_spec,
                           graph_output_dir=str(graphs_dir),
                           ingest_pipeline=_build_per_source_mock_ingest_pipeline(
                               storage_dir, fixture_paths_by_source))

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

    # --- Each contributing source has a finalized source build in the content-addressed
    # cache under storage/<source_id>/builds/<build_version>/. These are not graphs
    # and so don't appear in builder.build_results; they're inputs the parent merger
    # consumes. graphs_dir contains only the parent graph dir.
    for source_id in ('HGNC', 'CTD'):
        source_resolved = next(s for s in graph_spec.resolved_sources if s.id == source_id)
        source_build_dir = (storage_dir / source_id / 'builds'
                            / source_resolved.build_version)
        assert source_build_dir.is_dir(), f'Source build dir missing for {source_id}: {source_build_dir}'
        assert (source_build_dir / 'nodes.jsonl.gz').exists()
        assert (source_build_dir / 'edges.jsonl.gz').exists()
        assert (source_build_dir / 'qc-results.json').exists()
        build_metadata_path = source_build_dir / 'graph-metadata.json'
        assert build_metadata_path.exists()
        with open(build_metadata_path) as f:
            build_metadata = json.load(f)
        content_url = build_metadata['distribution'][0]['contentUrl']
        assert content_url.endswith(f'/sources/{source_id}/builds/{source_resolved.build_version}/')
        # No per-source graph dir is created anymore — source builds live only in storage.
        assert not (graphs_dir / source_id).exists()

    # --- Parent metadata records both sources (single source of truth: graph-metadata.json) ---
    parent_meta_file = parent_dir / 'graph-metadata.json'
    assert parent_meta_file.exists()
    with open(parent_meta_file) as f:
        parent_meta = json.load(f)
    assert parent_meta['version'] == release_version
    assert set(source_ids_from_graph_metadata(parent_meta)) == {'HGNC', 'CTD'}
    assert not (parent_dir / 'Multi_Source_Test.meta.json').exists()

    # --- build_results has only the user-facing graph; source builds live in the
    # content-addressed cache and aren't tracked here.
    assert set(builder.build_results) == {'Multi_Source_Test'}
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
    storage_dir = tmp_path / 'storage'
    storage_dir.mkdir()
    empty_spec_dir = tmp_path / 'specs'
    empty_spec_dir.mkdir()

    builder = GraphBuilder(graph_specs_dir=str(empty_spec_dir),
                           inline_graph_spec=inline_spec,
                           graph_output_dir=str(graphs_dir),
                           ingest_pipeline=_build_per_source_mock_ingest_pipeline(
                               storage_dir, fixture_paths_by_source))

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
    assert (subgraph_dir / 'graph-metadata.json').exists()

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

    # Parent graph-metadata.json records its constituent sources flat in hasPart: CTD
    # directly, plus HGNC contributed via the My_Subgraph subgraph.
    with open(parent_dir / 'graph-metadata.json') as f:
        parent_meta = json.load(f)
    assert parent_meta['version'] == parent_release_version
    assert set(source_ids_from_graph_metadata(parent_meta)) == {'CTD', 'HGNC'}

    # build_results tracks user-facing graphs only — parent + subgraph here. Source
    # builds for HGNC/CTD live in the content-addressed cache under storage_dir and
    # aren't graphs.
    assert set(builder.build_results) == {'Parent_Graph', 'My_Subgraph'}
    assert builder.build_results['Parent_Graph']['graph_dir'] == str(parent_dir)
    assert builder.build_results['My_Subgraph']['graph_dir'] == str(subgraph_dir)
    for source_id in ('HGNC', 'CTD'):
        assert (storage_dir / source_id / 'builds').is_dir()
        assert not (graphs_dir / source_id).exists()


def test_build_graph_continues_past_failed_source(tmp_path, monkeypatch):
    """When one source fails to resolve, remaining sources are still attempted.

    The parent graph build still fails (returns False) — there's no partial-merge
    mode — but every other source's single-source artifact is built and cached.
    A subsequent invocation of any graph that shares those sources reuses them
    instead of re-running their ingests.
    """
    # CTD gets a real fixture; HGNC will have its parser-output lookup return None
    # to simulate an ingest failure.
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

    # Pipeline returns proper file paths for CTD; returns None for HGNC (simulating an ingest that
    # produced no parser output), which causes finalization — and thus _build_source — to give up
    # on that source.
    storage_dir = tmp_path / 'storage'
    storage_dir.mkdir()
    mock_pipeline = _build_per_source_mock_ingest_pipeline(
        storage_dir,
        {'HGNC': None, 'CTD': fixture_paths_by_source['CTD']})

    inline_spec = {
        'graphs': [{
            'graph_id': 'Multi_Partial_Failure',
            'graph_name': 'Partial Failure Test',
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
                           graph_output_dir=str(graphs_dir),
                           ingest_pipeline=mock_pipeline)

    parent_spec = builder.graph_specs['Multi_Partial_Failure']

    # Parent build aborts because one dependency couldn't be resolved.
    assert builder.build_graph(parent_spec) is False

    # CTD's source build was still produced and cached despite the HGNC failure
    # earlier in the loop — that's the point of continuing past failures. A
    # subsequent build of any graph that needs CTD will hit the cache and skip the
    # ingest pipeline.
    ctd_source_builds = storage_dir / 'CTD' / 'builds'
    assert ctd_source_builds.is_dir()
    ctd_builds = list(ctd_source_builds.iterdir())
    assert len(ctd_builds) == 1
    ctd_build_dir = ctd_builds[0]
    assert (ctd_build_dir / 'nodes.jsonl.gz').exists()
    assert (ctd_build_dir / 'edges.jsonl.gz').exists()
    assert (ctd_build_dir / 'graph-metadata.json').exists()

    # HGNC's source build attempt did not succeed (mock returns None file paths),
    # so no source build files exist for it.
    hgnc_source_builds = storage_dir / 'HGNC' / 'builds'
    if hgnc_source_builds.exists():
        for build_dir in hgnc_source_builds.iterdir():
            assert not (build_dir / 'nodes.jsonl.gz').exists()
            assert not (build_dir / 'edges.jsonl.gz').exists()

    # No per-source graph dirs are ever created with the new design.
    assert not (graphs_dir / 'CTD').exists()
    assert not (graphs_dir / 'HGNC').exists()
    # build_results stays empty: parent failed, source builds aren't tracked there.
    assert builder.build_results == {}

    # The parent graph was never merged, so its release dir has no merged files.
    parent_dir = graphs_dir / 'Multi_Partial_Failure' / parent_spec.release_version
    if parent_dir.exists():
        assert not (parent_dir / 'nodes.jsonl').exists()
        assert not (parent_dir / 'nodes.jsonl.gz').exists()
    assert 'Multi_Partial_Failure' not in builder.build_results


def test_single_source_graph_consumes_source_build(tmp_path, monkeypatch):
    """A single-source graph build produces both a source build in the cache
    AND a user-facing graph dir. The graph dir's content comes from the source
    build via the merger pass; the source build is reusable by future graphs.
    """
    fixture_dir = tmp_path / 'parser_output'
    fixture_dir.mkdir()
    fixture_nodes = fixture_dir / 'normalized_nodes.jsonl'
    fixture_edges = fixture_dir / 'normalized_edges.jsonl'
    _write_jsonl(fixture_nodes, FIXTURE_NODES)
    _write_jsonl(fixture_edges, FIXTURE_EDGES)

    _patch_post_merge_heavy_steps(monkeypatch)

    inline_spec = {
        'graphs': [{
            'graph_id': 'Some_HGNC_Wrapper',
            'graph_name': 'Some HGNC Wrapper',
            'output_format': 'jsonl',
            'sources': [{'source_id': 'HGNC'}],
        }]
    }

    graphs_dir = tmp_path / 'graphs'
    graphs_dir.mkdir()
    storage_dir = tmp_path / 'storage'
    storage_dir.mkdir()
    empty_spec_dir = tmp_path / 'specs'
    empty_spec_dir.mkdir()

    builder = GraphBuilder(graph_specs_dir=str(empty_spec_dir),
                           inline_graph_spec=inline_spec,
                           graph_output_dir=str(graphs_dir),
                           ingest_pipeline=_build_mock_ingest_pipeline(
                               storage_dir, fixture_nodes, fixture_edges))

    graph_spec = builder.graph_specs['Some_HGNC_Wrapper']
    assert builder.build_graph(graph_spec) is True

    # Source build was created in the cache, keyed by build_version.
    source_resolved = next(s for s in graph_spec.resolved_sources if s.id == 'HGNC')
    source_build_dir = storage_dir / 'HGNC' / 'builds' / source_resolved.build_version
    assert source_build_dir.is_dir()
    assert (source_build_dir / 'nodes.jsonl.gz').exists()
    assert (source_build_dir / 'edges.jsonl.gz').exists()
    assert (source_build_dir / 'graph-metadata.json').exists()

    # The user-facing graph dir holds the merged graph regardless of its name.
    graph_dir = graphs_dir / 'Some_HGNC_Wrapper' / graph_spec.release_version
    assert (graph_dir / 'nodes.jsonl.gz').exists()
    assert (graph_dir / 'edges.jsonl.gz').exists()


def test_second_graph_reuses_existing_source_build(tmp_path, monkeypatch):
    """A second graph that consumes the same source as a previously-built graph
    hits the source builds cache instead of re-running the ingest pipeline.
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

    inline_spec = {
        'graphs': [
            {
                'graph_id': 'First_Graph',
                'graph_name': 'First Graph',
                'output_format': 'jsonl',
                'sources': [{'source_id': 'HGNC'}, {'source_id': 'CTD'}],
            },
            {
                'graph_id': 'Second_Graph',
                'graph_name': 'Second Graph',
                'output_format': 'jsonl',
                'sources': [{'source_id': 'HGNC'}, {'source_id': 'CTD'}],
            },
        ]
    }

    graphs_dir = tmp_path / 'graphs'
    graphs_dir.mkdir()
    storage_dir = tmp_path / 'storage'
    storage_dir.mkdir()
    empty_spec_dir = tmp_path / 'specs'
    empty_spec_dir.mkdir()

    builder = GraphBuilder(graph_specs_dir=str(empty_spec_dir),
                           inline_graph_spec=inline_spec,
                           graph_output_dir=str(graphs_dir),
                           ingest_pipeline=_build_per_source_mock_ingest_pipeline(
                               storage_dir, fixture_paths_by_source))

    assert builder.build_graph(builder.graph_specs['First_Graph']) is True

    # Reset the ingest mock; the second build should NOT call run_pipeline
    # since the source builds are already cached.
    builder.ingest_pipeline.run_pipeline.reset_mock()
    builder.ingest_pipeline.get_final_file_paths.reset_mock()
    assert builder.build_graph(builder.graph_specs['Second_Graph']) is True
    assert builder.ingest_pipeline.run_pipeline.call_count == 0
    assert builder.ingest_pipeline.get_final_file_paths.call_count == 0
