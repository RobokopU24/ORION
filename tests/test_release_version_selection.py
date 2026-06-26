"""Integration tests for GraphBuilder._known_release_versions and _select_release_version.

These two methods drive the semver assignment for every graph build. The pure
arithmetic (`next_release_version`) is covered by tests/test_graph_versioning.py;
this file covers the integration with the on-disk graphs directory: how prior
build metadata is discovered, how idempotency works (same build_version reuses
the existing release_version), and how malformed metadata is tolerated.

The remote graph registry is disabled by tests/conftest.py, so these tests
exercise the local-storage branch in isolation.
"""

import json
import os

import pytest

from orion.graph_pipeline import GraphBuilder
from orion.graph_versioning import DEFAULT_BASE_RELEASE_VERSION
from orion.metadata import Metadata


def _write_graph_meta(graphs_dir, graph_id, release_version, build_version,
                      build_status=Metadata.STABLE):
    """Drop a {graph_id}.meta.json at graphs_dir/{graph_id}/{release_version}/."""
    graph_dir = os.path.join(graphs_dir, graph_id, release_version)
    os.makedirs(graph_dir, exist_ok=True)
    meta = {
        'graph_id': graph_id,
        'graph_name': graph_id,
        'graph_description': '',
        'graph_url': '',
        'release_version': release_version,
        'build_version': build_version,
        'sources': [],
        'subgraphs': [],
        'build_status': build_status,
        'build_time': None,
        'build_error': None,
    }
    meta_path = os.path.join(graph_dir, f'{graph_id}.meta.json')
    with open(meta_path, 'w') as f:
        json.dump(meta, f)
    return meta_path


@pytest.fixture
def builder(tmp_path):
    """A GraphBuilder pointed at an empty tmp graphs_dir and empty spec dir."""
    graphs_dir = tmp_path / 'graphs'
    graphs_dir.mkdir()
    spec_dir = tmp_path / 'specs'
    spec_dir.mkdir()
    return GraphBuilder(graph_specs_dir=str(spec_dir),
                        graph_output_dir=str(graphs_dir))


# ---------------------------------------------------------------------------
# _known_release_versions
# ---------------------------------------------------------------------------

def test_known_release_versions_empty_graphs_dir(builder):
    assert builder._known_release_versions('Some_Graph') == {}


def test_known_release_versions_graph_dir_with_no_meta_files(builder):
    # Create an empty subdir for the graph_id but no meta files inside.
    os.makedirs(os.path.join(builder.graphs_dir, 'Some_Graph', '1.0.0'))
    assert builder._known_release_versions('Some_Graph') == {}


def test_known_release_versions_single_release(builder):
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.0', 'bv_abc')
    assert builder._known_release_versions('Some_Graph') == {'1.0.0': 'bv_abc'}


def test_known_release_versions_multiple_releases(builder):
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.0', 'bv_a')
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.1', 'bv_b')
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '2.0.0', 'bv_c')
    assert builder._known_release_versions('Some_Graph') == {
        '1.0.0': 'bv_a',
        '1.0.1': 'bv_b',
        '2.0.0': 'bv_c',
    }


def test_known_release_versions_only_includes_requested_graph(builder):
    _write_graph_meta(builder.graphs_dir, 'Graph_A', '1.0.0', 'bv_a')
    _write_graph_meta(builder.graphs_dir, 'Graph_B', '5.0.0', 'bv_b')
    assert builder._known_release_versions('Graph_A') == {'1.0.0': 'bv_a'}
    assert builder._known_release_versions('Graph_B') == {'5.0.0': 'bv_b'}


def test_known_release_versions_skips_meta_without_release_version(builder):
    """A meta file with release_version: null should be ignored (not raise)."""
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.0', 'bv_real')
    # Hand-craft a meta file with null release_version to simulate an aborted build.
    aborted_dir = os.path.join(builder.graphs_dir, 'Some_Graph', 'aborted_run')
    os.makedirs(aborted_dir)
    with open(os.path.join(aborted_dir, 'Some_Graph.meta.json'), 'w') as f:
        json.dump({'graph_id': 'Some_Graph', 'release_version': None,
                   'build_version': None, 'sources': [], 'subgraphs': []}, f)
    assert builder._known_release_versions('Some_Graph') == {'1.0.0': 'bv_real'}


def test_known_release_versions_tolerates_corrupt_meta(builder, caplog):
    """A meta file with invalid JSON should be skipped, not crash the scan."""
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.0', 'bv_real')
    corrupt_dir = os.path.join(builder.graphs_dir, 'Some_Graph', 'corrupt')
    os.makedirs(corrupt_dir)
    with open(os.path.join(corrupt_dir, 'Some_Graph.meta.json'), 'w') as f:
        f.write('{this is not valid json')
    assert builder._known_release_versions('Some_Graph') == {'1.0.0': 'bv_real'}


def test_known_release_versions_release_without_build_version(builder):
    """A legacy meta file with a release_version but no build_version returns build_version=None."""
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.0', None)
    assert builder._known_release_versions('Some_Graph') == {'1.0.0': None}


# ---------------------------------------------------------------------------
# _select_release_version
# ---------------------------------------------------------------------------

def test_select_release_version_no_existing(builder):
    """First-ever build for a graph: returns the base (default 1.0 → 1.0.0)."""
    selected = builder._select_release_version(
        'New_Graph', build_version='bv_x', base_release_version=DEFAULT_BASE_RELEASE_VERSION,
    )
    assert selected == '1.0.0'


def test_select_release_version_no_existing_with_explicit_base(builder):
    selected = builder._select_release_version(
        'New_Graph', build_version='bv_x', base_release_version='2.5',
    )
    assert selected == '2.5.0'


def test_select_release_version_reuses_when_build_version_matches(builder):
    """Idempotency: rebuilding with the same inputs reuses the existing release_version."""
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.0', 'bv_match')
    selected = builder._select_release_version(
        'Some_Graph', build_version='bv_match', base_release_version=DEFAULT_BASE_RELEASE_VERSION,
    )
    assert selected == '1.0.0'


def test_select_release_version_bumps_patch_when_build_version_is_new(builder):
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.0', 'bv_old')
    selected = builder._select_release_version(
        'Some_Graph', build_version='bv_new', base_release_version=DEFAULT_BASE_RELEASE_VERSION,
    )
    assert selected == '1.0.1'


def test_select_release_version_bumps_from_highest_existing(builder):
    for rv, bv in [('1.0.0', 'bv_a'), ('1.0.1', 'bv_b'), ('1.0.2', 'bv_c')]:
        _write_graph_meta(builder.graphs_dir, 'Some_Graph', rv, bv)
    selected = builder._select_release_version(
        'Some_Graph', build_version='bv_new', base_release_version=DEFAULT_BASE_RELEASE_VERSION,
    )
    assert selected == '1.0.3'


def test_select_release_version_match_wins_over_bump(builder):
    """If any existing release_version matches the build_version, that release is reused
    even when newer releases exist for different build_versions."""
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.0', 'bv_match')
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.1', 'bv_other')
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.2', 'bv_yet_another')
    selected = builder._select_release_version(
        'Some_Graph', build_version='bv_match', base_release_version=DEFAULT_BASE_RELEASE_VERSION,
    )
    assert selected == '1.0.0'


def test_select_release_version_respects_base_floor(builder):
    """If the highest existing release is below base_release_version, jump to the floor."""
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.5.0', 'bv_old')
    selected = builder._select_release_version(
        'Some_Graph', build_version='bv_new', base_release_version='2.0',
    )
    assert selected == '2.0.0'


def test_select_release_version_ignores_base_when_existing_is_higher(builder):
    """If the highest existing release is above base, just bump from there."""
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '3.1.0', 'bv_old')
    selected = builder._select_release_version(
        'Some_Graph', build_version='bv_new', base_release_version='2.0',
    )
    assert selected == '3.1.1'


def test_select_release_version_handles_match_with_none_build_version(builder):
    """A release with build_version=None should NOT match the new build (build_version match
    requires both sides to be non-None and equal)."""
    _write_graph_meta(builder.graphs_dir, 'Some_Graph', '1.0.0', None)
    selected = builder._select_release_version(
        'Some_Graph', build_version='bv_new', base_release_version=DEFAULT_BASE_RELEASE_VERSION,
    )
    assert selected == '1.0.1'  # bump because no real match was found