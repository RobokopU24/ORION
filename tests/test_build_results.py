import json
import os

import pytest

from orion.graph_pipeline import GraphBuilder


@pytest.fixture
def test_graph_spec_dir():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'graph_specs')


def _make_builder(test_graph_spec_dir, tmp_path):
    return GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                        graph_output_dir=str(tmp_path))


def test_write_build_results_no_builds_returns_none(test_graph_spec_dir, tmp_path):
    builder = _make_builder(test_graph_spec_dir, tmp_path)
    assert builder.write_build_results() is None
    assert not (tmp_path / '.build_results').exists()


def test_write_build_results_writes_records(test_graph_spec_dir, tmp_path):
    builder = _make_builder(test_graph_spec_dir, tmp_path)
    builder.build_results['Graph_A'] = {
        'graph_id': 'Graph_A',
        'release_version': '1.0.0',
        'build_version': 'abcdef',
        'graph_dir': str(tmp_path / 'Graph_A' / '1.0.0'),
        'build_status': 'stable',
        'build_time': '2026-05-18T14:30:22',
    }
    builder.build_results['Graph_B'] = {
        'graph_id': 'Graph_B',
        'release_version': '2.1.0',
        'build_version': '123456',
        'graph_dir': str(tmp_path / 'Graph_B' / '2.1.0'),
        'build_status': 'stable',
        'build_time': '2026-05-18T14:31:05',
    }

    results_path = builder.write_build_results()

    assert results_path is not None
    assert os.path.isfile(results_path)
    assert os.path.dirname(results_path) == str(tmp_path / '.build_results')
    assert results_path.endswith('.json')

    with open(results_path) as f:
        records = json.load(f)

    assert isinstance(records, list)
    assert len(records) == 2
    records_by_id = {r['graph_id']: r for r in records}
    assert records_by_id['Graph_A']['release_version'] == '1.0.0'
    assert records_by_id['Graph_A']['build_version'] == 'abcdef'
    assert records_by_id['Graph_B']['build_status'] == 'stable'
    assert records_by_id['Graph_B']['graph_dir'].endswith('Graph_B/2.1.0')
    for record in records:
        assert set(record.keys()) == {
            'graph_id', 'release_version', 'build_version',
            'graph_dir', 'build_status', 'build_time',
        }


def test_write_build_results_creates_results_dir(test_graph_spec_dir, tmp_path):
    builder = _make_builder(test_graph_spec_dir, tmp_path)
    builder.build_results['Graph_A'] = {
        'graph_id': 'Graph_A',
        'release_version': '1.0.0',
        'build_version': 'abcdef',
        'graph_dir': str(tmp_path / 'Graph_A' / '1.0.0'),
        'build_status': 'stable',
        'build_time': '2026-05-18T14:30:22',
    }
    assert not (tmp_path / '.build_results').exists()
    builder.write_build_results()
    assert (tmp_path / '.build_results').is_dir()