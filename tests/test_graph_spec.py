import os
import pytest
import requests.exceptions

from unittest.mock import MagicMock
from orion.graph_pipeline import GraphBuilder, GraphSpecError


@pytest.fixture(scope='module')
def test_graph_spec_dir():
    # this is ORION/tests/graph_specs not ORION/graph_specs
    testing_specs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'graph_specs')
    return testing_specs_dir


@pytest.fixture(scope='module')
def test_graph_output_dir():
    testing_output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'workspace')
    return testing_output_dir


def get_ingest_pipeline_mock():
    s_d_mock = MagicMock()
    s_d_mock.get_latest_source_version = MagicMock()
    s_d_mock.get_latest_source_version.side_effect = lambda arg: arg + '_v1'
    return s_d_mock


def test_missing_graph_specs_dir(test_graph_output_dir, tmp_path):
    bogus_dir = tmp_path / "does_not_exist"
    with pytest.raises(GraphSpecError):
        GraphBuilder(graph_specs_dir=str(bogus_dir),
                     graph_output_dir=test_graph_output_dir)


def test_invalid_additional_graph_spec_path(test_graph_spec_dir, test_graph_output_dir):
    with pytest.raises(GraphSpecError):
        GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                     additional_graph_spec='/nonexistent/path/to/spec.yaml',
                     graph_output_dir=test_graph_output_dir)


def test_invalid_additional_graph_spec_url(test_graph_spec_dir, test_graph_output_dir):
    with pytest.raises(requests.exceptions.ConnectionError):
        GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                     additional_graph_spec='http://localhost/invalid_graph_spec_url',
                     graph_output_dir=test_graph_output_dir)


# the graph specs in the directory are auto-loaded; no env var needed
def test_auto_load_graph_specs(test_graph_spec_dir, test_graph_output_dir):
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 graph_output_dir=test_graph_output_dir)
    assert len(graph_builder.graph_specs)
    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph', None)
    assert testing_graph_spec is not None
    assert len(testing_graph_spec.sources) == 2
    for source in testing_graph_spec.sources:
        assert source.version is None
        assert source.source_version is None


# graph spec sources are able to return versions once source_version(s) are set
def test_graph_spec_lazy_versions(test_graph_spec_dir, test_graph_output_dir):
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 graph_output_dir=test_graph_output_dir)
    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph', None)
    for source in testing_graph_spec.sources:
        assert source.version is None
    for source in testing_graph_spec.sources:
        source.source_version = source.id + "_1"
    for source in testing_graph_spec.sources:
        assert source.version is not None


# mock the source_data_manager to return deterministic source_versions
# then see if a graph with a subgraph can properly determine graph versions
def test_graph_spec_subgraph_version(test_graph_spec_dir, test_graph_output_dir):
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 graph_output_dir=test_graph_output_dir)
    graph_builder.ingest_pipeline = get_ingest_pipeline_mock()

    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph_2', None)
    assert testing_graph_spec.graph_version is None
    graph_builder.determine_graph_version(testing_graph_spec)
    for source in testing_graph_spec.sources:
        assert source.source_version == source.id + "_v1"
    testing_graph_spec_sub_graph = graph_builder.graph_specs.get('Testing_Graph', None)
    for source in testing_graph_spec_sub_graph.sources:
        assert source.source_version == source.id + "_v1"
    # TODO it would be nice to check against real version ids here
    # but without pinning specific versions in the graph spec the versions could/should change,
    # currently supplementation version is not specifiable, so it's impossible
    assert testing_graph_spec_sub_graph.graph_version is not None
    assert testing_graph_spec.graph_version is not None


# make sure a graph spec with an invalid subgraph fails with the appropriate exception
def test_graph_spec_invalid_subgraph(test_graph_spec_dir, test_graph_output_dir):
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 graph_output_dir=test_graph_output_dir)
    graph_builder.ingest_pipeline = get_ingest_pipeline_mock()
    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph_3', None)
    assert testing_graph_spec.graph_version is None
    with pytest.raises(GraphSpecError):
        graph_builder.determine_graph_version(testing_graph_spec)


# make sure a graph spec with an invalid subgraph version (which is otherwise valid) fails to build
def test_graph_spec_invalid_subgraph_version(test_graph_spec_dir, test_graph_output_dir):
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 graph_output_dir=test_graph_output_dir)
    graph_builder.ingest_pipeline = get_ingest_pipeline_mock()
    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph_4', None)
    graph_builder.determine_graph_version(testing_graph_spec)
    assert graph_builder.build_graph(testing_graph_spec) is False


# an additional graph spec can introduce new graph_ids alongside the bundled ones
def test_additional_graph_spec_adds_new_graph(test_graph_spec_dir, test_graph_output_dir, tmp_path):
    additional_path = tmp_path / "additional-graph-spec.yaml"
    additional_path.write_text(
        "graphs:\n"
        "  - graph_id: Additional_Graph\n"
        "    graph_name: Additional Graph\n"
        "    sources:\n"
        "      - source_id: HGNC\n"
    )
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 additional_graph_spec=str(additional_path),
                                 graph_output_dir=test_graph_output_dir)
    new_graph = graph_builder.graph_specs.get('Additional_Graph')
    assert new_graph is not None
    assert new_graph.graph_name == 'Additional Graph'
    # bundled graphs are still loaded
    assert graph_builder.graph_specs.get('Testing_Graph') is not None


# an additional graph spec that collides with a bundled graph_id is a hard error
def test_additional_graph_spec_collision_raises(test_graph_spec_dir, test_graph_output_dir, tmp_path):
    colliding_path = tmp_path / "colliding-graph-spec.yaml"
    colliding_path.write_text(
        "graphs:\n"
        "  - graph_id: Testing_Graph\n"
        "    graph_name: Colliding Testing Graph\n"
        "    sources:\n"
        "      - source_id: HGNC\n"
    )
    with pytest.raises(GraphSpecError):
        GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                     additional_graph_spec=str(colliding_path),
                     graph_output_dir=test_graph_output_dir)


# an inline graph spec dict can introduce a new graph_id alongside the bundled ones
def test_inline_graph_spec_adds_new_graph(test_graph_spec_dir, test_graph_output_dir):
    inline_spec = {
        'graphs': [{
            'graph_id': 'Inline_Graph',
            'graph_name': 'Inline Graph',
            'output_format': 'jsonl',
            'sources': [{'source_id': 'HGNC'}],
        }]
    }
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 inline_graph_spec=inline_spec,
                                 graph_output_dir=test_graph_output_dir)
    new_graph = graph_builder.graph_specs.get('Inline_Graph')
    assert new_graph is not None
    assert new_graph.graph_name == 'Inline Graph'
    assert len(new_graph.sources) == 1
    assert new_graph.sources[0].id == 'HGNC'
    # bundled graphs are still loaded
    assert graph_builder.graph_specs.get('Testing_Graph') is not None


# an inline graph spec that collides with a bundled graph_id is a hard error
def test_inline_graph_spec_collision_raises(test_graph_spec_dir, test_graph_output_dir):
    inline_spec = {
        'graphs': [{
            'graph_id': 'Testing_Graph',
            'graph_name': 'Colliding Inline Graph',
            'sources': [{'source_id': 'HGNC'}],
        }]
    }
    with pytest.raises(GraphSpecError):
        GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                     inline_graph_spec=inline_spec,
                     graph_output_dir=test_graph_output_dir)


# the `version:` key in a graph spec sets the release version floor
def test_graph_spec_base_version_parsed(test_graph_spec_dir, test_graph_output_dir, tmp_path):
    spec_path = tmp_path / "versioned-graph-spec.yaml"
    spec_path.write_text(
        "graphs:\n"
        "  - graph_id: Versioned_Graph\n"
        "    version: '2.0'\n"
        "    sources:\n"
        "      - source_id: HGNC\n"
    )
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 additional_graph_spec=str(spec_path),
                                 graph_output_dir=test_graph_output_dir)
    versioned_graph = graph_builder.graph_specs.get('Versioned_Graph')
    assert versioned_graph is not None
    assert versioned_graph.base_version == '2.0'


# a `version:` value that isn't semantic-version-shaped should fail spec parsing
def test_graph_spec_invalid_base_version_raises(test_graph_spec_dir, test_graph_output_dir, tmp_path):
    spec_path = tmp_path / "bad-version-graph-spec.yaml"
    spec_path.write_text(
        "graphs:\n"
        "  - graph_id: Bad_Version_Graph\n"
        "    version: not-a-version\n"
        "    sources:\n"
        "      - source_id: HGNC\n"
    )
    with pytest.raises(GraphSpecError):
        GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                     additional_graph_spec=str(spec_path),
                     graph_output_dir=test_graph_output_dir)
