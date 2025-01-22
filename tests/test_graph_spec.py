import os
import pytest
import requests.exceptions

from unittest.mock import MagicMock
from Common.build_manager import GraphBuilder, GraphSpecError


def clear_graph_spec_config():
    os.environ['ORION_GRAPH_SPEC'] = ''
    os.environ['ORION_GRAPH_SPEC_URL'] = ''


def reset_graph_spec_config():
    os.environ['ORION_GRAPH_SPEC'] = 'testing-graph-spec.yaml'
    os.environ['ORION_GRAPH_SPEC_URL'] = ''


def get_testing_graph_spec_dir():
    # this is ORION/tests/graph_specs not ORION/graph_specs
    testing_specs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'graph_specs')
    return testing_specs_dir


def get_source_data_manager_mock():
    s_d_mock = MagicMock()
    s_d_mock.get_latest_source_version = MagicMock()
    s_d_mock.get_latest_source_version.side_effect = lambda arg: arg + '_v1'
    return s_d_mock


def test_empty_graph_spec_config():
    clear_graph_spec_config()
    with pytest.raises(GraphSpecError):
        graph_builder = GraphBuilder(graph_specs_dir=get_testing_graph_spec_dir())


def test_invalid_graph_spec_config():
    clear_graph_spec_config()
    os.environ['ORION_GRAPH_SPEC'] = 'invalid-spec.yaml'
    with pytest.raises(GraphSpecError):
        graph_builder = GraphBuilder(graph_specs_dir=get_testing_graph_spec_dir())


def test_invalid_graph_spec_url_config():
    clear_graph_spec_config()
    os.environ['ORION_GRAPH_SPEC_URL'] = 'http://localhost/invalid_graph_spec_url'
    with pytest.raises(requests.exceptions.ConnectionError):
        graph_builder = GraphBuilder()


# the graph spec is loaded up properly but doesn't attempt to determine versions when unspecified
def test_valid_graph_spec_config():
    reset_graph_spec_config()
    graph_builder = GraphBuilder(graph_specs_dir=get_testing_graph_spec_dir())
    assert len(graph_builder.graph_specs)
    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph', None)
    assert testing_graph_spec is not None
    assert len(testing_graph_spec.sources) == 2
    for source in testing_graph_spec.sources:
        assert source.version is None
        assert source.source_version is None


# graph spec sources are able to return versions once source_version(s) are set
def test_graph_spec_lazy_versions():
    reset_graph_spec_config()
    graph_builder = GraphBuilder(graph_specs_dir=get_testing_graph_spec_dir())
    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph', None)
    for source in testing_graph_spec.sources:
        assert source.version is None
    for source in testing_graph_spec.sources:
        source.source_version = source.id + "_1"
    for source in testing_graph_spec.sources:
        assert source.version is not None


# mock the source_data_manager to return deterministic source_versions
# then see if a graph with a subgraph can properly determine graph versions
def test_graph_spec_subgraph_version():
    reset_graph_spec_config()
    graph_builder = GraphBuilder(graph_specs_dir=get_testing_graph_spec_dir())
    graph_builder.source_data_manager = get_source_data_manager_mock()

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
def test_graph_spec_invalid_subgraph():
    reset_graph_spec_config()
    graph_builder = GraphBuilder(graph_specs_dir=get_testing_graph_spec_dir())
    graph_builder.source_data_manager = get_source_data_manager_mock()
    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph_3', None)
    assert testing_graph_spec.graph_version is None
    with pytest.raises(GraphSpecError):
        graph_builder.determine_graph_version(testing_graph_spec)


# make sure a graph spec with an invalid subgraph version (which is otherwise valid) fails to build
def test_graph_spec_invalid_subgraph_version():
    reset_graph_spec_config()
    graph_builder = GraphBuilder(graph_specs_dir=get_testing_graph_spec_dir())
    graph_builder.source_data_manager = get_source_data_manager_mock()
    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph_4', None)
    graph_builder.determine_graph_version(testing_graph_spec)
    assert graph_builder.build_graph(testing_graph_spec) is False
