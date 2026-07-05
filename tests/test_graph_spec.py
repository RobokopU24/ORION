import os
import pytest
import requests.exceptions

from unittest.mock import MagicMock
from orion.graph_pipeline import GraphBuilder, GraphSpecError
from orion.ingest_pipeline import IngestPipeline


@pytest.fixture(scope='module')
def test_graph_spec_dir():
    # this is ORION/tests/graph_specs not ORION/graph_specs
    testing_specs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'graph_specs')
    return testing_specs_dir


# Each test gets a fresh, isolated graphs_dir so build artifacts can't bleed between tests or persist
@pytest.fixture
def test_graph_output_dir(tmp_path):
    return str(tmp_path)


def get_ingest_pipeline_mock(storage_dir=None):
    """A real IngestPipeline with every heavy stage stubbed out — so the source-build
    path helpers work, but no network/parsing/normalization/source-build work is
    performed. Source builds always "fail to produce" so callers that try to build a
    source bail out fast (these tests focus on spec/version resolution, not ingest).
    """
    pipeline = IngestPipeline(storage_dir=str(storage_dir)) if storage_dir is not None else IngestPipeline()
    pipeline.get_latest_source_version = MagicMock(side_effect=lambda arg: arg + '_v1')
    pipeline.get_latest_parsing_version = MagicMock(side_effect=lambda arg: arg + '_p1')
    mock_source_metadata = MagicMock()
    mock_source_metadata.get_build_info.return_value = {'parsing_info': {'note': 'fixture'}}
    pipeline.get_source_metadata = MagicMock(return_value=mock_source_metadata)
    pipeline.get_final_file_paths = MagicMock(return_value=None)
    pipeline.run_pipeline = MagicMock(return_value=True)
    return pipeline


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
        assert source.generate_build_version() is None
        assert source.source_version is None


# graph spec sources are able to return a build_version once source_version(s) and parsing_version(s)
# are set. Spec parsing leaves both unresolved to avoid eagerly importing every parser module
# referenced by any auto-loaded graph spec (see parse_data_source_spec).
def test_graph_spec_lazy_versions(test_graph_spec_dir, test_graph_output_dir):
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 graph_output_dir=test_graph_output_dir)
    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph', None)
    for source in testing_graph_spec.sources:
        assert source.generate_build_version() is None
        assert source.parsing_version is None
    for source in testing_graph_spec.sources:
        source.source_version = source.id + "_1"
        source.parsing_version = source.id + "_p1"
    for source in testing_graph_spec.sources:
        assert source.generate_build_version() is not None


# mock the source_data_manager to return deterministic source_versions
# then see if a graph with a subgraph can properly determine versions
def test_graph_spec_subgraph_version(test_graph_spec_dir, test_graph_output_dir):
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 graph_output_dir=test_graph_output_dir)
    graph_builder.ingest_pipeline = get_ingest_pipeline_mock()

    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph_2', None)
    assert testing_graph_spec.release_version is None
    graph_builder.determine_versions(testing_graph_spec)
    for source in testing_graph_spec.sources:
        if graph_builder._is_parser_source(source.id):
            assert source.source_version == source.id + "_v1"
        else:
            # a graph dependency contributes its resolved build_version to the parent's composite
            assert source.build_version is not None
            assert source.release_version is not None
    testing_graph_spec_sub_graph = graph_builder.graph_specs.get('Testing_Graph', None)
    for source in testing_graph_spec_sub_graph.sources:
        assert source.source_version == source.id + "_v1"
    # TODO it would be nice to check against real version ids here
    # but without pinning specific versions in the graph spec the versions could/should change,
    # currently supplementation version is not specifiable, so it's impossible
    assert testing_graph_spec_sub_graph.release_version is not None
    assert testing_graph_spec.release_version is not None

# Invalid_Graph is neither a valid parser nor graph so the spec fails validation outright 
def test_graph_spec_invalid_subgraph(test_graph_spec_dir, test_graph_output_dir, tmp_path):
    spec_path = tmp_path / "invalid-subgraph-graph-spec.yaml"
    spec_path.write_text(
        "graphs:\n"
        "  - graph_id: Invalid_Subgraph_Graph\n"
        "    sources:\n"
        "      - id: GtoPdb\n"
        "      - id: Invalid_Graph\n"
    )
    with pytest.raises(GraphSpecError):
        GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                     additional_graph_spec=str(spec_path),
                     graph_output_dir=test_graph_output_dir)


# a graph dependency pinned to a release_version with no known build_version is a dangling pin;
# versioning fails loudly rather than hashing an unresolvable reference into the parent build_version
def test_graph_spec_invalid_subgraph_version(test_graph_spec_dir, test_graph_output_dir, tmp_path):
    storage_dir = tmp_path / 'storage'
    storage_dir.mkdir()
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 graph_output_dir=test_graph_output_dir,
                                 ingest_pipeline=get_ingest_pipeline_mock(storage_dir=storage_dir))
    testing_graph_spec = graph_builder.graph_specs.get('Testing_Graph_4', None)
    with pytest.raises(GraphSpecError):
        graph_builder.determine_versions(testing_graph_spec)


# an additional graph spec can introduce new graph_ids alongside the bundled ones
def test_additional_graph_spec_adds_new_graph(test_graph_spec_dir, test_graph_output_dir, tmp_path):
    additional_path = tmp_path / "additional-graph-spec.yaml"
    additional_path.write_text(
        "graphs:\n"
        "  - graph_id: Additional_Graph\n"
        "    graph_name: Additional Graph\n"
        "    sources:\n"
        "      - id: HGNC\n"
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
        "      - id: HGNC\n"
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
            'sources': [{'id': 'HGNC'}],
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
            'sources': [{'id': 'HGNC'}],
        }]
    }
    with pytest.raises(GraphSpecError):
        GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                     inline_graph_spec=inline_spec,
                     graph_output_dir=test_graph_output_dir)


# the `base_release_version:` key in a graph spec sets the release_version floor
def test_graph_spec_base_release_version_parsed(test_graph_spec_dir, test_graph_output_dir, tmp_path):
    spec_path = tmp_path / "versioned-graph-spec.yaml"
    spec_path.write_text(
        "graphs:\n"
        "  - graph_id: Versioned_Graph\n"
        "    base_release_version: '2.0'\n"
        "    sources:\n"
        "      - id: HGNC\n"
    )
    graph_builder = GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                                 additional_graph_spec=str(spec_path),
                                 graph_output_dir=test_graph_output_dir)
    versioned_graph = graph_builder.graph_specs.get('Versioned_Graph')
    assert versioned_graph is not None
    assert versioned_graph.base_release_version == '2.0'


# a `base_release_version:` value that isn't semantic-version-shaped should fail spec parsing
def test_graph_spec_invalid_base_release_version_raises(test_graph_spec_dir, test_graph_output_dir, tmp_path):
    spec_path = tmp_path / "bad-version-graph-spec.yaml"
    spec_path.write_text(
        "graphs:\n"
        "  - graph_id: Bad_Version_Graph\n"
        "    base_release_version: not-a-version\n"
        "    sources:\n"
        "      - id: HGNC\n"
    )
    with pytest.raises(GraphSpecError):
        GraphBuilder(graph_specs_dir=test_graph_spec_dir,
                     additional_graph_spec=str(spec_path),
                     graph_output_dir=test_graph_output_dir)

def test_default_graph_spec_defines_robomouse(monkeypatch, test_graph_output_dir):
    default_specs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'graph_specs')
    graph_builder = GraphBuilder(graph_specs_dir=default_specs_dir,
                                 graph_output_dir=test_graph_output_dir)

    baseline_sources = [source.id for source in graph_builder.graph_specs['Baseline'].sources]
    assert 'HumanGOA' in baseline_sources
    assert 'MouseGOA' not in baseline_sources

    robomouse_graph = graph_builder.graph_specs['RoboMouseKG']
    assert [source.id for source in robomouse_graph.sources] == [
        'RobokopKG',
        'MouseGOA',
        'GenomeAllianceOrthologs',
        'OntologicalHierarchy',
    ]
