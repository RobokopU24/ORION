import gzip
import json
import os

import pytest

from orion.cli.publish_graphs import PublishError, publish_build, publish_graphs
from orion.kgx_bundle import KGXBundle


def make_build(graphs_dir,
               graph_id: str = 'TestGraph',
               build_version: str = 'abc123def456',
               release_version: str = '1.2.0',
               complete: bool = True,
               recorded_build_version: str = None):
    """A build directory laid out the way ORION builds them: {graphs_dir}/{graph_id}/{build_version}/."""
    build_dir = os.path.join(str(graphs_dir), graph_id, build_version)
    os.makedirs(build_dir)
    with gzip.open(os.path.join(build_dir, KGXBundle.NODES_FILENAME + '.gz'), 'wb') as f:
        f.write(b'{"id":"n1"}\n')
    if complete:
        with gzip.open(os.path.join(build_dir, KGXBundle.EDGES_FILENAME + '.gz'), 'wb') as f:
            f.write(b'{"id":"e1"}\n')
    graph_metadata = {'version': release_version,
                      'orion:buildVersion': recorded_build_version or build_version}
    with open(os.path.join(build_dir, KGXBundle.GRAPH_METADATA_FILENAME), 'w') as f:
        json.dump(graph_metadata, f)
    return build_dir


def test_publish_build_copies_to_release_version_path(tmp_path):
    graphs_dir = tmp_path / 'graphs'
    served = tmp_path / 'served'
    build_dir = make_build(graphs_dir)

    destination = publish_build(build_dir,
                                destination_root=str(served),
                                graph_id='TestGraph',
                                build_version='abc123def456')

    # the build_version directory ORION built into is served under its release_version
    assert destination == str(served / 'TestGraph' / '1.2.0')
    assert os.path.isdir(destination)
    assert sorted(os.listdir(destination)) == ['edges.jsonl.gz', 'graph-metadata.json', 'nodes.jsonl.gz']


def test_publish_leaves_an_already_published_release_alone(tmp_path):
    graphs_dir = tmp_path / 'graphs'
    served = tmp_path / 'served'
    build_dir = make_build(graphs_dir)
    publish_build(build_dir, destination_root=str(served), graph_id='TestGraph', build_version='abc123def456')

    published_file = served / 'TestGraph' / '1.2.0' / 'graph-metadata.json'
    published_file.write_text('{"published":"already"}')
    assert publish_build(build_dir, destination_root=str(served), graph_id='TestGraph',
                         build_version='abc123def456') is None
    assert published_file.read_text() == '{"published":"already"}'


def test_publish_overwrite_replaces_and_leaves_no_staging_directory(tmp_path):
    graphs_dir = tmp_path / 'graphs'
    served = tmp_path / 'served'
    build_dir = make_build(graphs_dir)
    publish_build(build_dir, destination_root=str(served), graph_id='TestGraph', build_version='abc123def456')

    (served / 'TestGraph' / '1.2.0' / 'graph-metadata.json').write_text('{"stale":true}')
    publish_build(build_dir, destination_root=str(served), graph_id='TestGraph',
                  build_version='abc123def456', overwrite=True)

    with open(served / 'TestGraph' / '1.2.0' / 'graph-metadata.json') as f:
        assert json.load(f)['version'] == '1.2.0'
    assert os.listdir(served / 'TestGraph') == ['1.2.0']


def test_publish_dry_run_copies_nothing(tmp_path):
    graphs_dir = tmp_path / 'graphs'
    served = tmp_path / 'served'
    build_dir = make_build(graphs_dir)

    destination = publish_build(build_dir, destination_root=str(served), graph_id='TestGraph',
                                build_version='abc123def456', dry_run=True)
    assert destination == str(served / 'TestGraph' / '1.2.0')
    assert not served.exists()


def test_publish_rejects_an_incomplete_build(tmp_path):
    build_dir = make_build(tmp_path / 'graphs', complete=False)
    with pytest.raises(PublishError, match='not a complete build'):
        publish_build(build_dir, destination_root=str(tmp_path / 'served'), graph_id='TestGraph',
                      build_version='abc123def456')


# A build directory whose metadata disagrees with its directory name was moved by hand, so its
# metadata can't be trusted to name a destination.
def test_publish_rejects_build_version_disagreeing_with_directory(tmp_path):
    build_dir = make_build(tmp_path / 'graphs', recorded_build_version='some-other-build')
    with pytest.raises(PublishError, match='records build version'):
        publish_build(build_dir, destination_root=str(tmp_path / 'served'), graph_id='TestGraph',
                      build_version='abc123def456')


def test_publish_rejects_a_non_semver_release_version(tmp_path):
    build_dir = make_build(tmp_path / 'graphs', release_version='latest')
    with pytest.raises(PublishError, match='not a semantic version'):
        publish_build(build_dir, destination_root=str(tmp_path / 'served'), graph_id='TestGraph',
                      build_version='abc123def456')


def test_publish_graphs_publishes_every_graph_and_reports_failures(tmp_path):
    graphs_dir = tmp_path / 'graphs'
    served = tmp_path / 'served'
    make_build(graphs_dir, graph_id='TestGraph', build_version='abc123def456', release_version='1.2.0')
    make_build(graphs_dir, graph_id='HGNC', build_version='99887766aabb', release_version='1.0.0')
    make_build(graphs_dir, graph_id='Broken', build_version='deadbeef', complete=False)

    failed = publish_graphs(graphs_dir=str(graphs_dir), destination_root=str(served))

    # one bad build is reported but doesn't stop the others
    assert failed == 1
    assert os.path.isdir(served / 'TestGraph' / '1.2.0')
    assert os.path.isdir(served / 'HGNC' / '1.0.0')
    assert not (served / 'Broken').exists()