"""Tests for the Neptune upload and bulk load logic.

The boto3 clients are mocked - these cover the parts ORION is responsible for: which files get
uploaded and where, the arguments the loader job is started with, and how a load's status is
interpreted. They do not exercise a real Neptune cluster.
"""

import json
from unittest.mock import MagicMock

import pytest

from orion.neptune_loader import (NeptuneLoadError,
                                  _release_s3_uri,
                                  _split_s3_uri,
                                  load_graph_into_neptune,
                                  neptune_endpoint_url,
                                  start_bulk_load,
                                  upload_csvs_to_s3,
                                  wait_for_load)
from orion.neptune_tools import NEPTUNE_MANIFEST_FILENAME


@pytest.fixture
def csv_directory(tmp_path):
    """A directory shaped like the output of create_neptune_csvs()."""
    for filename in ('neptune_TestGraph_nodes.csv.gz', 'neptune_TestGraph_edges.csv.gz'):
        (tmp_path / filename).write_bytes(b'not really gzip, never read here')
    manifest = {
        'graph_id': 'TestGraph',
        'release_version': '1.0.0',
        'format': 'opencypher',
        'userProvidedEdgeIds': True,
        'nodes': ['neptune_TestGraph_nodes.csv.gz'],
        'edges': ['neptune_TestGraph_edges.csv.gz']
    }
    (tmp_path / NEPTUNE_MANIFEST_FILENAME).write_text(json.dumps(manifest))
    return str(tmp_path)


def status_response(status, **overall_status):
    return {'payload': {'overallStatus': dict({'status': status}, **overall_status)}}


def test_split_s3_uri():
    assert _split_s3_uri('s3://bucket/graphs/MyGraph/1.0.0') == ('bucket', 'graphs/MyGraph/1.0.0')
    assert _split_s3_uri('s3://bucket/graphs/') == ('bucket', 'graphs')
    assert _split_s3_uri('s3://bucket') == ('bucket', '')


@pytest.mark.parametrize('bad_uri', ['https://bucket/key', 'bucket/key', 's3://'])
def test_split_s3_uri_rejects_non_s3_uris(bad_uri):
    with pytest.raises(NeptuneLoadError, match='s3://bucket/prefix'):
        _split_s3_uri(bad_uri)


def test_release_s3_uri_gives_every_release_its_own_prefix():
    # the loader reads every object under the prefix it is given, so two releases sharing one
    # prefix would both be loaded
    assert _release_s3_uri('s3://bucket/graphs',
                           {'graph_id': 'MyGraph', 'release_version': '1.0.0'}) == \
        's3://bucket/graphs/MyGraph/1.0.0'
    assert _release_s3_uri('s3://bucket/graphs/',
                           {'graph_id': 'MyGraph', 'release_version': '1.0.1'}) == \
        's3://bucket/graphs/MyGraph/1.0.1'
    # create_neptune_csvs() leaves the version out when it wasn't given one
    assert _release_s3_uri('s3://bucket/graphs',
                           {'graph_id': 'MyGraph', 'release_version': ''}) == \
        's3://bucket/graphs/MyGraph'


def test_neptune_endpoint_url():
    assert neptune_endpoint_url('my-cluster.neptune.amazonaws.com') == \
        'https://my-cluster.neptune.amazonaws.com:8182'
    # an endpoint that already has a scheme is left alone
    assert neptune_endpoint_url('https://my-cluster:8182') == 'https://my-cluster:8182'


def test_upload_separates_nodes_and_edges_by_prefix(csv_directory):
    s3_client = MagicMock()
    source_uris = upload_csvs_to_s3(csv_directory, 's3://my-bucket/graphs/TestGraph/1.0.0',
                                    s3_client=s3_client)

    uploaded_keys = [call.args[2] for call in s3_client.upload_file.call_args_list]
    # nodes and edges go to their own prefixes so the edges job can run with edgeOnlyLoad
    assert uploaded_keys == ['graphs/TestGraph/1.0.0/nodes/neptune_TestGraph_nodes.csv.gz',
                             'graphs/TestGraph/1.0.0/edges/neptune_TestGraph_edges.csv.gz']
    # the loader parses every object under the prefix as graph data, so the manifest stays local
    assert not any(NEPTUNE_MANIFEST_FILENAME in key for key in uploaded_keys)
    # the sources have to end in a slash so the loader treats them as prefixes
    assert source_uris == {'nodes': 's3://my-bucket/graphs/TestGraph/1.0.0/nodes/',
                           'edges': 's3://my-bucket/graphs/TestGraph/1.0.0/edges/'}


def test_upload_fails_when_a_manifest_file_is_missing(csv_directory, tmp_path):
    (tmp_path / 'neptune_TestGraph_edges.csv.gz').unlink()
    with pytest.raises(NeptuneLoadError, match='does not exist'):
        upload_csvs_to_s3(csv_directory, 's3://my-bucket/graphs', s3_client=MagicMock())


def test_start_bulk_load_arguments():
    neptune_client = MagicMock()
    neptune_client.start_loader_job.return_value = {'payload': {'loadId': 'load-1'}}

    load_id = start_bulk_load(neptune_client=neptune_client,
                              source_s3_uri='s3://my-bucket/graphs/',
                              iam_role_arn='arn:aws:iam::1:role/R',
                              region='us-east-1',
                              user_provided_edge_ids=False)

    assert load_id == 'load-1'
    load_arguments = neptune_client.start_loader_job.call_args.kwargs
    assert load_arguments['format'] == 'opencypher'
    assert load_arguments['userProvidedEdgeIds'] is False
    assert load_arguments['s3BucketRegion'] == 'us-east-1'
    # HIGH, Neptune's default, is documented to deadlock on openCypher loads
    assert load_arguments['parallelism'] == 'MEDIUM'
    assert load_arguments['failOnError'] is True
    # the off-by-default settings are left out of the request entirely unless they were asked for
    assert 'edgeOnlyLoad' not in load_arguments
    assert 'queueRequest' not in load_arguments
    assert 'dependencies' not in load_arguments


def test_load_queues_edges_behind_nodes(csv_directory, monkeypatch):
    neptune_client = MagicMock()
    neptune_client.start_loader_job.side_effect = [{'payload': {'loadId': 'nodes-load'}},
                                                   {'payload': {'loadId': 'edges-load'}}]
    boto3_module = MagicMock()
    boto3_module.client.return_value = neptune_client
    monkeypatch.setattr('orion.neptune_loader._import_boto3', lambda: boto3_module)

    load_ids = load_graph_into_neptune(csv_directory=csv_directory,
                                       s3_uri='s3://my-bucket/graphs',
                                       neptune_host='my-cluster',
                                       iam_role_arn='arn:aws:iam::1:role/R',
                                       region='us-east-1',
                                       skip_upload=True,
                                       wait=False)

    assert load_ids == ['nodes-load', 'edges-load']
    nodes_arguments, edges_arguments = [call.kwargs for call in
                                        neptune_client.start_loader_job.call_args_list]

    assert nodes_arguments['source'] == 's3://my-bucket/graphs/TestGraph/1.0.0/nodes/'
    assert 'dependencies' not in nodes_arguments
    assert 'edgeOnlyLoad' not in nodes_arguments

    # the edges job waits on the nodes job, so Neptune enforces the ordering rather than this process
    assert edges_arguments['source'] == 's3://my-bucket/graphs/TestGraph/1.0.0/edges/'
    assert edges_arguments['dependencies'] == ['nodes-load']
    assert edges_arguments['queueRequest'] is True
    # only edge files are under that prefix, so the loader can skip its file scanning pass
    assert edges_arguments['edgeOnlyLoad'] is True


def test_wait_for_load_polls_until_the_load_finishes():
    neptune_client = MagicMock()
    neptune_client.get_loader_job_status.side_effect = [
        status_response('LOAD_IN_QUEUE'),
        status_response('LOAD_IN_PROGRESS', totalRecords=5),
        status_response('LOAD_COMPLETED', totalRecords=10),
    ]

    final_status = wait_for_load(neptune_client, 'load-1', poll_interval=0)

    assert final_status['overallStatus']['totalRecords'] == 10
    assert neptune_client.get_loader_job_status.call_count == 3


def test_wait_for_load_raises_when_the_load_fails():
    neptune_client = MagicMock()
    neptune_client.get_loader_job_status.return_value = status_response('LOAD_FAILED',
                                                                        parsingErrors=3)

    with pytest.raises(NeptuneLoadError, match='LOAD_FAILED'):
        wait_for_load(neptune_client, 'load-1', poll_interval=0)