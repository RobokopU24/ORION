"""Copy Neptune csv files to S3 and run the bulk load that ingests them.

This is kept separate from building the csv files so that a load failure - which depends on AWS
credentials, network, and cluster state - never invalidates a good build. The files produced by
orion.neptune_tools can be re-loaded as many times as needed.

Requires boto3, which ORION installs with the "neptune" extra.
"""

import os
import time
from urllib.parse import urlparse

from orion.logging import get_orion_logger
from orion.neptune_tools import read_neptune_manifest

logger = get_orion_logger("orion.neptune_loader")

# The loader reports one of these for as long as the job is still running.
LOAD_IN_PROGRESS_STATUSES = frozenset({'LOAD_NOT_STARTED', 'LOAD_IN_QUEUE', 'LOAD_IN_PROGRESS'})
LOAD_COMPLETED_STATUS = 'LOAD_COMPLETED'

# Neptune's default parallelism (HIGH) is documented to deadlock on openCypher loads, reporting
# LOAD_DATA_DEADLOCK, with a lower setting as the recommended fix.
DEFAULT_PARALLELISM = 'MEDIUM'

# The port a Neptune cluster serves its data plane, including the loader, on.
NEPTUNE_DEFAULT_PORT = 8182

# The loader reads every object under the prefix it is given. Uploading nodes and edges to separate
# prefixes means the edges job only ever sees edge files, which lets it run with edgeOnlyLoad and
# skip the first pass that scans every file to work out whether it holds nodes or edges. AWS
# documents that pass as a significant part of load time when there are many edge files.
ENTITY_S3_PREFIXES = {'nodes': 'nodes', 'edges': 'edges'}


class NeptuneLoadError(RuntimeError):
    """Raised when files can not be uploaded, or when a bulk load does not complete."""
    pass


def _import_boto3():
    try:
        import boto3
    except ImportError as e:
        raise NeptuneLoadError('boto3 is required to upload files and run a Neptune bulk load, but it is '
                               'not installed. Install ORION with the neptune extra to get it: '
                               'pip install "robokop-orion[neptune]"') from e
    return boto3


def _split_s3_uri(s3_uri: str):
    parsed_uri = urlparse(s3_uri)
    if parsed_uri.scheme != 's3' or not parsed_uri.netloc:
        raise NeptuneLoadError(f'Expected an s3://bucket/prefix uri but got: {s3_uri}')
    return parsed_uri.netloc, parsed_uri.path.strip('/')


def _load_source_uri(bucket: str, prefix: str):
    """The uri the loader reads. It loads every object under the prefix, so it ends in a slash."""
    return f's3://{bucket}/{prefix}/' if prefix else f's3://{bucket}/'


def _entity_prefix(prefix: str, entity_type: str):
    entity_sub_prefix = ENTITY_S3_PREFIXES[entity_type]
    return f'{prefix}/{entity_sub_prefix}' if prefix else entity_sub_prefix


def _entity_source_uris(s3_uri: str):
    """The nodes and edges source uris under an S3 uri, without uploading anything."""
    bucket, prefix = _split_s3_uri(s3_uri)
    return {entity_type: _load_source_uri(bucket, _entity_prefix(prefix, entity_type))
            for entity_type in ENTITY_S3_PREFIXES}


def _release_s3_uri(s3_uri: str, manifest: dict):
    """The uri one release's files live under, given the base uri to keep releases in.

    The loader reads every object under the prefix it is given, and an upload replaces only the
    files it writes, so each release gets a prefix of its own. Sharing one prefix between releases
    would mean every load ingested every release ever uploaded to it, on top of a cluster the bulk
    loader only ever adds to.
    """
    release_path = '/'.join(part for part in (manifest['graph_id'], manifest['release_version'])
                            if part)
    return f'{s3_uri.rstrip("/")}/{release_path}' if release_path else s3_uri


def neptune_endpoint_url(neptune_host: str, port: int = NEPTUNE_DEFAULT_PORT):
    """Build the data plane url for a cluster from its hostname."""
    if neptune_host.startswith('http'):
        return neptune_host
    return f'https://{neptune_host}:{port}'


def upload_csvs_to_s3(csv_directory: str, s3_uri: str, region: str = None, s3_client=None):
    """Copy the csv files a graph's load manifest lists to s3_uri, nodes and edges kept apart.

    Only the files in the manifest are uploaded. The bulk loader reads every object under the
    prefix it is given, so anything else in the directory - the manifest itself included - would
    be handed to the loader as if it were graph data.

    Returns the source uri to load each entity type from.
    """
    manifest = read_neptune_manifest(csv_directory)
    bucket, prefix = _split_s3_uri(s3_uri)
    s3_client = s3_client if s3_client is not None else _import_boto3().client('s3',
                                                                              region_name=region)

    source_uris = {}
    for entity_type in ENTITY_S3_PREFIXES:
        entity_prefix = _entity_prefix(prefix, entity_type)
        for filename in manifest[entity_type]:
            local_filepath = os.path.join(csv_directory, filename)
            if not os.path.exists(local_filepath):
                raise NeptuneLoadError(f'{filename} is in the load manifest but {local_filepath} '
                                       f'does not exist.')
            object_key = f'{entity_prefix}/{filename}'
            logger.info(f'Uploading {filename} to s3://{bucket}/{object_key}...')
            s3_client.upload_file(local_filepath, bucket, object_key)
        source_uris[entity_type] = _load_source_uri(bucket, entity_prefix)

    logger.info(f'Uploaded Neptune csv files to s3://{bucket}/{prefix}')
    return source_uris


def start_bulk_load(neptune_client,
                    source_s3_uri: str,
                    iam_role_arn: str,
                    region: str,
                    user_provided_edge_ids: bool = True,
                    fail_on_error: bool = True,
                    parallelism: str = DEFAULT_PARALLELISM,
                    mode: str = 'NEW',
                    edge_only_load: bool = False,
                    queue_request: bool = False,
                    dependencies: list = None):
    """Start a bulk load of every file under source_s3_uri, returning its load id."""
    load_arguments: dict[str, object] = {
        'source': source_s3_uri,
        'format': 'opencypher',
        'iamRoleArn': iam_role_arn,
        's3BucketRegion': region,
        'mode': mode,
        'failOnError': fail_on_error,
        'parallelism': parallelism,
        'userProvidedEdgeIds': user_provided_edge_ids
    }
    # These three default to off in Neptune, so they are only sent when they are wanted.
    if edge_only_load:
        load_arguments['edgeOnlyLoad'] = True
    if queue_request:
        load_arguments['queueRequest'] = True
    if dependencies:
        load_arguments['dependencies'] = dependencies

    load_id = neptune_client.start_loader_job(**load_arguments)['payload']['loadId']
    logger.info(f'Started Neptune bulk load {load_id} from {source_s3_uri}')
    return load_id


def wait_for_load(neptune_client, load_id: str, poll_interval: int = 30):
    """Poll a bulk load until it stops, returning its final status payload.

    Raises NeptuneLoadError if the load ends as anything other than LOAD_COMPLETED.
    """
    while True:
        status_payload = neptune_client.get_loader_job_status(loadId=load_id,
                                                              details=True,
                                                              errors=True)['payload']
        overall_status = status_payload['overallStatus']
        load_status = overall_status['status']

        if load_status in LOAD_IN_PROGRESS_STATUSES:
            logger.info(f'Neptune load {load_id}: {load_status}, '
                        f'{overall_status.get("totalRecords", 0)} records so far...')
            time.sleep(poll_interval)
            continue

        if load_status == LOAD_COMPLETED_STATUS:
            logger.info(f'Neptune load {load_id} completed: {overall_status.get("totalRecords", 0)} records '
                        f'in {overall_status.get("totalTimeSpent", 0)} seconds, '
                        f'{overall_status.get("totalDuplicates", 0)} duplicates.')
            return status_payload

        raise NeptuneLoadError(f'Neptune load {load_id} ended with status {load_status}. '
                               f'Overall status: {overall_status}. '
                               f'Errors: {status_payload.get("errors")}')


def load_graph_into_neptune(csv_directory: str,
                            s3_uri: str,
                            neptune_host: str,
                            iam_role_arn: str,
                            region: str,
                            fail_on_error: bool = True,
                            parallelism: str = DEFAULT_PARALLELISM,
                            skip_upload: bool = False,
                            wait: bool = True):
    """Upload a graph's Neptune csv files and load them into a Neptune cluster.

    s3_uri is the base uri releases are kept under. The files for this one go to a prefix named
    for the graph and version in its manifest, which is also where a skip_upload load reads them.

    The S3 bucket must be in the same region as the cluster, and iam_role_arn must be a role that
    is attached to the cluster and can read the bucket.
    """
    manifest = read_neptune_manifest(csv_directory)
    release_s3_uri = _release_s3_uri(s3_uri, manifest)
    source_uris = _entity_source_uris(release_s3_uri) if skip_upload \
        else upload_csvs_to_s3(csv_directory, release_s3_uri, region=region)

    neptune_client = _import_boto3().client('neptunedata',
                                            endpoint_url=neptune_endpoint_url(neptune_host),
                                            region_name=region)
    load_arguments = {'neptune_client': neptune_client,
                      'iam_role_arn': iam_role_arn,
                      'region': region,
                      'user_provided_edge_ids': manifest['userProvidedEdgeIds'],
                      'fail_on_error': fail_on_error,
                      'parallelism': parallelism}

    # Both jobs are submitted up front and Neptune sequences them itself: the edges job is queued
    # behind the nodes job as a dependency, so the edges never load before the nodes they connect
    # even if this process goes away, and a failed nodes load cancels the edges load rather than
    # producing FROM_OR_TO_VERTEX_ARE_MISSING errors for every edge.
    nodes_load_id = start_bulk_load(source_s3_uri=source_uris['nodes'],
                                    queue_request=True,
                                    **load_arguments)
    edges_load_id = start_bulk_load(source_s3_uri=source_uris['edges'],
                                    queue_request=True,
                                    dependencies=[nodes_load_id],
                                    edge_only_load=True,
                                    **load_arguments)
    load_ids = [nodes_load_id, edges_load_id]

    if wait:
        for load_id in load_ids:
            wait_for_load(neptune_client, load_id)
    return load_ids