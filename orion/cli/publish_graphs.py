"""Copy finished graph builds to the location where they are served.

ORION builds into {graphs_dir}/{graph_id}/{build_version}/. Served graphs are keyed by release_version 
instead - the semantic version that graph-metadata.json advertises as its distribution contentUrl, 
and that the registry's endpoints are keyed by. This copies the former layout to the latter:

    {graphs_dir}/{graph_id}/{build_version}/   ->   {destination}/{graph_id}/{release_version}/
"""

import argparse
import os
import shutil
import sys

from orion.config import config
from orion.graph_versioning import is_semver
from orion.kgx_bundle import KGXBundle
from orion.kgx_metadata import ORION_BUILD_VERSION
from orion.logging import get_orion_logger

logger = get_orion_logger(__name__)

STAGING_PREFIX = '.publishing-'


class PublishError(Exception):
    pass


def read_release_version(build_dir: str, build_version: str) -> str:
    """The release_version a build directory should be published as, from its own graph-metadata.json.

    Raises PublishError when the bundle isn't publishable: incomplete, missing/invalid a release
    version, or recording a build_version that disagrees with the directory it sits in (which means
    the directory was moved by hand and its metadata can no longer be trusted to name a destination).
    """
    bundle = KGXBundle(build_dir)
    if not (bundle.has_nodes_and_edges() and bundle.has_graph_metadata()):
        raise PublishError(f'{build_dir} is not a complete build '
                           f'(needs nodes, edges, and {KGXBundle.GRAPH_METADATA_FILENAME}).')
    graph_metadata = bundle.load_graph_metadata()
    release_version = graph_metadata.get('version')
    recorded_build_version = graph_metadata.get(ORION_BUILD_VERSION)
    if not release_version:
        raise PublishError(f'{build_dir} records no release version, nothing to publish it as.')
    if not is_semver(release_version):
        raise PublishError(f'{build_dir} records release version {release_version}, which is not a '
                           f'semantic version.')
    if recorded_build_version and recorded_build_version != build_version:
        raise PublishError(f'{build_dir} records build version {recorded_build_version}, but sits in a '
                           f'directory named {build_version}.')
    return release_version


def publish_build(build_dir: str,
                  destination_root: str,
                  graph_id: str,
                  build_version: str,
                  overwrite: bool = False,
                  dry_run: bool = False) -> str | None:
    """Copy one build directory to {destination_root}/{graph_id}/{release_version}/.

    Returns the destination path, or None when an existing release was left alone.
    """
    release_version = read_release_version(build_dir, build_version)
    graph_destination_root = os.path.join(destination_root, graph_id)
    destination = os.path.join(graph_destination_root, release_version)

    if os.path.exists(destination) and not overwrite:
        logger.info(f'{graph_id} release {release_version} is already published at {destination}, '
                    f'skipping (use --overwrite to replace it).')
        return None

    if dry_run:
        logger.info(f'Would publish {build_dir} -> {destination}')
        return destination

    # Copy into a staging directory alongside the destination (same filesystem, so the rename into
    # place is atomic) and swap it in only once the copy is complete.
    staging = os.path.join(graph_destination_root, f'{STAGING_PREFIX}{release_version}')
    os.makedirs(graph_destination_root, exist_ok=True)
    if os.path.exists(staging):
        shutil.rmtree(staging)
    logger.info(f'Publishing {graph_id} build {build_version} as release {release_version} '
                f'({build_dir} -> {destination})...')
    shutil.copytree(build_dir, staging)
    if os.path.exists(destination):
        previous = f'{destination}{STAGING_PREFIX}previous'
        os.rename(destination, previous)
        os.rename(staging, destination)
        shutil.rmtree(previous)
    else:
        os.rename(staging, destination)
    logger.info(f'Published {graph_id} release {release_version} to {destination}.')
    return destination


def find_builds(graphs_dir: str, graph_id: str = None, build_version: str = None):
    """(graph_id, build_version, build_dir) for every build directory under graphs_dir."""
    if not os.path.isdir(graphs_dir):
        raise PublishError(f'Graphs directory {graphs_dir} does not exist.')
    graph_ids = [graph_id] if graph_id else sorted(os.listdir(graphs_dir))
    for current_graph_id in graph_ids:
        graph_root = os.path.join(graphs_dir, current_graph_id)
        if not os.path.isdir(graph_root):
            if graph_id:
                raise PublishError(f'Graph {graph_id} has no builds in {graphs_dir}.')
            continue
        build_versions = [build_version] if build_version else sorted(os.listdir(graph_root))
        for current_build_version in build_versions:
            build_dir = os.path.join(graph_root, current_build_version)
            if not os.path.isdir(build_dir) or current_build_version.startswith(STAGING_PREFIX):
                continue
            yield current_graph_id, current_build_version, build_dir


def publish_graphs(graphs_dir: str,
                   destination_root: str,
                   graph_id: str = None,
                   build_version: str = None,
                   overwrite: bool = False,
                   dry_run: bool = False) -> int:
    """Publish every matching build. Returns the number that failed."""
    published = skipped = failed = 0
    for current_graph_id, current_build_version, build_dir in find_builds(graphs_dir,
                                                                         graph_id=graph_id,
                                                                         build_version=build_version):
        try:
            if publish_build(build_dir,
                             destination_root=destination_root,
                             graph_id=current_graph_id,
                             build_version=current_build_version,
                             overwrite=overwrite,
                             dry_run=dry_run):
                published += 1
            else:
                skipped += 1
        except PublishError as e:
            # One unpublishable build shouldn't stop the rest; report and carry on.
            logger.error(f'Could not publish {current_graph_id} build {current_build_version}: {e}')
            failed += 1
    logger.info(f'Publishing complete. {published} published, {skipped} skipped, {failed} failed.')
    return failed


def main():
    from orion.logging import configure_cli_logging
    configure_cli_logging()

    ap = argparse.ArgumentParser(
        description='Copy finished graph builds to the directory they are served from, converting '
                    'the build_version directory layout ORION builds into to the release_version '
                    'layout that graph metadata advertises.')
    ap.add_argument('destination',
                    help='Root directory graphs are served from. Builds are copied to '
                         '<destination>/<graph_id>/<release_version>/.')
    ap.add_argument('--graph_id', help='Publish only this graph (default: every graph).')
    ap.add_argument('--build_version', help='Publish only this build version of --graph_id.')
    ap.add_argument('--graphs_dir', default=config.ORION_GRAPHS,
                    help='Directory ORION built into (default: the ORION_GRAPHS environment variable).')
    ap.add_argument('--overwrite', action='store_true',
                    help='Replace a release that is already published (default: leave it alone).')
    ap.add_argument('--dry_run', action='store_true', help='Log what would be published, copying nothing.')
    args = ap.parse_args()

    if args.build_version and not args.graph_id:
        ap.error('--build_version only makes sense with --graph_id.')
    if not args.graphs_dir:
        ap.error('No graphs directory: pass --graphs_dir or set ORION_GRAPHS.')

    try:
        failed = publish_graphs(graphs_dir=args.graphs_dir,
                                destination_root=args.destination,
                                graph_id=args.graph_id,
                                build_version=args.build_version,
                                overwrite=args.overwrite,
                                dry_run=args.dry_run)
    except PublishError as e:
        logger.error(str(e))
        sys.exit(1)
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()