"""Resolver chain that turns GraphSpec source declarations (GraphSource entries)
into GraphFileSource objects ready to merge into graphs, by either sourcing them
from local storage, downloading existing graphs from a remote registry, or
building them from scratch.

A GraphSource id refers to either a parser or another graph. build_dependencies
classifies the id and calls the matching resolver method:
  - resolve_graph:  an existing built graph keyed by release_version.
  - resolve_parser: an existing source build keyed by build_version.
"""

import json
import os

from orion.config import config
from orion.graph_registry import GraphRegistryClient, GraphRegistryError
from orion.kgx_bundle import KGXBundle
from orion.kgxmodel import GraphFileSource, GraphSource
from orion.logging import get_orion_logger

logger = get_orion_logger(__name__)


class LocalGraphResolver:
    """Look in local storage for a built artifact satisfying the spec.

    resolve_graph:  matches {graphs_dir}/{graph_id}/{release_version}/.
    resolve_parser: matches {storage}/{source_id}/builds/{build_version}/.
    """

    def __init__(self, graphs_dir: str, ingest_pipeline):
        self.graphs_dir = graphs_dir
        self.ingest_pipeline = ingest_pipeline

    def resolve_graph(self, spec: GraphSource) -> GraphFileSource | None:
        if not spec.release_version:
            return None
        graph_dir = os.path.join(self.graphs_dir, spec.id, spec.release_version)
        if not os.path.isdir(graph_dir):
            return None
        bundle = KGXBundle(graph_dir)
        if not (bundle.has_nodes_and_edges() and bundle.has_graph_metadata()):
            return None
        return GraphFileSource(
            id=spec.id,
            release_version=spec.release_version,
            file_paths=[bundle.nodes_path, bundle.edges_path],
            merge_strategy=spec.merge_strategy,
            kgx_graph_metadata=bundle.load_graph_metadata(),
        )

    def resolve_parser(self, spec: GraphSource) -> GraphFileSource | None:
        build_version = spec.generate_build_version()
        if not build_version:
            return None
        file_source = self.ingest_pipeline.load_source_build_file_source(spec.id, build_version, spec.merge_strategy)
        if file_source is not None:
            logger.info(f'LocalGraphResolver matched {spec.id} build_version {build_version} '
                        f'to existing source build.')
        return file_source


class RegistryGraphResolver:
    """Look in the remote graph registry for a built artifact, download it, and return it.

    resolve_graph:  requires graph_id + release_version present in the registry's /graphs catalog.
    resolve_parser: requires the registry's source-builds endpoint to have an entry for
                    (source_id, build_version). Downloads the build to local storage and returns
                    a GraphFileSource pointing at it.
    """

    def __init__(self,
                 graphs_dir: str,
                 ingest_pipeline,
                 client: GraphRegistryClient | None = None):
        self.graphs_dir = graphs_dir
        self.ingest_pipeline = ingest_pipeline
        if client is not None:
            self.client = client
        elif config.ORION_USE_GRAPH_REGISTRY:
            self.client = GraphRegistryClient(base_url=config.ORION_GRAPH_REGISTRY_URL)
        else:
            self.client = None

    def resolve_graph(self, spec: GraphSource) -> GraphFileSource | None:
        if self.client is None or not spec.release_version:
            return None
        try:
            kgx_graph_metadata = self.client.get_graph_metadata(spec.id, spec.release_version)
        except GraphRegistryError as e:
            logger.debug(f'Registry has no metadata for {spec.id}/{spec.release_version}: {e}')
            return None
        return self._materialize_subgraph(
            spec_id=spec.id,
            release_version=spec.release_version,
            kgx_graph_metadata=kgx_graph_metadata,
            merge_strategy=spec.merge_strategy,
        )

    def resolve_parser(self, spec: GraphSource) -> GraphFileSource | None:
        if self.client is None:
            return None
        build_version = spec.generate_build_version()
        if not build_version:
            return None
        try:
            build_metadata = self.client.get_source_build_metadata(spec.id, build_version)
        except GraphRegistryError as e:
            logger.debug(f'Registry has no source build for {spec.id}/{build_version}: {e}')
            return None
        if not self._materialize_source_build(spec, build_version, build_metadata):
            return None
        logger.info(f'RegistryGraphResolver downloaded {spec.id} build_version {build_version} '
                    f'into local source_builds cache.')
        return self.ingest_pipeline.load_source_build_file_source(spec.id, build_version, spec.merge_strategy)

    def _materialize_subgraph(self,
                              spec_id: str,
                              release_version: str,
                              kgx_graph_metadata: dict,
                              merge_strategy: str | None) -> GraphFileSource | None:
        graph_dir = os.path.join(self.graphs_dir, spec_id, release_version)
        os.makedirs(graph_dir, exist_ok=True)
        bundle = KGXBundle(graph_dir)

        if not bundle.has_graph_metadata():
            with open(bundle.graph_metadata_path, 'w') as f:
                json.dump(kgx_graph_metadata, f, indent=2)

        try:
            available_files = self.client.list_files(spec_id, release_version)
        except GraphRegistryError as e:
            logger.warning(f'Registry file listing failed for {spec_id}/{release_version}: {e}')
            return None

        available_basenames = {os.path.basename(f.get('file_path', '')): f for f in available_files}
        nodes_basename = self._pick_basename(available_basenames, KGXBundle.NODES_FILENAME)
        edges_basename = self._pick_basename(available_basenames, KGXBundle.EDGES_FILENAME)
        if not nodes_basename or not edges_basename:
            logger.warning(f'Registry release {spec_id}/{release_version} is missing nodes or edges files.')
            return None

        try:
            self._ensure_graph_file_local(spec_id, release_version, nodes_basename, graph_dir, kgx_graph_metadata)
            self._ensure_graph_file_local(spec_id, release_version, edges_basename, graph_dir, kgx_graph_metadata)
            self._ensure_graph_file_local(spec_id, release_version, KGXBundle.SCHEMA_FILENAME, graph_dir, kgx_graph_metadata)
        except GraphRegistryError as e:
            logger.warning(f'Registry download failed for {spec_id}/{release_version}: {e}')
            return None

        logger.info(f'RegistryGraphResolver materialized {spec_id}/{release_version} into {graph_dir}.')
        return GraphFileSource(
            id=spec_id,
            release_version=release_version,
            file_paths=[bundle.nodes_path, bundle.edges_path],
            merge_strategy=merge_strategy,
            kgx_graph_metadata=kgx_graph_metadata,
        )

    def _materialize_source_build(self,
                                  spec: GraphSource,
                                  build_version: str,
                                  build_metadata: dict) -> bool:
        """Download the registry's source build files into the local source_builds cache.

        Writes graph-metadata.json + nodes/edges into
        {STORAGE}/{source_id}/builds/{build_version}/. Returns False on failure.
        """
        bundle = self.ingest_pipeline.get_source_build_bundle(spec.id, build_version)
        target_dir = bundle.graph_dir
        os.makedirs(target_dir, exist_ok=True)

        with open(bundle.graph_metadata_path, 'w') as f:
            json.dump(build_metadata, f, indent=2)

        try:
            available_files = self.client.list_source_build_files(spec.id, build_version)
        except GraphRegistryError as e:
            logger.warning(f'Registry file listing failed for source build {spec.id}/{build_version}: {e}')
            return False

        available_basenames = {os.path.basename(f.get('file_path', '')): f for f in available_files}
        nodes_basename = self._pick_basename(available_basenames, KGXBundle.NODES_FILENAME)
        edges_basename = self._pick_basename(available_basenames, KGXBundle.EDGES_FILENAME)
        if not nodes_basename or not edges_basename:
            logger.warning(f'Registry source build {spec.id}/{build_version} is missing nodes or edges files.')
            return False

        try:
            self._ensure_source_build_file_local(spec.id, build_version, nodes_basename, target_dir)
            self._ensure_source_build_file_local(spec.id, build_version, edges_basename, target_dir)
        except GraphRegistryError as e:
            logger.warning(f'Registry source-build download failed for {spec.id}/{build_version}: {e}')
            return False
        return True

    @staticmethod
    def _pick_basename(available: dict, base: str) -> str | None:
        if f'{base}.gz' in available:
            return f'{base}.gz'
        if base in available:
            return base
        return None

    def _ensure_graph_file_local(self,
                                 graph_id: str,
                                 release_version: str,
                                 filename: str,
                                 graph_dir: str,
                                 kgx_graph_metadata: dict) -> str:
        local_path = os.path.join(graph_dir, filename)
        if os.path.exists(local_path):
            return local_path
        return self.client.download_file(
            graph_id=graph_id,
            release_version=release_version,
            filename=filename,
            destination_path=local_path,
            graph_metadata=kgx_graph_metadata,
        )

    def _ensure_source_build_file_local(self,
                                         source_id: str,
                                         build_version: str,
                                         filename: str,
                                         target_dir: str) -> str:
        local_path = os.path.join(target_dir, filename)
        if os.path.exists(local_path):
            return local_path
        return self.client.download_source_build_file(
            source_id=source_id,
            build_version=build_version,
            filename=filename,
            destination_path=local_path,
        )


class SubgraphBuildResolver:
    """Build a graph dependency from its own graph spec, then return its files."""

    def __init__(self, graph_pipeline):
        self.graph_pipeline = graph_pipeline

    def resolve_graph(self, spec: GraphSource) -> GraphFileSource | None:
        subgraph_graph_spec = self.graph_pipeline.graph_specs.get(spec.id)
        if not subgraph_graph_spec:
            logger.warning(f'Subgraph {spec.id} release_version {spec.release_version} not found and no '
                           f'graph spec available to build it.')
            return None
        if spec.release_version != subgraph_graph_spec.release_version:
            logger.error(f'Subgraph {spec.id} release_version {spec.release_version} requested, but the '
                         f'current graph spec produces release_version {subgraph_graph_spec.release_version}. '
                         f'Pin the existing release_version or remove it to use the latest.')
            return None
        logger.warning(f'Subgraph dependency {spec.id} not ready. Building now...')
        if not self.graph_pipeline.build_graph(subgraph_graph_spec):
            return None

        graph_dir = self.graph_pipeline.get_graph_dir_path(spec.id, spec.release_version)
        bundle = KGXBundle(graph_dir)
        if not (bundle.has_nodes_and_edges() and bundle.has_graph_metadata()):
            return None

        return GraphFileSource(
            id=spec.id,
            release_version=spec.release_version,
            file_paths=[bundle.nodes_path, bundle.edges_path],
            merge_strategy=spec.merge_strategy,
            kgx_graph_metadata=bundle.load_graph_metadata(),
        )


def resolve_source(spec: GraphSource, resolvers: list) -> GraphFileSource | None:
    """Run each resolver method in order for a GraphSource until one succeeds.

    `resolvers` is a list of bound resolver methods (e.g. local.resolve_parser,
    registry.resolve_parser) — build_dependencies picks the chain by producer type.
    Returns None if it could not be resolved.
    """
    for resolver in resolvers:
        resolved = resolver(spec)
        if resolved is not None:
            return resolved
    return None
