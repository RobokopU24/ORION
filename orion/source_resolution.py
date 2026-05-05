"""Resolver chain that turns spec-layer source declarations (DataSource,
SubGraphSource) into merge-layer GraphFileSource objects, by either downloading
existing graphs from a remote registry, sourcing them from local storage,
or building them from scratch.
"""

import json
import os

from orion.graph_registry import GraphRegistryClient, GraphRegistryError
from orion.kgx_bundle import KGXBundle
from orion.kgxmodel import (
    DataSource,
    GraphFileSource,
    GraphSource,
    SubGraphSource,
)
from orion.logging import get_orion_logger
from orion.metadata import GraphMetadata, Metadata

logger = get_orion_logger(__name__)


def _load_orion_graph_metadata(graph_id: str, graph_dir: str) -> GraphMetadata | None:
    """Best-effort load of the ORION-internal .meta.json for a built graph dir."""
    try:
        return GraphMetadata(graph_id, graph_dir)
    except Exception:
        return None


def _kgx_metadata_matches_single_source(kgx_graph_metadata: dict,
                                        source_id: str,
                                        release_version: str) -> bool:
    """True if the KGX metadata describes a single-source graph for source_id at release_version."""
    kg_sources = kgx_graph_metadata.get('hasPart', []) or []
    if len(kg_sources) != 1:
        return False
    kg_source_id = kg_sources[0].get('@id', '')
    parts = kg_source_id.rstrip('/').split('/')
    return (len(parts) >= 2
            and parts[-2] == source_id
            and parts[-1] == release_version)


class GraphSourceResolver:
    """Abstract resolver. Caller is responsible for only passing specs the
    resolver handles — the chain in build_dependencies is curated by spec type.
    """

    def resolve(self, spec: GraphSource) -> GraphFileSource | None:
        raise NotImplementedError


class LocalGraphResolver(GraphSourceResolver):
    """Look in the local graphs directory for a built graph satisfying the spec.

    For SubGraphSource: matches {graphs_dir}/{graph_id}/{graph_version}/.
    For DataSource:    matches any {graphs_dir}/{source_id}/<dir>/ whose KGX
                       graph-metadata.json describes a single-source graph for
                       source_id at the spec's release_version.
    """

    def __init__(self, graphs_dir: str):
        self.graphs_dir = graphs_dir

    def resolve(self, spec: GraphSource) -> GraphFileSource | None:
        if isinstance(spec, SubGraphSource):
            return self._resolve_subgraph(spec)
        if isinstance(spec, DataSource):
            return self._resolve_data_source(spec)
        return None

    @staticmethod
    def _build_file_source(graph_dir: str,
                           graph_id: str,
                           version: str,
                           merge_strategy: str | None) -> GraphFileSource | None:
        bundle = KGXBundle(graph_dir)
        if not bundle.has_nodes_and_edges():
            return None
        return GraphFileSource(
            id=graph_id,
            version=version,
            file_paths=[bundle.nodes_path, bundle.edges_path],
            merge_strategy=merge_strategy,
            kgx_graph_metadata=bundle.load_graph_metadata(),
            orion_graph_metadata=_load_orion_graph_metadata(graph_id, graph_dir),
        )

    def _resolve_subgraph(self, spec: SubGraphSource) -> GraphFileSource | None:
        if not spec.graph_version:
            return None
        graph_dir = os.path.join(self.graphs_dir, spec.id, spec.graph_version)
        if not os.path.isdir(graph_dir):
            return None
        orion_metadata = _load_orion_graph_metadata(spec.id, graph_dir)
        if orion_metadata is None or orion_metadata.get_build_status() != Metadata.STABLE:
            return None
        return self._build_file_source(
            graph_dir=graph_dir,
            graph_id=spec.id,
            version=spec.graph_version,
            merge_strategy=spec.merge_strategy,
        )

    def _resolve_data_source(self, spec: DataSource) -> GraphFileSource | None:
        release_version = spec.generate_version()
        if not release_version:
            return None
        source_graph_root = os.path.join(self.graphs_dir, spec.id)
        if not os.path.isdir(source_graph_root):
            return None
        for entry in os.listdir(source_graph_root):
            graph_dir = os.path.join(source_graph_root, entry)
            if not os.path.isdir(graph_dir):
                continue
            kgx_graph_metadata = KGXBundle(graph_dir).load_graph_metadata()
            if kgx_graph_metadata is None:
                continue
            if not _kgx_metadata_matches_single_source(kgx_graph_metadata, spec.id, release_version):
                continue
            file_source = self._build_file_source(
                graph_dir=graph_dir,
                graph_id=spec.id,
                version=release_version,
                merge_strategy=spec.merge_strategy,
            )
            if file_source is None:
                continue
            logger.info(f'LocalGraphResolver matched {spec.id} release {release_version} '
                        f'to existing graph at {graph_dir}.')
            return file_source
        return None


class RegistryGraphResolver(GraphSourceResolver):
    """Look in the remote graph registry for a built graph, download it, and return it.

    Matching mirrors LocalGraphResolver:
      SubGraphSource: graph_id + graph_version exists in the registry.
      DataSource:    a registry version of source_id exists whose KGX metadata
                     describes a single-source graph for source_id at release_version.
    """

    def __init__(self,
                 graphs_dir: str,
                 client: GraphRegistryClient | None = None):
        self.graphs_dir = graphs_dir
        self.client = client or GraphRegistryClient()

    def resolve(self, spec: GraphSource) -> GraphFileSource | None:
        if isinstance(spec, SubGraphSource):
            return self._resolve_subgraph(spec)
        if isinstance(spec, DataSource):
            return self._resolve_data_source(spec)
        return None

    def _resolve_subgraph(self, spec: SubGraphSource) -> GraphFileSource | None:
        if not spec.graph_version:
            return None
        try:
            kgx_graph_metadata = self.client.get_graph_metadata(spec.id, spec.graph_version)
        except GraphRegistryError as e:
            logger.debug(f'Registry has no metadata for {spec.id}/{spec.graph_version}: {e}')
            return None
        return self._materialize(
            spec_id=spec.id,
            graph_id=spec.id,
            graph_version=spec.graph_version,
            kgx_graph_metadata=kgx_graph_metadata,
            merge_strategy=spec.merge_strategy,
        )

    def _resolve_data_source(self, spec: DataSource) -> GraphFileSource | None:
        release_version = spec.generate_version()
        if not release_version:
            return None
        try:
            versions = self.client.get_versions(spec.id)
        except GraphRegistryError as e:
            logger.debug(f'Registry has no versions for {spec.id}: {e}')
            return None
        for version_record in versions:
            graph_version = version_record.get('version')
            if not graph_version:
                continue
            try:
                kgx_graph_metadata = self.client.get_graph_metadata(spec.id, graph_version)
            except GraphRegistryError as e:
                logger.debug(f'Registry metadata fetch failed for {spec.id}/{graph_version}: {e}')
                continue
            if not _kgx_metadata_matches_single_source(kgx_graph_metadata, spec.id, release_version):
                continue
            return self._materialize(
                spec_id=spec.id,
                graph_id=spec.id,
                graph_version=graph_version,
                kgx_graph_metadata=kgx_graph_metadata,
                merge_strategy=spec.merge_strategy,
            )
        return None

    def _materialize(self,
                     spec_id: str,
                     graph_id: str,
                     graph_version: str,
                     kgx_graph_metadata: dict,
                     merge_strategy: str | None) -> GraphFileSource | None:
        graph_dir = os.path.join(self.graphs_dir, graph_id, graph_version)
        os.makedirs(graph_dir, exist_ok=True)
        bundle = KGXBundle(graph_dir)

        # we already have the graph_metadata in memory here, just save it
        if not bundle.has_graph_metadata():
            with open(bundle.graph_metadata_path, 'w') as f:
                json.dump(kgx_graph_metadata, f, indent=2)

        try:
            available_files = self.client.list_files(graph_id, graph_version)
        except GraphRegistryError as e:
            logger.warning(f'Registry file listing failed for {graph_id}/{graph_version}: {e}')
            return None

        available_basenames = {os.path.basename(f.get('file_path', '')): f for f in available_files}
        nodes_basename = self._pick_basename(available_basenames, KGXBundle.NODES_FILENAME)
        edges_basename = self._pick_basename(available_basenames, KGXBundle.EDGES_FILENAME)
        if not nodes_basename or not edges_basename:
            logger.warning(f'Registry version {graph_id}/{graph_version} is missing nodes or edges files.')
            return None

        try:
            self._ensure_local(graph_id, graph_version, nodes_basename, graph_dir, kgx_graph_metadata)
            self._ensure_local(graph_id, graph_version, edges_basename, graph_dir, kgx_graph_metadata)
            self._ensure_local(graph_id, graph_version, KGXBundle.SCHEMA_FILENAME, graph_dir, kgx_graph_metadata)
        except GraphRegistryError as e:
            logger.warning(f'Registry download failed for {graph_id}/{graph_version}: {e}')
            return None

        logger.info(f'RegistryGraphResolver materialized {graph_id}/{graph_version} into {graph_dir}.')
        return GraphFileSource(
            id=spec_id,
            version=graph_version,
            file_paths=[bundle.nodes_path, bundle.edges_path],
            merge_strategy=merge_strategy,
            kgx_graph_metadata=kgx_graph_metadata,
            orion_graph_metadata=_load_orion_graph_metadata(graph_id, graph_dir),
        )

    @staticmethod
    def _pick_basename(available: dict, base: str) -> str | None:
        if f'{base}.gz' in available:
            return f'{base}.gz'
        if base in available:
            return base
        return None

    def _ensure_local(self,
                      graph_id: str,
                      graph_version: str,
                      filename: str,
                      graph_dir: str,
                      kgx_graph_metadata: dict) -> str:
        local_path = os.path.join(graph_dir, filename)
        if os.path.exists(local_path):
            return local_path
        return self.client.download_file(
            graph_id=graph_id,
            graph_version=graph_version,
            filename=filename,
            destination_path=local_path,
            graph_metadata=kgx_graph_metadata,
        )


class IngestPipelineResolver(GraphSourceResolver):
    """Run the ingest pipeline if needed and return its parser output as a
    GraphFileSource. The returned source has no kgx_graph_metadata; it
    represents raw parser output, not a built graph.
    """

    def __init__(self, ingest_pipeline):
        self.ingest_pipeline = ingest_pipeline

    def resolve(self, spec: DataSource) -> GraphFileSource | None:
        source_metadata = self.ingest_pipeline.get_source_metadata(spec.id, spec.source_version)
        release_version = spec.generate_version()
        release_metadata = source_metadata.get_release_info(release_version)
        if release_metadata is None:
            logger.info(f'Running ingest pipeline for {spec.id} release {release_version}.')
            pipeline_success = self.ingest_pipeline.run_pipeline(
                spec.id,
                source_version=spec.source_version,
                parsing_version=spec.parsing_version,
                normalization_scheme=spec.normalization_scheme,
                supplementation_version=spec.supplementation_version,
            )
            if not pipeline_success:
                logger.info(f'Ingest pipeline failed for {spec.id}.')
                return None
            release_metadata = source_metadata.get_release_info(release_version)
            if release_metadata is None:
                return None

        parser_file_paths = self.ingest_pipeline.get_final_file_paths(
            spec.id,
            spec.source_version,
            spec.parsing_version,
            spec.normalization_scheme.get_composite_normalization_version(),
            spec.supplementation_version,
        )
        return GraphFileSource(
            id=spec.id,
            version=release_version,
            file_paths=parser_file_paths,
            merge_strategy=spec.merge_strategy,
        )


class SubgraphBuildResolver(GraphSourceResolver):
    """Build a subgraph from its own graph spec, then return its files."""

    def __init__(self, build_manager):
        self.build_manager = build_manager

    def resolve(self, spec: SubGraphSource) -> GraphFileSource | None:
        subgraph_graph_spec = self.build_manager.graph_specs.get(spec.id)
        if not subgraph_graph_spec:
            logger.warning(f'Subgraph {spec.id} version {spec.graph_version} not found and no graph spec available '
                           f'to build it.')
            return None
        if spec.graph_version != subgraph_graph_spec.graph_version:
            logger.error(f'Subgraph {spec.id} version {spec.graph_version} requested, but the current graph spec '
                         f'produces version {subgraph_graph_spec.graph_version}. Pin the existing version or '
                         f'remove the version specification to use the latest.')
            return None
        logger.warning(f'Subgraph dependency {spec.id} not ready. Building now...')
        if not self.build_manager.build_graph(subgraph_graph_spec):
            return None

        graph_dir = self.build_manager.get_graph_dir_path(spec.id, spec.graph_version)
        orion_metadata = self.build_manager.get_graph_metadata(spec.id, spec.graph_version)
        if orion_metadata.get_build_status() != Metadata.STABLE:
            logger.warning(f'Subgraph {spec.id} version {spec.graph_version} did not reach STABLE status.')
            return None

        bundle = KGXBundle(graph_dir)
        if not bundle.has_nodes_and_edges():
            return None

        return GraphFileSource(
            id=spec.id,
            version=spec.graph_version,
            file_paths=[bundle.nodes_path, bundle.edges_path],
            merge_strategy=spec.merge_strategy,
            kgx_graph_metadata=bundle.load_graph_metadata(),
            orion_graph_metadata=orion_metadata,
        )


def resolve_source(spec: GraphSource, resolvers: list[GraphSourceResolver]) -> GraphFileSource | None:
    """Run each resolver in order for a GraphSource until successful. Return None if it could not be resolved."""
    for resolver in resolvers:
        resolved = resolver.resolve(spec)
        if resolved is not None:
            return resolved
    return None
