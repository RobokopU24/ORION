"""Resolve a GraphSpec source (a GraphSource) into a GraphFileSource ready to merge.

A GraphSource source id refers to either a parser or another graph; either way this module attempts to resolve it
as an on-disk KGX bundle by one of the following techniques, in order of preference:

  1. local    — find a KGX bundle already on disk.
  2. registry — download it from the graph registry.
  3. produce  — build it: run the ingest pipeline for a parser, or build the subgraph for a graph
                dependency. Only an unpinned source (a parser or graph without specific build or release
                versions specified) can be produced on a miss; a pinned source that misses both local and
                registry simply fails to resolve.
"""

import json
import os

from orion.config import config
from orion.graph_registry import GraphRegistryClient, GraphRegistryError
from orion.graph_versioning import DEFAULT_BASE_RELEASE_VERSION
from orion.kgx_bundle import KGXBundle
from orion.kgx_metadata import KGXGraphMetadata, KGXKnowledgeSource, KGXKnowledgeGraphSource
from orion.kgxmodel import GraphFileSource, GraphSource, GraphSpec
from orion.logging import get_orion_logger

logger = get_orion_logger(__name__)


class SourceResolver:

    def __init__(self, graph_builder, client: GraphRegistryClient | None = None):
        self.gb = graph_builder
        if client is not None:
            self.client = client
        elif config.ORION_USE_GRAPH_REGISTRY:
            self.client = GraphRegistryClient(base_url=config.ORION_GRAPH_REGISTRY_URL)
        else:
            self.client = None

    def resolve(self, source: GraphSource) -> GraphFileSource | None:
        return (self._resolve_local(source)
                or self._resolve_registry(source)
                or self._produce(source))

    def resolve_with_status(self, source: GraphSource) -> tuple[GraphFileSource | None, str, str | None]:
        """Like resolve(), but returns (result, status, error_message).
        status is one of: 'cached', 'built', 'failed'.
        """
        result = self._resolve_local(source)
        if result:
            return result, 'cached', None
        result = self._resolve_registry(source)
        if result:
            return result, 'cached', None
        result = self._produce(source)
        if result:
            return result, 'built', None
        return None, 'failed', getattr(self, '_last_error', None)

    # The known accessible release_versions of a graph, mapped to the build_version each came from,
    # gathered from the registry (when enabled) and local storage.
    def known_release_versions(self, graph_id: str) -> dict[str, str | None]:
        known: dict[str, str | None] = {}
        if self.client is not None:
            try:
                for record in self.client.get_versions(graph_id):
                    release_version = record.get('version')
                    if release_version:
                        known[release_version] = record.get('build_version')
            except GraphRegistryError as e:
                logger.warning(f'Graph registry unavailable while versioning {graph_id}: {e}')

        graph_root = os.path.join(self.gb.graphs_dir, graph_id)
        if os.path.isdir(graph_root):
            for entry in os.listdir(graph_root):
                bundle = KGXBundle(os.path.join(graph_root, entry))
                if not bundle.has_graph_metadata():
                    continue
                try:
                    graph_metadata = KGXGraphMetadata.from_dict(bundle.load_graph_metadata())
                    release_version = graph_metadata.get_release_version()
                    build_version = graph_metadata.get_build_version()
                except (json.JSONDecodeError, KeyError, OSError) as e:
                    logger.debug(f'Skipping unreadable graph metadata in {graph_root}/{entry}: {e}')
                    continue
                if not release_version:
                    continue
                # don't let a local entry with no recorded build_version clobber one the registry gave us
                if known.get(release_version) and not build_version:
                    continue
                known[release_version] = build_version
        return known

    def _resolve_local(self, source: GraphSource) -> GraphFileSource | None:
        return self._load_bundle(source, source.build_version)

    def _load_bundle(self, source: GraphSource, build_version: str | None) -> GraphFileSource | None:
        if not build_version:
            return None
        graph_dir = self.gb.get_graph_dir_path(source.id, build_version)
        if not os.path.isdir(graph_dir):
            return None
        bundle = KGXBundle(graph_dir)
        if not (bundle.has_nodes_and_edges() and bundle.has_graph_metadata()):
            return None
        return GraphFileSource(
            id=source.id,
            release_version=source.release_version,
            build_version=build_version,
            file_paths=[bundle.nodes_path, bundle.edges_path],
            merge_strategy=source.merge_strategy,
            kgx_graph_metadata=bundle.load_graph_metadata(),
        )

    def _resolve_registry(self, source: GraphSource) -> GraphFileSource | None:
        if self.client is None or not source.build_version:
            return None
        try:
            release_version = self.client.release_version_for_build_version(source.id, source.build_version)
            if not release_version:
                return None
            kgx_graph_metadata = self.client.get_graph_metadata(source.id, release_version)
            if kgx_graph_metadata is None:
                logger.warning(f'Registry lists {source.id}/{release_version} but returned no metadata for it.')
                return None
            if not self._download_bundle(source, release_version, kgx_graph_metadata):
                return None
        except GraphRegistryError as e:
            logger.warning(f'Graph registry unavailable resolving {source.id} '
                           f'build_version {source.build_version}: {e}')
            return None
        logger.info(f'Downloaded {source.id} build_version {source.build_version} from the registry.')
        return self._load_bundle(source, source.build_version)

    def _download_bundle(self, source: GraphSource, release_version: str, kgx_graph_metadata: dict) -> bool:
        # Local storage stays keyed by build_version even though the registry is fetched by release.
        graph_dir = self.gb.get_graph_dir_path(source.id, source.build_version)
        os.makedirs(graph_dir, exist_ok=True)
        bundle = KGXBundle(graph_dir)
        if not bundle.has_graph_metadata():
            with open(bundle.graph_metadata_path, 'w') as f:
                json.dump(kgx_graph_metadata, f, indent=2)

        try:
            available_files = self.client.list_files(source.id, release_version)
        except GraphRegistryError as e:
            logger.warning(f'Registry file listing failed for {source.id} release_version {release_version}: {e}')
            return False

        available = {os.path.basename(f.get('file_path', '')): f for f in available_files}
        nodes_basename = self._pick_basename(available, KGXBundle.NODES_FILENAME)
        edges_basename = self._pick_basename(available, KGXBundle.EDGES_FILENAME)
        if not nodes_basename or not edges_basename:
            logger.warning(f'Registry {source.id} build_version {source.build_version} is missing nodes or edges.')
            return False

        files_to_download = [nodes_basename, edges_basename]
        if KGXBundle.SCHEMA_FILENAME in available:
            files_to_download.append(KGXBundle.SCHEMA_FILENAME)

        try:
            for filename in files_to_download:
                self._download_file(source.id, filename, graph_dir, kgx_graph_metadata)
        except GraphRegistryError as e:
            logger.warning(f'Registry download failed for {source.id} build_version {source.build_version}: {e}')
            return False
        logger.info(f'Materialized {source.id} build_version {source.build_version} into {graph_dir}.')
        return True

    @staticmethod
    def _pick_basename(available: dict, base: str) -> str | None:
        if f'{base}.gz' in available:
            return f'{base}.gz'
        if base in available:
            return base
        return None

    def _download_file(self, graph_id: str, filename: str, graph_dir: str, kgx_graph_metadata: dict) -> str:
        local_path = os.path.join(graph_dir, filename)
        if os.path.exists(local_path):
            return local_path
        return self.client.download_file(
            graph_id=graph_id,
            filename=filename,
            destination_path=local_path,
            graph_metadata=kgx_graph_metadata,
        )

    def _produce(self, source: GraphSource) -> GraphFileSource | None:
        self._last_error = None
        if self.gb.is_parser_source(source.id):
            # Only an unpinned parser carries a recipe (normalization_scheme) to build from.
            if source.normalization_scheme is None:
                self._last_error = (f'Source {source.id} not found locally or in the registry, '
                                    f'and carries no recipe to build it.')
                logger.error(self._last_error)
                return None
            result = self._build_parser(source)
            if result is None:
                self._last_error = f'Ingest pipeline failed for {source.id}.'
            return result

        subgraph_spec = self.gb.graph_specs.get(source.id)
        if subgraph_spec is None:
            self._last_error = f'Source {source.id} is not a known data source and has no graph spec to build it.'
            logger.error(self._last_error)
            return None
        # A graph dependency is buildable only when the current spec reproduces the requested
        # build_version; a pin to any other version is lookup-only and fails on a miss.
        self.gb.determine_versions(subgraph_spec)
        if source.build_version != subgraph_spec.build_version:
            self._last_error = (f'Graph dependency {source.id} is pinned to build_version {source.build_version}, '
                                f'but the current graph spec produces {subgraph_spec.build_version}; '
                                f'not found locally or in the registry and cannot be rebuilt.')
            logger.error(self._last_error)
            return None
        logger.warning(f'Graph dependency {source.id} not ready. Building now...')
        if not self.gb.build_graph(subgraph_spec):
            self._last_error = f'Graph dependency {source.id} failed to build.'
            return None
        return self._load_bundle(source, source.build_version)

    # Build a parser source's single-source graph: run the ingest pipeline to materialize the raw
    # normalized parser output, then merge-and-finalize it into a bundle at {graphs_dir}/{id}/{build_version}/
    # exactly like any graph. The raw output carries a source.json-derived provenance carrier so the
    # bundle's graph-metadata.json records its knowledge source.
    def _build_parser(self, source: GraphSource) -> GraphFileSource | None:
        build_version = source.build_version
        ingest_pipeline = self.gb.ingest_pipeline
        logger.info(f'Running ingest pipeline for {source.id} build_version {build_version}.')
        if not ingest_pipeline.run_pipeline(source.id,
                                            source_version=source.source_version,
                                            parsing_version=source.parsing_version,
                                            normalization_scheme=source.normalization_scheme,
                                            supplementation_version=source.supplementation_version):
            logger.error(f'Ingest pipeline failed for {source.id}.')
            return None
        raw_file_paths = ingest_pipeline.get_final_file_paths(
            source.id,
            source.source_version,
            source.parsing_version,
            source.normalization_scheme.get_composite_normalization_version(),
            source.supplementation_version)
        if not raw_file_paths:
            logger.error(f'Parser output files for {source.id} build_version {build_version} were not found.')
            return None

        release_version = self.gb._select_release_version(source.id, build_version, DEFAULT_BASE_RELEASE_VERSION)
        graph_output_dir = self.gb.get_graph_dir_path(source.id, build_version)
        graph_output_url = self.gb.get_graph_output_url(source.id, release_version)
        carrier, graph_name = self._parser_source_carrier(source, build_version, graph_output_url)

        parser_graph_spec = GraphSpec(graph_id=source.id,
                                      graph_name=graph_name,
                                      graph_description='',
                                      graph_url='',
                                      graph_output_format='jsonl',
                                      sources=[source])
        parser_graph_spec.build_version = build_version
        parser_graph_spec.release_version = release_version
        parser_graph_spec.resolved_sources = [GraphFileSource(id=source.id,
                                                              build_version=build_version,
                                                              file_paths=raw_file_paths,
                                                              merge_strategy=source.merge_strategy,
                                                              kgx_graph_metadata=carrier)]
        if not self.gb.merge_and_finalize(parser_graph_spec, graph_output_dir, graph_output_url):
            return None
        return self._load_bundle(source, build_version)

    # A provenance carrier for a parser's raw output: hasPart (this source as a knowledge-graph source)
    # and isBasedOn (its knowledge source, from the parser's source.json). generate_kgx_metadata_files
    # copies these into the finished bundle's graph-metadata.json; the merge overrides the placeholder
    # node/edge counts. Returns the carrier plus the derived graph name.
    def _parser_source_carrier(self, source: GraphSource, build_version: str, source_url: str):
        parser_metadata = self.gb.ingest_pipeline.load_parser_metadata(source.id)
        graph_name = f'A ROBOKOP Knowledge Graph based on {parser_metadata.get("name", source.id)}'
        knowledge_source = KGXKnowledgeSource.from_dict(parser_metadata)
        knowledge_source.version = source.source_version
        kg_source = KGXKnowledgeGraphSource(id=source_url,
                                            name=graph_name,
                                            build_version=build_version,
                                            node_count=0,
                                            edge_count=0)
        carrier = {'hasPart': [kg_source.to_dict()],
                   'isBasedOn': [knowledge_source.to_dict()]}
        return carrier, graph_name