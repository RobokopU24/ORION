import os
import json
import yaml
import argparse
import datetime
import requests

from xxhash import xxh64_hexdigest

from orion.utils import GetDataPullError
from orion.logging import get_orion_logger
from orion.config import config
from orion.data_sources import get_available_data_sources
from orion.exceptions import DataVersionError, GraphSpecError
from orion.ingest_pipeline import IngestPipeline
from orion.kgx_file_merger import KGXFileMerger
from orion.kgx_validation import validate_graph
from orion.neo4j_tools import create_neo4j_dump
from orion.memgraph_tools import create_memgraph_dump
from orion.kgx_bundle import KGXBundle
from orion.graph_registry import GraphRegistryClient, GraphRegistryError
from orion.graph_versioning import DEFAULT_BASE_RELEASE_VERSION, next_release_version, parse_semver
from orion.kgxmodel import (
    DataSource,
    GraphFileSource,
    GraphSpec,
    SubGraphSource,
)
from orion.normalization import NormalizationScheme, get_current_node_norm_version
from orion.metadata import Metadata, GraphMetadata
from orion.source_resolution import (
    LocalGraphResolver,
    RegistryGraphResolver,
    SubgraphBuildResolver,
    resolve_source,
)
from orion.supplementation import SequenceVariantSupplementation
from orion.meta_kg import MetaKnowledgeGraphBuilder, META_KG_FILENAME, TEST_DATA_FILENAME, EXAMPLE_DATA_FILENAME
from orion.redundant_kg import generate_redundant_kg
from orion.answercoalesce_build import generate_ac_files
from orion.collapse_qualifiers import generate_collapsed_qualifiers_kg
from orion.kgx_metadata import (
    KGXGraphMetadata,
    KGXKnowledgeSource,
    KGXKnowledgeGraphSource,
    ORION_BUILD_VERSION,
    generate_kgx_schema_file,
)


logger = get_orion_logger("orion.graph_pipeline")

REDUNDANT_EDGES_FILENAME = 'redundant_edges.jsonl'
COLLAPSED_QUALIFIERS_FILENAME = 'collapsed_qualifier_edges.jsonl'


class GraphBuilder:

    def __init__(self,
                 additional_graph_spec=None,
                 inline_graph_spec=None,
                 graph_output_dir=None,
                 graph_specs_dir=None,
                 ingest_pipeline: IngestPipeline | None = None):

        self.graphs_dir = graph_output_dir if graph_output_dir else config.get_graphs_dir()
        self.ingest_pipeline = ingest_pipeline if ingest_pipeline is not None else IngestPipeline()
        self.graph_specs = {}  # graph_id -> GraphSpec
        self.load_graph_specs(graph_specs_dir=graph_specs_dir,
                              additional_graph_spec=additional_graph_spec,
                              inline_graph_spec=inline_graph_spec)
        self.build_results = {}

        # The following are dependency resolvers used to locate or build sources for graphs in the most efficient way.
        # LocalGraphResolver checks local disk storage for data sources builds or existing graphs.
        self.local_resolver = LocalGraphResolver(graphs_dir=self.graphs_dir,
                                                 ingest_pipeline=self.ingest_pipeline)
        # RegistryGraphResolver queries the graph registry API to find already-built sources or graphs for download.
        self.registry_resolver = RegistryGraphResolver(graphs_dir=self.graphs_dir,
                                                       ingest_pipeline=self.ingest_pipeline)
        # The SubgraphBuildResolver handles finding or building a subgraph dependency requested by a parent graph.
        self.subgraph_build_resolver = SubgraphBuildResolver(graph_pipeline=self)

    def build_graph(self, graph_spec: GraphSpec):

        graph_id = graph_spec.graph_id
        logger.info(f'Building graph {graph_id}...')

        release_version = self.determine_versions(graph_spec)
        graph_metadata = self.get_graph_metadata(graph_id, release_version)
        graph_output_dir = self.get_graph_dir_path(graph_id, release_version)
        graph_output_url = self.get_graph_output_url(graph_id, release_version)

        # check for previous builds of this same graph
        build_status = graph_metadata.get_build_status()
        if build_status == Metadata.IN_PROGRESS:
            logger.info(f'Graph {graph_id} release_version {release_version} has status: in progress. '
                             f'This means either the graph is already in the process of being built, '
                             f'or an error occurred previously that could not be handled. '
                             f'You may need to clean up and/or remove the failed build.')
            return False

        if build_status == Metadata.BROKEN or build_status == Metadata.FAILED:
            logger.info(f'Graph {graph_id} release_version {release_version} previously failed to build. Skipping..')
            return False

        if build_status == Metadata.STABLE:
            logger.info(f'Graph {graph_id} release_version {release_version} was already built.')
        else:
            # If we get here we need to build the graph
            logger.info(f'Building graph {graph_id} release_version {release_version}, checking dependencies...')
            if not self.build_dependencies(graph_spec):
                logger.warning(f'Aborting graph {graph_spec.graph_id} release_version {release_version}, '
                               f'resolving dependencies failed.')
                return False

            logger.info(f'Building graph {graph_id} release_version {release_version}. '
                             f'Dependencies ready, merging sources...')
            graph_metadata.set_build_status(Metadata.IN_PROGRESS)
            graph_metadata.set_release_version(release_version)
            graph_metadata.set_build_version(graph_spec.build_version)
            graph_metadata.set_graph_name(graph_spec.graph_name)
            graph_metadata.set_graph_description(graph_spec.graph_description)
            graph_metadata.set_graph_url(graph_spec.graph_url)
            graph_metadata.set_graph_spec(graph_spec.get_metadata_representation())

            # merge the sources and write the finalized graph kgx files
            source_merger = KGXFileMerger(graph_spec=graph_spec,
                                          output_directory=graph_output_dir,
                                          nodes_output_filename=KGXBundle.NODES_FILENAME,
                                          edges_output_filename=KGXBundle.EDGES_FILENAME)
            source_merger.merge()
            merge_metadata = source_merger.get_merge_metadata()

            current_time = datetime.datetime.now().isoformat(timespec='seconds')
            if "merge_error" in merge_metadata:
                graph_metadata.set_build_error(merge_metadata["merge_error"], current_time)
                graph_metadata.set_build_status(Metadata.FAILED)
                logger.error(f'Merge error occured while building graph {graph_id}: '
                                  f'{merge_metadata["merge_error"]}')
                return False

            graph_metadata.set_build_info(merge_metadata, current_time)
            graph_metadata.set_build_status(Metadata.STABLE)
            logger.info(f'Building graph {graph_id} complete!')

        kgx_bundle = KGXBundle(graph_output_dir)

        # On re-runs of an already-built graph, the raw jsonl files may have been
        # gzipped and removed by a previous run. Restore them so the trailing steps
        # (QC, metadata, neo4j/memgraph/AC dumps, etc.) can read raw jsonl.
        kgx_bundle.decompress_nodes_and_edges()

        if not graph_metadata.has_qc():
            logger.info(f'Running QC for graph {graph_id}...')
            qc_results = validate_graph(nodes_file_path=kgx_bundle.nodes_path,
                                        edges_file_path=kgx_bundle.edges_path,
                                        graph_id=graph_id,
                                        release_version=release_version,
                                        logger=logger)
            graph_metadata.set_qc_results(qc_results)
            if qc_results['pass']:
                logger.info(f'QC passed for graph {graph_id}.')
            else:
                logger.warning(f'QC failed for graph {graph_id}.')

        # Generate KGX metadata and schema files
        if not kgx_bundle.has_graph_metadata():
            logger.info(f'Generating KGX metadata for {graph_id}...')
            self.generate_kgx_metadata_files(graph_metadata=graph_metadata,
                                             graph_output_dir=graph_output_dir,
                                             graph_output_url=graph_output_url)
            logger.info(f'KGX metadata generated for {graph_id}.')
        if not kgx_bundle.has_schema():
            logger.info(f'Generating KGX Schema for {graph_id}...')
            generate_kgx_schema_file(nodes_filepath=kgx_bundle.nodes_path,
                                     edges_filepath=kgx_bundle.edges_path,
                                     output_dir=graph_output_dir,
                                     graph_output_url=graph_output_url,
                                     graph_name=graph_spec.graph_name,
                                     biolink_version=graph_metadata.get_biolink_version())
            logger.info(f'KGX Schema generated for {graph_id}.')

        needs_meta_kg = not self.has_meta_kg(graph_directory=graph_output_dir)
        needs_test_data = not self.has_test_data(graph_directory=graph_output_dir)
        if needs_meta_kg or needs_test_data:
            logger.info(f'Generating MetaKG and test data for {graph_id}...')
            self.generate_meta_kg_and_test_data(graph_directory=graph_output_dir,
                                                generate_meta_kg=needs_meta_kg,
                                                generate_test_data=needs_test_data)

        output_formats = graph_spec.graph_output_format.lower().split('+') if graph_spec.graph_output_format else []

        # TODO allow these to be specified in the graph spec
        node_property_ignore_list = {'robokop_variant_id'}
        edge_property_ignore_list = None

        # TODO revaluate how the output formats relate to each other, the combinations are getting unwieldy..
        #  For example, if redundant is requested should that be what is used for neo4j/memgraph etc? or do we output
        #  multiple db dumps for each possible variant? As of now all desired outputs must be specified independently
        #  except redundant graphs are always used for AC if present. It might be best to allow specification of
        #  separate chains of transformations to be processed in order, so that it's easy to be explicit about the
        #  combinations, like:
        #  output_format: [['redundant', 'neo4j', 'answercoalesce'], ['collapsed_qualifiers'], ['neo4j']]
        if 'redundant_jsonl' in output_formats:
            logger.info(f'Generating redundant edge KG for {graph_id}...')
            redundant_filepath = kgx_bundle.edges_path.replace(KGXBundle.EDGES_FILENAME, REDUNDANT_EDGES_FILENAME)
            generate_redundant_kg(kgx_bundle.edges_path, redundant_filepath)

        if 'redundant_neo4j' in output_formats:
            logger.info(f'Generating redundant edge KG for {graph_id}...')
            redundant_filepath = kgx_bundle.edges_path.replace(KGXBundle.EDGES_FILENAME, REDUNDANT_EDGES_FILENAME)
            if not os.path.exists(redundant_filepath):
                generate_redundant_kg(kgx_bundle.edges_path, redundant_filepath)
            logger.info(f'Starting Neo4j dump pipeline for redundant {graph_id}...')
            dump_success = create_neo4j_dump(nodes_filepath=kgx_bundle.nodes_path,
                                             edges_filepath=redundant_filepath,
                                             output_directory=graph_output_dir,
                                             graph_id=graph_id,
                                             release_version=release_version,
                                             node_property_ignore_list=node_property_ignore_list,
                                             edge_property_ignore_list=edge_property_ignore_list)
            if dump_success:
                graph_metadata.set_dump(dump_type="neo4j_redundant",
                                        dump_url=f'{graph_output_url}graph_{release_version}_redundant.db.dump')

        if 'collapsed_qualifiers_jsonl' in output_formats:
            logger.info(f'Generating collapsed qualifier predicates KG for {graph_id}...')
            collapsed_qualifiers_filepath = kgx_bundle.edges_path.replace(KGXBundle.EDGES_FILENAME, COLLAPSED_QUALIFIERS_FILENAME)
            generate_collapsed_qualifiers_kg(kgx_bundle.edges_path, collapsed_qualifiers_filepath)

        if 'collapsed_qualifiers_neo4j' in output_formats:
            logger.info(f'Generating collapsed qualifier predicates KG for {graph_id}...')
            collapsed_qualifiers_filepath = kgx_bundle.edges_path.replace(KGXBundle.EDGES_FILENAME, COLLAPSED_QUALIFIERS_FILENAME)
            if not os.path.exists(collapsed_qualifiers_filepath):
                generate_collapsed_qualifiers_kg(kgx_bundle.edges_path, collapsed_qualifiers_filepath)
            logger.info(f'Starting Neo4j dump pipeline for {graph_id} with collapsed qualifiers...')
            dump_success = create_neo4j_dump(nodes_filepath=kgx_bundle.nodes_path,
                                             edges_filepath=collapsed_qualifiers_filepath,
                                             output_directory=graph_output_dir,
                                             graph_id=graph_id,
                                             release_version=release_version,
                                             node_property_ignore_list=node_property_ignore_list,
                                             edge_property_ignore_list=edge_property_ignore_list)
            if dump_success:
                graph_metadata.set_dump(dump_type="neo4j_collapsed_qualifiers",
                                        dump_url=f'{graph_output_url}graph_{release_version}'
                                                     f'_collapsed_qualifiers.db.dump')

        if 'neo4j' in output_formats:
            logger.info(f'Starting Neo4j dump pipeline for {graph_id}...')
            dump_success = create_neo4j_dump(nodes_filepath=kgx_bundle.nodes_path,
                                             edges_filepath=kgx_bundle.edges_path,
                                             output_directory=graph_output_dir,
                                             graph_id=graph_id,
                                             release_version=release_version,
                                             node_property_ignore_list=node_property_ignore_list,
                                             edge_property_ignore_list=edge_property_ignore_list)
            if dump_success:
                graph_metadata.set_dump(dump_type="neo4j",
                                        dump_url=f'{graph_output_url}graph_{release_version}.db.dump')

        if 'memgraph' in output_formats:
            logger.info(f'Starting memgraph dump pipeline for {graph_id}...')
            dump_success = create_memgraph_dump(nodes_filepath=kgx_bundle.nodes_path,
                                                edges_filepath=kgx_bundle.edges_path,
                                                output_directory=graph_output_dir,
                                                graph_id=graph_id,
                                                release_version=release_version,
                                                node_property_ignore_list=node_property_ignore_list,
                                                edge_property_ignore_list=edge_property_ignore_list)
            if dump_success:
                graph_metadata.set_dump(dump_type="memgraph",
                                        dump_url=f'{graph_output_url}memgraph_{release_version}.cypher')

        if 'answercoalesce' in output_formats:
            logger.info(f'Generating answercoalesce files for {graph_id}...')
            if 'redundant_jsonl' in output_formats or 'redundant_neo4j' in output_formats:
                edge_filepath_to_use = kgx_bundle.edges_path.replace(KGXBundle.EDGES_FILENAME, REDUNDANT_EDGES_FILENAME)
            else:
                edge_filepath_to_use = kgx_bundle.edges_path
            ac_output_dir = os.path.join(graph_output_dir, "answercoalesce")
            os.makedirs(ac_output_dir, exist_ok=True)
            generate_ac_files(kgx_bundle.nodes_path, edge_filepath_to_use, ac_output_dir)

        # All processing is complete. Replace the final jsonl files with gzipped
        # versions so downloads are smaller/faster.
        logger.info(f'Compressing final jsonl files for {graph_id}...')
        kgx_bundle.compress_nodes_and_edges()

        # Compress any other jsonl nodes/edges files
        jsonl_files_to_compress = []
        if 'redundant_jsonl' in output_formats or 'redundant_neo4j' in output_formats:
            jsonl_files_to_compress.append(
                kgx_bundle.edges_path.replace(KGXBundle.EDGES_FILENAME, REDUNDANT_EDGES_FILENAME))
        if ('collapsed_qualifiers_jsonl' in output_formats
                or 'collapsed_qualifiers_neo4j' in output_formats):
            jsonl_files_to_compress.append(
                kgx_bundle.edges_path.replace(KGXBundle.EDGES_FILENAME, COLLAPSED_QUALIFIERS_FILENAME))
        for jsonl_path in jsonl_files_to_compress:
            kgx_bundle.compress_jsonl(jsonl_path)
        self._record_build_result(graph_spec, graph_metadata, release_version, graph_output_dir)
        return True

    # Determine the release_version (semver) for a graph, deriving its deterministic build_version
    # from the versions of its data sources and subgraphs along the way. If a release_version was
    # explicitly pinned on the spec, just return it.
    def determine_versions(self, graph_spec: GraphSpec):
        # if the release_version was set or previously determined just back out
        if graph_spec.release_version:
            return graph_spec.release_version
        try:
            # go out and find the latest version for any data source that doesn't have a version specified
            for source in graph_spec.sources:
                if not source.parsing_version:
                    source.parsing_version = self.ingest_pipeline.get_latest_parsing_version(source.id)
                if not source.source_version:
                    source.source_version = self.ingest_pipeline.get_latest_source_version(source.id)
                logger.info(f'Using {source.id} build_version: {source.generate_build_version()}')

            # for sub-graphs, if a release_version isn't specified,
            # use the graph spec for that subgraph to determine one
            for subgraph in graph_spec.subgraphs:
                if not subgraph.release_version:
                    subgraph_graph_spec = self.graph_specs.get(subgraph.id, None)
                    if subgraph_graph_spec:
                        subgraph.release_version = self.determine_versions(subgraph_graph_spec)
                        logger.info(f'Using subgraph {graph_spec.graph_id} release_version: {subgraph.release_version}')
                    else:
                        raise GraphSpecError(f'Subgraph {subgraph.id} requested for graph {graph_spec.graph_id} '
                                             f'but its release_version was not specified and could not be determined '
                                             f'without a graph spec for {subgraph.id}.')
        except (GetDataPullError, DataVersionError) as e:
            raise GraphSpecError(error_message=e.error_message)

        # Compose a string of each source/subgraph's unique identifier (a build_version hash for
        # data sources, a release_version semver for subgraphs) along with its merge strategy.
        # This is what we hash into the parent graph's build_version.
        def _identifier_for_composite(source) -> str:
            if isinstance(source, SubGraphSource):
                return source.release_version
            if isinstance(source, DataSource):
                return source.generate_build_version()
            raise GraphSpecError(f'Unexpected source type in graph spec: {type(source).__name__}')

        composite_parts = []
        for graph_source in (graph_spec.sources or []) + (graph_spec.subgraphs or []):
            identifier = _identifier_for_composite(graph_source)
            if graph_source.merge_strategy:
                composite_parts.append(f'{identifier}_{graph_source.merge_strategy}')
            else:
                composite_parts.append(identifier)
        composite_version_string = '_'.join(composite_parts)
        build_version = xxh64_hexdigest(composite_version_string)
        graph_spec.build_version = build_version
        release_version = self._select_release_version(graph_spec.graph_id, build_version, graph_spec.base_release_version)
        graph_spec.release_version = release_version
        logger.info(f'Versions determined for graph {graph_spec.graph_id}: '
                    f'release_version {release_version}, build_version {build_version} ({composite_version_string})')
        return release_version

    # Pick the release_version (semver) for a build. If a previously released release_version of this
    # graph already has this build_version, reuse it (the contents are identical). Otherwise bump to
    # the next release_version above whatever already exists, never going below the spec's declared
    # base_release_version.
    def _select_release_version(self, graph_id: str, build_version: str, base_release_version: str):
        known_release_versions = self._known_release_versions(graph_id)
        for release_str, recorded_build_version in known_release_versions.items():
            if recorded_build_version and recorded_build_version == build_version:
                logger.info(f'Graph {graph_id} build_version {build_version} was already released as '
                            f'release_version {release_str}.')
                return release_str
        next_release = next_release_version(list(known_release_versions.keys()), base_release_version)
        if known_release_versions:
            logger.info(f'Graph {graph_id} build_version {build_version} is new; releasing as '
                        f'release_version {next_release} (existing release_versions: {sorted(known_release_versions)}).')
        else:
            logger.info(f'Graph {graph_id} has no previous releases; releasing as release_version {next_release}.')
        return next_release

    # Collect known release_versions of a graph, mapped to the build_version each one came from
    # (None when unknown). Looks at the remote graph registry first (if enabled), then local storage.
    def _known_release_versions(self, graph_id: str) -> dict:
        known_release_versions = {}
        if config.ORION_USE_GRAPH_REGISTRY:
            try:
                registry_client = GraphRegistryClient(base_url=config.ORION_GRAPH_REGISTRY_URL, timeout=10.0)
                for version_record in registry_client.get_versions(graph_id):
                    release_version = version_record.get('version')
                    if not release_version:
                        continue
                    build_version = version_record.get('build_version')
                    if build_version is None:
                        try:
                            build_version = registry_client.get_graph_metadata(graph_id, release_version).get(
                                ORION_BUILD_VERSION)
                        except GraphRegistryError as e:
                            logger.debug(f'Could not read registry metadata for {graph_id}/{release_version}: {e}')
                    known_release_versions[release_version] = build_version
            except GraphRegistryError as e:
                logger.debug(f'Graph registry unavailable while versioning {graph_id}: {e}')

        graph_root = os.path.join(self.graphs_dir, graph_id)
        if os.path.isdir(graph_root):
            for entry in os.listdir(graph_root):
                entry_dir = os.path.join(graph_root, entry)
                if not os.path.isfile(os.path.join(entry_dir, f'{graph_id}.meta.json')):
                    continue
                try:
                    graph_metadata = GraphMetadata(graph_id, entry_dir)
                    release_version = graph_metadata.get_release_version()
                    build_version = graph_metadata.get_build_version()
                except (json.JSONDecodeError, KeyError, OSError) as e:
                    logger.debug(f'Skipping unreadable graph metadata in {entry_dir}: {e}')
                    continue
                if not release_version:
                    continue
                # don't let a local entry with no recorded build_version clobber one the registry gave us
                if known_release_versions.get(release_version) and not build_version:
                    continue
                known_release_versions[release_version] = build_version
        return known_release_versions

    # Resolve every spec entry into a GraphFileSource on graph_spec.resolved_sources. Subgraphs
    # run through Local → Registry → SubgraphBuild; data sources run through Local → Registry,
    # then a fresh ingest + source build on a miss. See _resolve_or_build_data_source.
    #
    # When a dependency fails to resolve we continue through the rest of the spec rather than
    # bailing immediately. This still aborts the parent graph (we return False), but lets
    # every dependency get one ingest attempt per invocation. Subsequent runs of any graph
    # that shares those dependencies pick up the cached single-source artifacts instead of
    # re-running the ingests.
    def build_dependencies(self, graph_spec: GraphSpec):
        graph_spec.resolved_sources = []
        all_resolved = True

        subgraph_chain = [self.local_resolver, self.registry_resolver, self.subgraph_build_resolver]
        for subgraph_spec in graph_spec.subgraphs or []:
            resolved = resolve_source(subgraph_spec, subgraph_chain)
            if resolved is None:
                logger.warning(f'Could not resolve subgraph dependency {subgraph_spec.id} '
                               f'release_version {subgraph_spec.release_version} for graph {graph_spec.graph_id}.')
                all_resolved = False
                continue
            graph_spec.resolved_sources.append(resolved)

        for data_source_spec in graph_spec.sources or []:
            resolved = self._resolve_or_build_data_source(data_source_spec)
            if resolved is None:
                logger.info(f'Could not resolve data source {data_source_spec.id} for graph '
                            f'{graph_spec.graph_id}.')
                all_resolved = False
                continue
            graph_spec.resolved_sources.append(resolved)
        return all_resolved

    # Resolve a DataSource by trying the existing-artifact chain first (local then registry source
    # builds); on miss, run the ingest pipeline and produce a finalized source build, returning a
    # GraphFileSource pointing at the files.
    def _resolve_or_build_data_source(self,
                                      data_source_spec: DataSource) -> GraphFileSource | None:
        resolved = resolve_source(data_source_spec, [self.local_resolver, self.registry_resolver])
        if resolved is not None:
            return resolved
        build_version = data_source_spec.generate_build_version()
        if not build_version:
            logger.error(f'Build version could not be resolved for {data_source_spec.id}.')
            return None
        if not self.ingest_pipeline.has_source_build(data_source_spec.id, build_version):
            logger.info(f'Running ingest pipeline for {data_source_spec.id} build_version {build_version}.')
            if not self.ingest_pipeline.run_pipeline(
                    data_source_spec.id,
                    source_version=data_source_spec.source_version,
                    parsing_version=data_source_spec.parsing_version,
                    normalization_scheme=data_source_spec.normalization_scheme,
                    supplementation_version=data_source_spec.supplementation_version):
                logger.error(f'Ingest pipeline failed for {data_source_spec.id}.')
                return None
        return self.ingest_pipeline.load_source_build_file_source(data_source_spec.id,
                                                                  build_version,
                                                                  data_source_spec.merge_strategy)

    @staticmethod
    def has_meta_kg(graph_directory: str):
        return os.path.exists(os.path.join(graph_directory, META_KG_FILENAME))

    @staticmethod
    def has_test_data(graph_directory: str):
        return os.path.exists(os.path.join(graph_directory, TEST_DATA_FILENAME))

    @staticmethod
    def generate_meta_kg_and_test_data(graph_directory: str,
                                       generate_meta_kg: bool = True,
                                       generate_test_data: bool = True,
                                       generate_example_data: bool = True):
        graph_nodes_file_path = os.path.join(graph_directory, KGXBundle.NODES_FILENAME)
        graph_edges_file_path = os.path.join(graph_directory, KGXBundle.EDGES_FILENAME)
        mkgb = MetaKnowledgeGraphBuilder(nodes_file_path=graph_nodes_file_path,
                                         edges_file_path=graph_edges_file_path,
                                         logger=logger)
        if generate_meta_kg:
            meta_kg_file_path = os.path.join(graph_directory, META_KG_FILENAME)
            mkgb.write_meta_kg_to_file(meta_kg_file_path)
        if generate_test_data:
            test_data_file_path = os.path.join(graph_directory, TEST_DATA_FILENAME)
            mkgb.write_test_data_to_file(test_data_file_path)
        if generate_example_data:
            example_data_file_path = os.path.join(graph_directory, EXAMPLE_DATA_FILENAME)
            mkgb.write_example_data_to_file(example_data_file_path)

    # TODO robokop specific metadata should be configurable
    def generate_kgx_metadata_files(self,
                                    graph_metadata: GraphMetadata,
                                    graph_output_dir: str,
                                    graph_output_url: str):

        all_sources = graph_metadata.get_all_sources_metadata()

        kg_sources = []
        knowledge_sources = []
        for source in all_sources:
            kg_sources_part, ks_part = self._kgx_metadata_from_contribution(source)
            kg_sources.extend(kg_sources_part)
            knowledge_sources.extend(ks_part)

        # Create KGXGraphMetadata
        kgx_graph_metadata = KGXGraphMetadata(
            id=graph_output_url,
            name=graph_metadata.get_graph_name(),
            description=graph_metadata.get_graph_description(),
            license='https://spdx.org/licenses/MIT',
            url=graph_output_url,
            version=graph_metadata.get_release_version(),
            build_version=graph_metadata.get_build_version(),
            date_created=graph_metadata.get_build_time(),
            date_modified=graph_metadata.get_build_time(),
            keywords=["knowledge graph", "biomedical", "drug discovery", "translational research", "gene", "disease",
                      "drug", "phenotype", "pathway"],
            creator=[{"@type": "Organization",
                      "@id": "https://ror.org/0130frc33",
                      "name": "Renaissance Computing Institute (RENCI)",
                      "url": "https://renci.org"}],
            contact_point=[{"@type": "ContactPoint",
                            "contactType": "developer",
                            "url": "https://github.com/RobokopU24/ORION/issues"}],
            funder=[
                {
                  "@type": "Organization",
                  "@id": "https://ror.org/05wvpxv85",
                  "name": "National Center for Advancing Translational Sciences (NCATS)",
                  "url": "https://ncats.nih.gov"
                },
                {
                  "@type": "Organization",
                  "@id": "https://ror.org/00j4k1h63",
                  "name": "National Institute of Environmental Health Sciences (NIEHS)",
                  "url": "https://www.niehs.nih.gov"
                },
                {
                  "@type": "Organization",
                  "@id": "https://ror.org/01cwqze88",
                  "name": "National Institutes of Health (NIH)",
                  "url": "https://www.nih.gov"
                }
            ],
            conforms_to=[
                {
                  "@id": "https://w3id.org/biolink/",
                  "name": "Biolink Model"
                },
                {
                  "@id": "https://github.com/biolink/kgx/blob/master/docs/kgx_format.md",
                  "name": "KGX Format"
                }
            ],
            schema={
                "@type": "Dataset",
                "@id": f"{graph_output_url}schema.json",
                "name": "RobokopKG Data Schema",
                "description": "JSON-LD Schema describing the contents of the knowledge graph",
                "encodingFormat": "application/ld+json"
            },
            biolink_version=graph_metadata.get_biolink_version(),
            babel_version=graph_metadata.get_babel_version(),
            kg_sources=kg_sources,
            knowledge_sources=knowledge_sources,
            distribution=[{
                "@type":"DataDownload",
                "encodingFormat":"biolink:KGX",
                "contentUrl":graph_output_url,
            }]
        )

        # Write graph metadata file
        graph_metadata_filepath = os.path.join(graph_output_dir, KGXBundle.GRAPH_METADATA_FILENAME)
        with open(graph_metadata_filepath, 'w') as f:
            f.write(kgx_graph_metadata.to_json())

    # KGX-metadata contribution from a single resolved source. The kgx_graph_metadata on the
    # contribution is what gets copied into the parent's hasPart/isBasedOn; for a source build it
    # was generated by IngestPipeline and stored in its graph-metadata.json, for subgraphs/built graphs
    # it was loaded from the graph's own graph-metadata.json. When hasPart has exactly one entry we
    # override its node/edge counts with this build's merger counts, since the carrier's own counts
    # came from its internal merge and aren't right for this graph. When hasPart has many entries we
    # pass them through unchanged — the merger only has an aggregate count for the carrier as a whole.
    @staticmethod
    def _kgx_metadata_from_contribution(source: dict):
        carrier = source.get('kgx_graph_metadata') or {}
        carrier_kg_sources = carrier.get('hasPart') or []
        carrier_knowledge_sources = carrier.get('isBasedOn') or []

        kg_sources = []
        if len(carrier_kg_sources) == 1:
            kg_source = KGXKnowledgeGraphSource.from_dict(carrier_kg_sources[0])
            if source.get('node_count') is not None:
                kg_source.node_count = source.get('node_count')
            if source.get('edge_count') is not None:
                kg_source.edge_count = source.get('edge_count')
            kg_sources.append(kg_source.to_dict())
        else:
            for kg_source_entry in carrier_kg_sources:
                kg_sources.append(dict(kg_source_entry))

        knowledge_sources = [KGXKnowledgeSource.from_dict(ks_dict)
                             for ks_dict in carrier_knowledge_sources]
        return kg_sources, knowledge_sources

    def load_graph_specs(self, graph_specs_dir=None, additional_graph_spec=None, inline_graph_spec=None):
        # if a graph spec directory was not provided, default to the one included in the codebase
        if not graph_specs_dir:
            graph_specs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'graph_specs')

        # make sure it's a valid directory
        if not os.path.isdir(graph_specs_dir):
            raise GraphSpecError(f'Configuration Error - Graph Specs directory not found: {graph_specs_dir}')

        spec_filenames = sorted(f for f in os.listdir(graph_specs_dir)
                                if f.endswith('.yaml'))
        for spec_filename in spec_filenames:
            spec_path = os.path.join(graph_specs_dir, spec_filename)
            logger.debug(f'Loading graph spec: {spec_filename}')
            with open(spec_path) as spec_file:
                spec_yaml = yaml.safe_load(spec_file)
            self.parse_graph_spec(spec_yaml)

        if additional_graph_spec:
            logger.info(f'Loading additional graph spec: {additional_graph_spec}')
            self.load_additional_graph_spec(additional_graph_spec)

        if inline_graph_spec:
            inline_graph_ids = [g.get('graph_id') for g in inline_graph_spec.get('graphs', [])]
            logger.info(f'Loading inline graph spec with graph_id(s): {inline_graph_ids}')
            self.parse_graph_spec(inline_graph_spec)

    def load_additional_graph_spec(self, additional_graph_spec: str):
        if additional_graph_spec.startswith('http://') or additional_graph_spec.startswith('https://'):
            logger.info(f'Loading additional graph spec from URL: {additional_graph_spec}')
            response = requests.get(additional_graph_spec)
            response.raise_for_status()
            spec_yaml = yaml.safe_load(response.text)
        else:
            if not os.path.isfile(additional_graph_spec):
                raise GraphSpecError(f'Additional graph spec file not found: {additional_graph_spec}')
            logger.info(f'Loading additional graph spec: {additional_graph_spec}')
            with open(additional_graph_spec) as spec_file:
                spec_yaml = yaml.safe_load(spec_file)

        self.parse_graph_spec(spec_yaml)

    def parse_graph_spec(self, graph_spec_yaml):
        graph_id = None
        try:
            for graph_yaml in graph_spec_yaml['graphs']:
                graph_id = graph_yaml['graph_id']
                graph_name = graph_yaml.get('graph_name', '')
                graph_description = graph_yaml.get('graph_description', '')
                graph_url = graph_yaml.get('graph_url', '')

                # parse the list of data sources
                data_sources = [self.parse_data_source_spec(data_source)
                                for data_source in graph_yaml.get('sources', [])]

                # parse the list of subgraphs
                subgraph_sources = [self.parse_subgraph_spec(subgraph)
                                    for subgraph in graph_yaml.get('subgraphs', [])]

                if not data_sources and not subgraph_sources:
                    raise GraphSpecError('Error: No sources were provided for graph: {graph_id}.')

                # see if there are any normalization scheme parameters specified at the graph level
                graph_wide_node_norm_version = graph_yaml.get('node_normalization_version', None)
                graph_wide_edge_norm_version = graph_yaml.get('edge_normalization_version', None)
                graph_wide_conflation = graph_yaml.get('conflation', None)
                graph_wide_strict_norm = graph_yaml.get('strict_normalization', None)
                add_edge_id = graph_yaml.get('add_edge_id', None)
                edge_id_type = graph_yaml.get('edge_id_type', None)
                overwrite_edge_ids = graph_yaml.get('overwrite_edge_ids', True)
                edge_merging_attributes = graph_yaml.get('edge_merging_attributes', None)
                if graph_wide_conflation is not None and type(graph_wide_conflation) != bool:
                    raise GraphSpecError(f'Invalid type (conflation: {graph_wide_conflation}), must be true or false.')
                if graph_wide_strict_norm is not None and type(graph_wide_strict_norm) != bool:
                    raise GraphSpecError(f'Invalid type (strict_normalization: {graph_wide_strict_norm}), must be true or false.')
                if add_edge_id is not None and type(add_edge_id) != bool:
                    raise GraphSpecError(f'Invalid type (add_edge_id: {add_edge_id}), must be true or false.')
                if edge_id_type is not None and edge_id_type not in ('orion', 'uuid'):
                    raise GraphSpecError(f'Invalid edge_id_type: {edge_id_type}, must be "orion" or "uuid".')
                if type(overwrite_edge_ids) != bool:
                    raise GraphSpecError(f'Invalid type (overwrite_edge_ids: {overwrite_edge_ids}), must be true or false.')
                if edge_id_type is not None and add_edge_id is None or add_edge_id is False:
                    add_edge_id = True
                if graph_wide_node_norm_version == 'latest':
                    graph_wide_node_norm_version = get_current_node_norm_version()
                if graph_wide_edge_norm_version == 'latest':
                    graph_wide_edge_norm_version = config.BL_VERSION

                # apply them to all the data sources, this will overwrite anything defined at the source level
                for data_source in data_sources:
                    if data_source.merge_strategy == KGXFileMerger.DONT_MERGE and add_edge_id is not None:
                        raise GraphSpecError(f'Graph {graph_id}, source {data_source.name} has merge_strategy:'
                                             f' dont_merge, which is incompatible with add_edge_id.')
                    if graph_wide_node_norm_version is not None:
                        data_source.normalization_scheme.node_normalization_version = graph_wide_node_norm_version
                    if graph_wide_edge_norm_version is not None:
                        data_source.normalization_scheme.edge_normalization_version = graph_wide_edge_norm_version
                    if graph_wide_conflation is not None:
                        data_source.normalization_scheme.conflation = graph_wide_conflation
                    if graph_wide_strict_norm is not None:
                        data_source.normalization_scheme.strict = graph_wide_strict_norm

                if graph_id in self.graph_specs:
                    raise GraphSpecError(
                        f'Duplicate graph_id encountered: {graph_id}. Every graph_id must '
                        f'be unique across included and user provided Graph Specs. Choose a different graph_id.')

                graph_output_format = graph_yaml.get('output_format', '')

                # Optional release_version floor (semver). Bare yaml numbers like
                # `base_release_version: 2.0` parse as floats, so coerce to str before validating.
                base_release_version = graph_yaml.get('base_release_version', None)
                if base_release_version is None:
                    base_release_version = DEFAULT_BASE_RELEASE_VERSION
                else:
                    base_release_version = str(base_release_version)
                    if parse_semver(base_release_version) is None:
                        raise GraphSpecError(
                            f'Graph {graph_id} has an invalid base_release_version: {base_release_version}. '
                            f'Use a semantic version like "2.0" or "2.1.0" (quote it in yaml).')

                graph_spec = GraphSpec(graph_id=graph_id,
                                       graph_name=graph_name,
                                       graph_description=graph_description,
                                       graph_url=graph_url,
                                       graph_output_format=graph_output_format,
                                       base_release_version=base_release_version,
                                       add_edge_id=add_edge_id,
                                       edge_id_type=edge_id_type,
                                       overwrite_edge_ids=overwrite_edge_ids,
                                       edge_merging_attributes=edge_merging_attributes,
                                       subgraphs=subgraph_sources,
                                       sources=data_sources)
                self.graph_specs[graph_id] = graph_spec
        except KeyError as e:
            error_message = f'Graph Spec missing required field: {e}'
            if graph_id is not None:
                error_message += f"(in graph {graph_id})"
            raise GraphSpecError(error_message)

    def parse_subgraph_spec(self, subgraph_yml):
        subgraph_id = subgraph_yml['graph_id']
        subgraph_release_version = subgraph_yml.get('release_version', None)
        merge_strategy = subgraph_yml.get('merge_strategy', None)
        if merge_strategy == 'default':
            merge_strategy = None
        subgraph_source = SubGraphSource(id=subgraph_id,
                                         release_version=subgraph_release_version,
                                         merge_strategy=merge_strategy)
        return subgraph_source

    def parse_data_source_spec(self, source_yml):
        # get the source id and make sure it's valid
        source_id = source_yml['source_id']
        if source_id not in get_available_data_sources():
            error_message = f'Data source {source_id} is not a valid data source id.'
            logger.error(error_message + " " +
                              f'Valid sources are: {", ".join(get_available_data_sources())}')
            raise GraphSpecError(error_message)

        # read version and normalization specifications from the graph spec
        source_version = source_yml.get('source_version', None)
        parsing_version = source_yml.get('parsing_version', None)
        merge_strategy = source_yml.get('merge_strategy', None)
        node_normalization_version = source_yml.get('node_normalization_version', None)
        edge_normalization_version = source_yml.get('edge_normalization_version', None)
        strict_normalization = source_yml.get('strict_normalization', True)
        conflation = source_yml.get('conflation', False)

        # supplementation and normalization code version cannot be specified, set them to the current version
        supplementation_version = SequenceVariantSupplementation.SUPPLEMENTATION_VERSION

        # source_version and parsing_version are intentionally left unresolved here — resolving them
        # eagerly would import every parser referenced by every auto-loaded graph spec, even when the
        # user is only building one. determine_versions fills them in lazily for the graph
        # actually being built.
        if parsing_version == 'latest':
            parsing_version = None
        if not edge_normalization_version or edge_normalization_version == 'latest':
            edge_normalization_version = config.BL_VERSION

        # do some validation
        if type(strict_normalization) != bool:
            raise GraphSpecError(f'Invalid type (strict_normalization: {strict_normalization}), must be true or false.')
        if type(conflation) != bool:
            raise GraphSpecError(f'Invalid type (conflation: {conflation}), must be true or false.')
        if merge_strategy == 'default':
            merge_strategy = None

        normalization_scheme = NormalizationScheme(node_normalization_version=node_normalization_version,
                                                   edge_normalization_version=edge_normalization_version,
                                                   strict=strict_normalization,
                                                   conflation=conflation)
        data_source = DataSource(id=source_id,
                                 source_version=source_version,
                                 merge_strategy=merge_strategy,
                                 normalization_scheme=normalization_scheme,
                                 parsing_version=parsing_version,
                                 supplementation_version=supplementation_version)
        return data_source

    def get_graph_dir_path(self, graph_id: str, release_version: str):
        return os.path.join(self.graphs_dir, graph_id, release_version)

    @staticmethod
    def get_graph_output_url(graph_id: str, release_version: str):
        return f'{config.ORION_OUTPUT_URL}/{graph_id}/{release_version}/'

    def get_graph_metadata(self, graph_id: str, release_version: str):
        # make sure the output directory exists (where we check for existing GraphMetadata)
        graph_output_dir = self.get_graph_dir_path(graph_id, release_version)

        # load existing or create new metadata file
        return GraphMetadata(graph_id, graph_output_dir)

    def _record_build_result(self,
                             graph_spec: GraphSpec,
                             graph_metadata: GraphMetadata,
                             release_version: str,
                             graph_output_dir: str):
        self.build_results[graph_spec.graph_id] = {
            'graph_id': graph_spec.graph_id,
            'release_version': release_version,
            'build_version': graph_spec.build_version,
            'graph_dir': graph_output_dir,
            'build_status': graph_metadata.get_build_status(),
            'build_time': graph_metadata.get_build_time(),
        }

    # Write the build results from this invocation to graphs_dir/.build_results/<timestamp>.json.
    # Subsequent deployment stages read the most-recent file to discover what just built.
    # Returns the file path written, or None if nothing was built.
    def write_build_results(self) -> str | None:
        if not self.build_results:
            return None
        results_dir = os.path.join(self.graphs_dir, '.build_results')
        os.makedirs(results_dir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H%M%S')
        results_path = os.path.join(results_dir, f'{timestamp}.json')
        with open(results_path, 'w') as f:
            json.dump(list(self.build_results.values()), f, indent=2)
        return results_path


def _generate_inline_graph_spec(graph_id: str, sources_arg: str, output_format: str) -> dict:
    source_ids = [s.strip() for s in sources_arg.split(',') if s.strip()]
    if not source_ids:
        raise GraphSpecError('--sources must list at least one source id (comma-separated).')
    return {
        'graphs': [{
            'graph_id': graph_id,
            'graph_name': graph_id,
            'output_format': output_format or 'jsonl',
            'sources': [{'source_id': s} for s in source_ids],
        }]
    }


def main():
    from orion.logging import configure_cli_logging
    configure_cli_logging()

    parser = argparse.ArgumentParser(description="Merge data sources into complete graphs.")
    parser.add_argument('graph_id',
                        help='ID of the graph to build. Either specify the ID of a graph in a Graph Spec '
                             'or provide a new one along with --sources to build a simple graph without '
                             'using a Graph Spec file.')
    spec_group = parser.add_mutually_exclusive_group()
    spec_group.add_argument('--graph_spec', type=str, default=None,
                            help='Path or URL for an additional Graph Spec yaml file. Its graphs are added '
                                 'to the specs provided automatically (from the graph_specs/ directory).')
    spec_group.add_argument('--sources', type=str, default=None,
                            help='Comma-separated list of data sources to include in a graph.')
    parser.add_argument('--output_format', type=str, default=None,
                        help='Output format for a graph (e.g. jsonl, neo4j). '
                             'Only valid when used with command line --sources.')
    args = parser.parse_args()
    graph_id_arg = args.graph_id
    additional_graph_spec = args.graph_spec
    inline_graph_spec = None

    if args.sources:
        inline_graph_spec = _generate_inline_graph_spec(graph_id_arg, args.sources, args.output_format)
    elif args.output_format:
        parser.error('--output_format is only valid together with --sources.')

    graph_builder = GraphBuilder(additional_graph_spec=additional_graph_spec,
                                 inline_graph_spec=inline_graph_spec)
    if graph_id_arg == "all":
        for graph_spec in graph_builder.graph_specs.values():
            graph_builder.build_graph(graph_spec)
    else:
        graph_spec = graph_builder.graph_specs.get(graph_id_arg, None)
        if graph_spec:
            graph_builder.build_graph(graph_spec)
        else:
            print(f'Invalid graph spec requested: {graph_id_arg}')
    results_path = graph_builder.write_build_results()
    if results_path:
        print(f'Build results written to {results_path}')
    else:
        print('No graphs were built.')


if __name__ == '__main__':
    main()
