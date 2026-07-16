import os
import json
import shutil
import yaml
import argparse
import datetime
import requests

from xxhash import xxh64_hexdigest

from orion.utils import GetDataPullError
from orion.logging import get_orion_logger
from orion.config import config, standardize_biolink_model_version
from orion.data_sources import get_available_data_sources
from orion.exceptions import DataVersionError, GraphSpecError
from orion.ingest_pipeline import IngestPipeline
from orion.kgx_file_merger import KGXFileMerger
from orion.kgx_validation import validate_graph
from orion.neo4j_tools import create_neo4j_dump
from orion.memgraph_tools import create_memgraph_dump
from orion.kgx_bundle import KGXBundle
from orion.graph_versioning import DEFAULT_BASE_RELEASE_VERSION, next_release_version, parse_semver
from orion.kgxmodel import (
    GraphSource,
    GraphSpec,
)
from orion.normalization import NormalizationScheme, get_current_node_norm_version
from orion.metadata import Metadata
from orion.source_resolution import SourceResolver
from orion.supplementation import SequenceVariantSupplementation
from orion.meta_kg import MetaKnowledgeGraphBuilder, META_KG_FILENAME, TEST_DATA_FILENAME, EXAMPLE_DATA_FILENAME
from orion.redundant_kg import generate_redundant_kg
from orion.answercoalesce_build import generate_ac_files
from orion.collapse_qualifiers import generate_collapsed_qualifiers_kg
from orion.kgx_metadata import (
    KGXGraphMetadata,
    KGXKnowledgeSource,
    KGXKnowledgeGraphSource,
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
                 conflation=False,
                 ingest_pipeline: IngestPipeline | None = None):

        self.graphs_dir = graph_output_dir if graph_output_dir else config.get_graphs_dir()
        self.ingest_pipeline = ingest_pipeline if ingest_pipeline is not None else IngestPipeline()
        self._parser_source_ids = set(get_available_data_sources())
        # When set, force conflation on for every source's normalization scheme, overriding any spec.
        self.conflation = conflation
        self.graph_specs = {}  # graph_id -> GraphSpec
        self.load_graph_specs(graph_specs_dir=graph_specs_dir,
                              additional_graph_spec=additional_graph_spec,
                              inline_graph_spec=inline_graph_spec)
        self._validate_specs()
        self.build_results = {}

        # The SourceResolver is used to locate or build the sources specified in a GraphSpec
        self.source_resolver = SourceResolver(graph_builder=self)

    def build_graph(self, graph_spec: GraphSpec):
        graph_id = graph_spec.graph_id
        logger.info(f'Building graph {graph_id}...')

        self.determine_versions(graph_spec)
        graph_output_dir = self.get_graph_dir_path(graph_id, graph_spec.build_version)
        graph_output_url = self.get_graph_output_url(graph_id, graph_spec.release_version)

        # A complete bundle needs no dependencies resolved; otherwise resolve them before (re)building.
        bundle = KGXBundle(graph_output_dir)
        if not (bundle.has_nodes_and_edges() and bundle.has_graph_metadata()):
            logger.info(f'Building graph {graph_id} release_version {graph_spec.release_version}, checking dependencies...')
            if not self.build_dependencies(graph_spec):
                logger.warning(f'Aborting graph {graph_id} release_version {graph_spec.release_version}, '
                               f'resolving dependencies failed.')
                return False
        return self.merge_and_finalize(graph_spec, graph_output_dir, graph_output_url)

    # Materialize a parser source directly as a graph.
    # Used when orion-build is called with a parser id instead of a graph spec id.
    def build_source_graph(self, source_id: str) -> bool:
        logger.info(f'Building source graph {source_id}...')
        source = self.parse_source_spec({'id': source_id}, source_id)
        if source.normalization_scheme is not None and self.conflation:
            source.normalization_scheme.conflation = True
        self._source_build_version(source, source_id)
        if self.source_resolver.resolve(source) is None:
            logger.warning(f'Failed to build source graph {source_id}.')
            return False
        return True

    # Merge a graph_spec's already-resolved sources into a finished KGX bundle at graph_output_dir and
    # generate all of its artifacts (metadata, QC, schema, meta KG, requested db dumps). This is the one
    # bundle producer: graph builds reach it with sources resolved by build_dependencies, parser builds
    # with a single raw parser-output source. Reuses an already-complete bundle, backfilling artifacts.
    def merge_and_finalize(self, graph_spec: GraphSpec, graph_output_dir: str, graph_output_url: str) -> bool:
        graph_id = graph_spec.graph_id
        release_version = graph_spec.release_version
        kgx_bundle = KGXBundle(graph_output_dir)

        # A build is complete/STABLE when its bundle has nodes, edges, and graph-metadata.json.
        build_complete = kgx_bundle.has_nodes_and_edges() and kgx_bundle.has_graph_metadata()
        if build_complete:
            logger.info(f'Graph {graph_id} release_version {release_version} was already built.')
        else:
            if os.path.isdir(graph_output_dir):
                logger.info(f'Clearing incomplete build remnants for {graph_id} release_version {release_version}.')
                shutil.rmtree(graph_output_dir)

            logger.info(f'Building graph {graph_id} release_version {release_version}. Merging sources...')
            os.makedirs(graph_output_dir, exist_ok=True)
            source_merger = KGXFileMerger(graph_spec=graph_spec,
                                          output_directory=graph_output_dir,
                                          nodes_output_filename=KGXBundle.NODES_FILENAME,
                                          edges_output_filename=KGXBundle.EDGES_FILENAME)
            source_merger.merge()
            merge_metadata = source_merger.get_merge_metadata()
            if "merge_error" in merge_metadata:
                logger.error(f'Merge error occured while building graph {graph_id}: '
                                  f'{merge_metadata["merge_error"]}')
                # Leave the incomplete dir behind; the next run clears and retries it.
                return False

            build_time = datetime.datetime.now().isoformat(timespec='seconds')
            biolink_version = self._graph_biolink_version(graph_spec)
            babel_version = self._graph_babel_version(graph_spec)

            logger.info(f'Generating KGX metadata for {graph_id}...')
            self.generate_kgx_metadata_files(graph_spec=graph_spec,
                                             merge_metadata=merge_metadata,
                                             graph_output_dir=graph_output_dir,
                                             graph_output_url=graph_output_url,
                                             build_time=build_time,
                                             biolink_version=biolink_version,
                                             babel_version=babel_version)
            logger.info(f'Building graph {graph_id} complete!')

        # --- Additional artifacts (QC, schema, meta KG, dumps, alternate formats). These run
        #     whether the core bundle was just built or already existed, backfilling anything
        #     missing. ---

        # On re-runs of an already-built graph, the raw jsonl files may have been
        # gzipped and removed by a previous run. Restore them so the trailing steps
        # (QC, schema, neo4j/memgraph/AC dumps, etc.) can read raw jsonl.
        kgx_bundle.decompress_nodes_and_edges()

        if not kgx_bundle.has_qc_results():
            logger.info(f'Running QC for graph {graph_id}...')
            qc_results = validate_graph(nodes_file_path=kgx_bundle.nodes_path,
                                        edges_file_path=kgx_bundle.edges_path,
                                        graph_id=graph_id,
                                        release_version=release_version,
                                        logger=logger)
            with open(kgx_bundle.qc_results_path, 'w') as qc_out:
                json.dump(qc_results, qc_out, indent=2)
            if qc_results['pass']:
                logger.info(f'QC passed for graph {graph_id}.')
            else:
                logger.warning(f'QC failed for graph {graph_id}.')

        if not kgx_bundle.has_schema():
            logger.info(f'Generating KGX Schema for {graph_id}...')
            generate_kgx_schema_file(nodes_filepath=kgx_bundle.nodes_path,
                                     edges_filepath=kgx_bundle.edges_path,
                                     output_dir=graph_output_dir,
                                     graph_output_url=graph_output_url,
                                     graph_name=graph_spec.graph_name,
                                     biolink_version=self._graph_biolink_version(graph_spec))
            logger.info(f'KGX Schema generated for {graph_id}.')

        # Dump URLs produced below are recorded as distribution entries on graph-metadata.json.
        dump_distribution_entries = []

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
                dump_distribution_entries.append(self._dump_distribution_entry(
                    "neo4j_redundant", f'{graph_output_url}graph_{release_version}_redundant.db.dump'))

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
                dump_distribution_entries.append(self._dump_distribution_entry(
                    "neo4j_collapsed_qualifiers",
                    f'{graph_output_url}graph_{release_version}_collapsed_qualifiers.db.dump'))

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
                dump_distribution_entries.append(self._dump_distribution_entry(
                    "neo4j", f'{graph_output_url}graph_{release_version}.db.dump'))

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
                dump_distribution_entries.append(self._dump_distribution_entry(
                    "memgraph", f'{graph_output_url}memgraph_{release_version}.cypher'))

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

        # Record any db dumps that were produced as distribution entries on graph-metadata.json.
        self._append_distribution_entries(kgx_bundle.graph_metadata_path, dump_distribution_entries)
        self._record_build_result(graph_spec, release_version, graph_output_dir)
        return True

    # Determine the release_version and build_version for a graph.
    def determine_versions(self, graph_spec: GraphSpec):
        # If a release_version was explicitly pinned on the spec, just return it
        if graph_spec.release_version:
            return graph_spec.release_version

        # Otherwise generate the deterministic build version.
        # Compose a string with the build version of each underlying source
        # and other attributes that affect the content to be included, then hash it.
        # That way any graph with the same exact content should have the same build_version.
        try:
            composite_parts = []
            for source in graph_spec.sources or []:
                build_version = self._source_build_version(source, graph_spec.graph_id)
                if source.merge_strategy:
                    composite_parts.append(f'{build_version}_{source.merge_strategy}')
                else:
                    composite_parts.append(build_version)
        except (GetDataPullError, DataVersionError) as e:
            raise GraphSpecError(error_message=e.error_message)
        composite_version_string = '_'.join(composite_parts)
        build_version = xxh64_hexdigest(composite_version_string)
        graph_spec.build_version = build_version
        # Use the build version to determine the release version.
        release_version = self._select_release_version(graph_spec.graph_id, build_version, graph_spec.base_release_version)
        graph_spec.release_version = release_version
        logger.info(f'Versions determined for graph {graph_spec.graph_id}: '
                    f'release_version {release_version}, build_version {build_version} ({composite_version_string})')

    # Resolve and store a single spec source's build_version, which it contributes to its parent's
    # composite and which the resolvers then look it up by. Every source ends up with build_version set:
    #  - pinned by build_version: use it directly.
    #  - pinned by release_version: resolve release -> build_version (local + registry).
    #  - unpinned parser source: hash its recipe (resolving 'latest' source/parsing versions first).
    #  - unpinned graph dependency: recursively determine its versions from its own graph spec.
    def _source_build_version(self, source: GraphSource, parent_graph_id: str) -> str:
        if source.build_version:
            return source.build_version
        if source.release_version:
            build_version = self.source_resolver.known_release_versions(source.id).get(source.release_version)
            if not build_version:
                raise GraphSpecError(f'Source {source.id} for graph {parent_graph_id} is pinned to '
                                     f'release_version {source.release_version}, which could not be found.')
            source.build_version = build_version
            return build_version
        if self.is_parser_source(source.id):
            if not source.parsing_version:
                source.parsing_version = self.ingest_pipeline.get_latest_parsing_version(source.id)
            if not source.source_version:
                source.source_version = self.ingest_pipeline.get_latest_source_version(source.id)
            source.build_version = source.generate_build_version()
            return source.build_version
        subgraph_graph_spec = self.graph_specs.get(source.id)
        if not subgraph_graph_spec:
            raise GraphSpecError(f'Source {source.id} requested for graph {parent_graph_id} is not a known '
                                 f'data source and has no graph spec to build it.')
        self.determine_versions(subgraph_graph_spec)
        source.release_version = subgraph_graph_spec.release_version
        source.build_version = subgraph_graph_spec.build_version
        return source.build_version

    # Pick the release_version (semver) for a build. If a previously released release_version of this
    # graph already has this build_version, reuse it (the contents are identical). Otherwise bump to
    # the next release_version above whatever already exists, never going below the spec's declared
    # base_release_version.
    def _select_release_version(self, graph_id: str, build_version: str, base_release_version: str):
        known_release_versions = self.source_resolver.known_release_versions(graph_id)
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

    def is_parser_source(self, source_id: str) -> bool:
        """True if source_id refers to a parser"""
        return source_id in self._parser_source_ids

    # Resolve every spec source into a GraphFileSource on graph_spec.resolved_sources using the SourceResolver.
    def build_dependencies(self, graph_spec: GraphSpec):
        graph_spec.resolved_sources = []
        all_resolved = True
        for source in graph_spec.sources or []:
            resolved = self.source_resolver.resolve(source)
            if resolved is None:
                logger.info(f'Could not resolve source {source.id} for graph {graph_spec.graph_id}.')
                all_resolved = False
                continue
            graph_spec.resolved_sources.append(resolved)
        return all_resolved

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
                                    graph_spec: GraphSpec,
                                    merge_metadata: dict,
                                    graph_output_dir: str,
                                    graph_output_url: str,
                                    build_time: str,
                                    biolink_version: str,
                                    babel_version: str):

        # Each merged source/subgraph contributes its kgx_graph_metadata (carrier) and the
        # merge's own node/edge counts; that's all _kgx_metadata_from_contribution needs.
        all_sources = list(merge_metadata.get('sources', {}).values())

        kg_sources = []
        knowledge_sources = []
        for source in all_sources:
            kg_sources_part, ks_part = self._kgx_metadata_from_contribution(source)
            kg_sources.extend(kg_sources_part)
            knowledge_sources.extend(ks_part)

        # Create KGXGraphMetadata
        kgx_graph_metadata = KGXGraphMetadata(
            id=graph_output_url,
            name=graph_spec.graph_name,
            description=graph_spec.graph_description,
            license='https://spdx.org/licenses/MIT',
            url=graph_output_url,
            version=graph_spec.release_version,
            build_version=graph_spec.build_version,
            date_created=build_time,
            date_modified=build_time,
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
            biolink_version=biolink_version,
            babel_version=babel_version,
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

    # Graph-wide biolink/babel versions, read off the first parser source's normalization scheme
    # (the spec parser applies graph-wide values to every parser source when present). Graph
    # dependencies carry no normalization scheme; a graph with only those (or no sources) falls
    # back to the config default (biolink) / None (babel).
    @staticmethod
    def _first_normalization_scheme(graph_spec: GraphSpec) -> NormalizationScheme | None:
        for source in graph_spec.sources or []:
            if source.normalization_scheme is not None:
                return source.normalization_scheme
        return None

    @staticmethod
    def _graph_biolink_version(graph_spec: GraphSpec) -> str:
        normalization_scheme = GraphBuilder._first_normalization_scheme(graph_spec)
        if normalization_scheme is not None:
            return normalization_scheme.edge_normalization_version
        return config.BL_VERSION

    @staticmethod
    def _graph_babel_version(graph_spec: GraphSpec):
        normalization_scheme = GraphBuilder._first_normalization_scheme(graph_spec)
        if normalization_scheme is not None:
            return normalization_scheme.node_normalization_version
        return None

    @staticmethod
    def _dump_distribution_entry(name: str, content_url: str) -> dict:
        return {"@type": "DataDownload", "name": name, "contentUrl": content_url}

    @staticmethod
    def _append_distribution_entries(graph_metadata_path: str, entries: list[dict]):
        """Add db-dump distribution entries to an existing graph-metadata.json, deduped by URL."""
        if not entries:
            return
        with open(graph_metadata_path) as f:
            metadata = json.load(f)
        distribution = metadata.get('distribution') or []
        existing_urls = {d.get('contentUrl') for d in distribution}
        for entry in entries:
            if entry.get('contentUrl') not in existing_urls:
                distribution.append(entry)
        metadata['distribution'] = distribution
        with open(graph_metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

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

                # parse the list of sources
                sources = [self.parse_source_spec(source, graph_id)
                           for source in graph_yaml.get('sources', [])]
                if not sources:
                    raise GraphSpecError(f'Error: No sources were provided for graph: {graph_id}.')

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
                elif graph_wide_edge_norm_version is not None:
                    graph_wide_edge_norm_version = standardize_biolink_model_version(graph_wide_edge_norm_version)

                # Apply graph-wide normalization to parser sources, overwriting anything set
                # at the source level. Pinned sources and graph dependencies carry no normalization
                # scheme and are skipped.
                for source in sources:
                    if source.merge_strategy == KGXFileMerger.DONT_MERGE and add_edge_id is not None:
                        raise GraphSpecError(f'Graph {graph_id}, source {source.id} has merge_strategy:'
                                             f' dont_merge, which is incompatible with add_edge_id.')
                    if source.normalization_scheme is None:
                        continue
                    if graph_wide_node_norm_version is not None:
                        source.normalization_scheme.node_normalization_version = graph_wide_node_norm_version
                    if graph_wide_edge_norm_version is not None:
                        source.normalization_scheme.edge_normalization_version = graph_wide_edge_norm_version
                    if graph_wide_conflation is not None:
                        source.normalization_scheme.conflation = graph_wide_conflation
                    if graph_wide_strict_norm is not None:
                        source.normalization_scheme.strict = graph_wide_strict_norm
                    # A command-line --conflation flag forces conflation on for every source,
                    # overriding whatever the spec set at the source or graph level.
                    if self.conflation:
                        source.normalization_scheme.conflation = True

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
                                       sources=sources)
                self.graph_specs[graph_id] = graph_spec
        except KeyError as e:
            error_message = f'Graph Spec missing required field: {e}'
            if graph_id is not None:
                error_message += f"(in graph {graph_id})"
            raise GraphSpecError(error_message)

    # Recipe settings that only make sense for an unpinned parser source. Present on a pinned source,
    # or on a graph dependency, they're a spec error.
    _RECIPE_SETTINGS = ('source_version', 'parsing_version', 'supplementation_version',
                        'node_normalization_version', 'edge_normalization_version',
                        'strict_normalization', 'conflation')

    def parse_source_spec(self, source_yml, graph_id):
        source_id = source_yml['id']
        merge_strategy = source_yml.get('merge_strategy', None)
        if merge_strategy == 'default':
            merge_strategy = None

        release_version = source_yml.get('release_version', None)
        build_version = source_yml.get('build_version', None)
        pinned = release_version is not None or build_version is not None
        recipe_present = any(source_yml.get(setting) is not None for setting in self._RECIPE_SETTINGS)
        is_parser = self.is_parser_source(source_id)

        # validation
        if release_version is not None and build_version is not None:
            raise GraphSpecError(f'Graph {graph_id}, source {source_id}: specify only one of '
                                 f'release_version or build_version, not both.')
        if pinned and recipe_present:
            raise GraphSpecError(f'Graph {graph_id}, source {source_id}: a pinned source '
                                 f'(with a release_version/build_version) cannot also set recipe options '
                                 f'(source_version, parsing_version, normalization, etc.).')
        if recipe_present and not is_parser:
            raise GraphSpecError(f'Graph {graph_id}, source {source_id}: parser settings only apply to '
                                 f'parser sources. A graph dependency is built from its own graph spec; '
                                 f'pin a release_version/build_version or leave it unpinned.')

        # A pinned source, or a graph dependency, carries no recipe — resolved by lookup or by
        # building from its own graph spec.
        if pinned or not is_parser:
            return GraphSource(id=source_id,
                               merge_strategy=merge_strategy,
                               release_version=release_version,
                               build_version=build_version)

        # Otherwise it's an unpinned parser source: build the normalization recipe. source_version
        # and parsing_version are left unresolved here (determine_versions fills in 'latest' lazily
        # for the graph actually being built, to avoid importing every referenced parser at load).
        source_version = source_yml.get('source_version', None)
        parsing_version = source_yml.get('parsing_version', None)
        if parsing_version == 'latest':
            parsing_version = None
        node_normalization_version = source_yml.get('node_normalization_version', None)
        edge_normalization_version = source_yml.get('edge_normalization_version', None)
        strict_normalization = source_yml.get('strict_normalization', True)
        conflation = source_yml.get('conflation', False)
        if not edge_normalization_version or edge_normalization_version == 'latest':
            edge_normalization_version = config.BL_VERSION
        if type(strict_normalization) != bool:
            raise GraphSpecError(f'Invalid type (strict_normalization: {strict_normalization}), must be true or false.')
        if type(conflation) != bool:
            raise GraphSpecError(f'Invalid type (conflation: {conflation}), must be true or false.')

        normalization_scheme = NormalizationScheme(node_normalization_version=node_normalization_version,
                                                   edge_normalization_version=edge_normalization_version,
                                                   strict=strict_normalization,
                                                   conflation=conflation)
        return GraphSource(id=source_id,
                           merge_strategy=merge_strategy,
                           source_version=source_version,
                           parsing_version=parsing_version,
                           supplementation_version=SequenceVariantSupplementation.SUPPLEMENTATION_VERSION,
                           normalization_scheme=normalization_scheme)

    # Validate the fully-loaded set of graph specs: every source id must resolve to exactly one
    # producer (a parser or a graph spec), and the two id namespaces must be disjoint.
    def _validate_specs(self):
        collisions = self._parser_source_ids & set(self.graph_specs)
        if collisions:
            raise GraphSpecError(f'Graph ids collide with data source ids (each id must map to one '
                                 f'producer): {", ".join(sorted(collisions))}.')
        for graph_id, graph_spec in self.graph_specs.items():
            for source in graph_spec.sources or []:
                if not self.is_parser_source(source.id) and source.id not in self.graph_specs:
                    raise GraphSpecError(f'Graph {graph_id} references source {source.id}, which is '
                                         f'neither a known data source nor a graph spec.')

    def get_graph_dir_path(self, graph_id: str, build_version: str):
        return os.path.join(self.graphs_dir, graph_id, build_version)

    @staticmethod
    def get_graph_output_url(graph_id: str, release_version: str):
        return f'{config.ORION_OUTPUT_URL}/{graph_id}/{release_version}/'

    def _record_build_result(self,
                             graph_spec: GraphSpec,
                             release_version: str,
                             graph_output_dir: str):
        bundle = KGXBundle(graph_output_dir)
        graph_metadata = KGXGraphMetadata.from_dict(bundle.load_graph_metadata() or {})
        self.build_results[graph_spec.graph_id] = {
            'graph_id': graph_spec.graph_id,
            'release_version': release_version,
            'build_version': graph_spec.build_version,
            'graph_dir': graph_output_dir,
            'build_status': Metadata.STABLE,
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
            'sources': [{'id': s} for s in source_ids],
        }]
    }


def main():
    from orion.logging import configure_cli_logging
    configure_cli_logging()

    parser = argparse.ArgumentParser(description="Merge data sources into complete graphs.")
    parser.add_argument('graph_id',
                        help='ID of the graph to build. Specify the ID of a graph in a Graph Spec, '
                             'provide a new one along with --sources to build a simple graph without '
                             'using a Graph Spec file, or pass a data source id to build that '
                             'source alone as a graph.')
    spec_group = parser.add_mutually_exclusive_group()
    spec_group.add_argument('--graph_spec', type=str, default=None,
                            help='Path or URL for an additional Graph Spec yaml file. Its graphs are added '
                                 'to the specs provided automatically (from the graph_specs/ directory).')
    spec_group.add_argument('--sources', type=str, default=None,
                            help='Comma-separated list of data sources to include in a graph.')
    parser.add_argument('--output_format', type=str, default=None,
                        help='Output format for a graph (e.g. jsonl, neo4j). '
                             'Only valid when used with command line --sources.')
    parser.add_argument('-c', '--conflation',
                        action='store_true',
                        help='Apply conflation during normalization for all sources in the graph(s), '
                             'overriding the graph spec. See https://github.com/NCATSTranslator/Babel/ for '
                             'more information.')
    args = parser.parse_args()
    graph_id_arg = args.graph_id
    additional_graph_spec = args.graph_spec
    inline_graph_spec = None

    if args.sources:
        inline_graph_spec = _generate_inline_graph_spec(graph_id_arg, args.sources, args.output_format)
    elif args.output_format:
        parser.error('--output_format is only valid together with --sources.')

    graph_builder = GraphBuilder(additional_graph_spec=additional_graph_spec,
                                 inline_graph_spec=inline_graph_spec,
                                 conflation=args.conflation)
    if graph_id_arg == "all":
        for graph_spec in graph_builder.graph_specs.values():
            graph_builder.build_graph(graph_spec)
    else:
        for graph_id in (gid.strip() for gid in graph_id_arg.split(',') if gid.strip()):
            graph_spec = graph_builder.graph_specs.get(graph_id, None)
            if graph_spec:
                graph_builder.build_graph(graph_spec)
            elif graph_builder.is_parser_source(graph_id):
                # Not a graph spec id, but a parser id: materialize it directly as a source graph.
                graph_builder.build_source_graph(graph_id)
            else:
                print(f'Invalid graph spec requested: {graph_id}')
    results_path = graph_builder.write_build_results()
    if results_path:
        print(f'Build results written to {results_path}')
    else:
        print('No graphs were built.')


if __name__ == '__main__':
    main()
