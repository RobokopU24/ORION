import os
import gzip
import json
import shutil
import yaml
import argparse
import datetime
import requests

from pathlib import Path
from xxhash import xxh64_hexdigest

from orion.utils import GetDataPullError
from orion.logging import get_orion_logger
from orion.config import config
from orion.data_sources import get_available_data_sources, get_data_source_metadata_path
from orion.exceptions import DataVersionError, GraphSpecError
from orion.ingest_pipeline import IngestPipeline
from orion.kgx_file_merger import KGXFileMerger
from orion.kgx_validation import validate_graph
from orion.neo4j_tools import create_neo4j_dump
from orion.memgraph_tools import create_memgraph_dump
from orion.kgx_bundle import KGXBundle
from orion.kgxmodel import (
    DataSource,
    GraphFileSource,
    GraphSpec,
    SubGraphSource,
)
from orion.normalization import NormalizationScheme, get_current_node_norm_version
from orion.metadata import Metadata, GraphMetadata
from orion.source_resolution import (
    IngestPipelineResolver,
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
from orion.kgx_metadata import KGXGraphMetadata, KGXKnowledgeSource, generate_kgx_schema_file


logger = get_orion_logger("orion.graph_pipeline")

REDUNDANT_EDGES_FILENAME = 'redundant_edges.jsonl'
COLLAPSED_QUALIFIERS_FILENAME = 'collapsed_qualifier_edges.jsonl'


def _is_single_source_spec(graph_spec: GraphSpec) -> bool:
    sources = graph_spec.sources or []
    return (len(sources) == 1
            and not graph_spec.subgraphs
            and sources[0].id == graph_spec.graph_id)


def _synthesize_single_source_spec(data_source: DataSource) -> GraphSpec:
    return GraphSpec(
        graph_id=data_source.id,
        graph_name=data_source.id,
        graph_description='',
        graph_url='',
        graph_version=None,
        graph_output_format='jsonl',
        sources=[data_source],
        subgraphs=[],
    )


class GraphBuilder:

    def __init__(self,
                 additional_graph_spec=None,
                 inline_graph_spec=None,
                 graph_output_dir=None,
                 graph_specs_dir=None):

        self.graphs_dir = graph_output_dir if graph_output_dir else self.get_graph_output_dir()
        self.ingest_pipeline = IngestPipeline()  # access to the data sources and their metadata
        self.graph_specs = {}   # graph_id -> GraphSpec all potential graphs that could be built, including sub-graphs
        self.load_graph_specs(graph_specs_dir=graph_specs_dir,
                              additional_graph_spec=additional_graph_spec,
                              inline_graph_spec=inline_graph_spec)
        self.build_results = {}

    def build_graph(self, graph_spec: GraphSpec):

        graph_id = graph_spec.graph_id
        logger.info(f'Building graph {graph_id}...')

        graph_version = self.determine_graph_version(graph_spec)
        graph_metadata = self.get_graph_metadata(graph_id, graph_version)
        graph_output_dir = self.get_graph_dir_path(graph_id, graph_version)
        graph_output_url = self.get_graph_output_url(graph_id, graph_version)

        # check for previous builds of this same graph
        build_status = graph_metadata.get_build_status()
        if build_status == Metadata.IN_PROGRESS:
            logger.info(f'Graph {graph_id} version {graph_version} has status: in progress. '
                             f'This means either the graph is already in the process of being built, '
                             f'or an error occurred previously that could not be handled. '
                             f'You may need to clean up and/or remove the failed build.')
            return False

        if build_status == Metadata.BROKEN or build_status == Metadata.FAILED:
            logger.info(f'Graph {graph_id} version {graph_version} previously failed to build. Skipping..')
            return False

        if build_status == Metadata.STABLE:
            self.build_results[graph_id] = {'version': graph_version}
            logger.info(f'Graph {graph_id} version {graph_version} was already built.')
        else:
            # If we get here we need to build the graph
            logger.info(f'Building graph {graph_id} version {graph_version}, checking dependencies...')
            if not self.build_dependencies(graph_spec):
                logger.warning(f'Aborting graph {graph_spec.graph_id} version {graph_version}, resolving '
                                    f'dependencies failed.')
                return False

            logger.info(f'Building graph {graph_id} version {graph_version}. '
                             f'Dependencies ready, merging sources...')
            graph_metadata.set_build_status(Metadata.IN_PROGRESS)
            graph_metadata.set_graph_version(graph_version)
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
            self.build_results[graph_id] = {'version': graph_version}

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
                                        graph_version=graph_version,
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
                                             graph_version=graph_version,
                                             node_property_ignore_list=node_property_ignore_list,
                                             edge_property_ignore_list=edge_property_ignore_list)
            if dump_success:
                graph_metadata.set_dump(dump_type="neo4j_redundant",
                                        dump_url=f'{graph_output_url}graph_{graph_version}_redundant.db.dump')

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
                                             graph_version=graph_version,
                                             node_property_ignore_list=node_property_ignore_list,
                                             edge_property_ignore_list=edge_property_ignore_list)
            if dump_success:
                graph_metadata.set_dump(dump_type="neo4j_collapsed_qualifiers",
                                        dump_url=f'{graph_output_url}graph_{graph_version}'
                                                     f'_collapsed_qualifiers.db.dump')

        if 'neo4j' in output_formats:
            logger.info(f'Starting Neo4j dump pipeline for {graph_id}...')
            dump_success = create_neo4j_dump(nodes_filepath=kgx_bundle.nodes_path,
                                             edges_filepath=kgx_bundle.edges_path,
                                             output_directory=graph_output_dir,
                                             graph_id=graph_id,
                                             graph_version=graph_version,
                                             node_property_ignore_list=node_property_ignore_list,
                                             edge_property_ignore_list=edge_property_ignore_list)
            if dump_success:
                graph_metadata.set_dump(dump_type="neo4j",
                                        dump_url=f'{graph_output_url}graph_{graph_version}.db.dump')

        if 'memgraph' in output_formats:
            logger.info(f'Starting memgraph dump pipeline for {graph_id}...')
            dump_success = create_memgraph_dump(nodes_filepath=kgx_bundle.nodes_path,
                                                edges_filepath=kgx_bundle.edges_path,
                                                output_directory=graph_output_dir,
                                                graph_id=graph_id,
                                                graph_version=graph_version,
                                                node_property_ignore_list=node_property_ignore_list,
                                                edge_property_ignore_list=edge_property_ignore_list)
            if dump_success:
                graph_metadata.set_dump(dump_type="memgraph",
                                        dump_url=f'{graph_output_url}memgraph_{graph_version}.cypher')

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
        return True

    # determine a graph version utilizing versions of data sources, or just return the graph version specified
    def determine_graph_version(self, graph_spec: GraphSpec):
        # if the version was set or previously determined just back out
        if graph_spec.graph_version:
            return graph_spec.graph_version
        try:
            # go out and find the latest version for any data source that doesn't have a version specified
            for source in graph_spec.sources:
                if not source.source_version:
                    source.source_version = self.ingest_pipeline.get_latest_source_version(source.id)
                logger.info(f'Using {source.id} version: {source.version}')

            # for sub-graphs, if a graph version isn't specified,
            # use the graph spec for that subgraph to determine a graph version
            for subgraph in graph_spec.subgraphs:
                if not subgraph.graph_version:
                    subgraph_graph_spec = self.graph_specs.get(subgraph.id, None)
                    if subgraph_graph_spec:
                        subgraph.graph_version = self.determine_graph_version(subgraph_graph_spec)
                        logger.info(f'Using subgraph {graph_spec.graph_id} version: {subgraph.graph_version}')
                    else:
                        raise GraphSpecError(f'Subgraph {subgraph.id} requested for graph {graph_spec.graph_id} '
                                             f'but the version was not specified and could not be determined without '
                                             f'a graph spec for {subgraph.id}.')
        except (GetDataPullError, DataVersionError) as e:
            raise GraphSpecError(error_message=e.error_message)

        # make a string that is a composite of versions and their merge strategy for each source
        composite_version_string = ""
        if graph_spec.sources:
            composite_version_string += '_'.join([graph_source.version + '_' + graph_source.merge_strategy
                                                  if graph_source.merge_strategy else graph_source.version
                                                  for graph_source in graph_spec.sources])
        if graph_spec.subgraphs:
            if composite_version_string:
                composite_version_string += '_'
            composite_version_string += '_'.join([sub_graph_source.version + '_' + sub_graph_source.merge_strategy
                                                  if sub_graph_source.merge_strategy else sub_graph_source.version
                                                  for sub_graph_source in graph_spec.subgraphs])
        graph_version = xxh64_hexdigest(composite_version_string)
        graph_spec.graph_version = graph_version
        logger.info(f'Version determined for graph {graph_spec.graph_id}: {graph_version} ({composite_version_string})')
        return graph_version

    # Resolve every spec entry into a GraphFileSource on graph_spec.resolved_sources.
    # For each DataSource, try Local then Registry first; if neither resolves and
    # the spec isn't itself a single-source build, recursively build a single-source
    # graph for it so it can be resolved with a Local lookup.
    def build_dependencies(self, graph_spec: GraphSpec):

        graph_spec.resolved_sources = []

        local_resolver = LocalGraphResolver(graphs_dir=self.graphs_dir)
        registry_resolver = RegistryGraphResolver(graphs_dir=self.graphs_dir)
        ingest_resolver = IngestPipelineResolver(ingest_pipeline=self.ingest_pipeline)
        subgraph_build_resolver = SubgraphBuildResolver(graph_pipeline=self)

        subgraph_chain = [local_resolver, registry_resolver, subgraph_build_resolver]
        for subgraph_spec in graph_spec.subgraphs or []:
            resolved = resolve_source(subgraph_spec, subgraph_chain)
            if resolved is None:
                logger.warning(f'Could not resolve subgraph dependency {subgraph_spec.id} '
                               f'version {subgraph_spec.graph_version} for graph {graph_spec.graph_id}.')
                return False
            graph_spec.resolved_sources.append(resolved)

        existing_graph_chain = [local_resolver, registry_resolver]
        for data_source_spec in graph_spec.sources or []:
            # first try to resolve the data sources by checking the local storage and the graph registry
            resolved = resolve_source(data_source_spec, existing_graph_chain)
            if resolved is not None:
                graph_spec.resolved_sources.append(resolved)
                continue

            # If it was not found, we need to ingest the source and/or build a single-source graph with it.
            if _is_single_source_spec(graph_spec):
                # This is a graph spec for a single source graph, we need to run the ingest pipeline.
                resolved = ingest_resolver.resolve(data_source_spec)
            else:
                # Here we're looking at one otherwise-unresolved source from a multisource graph spec.
                # Run build_graph recursively with a synthesized graph spec so that the dependency resolver can
                # trigger the _is_single_source_spec clause, run the ingest pipeline, and make a single-source graph.
                if not self.build_graph(_synthesize_single_source_spec(data_source_spec)):
                    logger.error(f'Failed to build single-source graph for {data_source_spec.id} '
                                 f'(dependency of {graph_spec.graph_id}).')
                    return False
                # After a single source graph is built, we can resolve it locally like we would have if it already
                # existed.
                resolved = local_resolver.resolve(data_source_spec)

            if resolved is None:
                logger.info(f'Could not resolve data source {data_source_spec.id} for graph '
                            f'{graph_spec.graph_id}.')
                return False
            graph_spec.resolved_sources.append(resolved)
        return True

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
        orion_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
        for source in all_sources:
            if source.get('kgx_graph_metadata'):
                kg_sources_part, ks_part = self._kgx_metadata_from_built_graph(source)
            else:
                kg_sources_part, ks_part = self._kgx_metadata_from_parser_source(source, orion_root)
            kg_sources.extend(kg_sources_part)
            knowledge_sources.extend(ks_part)

        # Create KGXGraphMetadata
        kgx_graph_metadata = KGXGraphMetadata(
            id=graph_output_url,
            name=graph_metadata.get_graph_name(),
            description=graph_metadata.get_graph_description(),
            license='https://spdx.org/licenses/MIT',
            url=graph_output_url,
            version=graph_metadata.get_graph_version(),
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
        graph_metadata_filepath = os.path.join(graph_output_dir, 'graph-metadata.json')
        with open(graph_metadata_filepath, 'w') as f:
            f.write(kgx_graph_metadata.to_json())

    # KGX-metadata contribution from a built graph: copy the carrier's hasPart
    # and isBasedOn entries. If hasPart has exactly one entry (a single-source
    # graph used as input here), override its node/edge counts with the current
    # build's merger counts — the carrier's counts come from its own internal
    # merge and aren't the right number for this graph. When hasPart has many
    # entries we pass them through; the merger only has an aggregate count for
    # the carrier as a whole, with no way to attribute it across constituents.
    @staticmethod
    def _kgx_metadata_from_built_graph(source: dict):
        carrier = source.get('kgx_graph_metadata') or {}
        carrier_kg_sources = carrier.get('hasPart') or []
        carrier_knowledge_sources = carrier.get('isBasedOn') or []

        kg_sources = []
        if len(carrier_kg_sources) == 1:
            entry = dict(carrier_kg_sources[0])
            if source.get('node_count') is not None:
                entry['orion:nodeCount'] = source.get('node_count')
            if source.get('edge_count') is not None:
                entry['orion:edgeCount'] = source.get('edge_count')
            kg_sources.append(entry)
        else:
            for kg_source_entry in carrier_kg_sources:
                kg_sources.append(dict(kg_source_entry))

        knowledge_sources = [KGXKnowledgeSource.from_dict(ks_dict)
                             for ks_dict in carrier_knowledge_sources]
        return kg_sources, knowledge_sources

    # KGX-metadata contribution from a source that came from raw parser output:
    # synthesize a single hasPart entry from the source record and load the
    # parser's *.source.json for the isBasedOn knowledge source.
    def _kgx_metadata_from_parser_source(self, source: dict, orion_root: str):
        source_id = source.get('source_id', '')
        source_version = source.get('source_version', '')
        source_name = source.get('name', source_id)
        release_version = source.get('release_version', '')
        kg_sources = [{
            '@id': self.get_graph_output_url(graph_id=source_id, graph_version=release_version),
            'name': f'A ROBOKOP Knowledge Graph based on {source_name}',
            'orion:nodeCount': source.get('node_count'),
            'orion:edgeCount': source.get('edge_count'),
        }]
        parser_metadata_path = os.path.join(orion_root, get_data_source_metadata_path(source_id))
        with open(parser_metadata_path) as f:
            parser_metadata = json.load(f)
        knowledge_sources = [KGXKnowledgeSource.from_dict({**parser_metadata, 'version': source_version})]
        return kg_sources, knowledge_sources

    def load_graph_specs(self, graph_specs_dir=None, additional_graph_spec=None, inline_graph_spec=None):
        # if a graph spec directory was not provided, default to the one included in the codebase
        if not graph_specs_dir:
            graph_specs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'graph_specs')

        # make sure it's a valid directory
        if not os.path.isdir(graph_specs_dir):
            raise GraphSpecError(f'Configuration Error - Graph Specs directory not found: {graph_specs_dir}')

        spec_filenames = sorted(f for f in os.listdir(graph_specs_dir)
                                if f.endswith('.yaml') or f.endswith('.yml'))
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
                graph_spec = GraphSpec(graph_id=graph_id,
                                       graph_name=graph_name,
                                       graph_description=graph_description,
                                       graph_url=graph_url,
                                       graph_version=None,  # this will get populated when a build is triggered
                                       graph_output_format=graph_output_format,
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
        subgraph_version = subgraph_yml.get('graph_version', None)
        merge_strategy = subgraph_yml.get('merge_strategy', None)
        if merge_strategy == 'default':
            merge_strategy = None
        subgraph_source = SubGraphSource(id=subgraph_id,
                                         graph_version=subgraph_version,
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

        # if versions are not specified, set them to the current latest
        # source_version is intentionally not handled here because we want to do it lazily and avoid if not needed
        if not parsing_version or parsing_version == 'latest':
            parsing_version = self.ingest_pipeline.get_latest_parsing_version(source_id)
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

    def get_graph_dir_path(self, graph_id: str, graph_version: str):
        return os.path.join(self.graphs_dir, graph_id, graph_version)

    @staticmethod
    def get_graph_output_url(graph_id: str, graph_version: str):
        return f'{config.ORION_OUTPUT_URL}/{graph_id}/{graph_version}/'

    @staticmethod
    def get_graph_nodes_file_path(graph_output_dir: str):
        return os.path.join(graph_output_dir, KGXBundle.NODES_FILENAME)

    @staticmethod
    def get_graph_edges_file_path(graph_output_dir: str):
        return os.path.join(graph_output_dir, KGXBundle.EDGES_FILENAME)

    def check_for_existing_graph_dir(self, graph_id: str, graph_version: str):
        graph_output_dir = self.get_graph_dir_path(graph_id, graph_version)
        if not os.path.isdir(graph_output_dir):
            return False
        return True

    def get_graph_metadata(self, graph_id: str, graph_version: str):
        # make sure the output directory exists (where we check for existing GraphMetadata)
        graph_output_dir = self.get_graph_dir_path(graph_id, graph_version)

        # load existing or create new metadata file
        return GraphMetadata(graph_id, graph_output_dir)

    @staticmethod
    def get_graph_output_dir():
        # confirm the directory specified by the environment variable ORION_GRAPHS is valid
        graphs_dir = config.ORION_GRAPHS
        if graphs_dir and Path(graphs_dir).is_dir():
            return graphs_dir

        # if invalid or not specified back out
        raise IOError('ORION graphs directory not configured properly. '
                      'Specify a valid directory with environment variable ORION_GRAPHS.')


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
    for results_graph_id, results in graph_builder.build_results.items():
        print(f'{results_graph_id}\t{results["version"]}')


if __name__ == '__main__':
    main()
