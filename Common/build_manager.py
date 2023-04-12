import os
import yaml
import argparse
import datetime
import requests
import json
import time
import Common.kgx_file_converter as kgx_file_converter
from xxhash import xxh64_hexdigest
from collections import defaultdict
from Common.biolink_utils import BiolinkUtils
from Common.utils import LoggingUtil, quick_jsonl_file_iterator
from Common.data_sources import get_available_data_sources
from Common.load_manager import SourceDataManager
from Common.kgx_file_merger import KGXFileMerger
from Common.neo4j_tools import Neo4jTools
from Common.kgxmodel import GraphSpec, SubGraphSource, DataSource, NormalizationScheme
from Common.metadata import Metadata, GraphMetadata, SourceMetadata
from Common.supplementation import SequenceVariantSupplementation
from Common.node_types import ROOT_ENTITY, PRIMARY_KNOWLEDGE_SOURCE, PREDICATE, AGGREGATOR_KNOWLEDGE_SOURCES

NODES_FILENAME = 'nodes.jsonl'
EDGES_FILENAME = 'edges.jsonl'
META_KG_FILENAME = 'meta_kg.json'
SRI_TESTING_FILENAME = 'sri_testing_data.json'


class GraphBuilder:

    def __init__(self):

        self.logger = LoggingUtil.init_logging("Data_services.Common.GraphBuilder",
                                               line_format='medium',
                                               log_file_path=os.environ['DATA_SERVICES_LOGS'])

        self.current_graph_versions = {}
        self.graphs_dir = self.init_graphs_dir()  # path to the graphs output directory
        self.source_data_manager = SourceDataManager()  # access to the data sources and their metadata
        self.graph_specs = self.load_graph_specs()  # list of graphs to build (GraphSpec objects)
        self.build_results = {}
        self.bl_utils = BiolinkUtils()

    def build_graph(self, graph_id: str):

        self.logger.info(f'Building graph {graph_id}. Checking dependencies...')
        graph_spec = self.get_graph_spec(graph_id)
        if self.build_dependencies(graph_spec):
            self.logger.info(f'Building graph {graph_id}. Dependencies are ready...')
        else:
            self.logger.warning(f'Aborting graph {graph_spec.graph_id}, building dependencies failed.')
            return

        # check the status for previous builds of this version
        graph_version = graph_spec.graph_version
        graph_metadata = self.get_graph_metadata(graph_id, graph_version)
        build_status = graph_metadata.get_build_status()
        if build_status == Metadata.IN_PROGRESS:
            self.logger.info(f'Graph {graph_id} version {graph_version} is already in progress. Skipping..')
            return

        if build_status == Metadata.BROKEN or build_status == Metadata.FAILED:
            self.logger.info(f'Graph {graph_id} version {graph_version} previously failed to build. Skipping..')
            return

        graph_output_dir = self.get_graph_dir_path(graph_id, graph_version)
        if build_status != Metadata.STABLE:

            # if we get here we need to build the graph
            self.logger.info(f'Building graph {graph_id} version {graph_version}. Merging sources...')
            graph_metadata.set_build_status(Metadata.IN_PROGRESS)
            graph_metadata.set_graph_version(graph_version)
            graph_metadata.set_graph_name(graph_spec.graph_name)
            graph_metadata.set_graph_description(graph_spec.graph_description)
            graph_metadata.set_graph_url(graph_spec.graph_url)
            graph_metadata.set_graph_spec(graph_spec.get_metadata_representation())

            # merge the sources and write the finalized graph kgx files
            source_merger = KGXFileMerger(output_directory=graph_output_dir)
            merge_metadata = source_merger.merge(graph_spec,
                                                 nodes_output_filename=NODES_FILENAME,
                                                 edges_output_filename=EDGES_FILENAME)

            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            if "merge_error" in merge_metadata:
                graph_metadata.set_build_error(merge_metadata["merge_error"], current_time)
                graph_metadata.set_build_status(Metadata.FAILED)
                self.logger.error(f'Error building graph {graph_id}.')
                return

            graph_metadata.set_build_info(merge_metadata, current_time)
            graph_metadata.set_build_status(Metadata.STABLE)
            self.logger.info(f'Building graph {graph_id} complete!')
            self.build_results[graph_id] = {'version': graph_version, 'success': True}
        else:
            self.logger.info(f'Graph {graph_id} version {graph_version} was already built.')
            self.build_results[graph_id] = {'version': graph_version, 'success': False}

        if not graph_metadata.has_qc():
            self.logger.info(f'Running QC for graph {graph_id}...')
            qc_results = self.run_qc(graph_id, graph_version, graph_directory=graph_output_dir)
            graph_metadata.set_qc_results(qc_results)
            self.logger.info(f'QC complete for graph {graph_id}.')

        if 'neo4j' in graph_spec.graph_output_format.lower():
            self.logger.info(f'Starting Neo4j dump pipeline for {graph_id}...')
            dump_success = self.create_neo4j_dump(graph_id=graph_id,
                                                  graph_directory=graph_output_dir)
            if dump_success:
                graph_output_url = self.get_graph_output_URL(graph_id, graph_version)
                graph_metadata.set_dump_url(f'{graph_output_url}graph_{graph_version}.db.dump')
                # graph_metadata.set_metakg_url(f'{graph_output_url}{META_KG_FILENAME}')
                # graph_metadata.set_test_data_url(f'{graph_output_url}{SRI_TESTING_FILENAME}')

    def build_dependencies(self, graph_spec: GraphSpec):
        for subgraph_source in graph_spec.subgraphs:
            subgraph_id = subgraph_source.id
            subgraph_version = subgraph_source.version
            if self.check_for_existing_graph_dir(subgraph_id, subgraph_version):
                # load previous metadata
                graph_metadata = self.get_graph_metadata(subgraph_id, subgraph_version)
                subgraph_source.graph_metadata = graph_metadata.metadata
            elif self.current_graph_versions[subgraph_id] == subgraph_version:
                self.logger.warning(f'For graph {graph_spec.graph_id} subgraph dependency '
                                    f'{subgraph_id} version {subgraph_version} is not ready. Building now...')
                self.build_graph(subgraph_id)
            else:
                self.logger.warning(f'Building graph {graph_spec.graph_id} failed, '
                                    f'subgraph {subgraph_id} had version {subgraph_version} specified, '
                                    f'but that version of the graph was not found in the graphs directory.')
                return False

            graph_metadata = self.get_graph_metadata(subgraph_id, subgraph_version)
            if graph_metadata.get_build_status() == Metadata.STABLE:
                # we found the sub graph and it's stable - update the GraphSource in preparation for building the graph
                subgraph_dir = self.get_graph_dir_path(subgraph_id, subgraph_version)
                subgraph_nodes_path = self.get_graph_nodes_file_path(subgraph_dir)
                subgraph_edges_path = self.get_graph_edges_file_path(subgraph_dir)
                subgraph_source.file_paths = [subgraph_nodes_path, subgraph_edges_path]
            else:
                self.logger.warning(
                    f'Attempting to build graph {graph_spec.graph_id} failed, dependency '
                    f'subgraph {subgraph_id} version {subgraph_version} was not built successfully.')
                return False

        for data_source in graph_spec.sources:
            source_id = data_source.id
            if source_id not in get_available_data_sources():
                self.logger.warning(
                    f'Attempting to build graph {graph_spec.graph_id} failed: '
                    f'{source_id} is not a valid data source id. ')
                return False

            source_metadata: SourceMetadata = self.source_data_manager.get_source_metadata(source_id,
                                                                                           data_source.source_version)
            release_version = source_metadata.get_release_version(parsing_version=data_source.parsing_version,
                                                                  normalization_version=data_source.normalization_scheme.get_composite_normalization_version(),
                                                                  supplementation_version=data_source.supplementation_version)
            if release_version is None:
                self.logger.info(
                    f'Attempting to build graph {graph_spec.graph_id}, '
                    f'dependency {source_id} is not ready. Building now...')
                release_version = self.source_data_manager.run_pipeline(source_id,
                                                                        source_version=data_source.source_version,
                                                                        parsing_version=data_source.parsing_version,
                                                                        normalization_scheme=data_source.normalization_scheme,
                                                                        supplementation_version=data_source.supplementation_version)
                if not release_version:
                    self.logger.info(
                        f'While attempting to build {graph_spec.graph_id}, dependency pipeline failed for {source_id}...')
                    return False

            data_source.version = release_version
            data_source.release_info = source_metadata.get_release_info(release_version)
            data_source.file_paths = self.source_data_manager.get_final_file_paths(source_id,
                                                                                   data_source.source_version,
                                                                                   data_source.parsing_version,
                                                                                   data_source.normalization_scheme.get_composite_normalization_version(),
                                                                                   data_source.supplementation_version)
        return True

    def run_qc(self,
               graph_id: str,
               graph_version: str,
               graph_directory: str):

        knowledge_sources = set()
        edge_properties = set()
        predicate_counts = defaultdict(int)
        graph_edges_file_path = os.path.join(graph_directory, EDGES_FILENAME)
        for edge_json in quick_jsonl_file_iterator(graph_edges_file_path):
            knowledge_sources.add(edge_json[PRIMARY_KNOWLEDGE_SOURCE])
            for key in edge_json.keys():
                if (key is not PRIMARY_KNOWLEDGE_SOURCE and
                        key is not AGGREGATOR_KNOWLEDGE_SOURCES and
                        key is not PREDICATE):
                    edge_properties.add(key)
                predicate_counts[edge_json[PREDICATE]] += 1
        qc_metadata = {
            'primary_knowledge_sources': list(knowledge_sources),
            'edge_properties': list(edge_properties),
            'predicate_counts': {k: v for k, v in predicate_counts.items()}
        }
        return qc_metadata

    def create_neo4j_dump(self,
                          graph_id: str,
                          graph_directory: str):

        graph_spec = self.get_graph_spec(graph_id)
        graph_version = graph_spec.graph_version

        nodes_csv_filename = f'{NODES_FILENAME}.csv'
        edges_csv_filename = f'{EDGES_FILENAME}.csv'
        graph_nodes_file_path = os.path.join(graph_directory, NODES_FILENAME)
        graph_edges_file_path = os.path.join(graph_directory, EDGES_FILENAME)
        csv_nodes_file_path = os.path.join(graph_directory, nodes_csv_filename)
        csv_edges_file_path = os.path.join(graph_directory, edges_csv_filename)
        if os.path.exists(csv_nodes_file_path) and os.path.exists(csv_edges_file_path):
            self.logger.info(f'CSV files were already created for {graph_id}({graph_version})')
        else:
            self.__convert_kgx_to_csv(graph_id=graph_id,
                                      graph_version=graph_version,
                                      nodes_input_file=graph_nodes_file_path,
                                      edges_input_file=graph_edges_file_path,
                                      nodes_output_file=csv_nodes_file_path,
                                      edges_output_file=csv_edges_file_path)

        graph_dump_file_path = os.path.join(graph_directory, f'graph_{graph_version}.db.dump')
        if os.path.exists(graph_dump_file_path):
            self.logger.info(f'Neo4j dump already exists for {graph_id}({graph_version})')
            return True

        neo4j_access = Neo4jTools(graph_id=graph_id, graph_version=graph_version)
        try:
            password_exit_code = neo4j_access.set_initial_password()
            if password_exit_code != 0:
                return False

            import_exit_code = neo4j_access.import_csv_files(graph_directory=graph_directory,
                                                             csv_nodes_filename=nodes_csv_filename,
                                                             csv_edges_filename=edges_csv_filename)
            if import_exit_code != 0:
                return False

            start_exit_code = neo4j_access.start_neo4j()
            if start_exit_code != 0:
                return False

            waiting_exit_code = neo4j_access.wait_for_neo4j_initialization()
            if waiting_exit_code != 0:
                return False

            indexes_exit_code = neo4j_access.add_db_indexes()
            if indexes_exit_code != 0:
                return False

            self.generate_meta_kg_and_sri_test_data(neo4j_access=neo4j_access,
                                                    output_directory=graph_directory)

            stop_exit_code = neo4j_access.stop_neo4j()
            if stop_exit_code != 0:
                return False

            dump_exit_code = neo4j_access.create_backup_dump(graph_dump_file_path)
            if dump_exit_code != 0:
                return False

        finally:
            neo4j_access.close()

        self.logger.info(f'Success! Neo4j dump created with indexes for {graph_id}({graph_version})')
        return True

    def __convert_kgx_to_csv(self,
                             graph_id: str,
                             graph_version: str,
                             nodes_input_file: str,
                             edges_input_file: str,
                             nodes_output_file: str,
                             edges_output_file: str):
        self.logger.info(f'Creating CSV files for {graph_id}({graph_version})...')
        kgx_file_converter.convert_jsonl_to_neo4j_csv(nodes_input_file=nodes_input_file,
                                                      edges_input_file=edges_input_file,
                                                      nodes_output_file=nodes_output_file,
                                                      edges_output_file=edges_output_file)
        self.logger.info(f'CSV files created for {graph_id}({graph_version})...')

    # This was mostly adapted (stolen) from Plater
    def generate_meta_kg_and_sri_test_data(self, neo4j_access: Neo4jTools, output_directory: str):

        # used to keep track of derived inverted predicates
        inverted_predicate_tracker = defaultdict(lambda: defaultdict(set))

        schema_query = """ MATCH (a)-[x]->(b) RETURN DISTINCT labels(a) as source_labels, type(x) as predicate, labels(b) as target_labels"""
        self.logger.info(f"Starting schema query {schema_query} on graph... this might take a few.")
        before_time = time.time()
        schema_query_results = neo4j_access.execute_read_cypher_query(schema_query)
        after_time = time.time()
        self.logger.info(f"Completed schema query ({after_time - before_time} seconds). Preparing initial schema.")

        schema = defaultdict(lambda: defaultdict(set))
        #  avoids adding nodes with only a ROOT_ENTITY label (currently NamedThing)
        filter_named_thing = lambda x: list(filter(lambda y: y != ROOT_ENTITY, x))
        for schema_result in schema_query_results:
            source_labels, predicate, target_labels = \
                self.bl_utils.find_biolink_leaves(filter_named_thing(schema_result['source_labels'])), \
                schema_result['predicate'], \
                self.bl_utils.find_biolink_leaves(filter_named_thing(schema_result['target_labels']))
            for source_label in source_labels:
                for target_label in target_labels:
                    schema[source_label][target_label].add(predicate)

        # find and add the inverse for each predicate if there is one,
        # keep track of inverted predicates we added so we don't query the graph for them
        for source_label in list(schema.keys()):
            for target_label in list(schema[source_label].keys()):
                inverted_predicates = set()
                for predicate in schema[source_label][target_label]:
                    inverse_predicate = self.bl_utils.invert_predicate(predicate)
                    if inverse_predicate is not None and \
                            inverse_predicate not in schema[target_label][source_label]:
                        inverted_predicates.add(inverse_predicate)
                        inverted_predicate_tracker[target_label][source_label].add(inverse_predicate)
                schema[target_label][source_label].update(inverted_predicates)

        meta_kg_nodes = {}
        meta_kg_edges = []
        test_edges = []
        self.logger.info(f"Starting curie prefix and example edge queries...")
        before_time = time.time()
        for subject_node_type in schema:
            if subject_node_type not in meta_kg_nodes:
                curies, attributes = self.get_curie_prefixes_by_node_type(neo4j_access,
                                                                          subject_node_type)
                meta_kg_nodes[subject_node_type] = {'id_prefixes': curies, "attributes": attributes}
            for object_node_type in schema[subject_node_type]:
                if object_node_type not in meta_kg_nodes:
                    curies, attributes = self.get_curie_prefixes_by_node_type(neo4j_access,
                                                                              object_node_type)
                    meta_kg_nodes[object_node_type] = {'id_prefixes': curies, "attributes": attributes}
                for predicate in schema[subject_node_type][object_node_type]:
                    meta_kg_edges.append({
                        'subject': subject_node_type,
                        'object': object_node_type,
                        'predicate': predicate
                    })
                    if predicate not in inverted_predicate_tracker[subject_node_type][object_node_type]:
                        has_qualifiers = self.bl_utils.predicate_has_qualifiers(predicate)
                        example_edges = self.get_examples(neo4j_access=neo4j_access,
                                                          subject_node_type=subject_node_type,
                                                          object_node_type=object_node_type,
                                                          predicate=predicate,
                                                          num_examples=1,
                                                          use_qualifiers=has_qualifiers)

                        # sometimes a predicate could have qualifiers but there is not an example of one
                        if not example_edges and has_qualifiers:
                            example_edges = self.get_examples(neo4j_access=neo4j_access,
                                                              subject_node_type=subject_node_type,
                                                              object_node_type=object_node_type,
                                                              predicate=predicate,
                                                              num_examples=1,
                                                              use_qualifiers=False)

                        if example_edges:
                            neo4j_subject = example_edges[0]['subject']
                            neo4j_object = example_edges[0]['object']
                            neo4j_edge = example_edges[0]['edge']
                            test_edge = {
                                "subject_category": subject_node_type,
                                "object_category": object_node_type,
                                "predicate": predicate,
                                "subject_id": neo4j_subject['id'],
                                "object_id": neo4j_object['id']
                            }
                            if has_qualifiers:
                                qualifiers = []
                                for prop in neo4j_edge:
                                    if 'qualifie' in prop:
                                        qualifiers.append({
                                            "qualifier_type_id": f"biolink:{prop}" if not prop.startswith(
                                                "biolink:") else prop,
                                            "qualifier_value": neo4j_edge[prop]
                                        })
                                if qualifiers:
                                    test_edge["qualifiers"] = qualifiers
                            test_edges.append(test_edge)
                        else:
                            self.logger.info(f'Failed to find an example for '
                                             f'{subject_node_type}->{predicate}->{object_node_type}')

        after_time = time.time()
        self.logger.info(f"Completed curie prefix and example queries ({after_time - before_time} seconds).")
        self.logger.info(f'Meta KG and SRI Testing data complete. Generated {len(test_edges)} test edges. Writing to file..')

        meta_kg = {
            "nodes": meta_kg_nodes,
            "edges": meta_kg_edges
        }
        meta_kg_file_path = os.path.join(output_directory, META_KG_FILENAME)
        with open(meta_kg_file_path, 'w') as meta_kg_file:
            meta_kg_file.write(json.dumps(meta_kg, indent=4))

        sri_testing_data = {
            "source_type": "primary",
            "edges": test_edges
        }
        sri_testing_file_path = os.path.join(output_directory, SRI_TESTING_FILENAME)
        with open(sri_testing_file_path, 'w') as sri_testing_file:
            sri_testing_file.write(json.dumps(sri_testing_data, indent=4))

    def get_curie_prefixes_by_node_type(self, neo4j_access: Neo4jTools, node_type: str):
        curies_query = f"""
        MATCH (n:`{node_type}`) return collect(n.id) as ids , collect(keys(n)) as attributes
        """
        self.logger.debug(f"Starting {node_type} curies query... this might take a few.")
        before_time = time.time()
        curie_query_results = neo4j_access.execute_read_cypher_query(curies_query)
        after_time = time.time()
        self.logger.debug(f"Completed {node_type} curies query ({after_time - before_time} seconds).")

        curie_prefixes = set()
        for i in curie_query_results[0]['ids']:
            curie_prefixes.add(i.split(':')[0])
        # sort according to bl model - this can throw an exception if id_prefixes are not found, default to empty
        try:
            node_bl_def = self.bl_utils.toolkit.get_element(node_type)
            id_prefixes = node_bl_def.id_prefixes
            sorted_curie_prefixes = [i for i in id_prefixes if i in curie_prefixes]  # gives precedence to what's in BL
        except Exception as e:
            sorted_curie_prefixes = []
        # add other ids even if not in BL next
        sorted_curie_prefixes += [i for i in curie_prefixes if i not in sorted_curie_prefixes]
        all_keys = set()
        for keys in curie_query_results[0]['attributes']:
            for k in keys:
                all_keys.add(k)

        attributes_as_bl_types = []
        for key in all_keys:
            attr_data = self.bl_utils.get_attribute_bl_info(key)
            if attr_data:
                attr_data['original_attribute_names'] = [key]
                attributes_as_bl_types.append(attr_data)
        return sorted_curie_prefixes, attributes_as_bl_types

    def get_examples(self,
                     neo4j_access: Neo4jTools,
                     subject_node_type,
                     object_node_type,
                     predicate=None,
                     num_examples=1,
                     use_qualifiers=False):
        """
        return example edges
        """
        qualifiers_check = " WHERE edge.qualified_predicate IS NOT NULL " if use_qualifiers else ""
        if object_node_type and predicate:
            query = f"MATCH (subject:`{subject_node_type}`)-[edge:`{predicate}`]->(object:`{object_node_type}`) " \
                    f"{qualifiers_check} return subject, edge, object limit {num_examples}"
            response = neo4j_access.execute_read_cypher_query(query)
            return response
        elif object_node_type:
            query = f"MATCH (subject:`{subject_node_type}`)-[edge]->(object:`{object_node_type}`) " \
                    f"{qualifiers_check} return subject, edge, object limit {num_examples}"
            response = neo4j_access.execute_read_cypher_query(query)
            return response

    def load_graph_specs(self):
        if 'DATA_SERVICES_GRAPH_SPEC' in os.environ and os.environ['DATA_SERVICES_GRAPH_SPEC']:
            # this is a messy way to find the graph spec path, mainly for testing - URL is preferred
            graph_spec_file = os.environ['DATA_SERVICES_GRAPH_SPEC']
            graph_spec_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'graph_specs', graph_spec_file)
            if os.path.exists(graph_spec_path):
                self.logger.info(f'Loading graph spec: {graph_spec_file}')
                with open(graph_spec_path) as graph_spec_file:
                    graph_spec_yaml = yaml.full_load(graph_spec_file)
                    return self.parse_graph_spec(graph_spec_yaml)
            else:
                raise Exception(f'Configuration Error - Graph Spec could not be found: {graph_spec_file}')
        elif 'DATA_SERVICES_GRAPH_SPEC_URL' in os.environ:
            graph_spec_url = os.environ['DATA_SERVICES_GRAPH_SPEC_URL']
            graph_spec_request = requests.get(graph_spec_url)
            graph_spec_request.raise_for_status()
            graph_spec_yaml = yaml.full_load(graph_spec_request.text)
            return self.parse_graph_spec(graph_spec_yaml)
        else:
            raise Exception(f'Configuration Error - No Graph Spec was configured. Set the environment variable '
                            f'DATA_SERVICES_GRAPH_SPEC_URL to a URL with a valid Graph Spec yaml file. '
                            f'See the README for more info.')

    def parse_graph_spec(self, graph_spec_yaml):
        graph_specs = []
        graph_id = ""
        try:
            for graph_yaml in graph_spec_yaml['graphs']:
                graph_id = graph_yaml['graph_id']
                graph_name = graph_yaml['graph_name'] if 'graph_name' in graph_yaml else ""
                graph_description = graph_yaml['graph_description'] if 'graph_description' in graph_yaml else ""
                graph_url = graph_yaml['graph_url'] if 'graph_url' in graph_yaml else ""

                # parse the list of data sources
                data_sources = [self.parse_data_source_spec(data_source) for data_source in graph_yaml['sources']] \
                    if 'sources' in graph_yaml else []

                # parse the list of subgraphs
                subgraph_sources = [self.parse_subgraph_spec(subgraph) for subgraph in graph_yaml['subgraphs']] \
                    if 'subgraphs' in graph_yaml else []

                if not data_sources and not subgraph_sources:
                    self.logger.error(f'Error: No sources were provided for graph: {graph_id}.')
                    continue

                # take any normalization scheme parameters specified at the graph level
                graph_wide_node_norm_version = graph_yaml['node_normalization_version'] \
                    if 'node_normalization_version' in graph_yaml else None
                if graph_wide_node_norm_version == 'latest':
                    graph_wide_node_norm_version = self.source_data_manager.get_latest_node_normalization_version()
                graph_wide_edge_norm_version = graph_yaml['edge_normalization_version'] \
                    if 'edge_normalization_version' in graph_yaml else None
                if graph_wide_edge_norm_version == 'latest':
                    graph_wide_edge_norm_version = self.source_data_manager.get_latest_edge_normalization_version()
                graph_wide_conflation = graph_yaml['conflation'] \
                    if 'conflation' in graph_yaml else None
                graph_wide_strict_norm = graph_yaml['strict_normalization'] \
                    if 'strict_normalization' in graph_yaml else None

                # apply them to all of the data sources, this will overwrite anything defined at the source level
                for data_source in data_sources:
                    if graph_wide_node_norm_version is not None:
                        data_source.normalization_scheme.node_normalization_version = graph_wide_node_norm_version
                    if graph_wide_edge_norm_version is not None:
                        data_source.normalization_scheme.edge_normalization_version = graph_wide_edge_norm_version
                    if graph_wide_conflation is not None:
                        data_source.normalization_scheme.conflation = graph_wide_conflation
                    if graph_wide_strict_norm is not None:
                        data_source.normalization_scheme.strict = graph_wide_strict_norm

                graph_output_format = graph_yaml['output_format'] if 'output_format' in graph_yaml else ""
                current_graph_spec = GraphSpec(graph_id=graph_id,
                                               graph_name=graph_name,
                                               graph_description=graph_description,
                                               graph_url=graph_url,
                                               graph_version=None,  # this will get populated later
                                               graph_output_format=graph_output_format,
                                               subgraphs=subgraph_sources,
                                               sources=data_sources)
                graph_version = self.generate_graph_version(current_graph_spec)
                current_graph_spec.graph_version = graph_version
                self.current_graph_versions[graph_id] = graph_version
                graph_specs.append(current_graph_spec)
        except Exception as e:
            self.logger.error(f'Error parsing Graph Spec ({graph_id}), formatting error or missing information: {repr(e)}')
            raise e
        return graph_specs

    def parse_subgraph_spec(self, subgraph_yml):
        subgraph_id = subgraph_yml['graph_id']
        subgraph_version = subgraph_yml['graph_version'] if 'graph_version' in subgraph_yml else 'current'
        if subgraph_version == 'current':
            if subgraph_id in self.current_graph_versions:
                subgraph_version = self.current_graph_versions[subgraph_id]
            else:
                raise Exception(f'Graph Spec Error - Could not determine version of subgraph {subgraph_id}. '
                                f'Either specify an existing version, already built in your graphs directory, '
                                f'or the subgraph must be defined previously in the same Graph Spec.')
        merge_strategy = subgraph_yml['merge_strategy'] if 'merge_strategy' in subgraph_yml else 'default'
        subgraph_source = SubGraphSource(id=subgraph_id,
                                         version=subgraph_version,
                                         merge_strategy=merge_strategy)
        return subgraph_source

    def parse_data_source_spec(self, source_yml):
        source_id = source_yml['source_id']
        if source_id not in get_available_data_sources():
            error_message = f'Data source {source_id} is not a valid data source id.'
            self.logger.error(error_message + " " +
                              f'Valid sources are: {", ".join(get_available_data_sources())}')
            raise Exception(error_message)

        source_version = source_yml['source_version'] if 'source_version' in source_yml \
            else self.source_data_manager.get_latest_source_version(source_id)
        if source_version is None:
            # TODO it would be great if we could default to the last stable version already built somehow
            error_message = f'Data source {source_id} could not determine the latest version. The service may be down.'
            raise Exception(error_message)

        parsing_version = source_yml['parsing_version'] if 'parsing_version' in source_yml \
            else self.source_data_manager.get_latest_parsing_version(source_id)
        merge_strategy = source_yml['merge_strategy'] if 'merge_strategy' in source_yml else 'default'
        node_normalization_version = source_yml['node_normalization_version'] \
            if 'node_normalization_version' in source_yml \
            else self.source_data_manager.get_latest_node_normalization_version()
        edge_normalization_version = source_yml['edge_normalization_version'] \
            if 'edge_normalization_version' in source_yml \
            else self.source_data_manager.get_latest_edge_normalization_version()
        strict_normalization = source_yml['strict_normalization'] \
            if 'strict_normalization' in source_yml else True
        conflation = source_yml['conflation'] \
            if 'conflation' in source_yml else False
        normalization_scheme = NormalizationScheme(node_normalization_version=node_normalization_version,
                                                   edge_normalization_version=edge_normalization_version,
                                                   strict=strict_normalization,
                                                   conflation=conflation)
        supplementation_version = SequenceVariantSupplementation.SUPPLEMENTATION_VERSION
        graph_source = DataSource(id=source_id,
                                  version=None,  # this will get populated later in build_dependencies
                                  source_version=source_version,
                                  merge_strategy=merge_strategy,
                                  normalization_scheme=normalization_scheme,
                                  parsing_version=parsing_version,
                                  supplementation_version=supplementation_version)
        return graph_source

    def get_graph_spec(self, graph_id: str):
        for graph_spec in self.graph_specs:
            if graph_spec.graph_id == graph_id:
                return graph_spec
        return None

    def get_graph_dir_path(self, graph_id: str, graph_version: str):
        return os.path.join(self.graphs_dir, graph_id, graph_version)

    def get_graph_output_URL(self, graph_id: str, graph_version: str):
        graph_output_url = os.environ['DATA_SERVICES_OUTPUT_URL']
        if graph_output_url[-1] != '/':
            graph_output_url += '/'
        return f'{graph_output_url}{graph_id}/{graph_version}/'

    def get_graph_nodes_file_path(self, graph_output_dir: str):
        return os.path.join(graph_output_dir, NODES_FILENAME)

    def get_graph_edges_file_path(self, graph_output_dir: str):
        return os.path.join(graph_output_dir, EDGES_FILENAME)

    def check_for_existing_graph_dir(self, graph_id: str, graph_version: str):
        graph_output_dir = self.get_graph_dir_path(graph_id, graph_version)
        if not os.path.isdir(graph_output_dir):
            return False
        return True

    def get_graph_metadata(self, graph_id: str, graph_version: str):
        # make sure the output directory exists (where we check for existing GraphMetadata)
        graph_output_dir = self.get_graph_dir_path(graph_id, graph_version)
        if not os.path.isdir(graph_output_dir):
            os.makedirs(graph_output_dir)

        # load existing or create new metadata file
        return GraphMetadata(graph_id, graph_output_dir)

    @staticmethod
    def generate_graph_version(graph_spec: GraphSpec):
        sources_string = ''.join(
            [json.dumps(graph_source.get_metadata_representation())
             for graph_source in graph_spec.sources])
        subgraphs_string = ''.join(
            [''.join([subgraph.id, subgraph.version, subgraph.merge_strategy])
             for subgraph in graph_spec.subgraphs])
        graph_version = xxh64_hexdigest(sources_string + subgraphs_string)
        return graph_version

    @staticmethod
    def init_graphs_dir():
        # use the directory specified by the environment variable DATA_SERVICES_GRAPHS
        if 'DATA_SERVICES_GRAPHS' in os.environ and os.path.isdir(os.environ['DATA_SERVICES_GRAPHS']):
            return os.environ['DATA_SERVICES_GRAPHS']
        else:
            # if graph dir is invalid or not specified back out
            raise IOError(
                'GraphBuilder graphs directory not found. '
                'Specify a valid directory with environment variable DATA_SERVICES_GRAPHS.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Merge data source files into complete graphs.")
    parser.add_argument('graph_id',
                        help='ID of the graph to build. Must match an ID from the configured Graph Spec.')
    parser.add_argument('-v', '--version',
                        action='store_true',
                        help='Only retrieve a generated version for graphs from the graph spec.')
    args = parser.parse_args()
    graph_id_arg = args.graph_id
    retrieve_version = args.version

    graph_builder = GraphBuilder()
    if graph_id_arg == "all":
        if retrieve_version:
            graph_versions = [graph_spec.graph_version for graph_spec in graph_builder.graph_specs]
            print('\n'.join(graph_versions))
        else:
            for g_id in [graph_spec.graph_id for graph_spec in graph_builder.graph_specs]:
                graph_builder.build_graph(g_id)
    else:
        graph_spec = graph_builder.get_graph_spec(graph_id_arg)
        if graph_spec:
            if retrieve_version:
                print(graph_spec.graph_version)
            else:
                graph_builder.build_graph(graph_id_arg)
        else:
            print(f'Invalid graph spec requested: {graph_id_arg}')
    for results_graph_id, results in graph_builder.build_results.items():
        if results['success']:
            print(f'{results_graph_id}\t{results["version"]}')


