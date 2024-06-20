import os
import orjson
from collections import defaultdict

from Common.utils import quick_jsonl_file_iterator
from Common.biolink_utils import BiolinkUtils, BiolinkInformationResources, \
    INFORES_STATUS_INVALID, INFORES_STATUS_DEPRECATED
from Common.biolink_constants import *


def validate_graph(nodes_file_path: str,
                   edges_file_path: str,
                   graph_id: str = None,
                   graph_version: str = None,
                   validation_results_directory: str = None,
                   save_validation_results: bool = False,
                   logger=None):

    qc_metadata = {
        'pass': True,
        'warnings': {},
        'errors': {}
    }

    bl_utils = BiolinkUtils()

    # Nodes QC
    # - run some QC and validation for the nodes file
    # - generate some metadata about the nodes
    # - store some node information for lookups
    node_curie_prefixes = defaultdict(int)
    node_type_lookup = {}
    invalid_curies = 0
    # all_node_types = set()
    for node in quick_jsonl_file_iterator(nodes_file_path):
        node_curie = node['id']
        split_node_curie = node_curie.split(':')
        if len(split_node_curie) < 2:
            invalid_curies += 1
        node_prefix = split_node_curie[0]
        node_curie_prefixes[node_prefix] += 1
        # all_node_types.update(node[NODE_TYPES])
        node_type_lookup[node_curie] = bl_utils.find_biolink_leaves(set(node[NODE_TYPES]))

    # validate the node types with the biolink model
    # for node_type in all_node_types:
    #     TODO

    # Edges QC
    # Iterate through the edges and find all knowledge sources, edge properties, and predicates
    primary_knowledge_sources = set()
    aggregator_knowledge_sources = set()
    edge_properties = set()
    predicate_counts = defaultdict(int)
    predicate_counts_by_ks = defaultdict(lambda: defaultdict(int))
    edges_with_publications = defaultdict(int)  # predicate to num of edges with that predicate and publications
    invalid_edges_due_to_predicate_plus_node_types = 0

    invalid_edge_output = open(os.path.join(validation_results_directory, 'invalid_predicate_and_node_types.jsonl')) \
        if save_validation_results else None

    # iterate through every edge
    for edge_json in quick_jsonl_file_iterator(edges_file_path):

        # get references to some edge properties
        predicate = edge_json[PREDICATE]
        primary_knowledge_source = edge_json[PRIMARY_KNOWLEDGE_SOURCE]

        # update predicate counts
        predicate_counts[predicate] += 1
        predicate_counts_by_ks[primary_knowledge_source][predicate] += 1

        # add all properties to edge_properties set
        edge_properties.update(edge_json.keys())

        # gather metadata about knowledge sources
        primary_knowledge_sources.add(primary_knowledge_source)
        if AGGREGATOR_KNOWLEDGE_SOURCES in edge_json:
            for ks in edge_json[AGGREGATOR_KNOWLEDGE_SOURCES]:
                aggregator_knowledge_sources.add(ks)

        # update publication by predicate counts
        if PUBLICATIONS in edge_json and edge_json[PUBLICATIONS]:
            edges_with_publications[edge_json[PREDICATE]] += 1

        # get the leaf node types for the subject and object and validate the predicate against the node types
        possible_predicates_based_on_domain = set()
        possible_predicates_based_on_range = set()
        for subject_type in node_type_lookup[edge_json[SUBJECT_ID]]:
            possible_predicates_based_on_domain.update(
                bl_utils.toolkit.get_all_predicates_with_class_domain(subject_type,
                                                                      formatted=True,
                                                                      check_ancestors=True))
        for object_type in node_type_lookup[edge_json[OBJECT_ID]]:
            possible_predicates_based_on_range.update(
                bl_utils.toolkit.get_all_predicates_with_class_range(object_type,
                                                                     formatted=True,
                                                                     check_ancestors=True))
        possible_predicates = possible_predicates_based_on_domain & possible_predicates_based_on_range
        if predicate not in possible_predicates:
            invalid_edges_due_to_predicate_plus_node_types += 1
            if save_validation_results:
                invalid_edge_output.write(f'{orjson.dumps(edge_json)}\n')

    if save_validation_results:
        invalid_edge_output.close()

    # validate the knowledge sources with the biolink model
    bl_inforesources = BiolinkInformationResources()
    deprecated_infores_ids = []
    invalid_infores_ids = []
    all_knowledge_sources = primary_knowledge_sources | aggregator_knowledge_sources
    for knowledge_source in all_knowledge_sources:
        infores_status = bl_inforesources.get_infores_status(knowledge_source)
        if infores_status == INFORES_STATUS_DEPRECATED:
            deprecated_infores_ids.append(knowledge_source)
            warning_message = f'QC for graph {graph_id} version {graph_version} ' \
                              f'found a deprecated infores identifier: {knowledge_source}'
            if logger:
                logger.warning(warning_message)
            else:
                print(warning_message)
        elif infores_status == INFORES_STATUS_INVALID:
            invalid_infores_ids.append(knowledge_source)
            warning_message = f'QC for graph {graph_id} version {graph_version} ' \
                              f'found an invalid infores identifier: {knowledge_source}'
            if logger:
                logger.warning(warning_message)
            else:
                print(warning_message)

    qc_metadata['primary_knowledge_sources'] = list(primary_knowledge_sources),
    qc_metadata['aggregator_knowledge_sources'] = list(aggregator_knowledge_sources),
    qc_metadata['predicate_totals'] = {k: v for k, v in predicate_counts.items()},
    qc_metadata['predicates_by_knowledge_source'] = {ks: {predicate: count for predicate, count in ks_to_p.items()}
                                           for ks, ks_to_p in predicate_counts_by_ks.items()},
    qc_metadata['node_curie_prefixes'] = {k: v for k, v in node_curie_prefixes.items()},
    qc_metadata['edges_with_publications'] = {k: v for k, v in edges_with_publications.items()},
    qc_metadata['edge_properties'] = list(edge_properties),
    qc_metadata['invalid_edges_due_to_predicate_plus_node_types'] = invalid_edges_due_to_predicate_plus_node_types

    if deprecated_infores_ids:
        qc_metadata['warnings']['deprecated_knowledge_sources'] = deprecated_infores_ids
    if invalid_infores_ids:
        qc_metadata['warnings']['invalid_knowledge_sources'] = invalid_infores_ids

    return qc_metadata
