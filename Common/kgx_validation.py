import os
import orjson
from collections import defaultdict

from Common.utils import quick_jsonl_file_iterator
from Common.biolink_utils import BiolinkUtils, BiolinkInformationResources, \
    INFORES_STATUS_INVALID, INFORES_STATUS_DEPRECATED
from Common.biolink_constants import *


def sort_dict_by_values(dict_to_sort):
    return dict(sorted(dict_to_sort.items(), key=lambda item_tuple: item_tuple[1], reverse=True))


def validate_graph(nodes_file_path: str,
                   edges_file_path: str,
                   graph_id: str = None,
                   graph_version: str = None,
                   validation_results_directory: str = None,
                   save_invalid_edges: bool = False,
                   logger=None):

    qc_metadata = {
        'pass': True,
        'warnings': {},
        'errors': {}
    }

    bl_utils = BiolinkUtils()

    # Nodes QC
    # - count the number of nodes with each curie prefix
    # - store the leaf node types for each node for lookup
    node_curie_prefixes = defaultdict(int)
    node_type_lookup = {}
    all_node_types = set()
    all_node_properties = set()
    for node in quick_jsonl_file_iterator(nodes_file_path):
        node_curie = node['id']
        node_curie_prefixes[node_curie.split(':')[0]] += 1
        node_type_lookup[node_curie] = bl_utils.find_biolink_leaves(frozenset(node[NODE_TYPES]))
        all_node_properties.update(node.keys())
        all_node_types.update(node[NODE_TYPES])

    # make a list of invalid node types according to biolink
    invalid_node_types = []
    for node_type in all_node_types:
        if not bl_utils.is_valid_node_type(node_type):
            invalid_node_types.append(node_type)

    # Edges QC
    # Iterate through the edges and find all knowledge sources, edge properties, and predicates
    all_primary_knowledge_sources = set()
    all_aggregator_knowledge_sources = set()
    all_edge_properties = set()
    predicate_counts = defaultdict(int)
    predicate_counts_by_ks = defaultdict(lambda: defaultdict(int))
    edges_with_publications = defaultdict(int)  # predicate to num of edges with that predicate and publications

    invalid_edges_due_to_predicate_and_node_types = 0
    invalid_edges_due_to_missing_primary_ks = 0
    invalid_edge_output = open(os.path.join(validation_results_directory, 'invalid_predicate_and_node_types.jsonl')) \
        if save_invalid_edges else None

    # iterate through every edge
    for edge_json in quick_jsonl_file_iterator(edges_file_path):

        # get references to some edge properties
        predicate = edge_json[PREDICATE]
        subject_node_types = node_type_lookup[edge_json[SUBJECT_ID]]
        object_node_types = node_type_lookup[edge_json[OBJECT_ID]]

        try:
            primary_knowledge_source = edge_json[PRIMARY_KNOWLEDGE_SOURCE]
        except KeyError:
            invalid_edges_due_to_missing_primary_ks += 1
            primary_knowledge_source = 'missing_primary_knowledge_source'
            if save_invalid_edges:
                invalid_edge_output.write(f'{orjson.dumps(edge_json)}\n')

        # update predicate counts
        predicate_counts[predicate] += 1
        predicate_counts_by_ks[primary_knowledge_source][predicate] += 1

        # add all properties to edge_properties set
        all_edge_properties.update(edge_json.keys())

        # gather metadata about knowledge sources
        all_primary_knowledge_sources.add(primary_knowledge_source)
        aggregator_knowledge_sources = edge_json.get(AGGREGATOR_KNOWLEDGE_SOURCES, None)
        if aggregator_knowledge_sources:
            all_aggregator_knowledge_sources.update(aggregator_knowledge_sources)

        # update publication by predicate counts
        if edge_json.get(PUBLICATIONS, False):
            edges_with_publications[predicate] += 1

        # use the leaf node types for the subject and object and validate the predicate against the node types
        if not bl_utils.validate_edge(subject_node_types, predicate, object_node_types):
            invalid_edges_due_to_predicate_and_node_types += 1
            if save_invalid_edges:
                invalid_edge_output.write(f'{orjson.dumps(edge_json)}\n')

    if save_invalid_edges:
        invalid_edge_output.close()

    # validate the knowledge sources with the biolink model
    bl_inforesources = BiolinkInformationResources()
    deprecated_infores_ids = []
    invalid_infores_ids = []
    all_knowledge_sources = all_primary_knowledge_sources | all_aggregator_knowledge_sources
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
    qc_metadata['primary_knowledge_sources'] = sorted(all_primary_knowledge_sources)
    qc_metadata['aggregator_knowledge_sources'] = sorted(all_aggregator_knowledge_sources)
    qc_metadata['predicate_totals'] = sort_dict_by_values({k: v for k, v in predicate_counts.items()})
    qc_metadata['predicates_by_knowledge_source'] = {ks: sort_dict_by_values(
        {predicate: count for predicate, count in ks_to_p.items()})
        for ks, ks_to_p in predicate_counts_by_ks.items()}
    qc_metadata['edges_with_publications'] = sort_dict_by_values({k: v for k, v in edges_with_publications.items()})
    qc_metadata['edge_properties'] = sorted(all_edge_properties)
    qc_metadata['node_curie_prefixes'] = sort_dict_by_values({k: v for k, v in node_curie_prefixes.items()})
    qc_metadata['node_properties'] = sorted(all_node_properties)
    qc_metadata['invalid_edges_due_to_predicate_and_node_types'] = invalid_edges_due_to_predicate_and_node_types
    qc_metadata['invalid_edges_due_to_missing_primary_ks'] = invalid_edges_due_to_missing_primary_ks

    if deprecated_infores_ids:
        qc_metadata['warnings']['deprecated_knowledge_sources'] = deprecated_infores_ids
    if invalid_infores_ids:
        qc_metadata['warnings']['invalid_knowledge_sources'] = invalid_infores_ids
    if invalid_node_types:
        qc_metadata['warnings']['invalid_node_types'] = invalid_node_types

    return qc_metadata
