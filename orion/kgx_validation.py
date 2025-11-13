import os
import orjson
from collections import defaultdict

from orion.utils import quick_jsonl_file_iterator
from orion.biolink_utils import BiolinkUtils, BiolinkInformationResources, \
    INFORES_STATUS_INVALID, INFORES_STATUS_DEPRECATED
from orion.biolink_constants import *


# this just sorts a dicts keys by its values
def sort_dict_by_values(dict_to_sort):
    return dict(sorted(dict_to_sort.items(), key=lambda item_tuple: item_tuple[1], reverse=True))

def convert_type_count_dict_to_list(type_count_dict):
    type_count_dict = sort_dict_by_values(type_count_dict)
    return [{"type": list(types) if types != [None] else None,
             "count": count} for types, count in type_count_dict.items()]

def convert_spo_count_to_dict(spo_count_dict):
    return [{'subject_type': list(subject_type),
             'predicate': predicate,
             'object_type': list(object_type),
             'count': count}
            for (subject_type, predicate, object_type), count in spo_count_dict.items()]

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
    node_type_counts = defaultdict(int)
    node_type_lookup = {}
    all_node_properties = set()
    for node in quick_jsonl_file_iterator(nodes_file_path):
        node_curie = node['id']
        node_curie_prefixes[node_curie.split(':')[0]] += 1

        all_node_properties.update(node.keys())

        node_type = bl_utils.find_biolink_leaves(frozenset(node[NODE_TYPES]))
        node_type_lookup[node_curie] = node_type
        node_type_counts[node_type] += 1

    all_node_types = set()
    for node_types, count in node_type_counts.items():
        # gather the union of all the node types
        all_node_types.update(node_types)

    # convert the node_type_counts dict (frozenset -> count)
    # to a format for the output (a list of dicts like {type: node_types, count: count})
    node_type_counts = sort_dict_by_values(node_type_counts)
    node_type_counts = convert_type_count_dict_to_list(node_type_counts)

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
    spo_type_counts = defaultdict(int)

    source_breakdown = defaultdict(lambda: defaultdict(dict))

    invalid_edges_due_to_predicate_and_node_types = 0
    invalid_edges_due_to_missing_primary_ks = 0
    invalid_edge_output = open(os.path.join(validation_results_directory, 'invalid_predicate_and_node_types.jsonl')) \
        if save_invalid_edges else None

    # iterate through every edge
    for edge_json in quick_jsonl_file_iterator(edges_file_path):

        # get references to some edge properties
        subject_id = edge_json[SUBJECT_ID]
        object_id = edge_json[OBJECT_ID]
        predicate = edge_json[PREDICATE]
        subject_node_types = node_type_lookup[subject_id]
        object_node_types = node_type_lookup[object_id]
        # increment a counter for the combination of node types and predicate
        # TODO we could account for symmetric predicates here, right now it would count either direction separately
        spo_type_counts[(subject_node_types, predicate, object_node_types)] += 1

        try:
            primary_knowledge_source = edge_json[PRIMARY_KNOWLEDGE_SOURCE]
        except KeyError:
            invalid_edges_due_to_missing_primary_ks += 1
            primary_knowledge_source = 'missing_primary_knowledge_source'
            if save_invalid_edges:
                invalid_edge_output.write(f'{orjson.dumps(edge_json)}\n')

        all_primary_knowledge_sources.add(primary_knowledge_source)

        # Get aggregator knowledge sources
        aggregator_knowledge_sources = edge_json.get(AGGREGATOR_KNOWLEDGE_SOURCES)
        if aggregator_knowledge_sources is not None:
            all_aggregator_knowledge_sources.update(aggregator_knowledge_sources)
        else:
            # if there isn't one use the graph id as one to organize the source breakdown
            aggregator_knowledge_sources = [None]

        agg_key = frozenset(sorted(aggregator_knowledge_sources))
        agg_entry = source_breakdown[agg_key]
        agg_entry[primary_knowledge_source] = {
            "node_count": 0,
            "edge_count": 0,
            "subject_prefixes": defaultdict(int),
            "subject_types": defaultdict(int),
            "predicates": defaultdict(int),
            "object_prefixes": defaultdict(int),
            "object_types": defaultdict(int),
            "s-p-o_types": defaultdict(int),
            "node_set": set()
        }
        primary_entry = agg_entry[primary_knowledge_source]

        # Update counts within the primary entry
        primary_entry["edge_count"] += 1
        primary_entry["node_set"].add(subject_id)
        primary_entry["node_set"].add(object_id)

        # Extract prefixes
        subject_prefix = subject_id.split(":")[0]
        object_prefix = object_id.split(":")[0]

        # Update all the detailed counts
        primary_entry["subject_prefixes"][subject_prefix] += 1
        primary_entry["subject_types"][subject_node_types] += 1
        primary_entry["predicates"][predicate] += 1
        primary_entry["object_prefixes"][object_prefix] += 1
        primary_entry["object_types"][object_node_types] += 1
        primary_entry["s-p-o_types"][(subject_node_types, predicate, object_node_types)] += 1

        # update predicate counts
        predicate_counts[predicate] += 1
        predicate_counts_by_ks[primary_knowledge_source][predicate] += 1

        # add all properties to edge_properties set
        all_edge_properties.update(edge_json.keys())

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

    for agg_key, agg_data in source_breakdown.items():
        for prim_key, prim_data in agg_data.items():
            prim_data["node_count"] = len(prim_data["node_set"])
            del prim_data["node_set"]

            prim_data["subject_prefixes"] = sort_dict_by_values(dict(prim_data["subject_prefixes"]))
            prim_data["subject_types"] = convert_type_count_dict_to_list(prim_data["subject_types"])
            prim_data["predicates"] = sort_dict_by_values(dict(prim_data["predicates"]))
            prim_data["object_prefixes"] = sort_dict_by_values(dict(prim_data["object_prefixes"]))
            prim_data["object_types"] = convert_type_count_dict_to_list(prim_data["object_types"])

            prim_data["s-p-o_types"] = convert_spo_count_to_dict(prim_data["s-p-o_types"])
    source_breakdown = [{"aggregator": list(agg), "breakdown": dict(breakdown)}
                        for agg, breakdown in source_breakdown.items()]

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
    qc_metadata['node_curie_prefixes'] = sort_dict_by_values({k: v for k, v in node_curie_prefixes.items()})
    qc_metadata['node_types'] = node_type_counts
    qc_metadata['node_properties'] = sorted(all_node_properties)
    qc_metadata['predicate_totals'] = sort_dict_by_values({k: v for k, v in predicate_counts.items()})
    qc_metadata['edges_with_publications'] = sort_dict_by_values({k: v for k, v in edges_with_publications.items()})
    qc_metadata['edge_properties'] = sorted(all_edge_properties)
    qc_metadata['s-p-o_types'] = convert_spo_count_to_dict(spo_type_counts)
    qc_metadata['source_breakdown'] = source_breakdown
    qc_metadata['invalid_edges_due_to_predicate_and_node_types'] = invalid_edges_due_to_predicate_and_node_types
    qc_metadata['invalid_edges_due_to_missing_primary_ks'] = invalid_edges_due_to_missing_primary_ks

    if deprecated_infores_ids:
        qc_metadata['warnings']['deprecated_knowledge_sources'] = deprecated_infores_ids
    if invalid_infores_ids:
        qc_metadata['warnings']['invalid_knowledge_sources'] = invalid_infores_ids
    if invalid_node_types:
        qc_metadata['warnings']['invalid_node_types'] = invalid_node_types

    return qc_metadata

