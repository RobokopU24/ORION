import os
import json
import jsonlines
import logging
from collections import defaultdict
from copy import deepcopy
from Common.node_types import SEQUENCE_VARIANT, ORIGINAL_KNOWLEDGE_SOURCE, PRIMARY_KNOWLEDGE_SOURCE, \
    AGGREGATOR_KNOWLEDGE_SOURCES, PUBLICATIONS, OBJECT_ID, SUBJECT_ID, RELATION, PREDICATE
from Common.utils import LoggingUtil
from Common.utils import NodeNormUtils, EdgeNormUtils, EdgeNormalizationResult
from Common.kgx_file_writer import KGXFileWriter


class NormalizationBrokenError(Exception):
    def __init__(self, error_message: str, actual_error: str = ''):
        self.error_message = error_message
        self.actual_error = actual_error


class NormalizationFailedError(Exception):
    def __init__(self, error_message: str, actual_error: str = ''):
        self.error_message = error_message
        self.actual_error = actual_error


EDGE_PROPERTIES_THAT_SHOULD_BE_SETS = {AGGREGATOR_KNOWLEDGE_SOURCES, PUBLICATIONS}


#
# This piece takes KGX-like files and normalizes the nodes and edges for biolink compliance.
# It then writes all of the normalized nodes and edges to new files.
#
class KGXFileNormalizer:

    logger = LoggingUtil.init_logging("Data_services.Common.KGXFileNormalizer",
                                      line_format='medium',
                                      level=logging.DEBUG,
                                      log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def __init__(self,
                 source_nodes_file_path: str,
                 nodes_output_file_path: str,
                 node_norm_map_file_path: str,
                 node_norm_failures_file_path: str,
                 source_edges_file_path: str,
                 edges_output_file_path: str,
                 edge_norm_predicate_map_file_path: str,
                 edge_subject_pre_normalized: bool = False,
                 edge_object_pre_normalized: bool = False,
                 has_sequence_variants: bool = False,
                 strict_normalization: bool = True):
        self.source_nodes_file_path = source_nodes_file_path
        self.nodes_output_file_path = nodes_output_file_path
        self.node_norm_map_file_path = node_norm_map_file_path
        self.node_norm_failures_file_path = node_norm_failures_file_path
        self.source_edges_file_path = source_edges_file_path
        self.edges_output_file_path = edges_output_file_path
        self.edge_norm_predicate_map_file_path = edge_norm_predicate_map_file_path
        # in some cases we start with normalized nodes on one end of the edge,
        # these flags indicate we should skip normalizing those IDs
        # this is important because those IDs are probably missing from the supplied nodes file
        self.edge_subject_pre_normalized = edge_subject_pre_normalized
        self.edge_object_pre_normalized = edge_object_pre_normalized
        self.has_sequence_variants = has_sequence_variants
        self.normalization_metadata = {'strict_normalization': strict_normalization}

        # instances of the normalization service wrappers
        # strict normalization flag tells normalizer to throw away any nodes that don't normalize
        self.node_normalizer = NodeNormUtils(strict_normalization=strict_normalization)
        self.edge_normalizer = EdgeNormUtils()

    def normalize_kgx_files(self):
        self.normalize_node_file()
        self.normalize_edge_file()
        orphan_nodes_removed = remove_orphan_nodes(self.nodes_output_file_path, self.edges_output_file_path)
        self.normalization_metadata['orphan_nodes_removed'] = orphan_nodes_removed
        return self.normalization_metadata

    # given file paths to the source data node file and an output file,
    # normalize the nodes and write them to the new file
    # also write a file with the node ids that did not successfully normalize
    def normalize_node_file(self):

        # get the current node normalizer version
        node_norm_version = NodeNormUtils.get_current_node_norm_version()
        self.normalization_metadata['regular_node_norm_version'] = node_norm_version

        # read all of the source nodes from the provided jsonlines file
        self.logger.debug(f'Normalizing Node File {self.source_nodes_file_path}...')
        try:
            with jsonlines.open(self.source_nodes_file_path) as source_json_reader:
                self.logger.debug(f'Parsing Node File {self.source_nodes_file_path}...')
                source_nodes = [node for node in source_json_reader]
        except IOError as e:
            norm_error_msg = f'Error reading nodes file {self.source_nodes_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e)
        except jsonlines.InvalidLineError as e:
            norm_error_msg = f'Error decoding json from {self.source_nodes_file_path} on line number {e.lineno}: ' \
                             f'{e.line}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e)

        regular_nodes_pre_norm = 0
        regular_nodes_post_norm = 0
        variant_nodes_pre_norm = 0
        variant_nodes_split_count = 0
        variant_nodes_post_norm = 0

        self.logger.debug(f'Normalizing {len(source_nodes)} nodes and writing to file...')
        with KGXFileWriter(nodes_output_file_path=self.nodes_output_file_path) as output_file_writer:

            has_more_nodes = True
            while has_more_nodes:
                nodes_subset = source_nodes[:50000]
                if len(nodes_subset) == 0:
                    has_more_nodes = False
                else:
                    del source_nodes[:50000]
                    # if this source has sequence variants segregate them for the different norm services
                    variant_nodes = []
                    regular_nodes = []
                    if self.has_sequence_variants:
                        for node in nodes_subset:
                            if SEQUENCE_VARIANT in node['category']:
                                variant_nodes.append(node)
                            else:
                                regular_nodes.append(node)
                    else:
                        # otherwise assume all the source nodes are regular nodes and continue
                        regular_nodes = nodes_subset

                    # send the regular nodes through the normalizer - they will be normalized in place
                    # the number of nodes may change if strict normalization is on
                    # because nodes that fail to normalize are removed from the list
                    regular_nodes_pre_norm += len(regular_nodes)
                    if regular_nodes:
                        #self.logger.debug(f'Normalizing {len(regular_nodes)} regular nodes...')
                        self.node_normalizer.normalize_node_data(regular_nodes)
                    regular_nodes_post_norm += len(regular_nodes)

                    variant_nodes_pre_norm += len(variant_nodes)
                    if self.has_sequence_variants:
                        #self.logger.debug(f'Normalizing {len(variant_nodes)} sequence variant nodes...')
                        self.node_normalizer.normalize_sequence_variants(variant_nodes)
                    variant_nodes_post_norm += len(variant_nodes)

                    if self.has_sequence_variants:
                        # variant_node_splits is a dictionary of key: variant node ID, value: list of normalized variant node IDs
                        # these occur when one variant ID is ambiguous and could normalize to multiple different IDs
                        variant_node_splits = self.node_normalizer.variant_node_splits
                        # this takes the sum of the lengths of the values and subtracts the number of keys
                        # resulting in the number of variants that were added after splitting the original IDs
                        # if this makes absolutely no sense email Evan Morris and he will explain
                        variant_split_count = len(
                            [variant for sublist in variant_node_splits.values() for variant in sublist]) \
                                              - len(variant_node_splits.keys())
                        variant_nodes_split_count += variant_split_count

                    if regular_nodes:
                        #self.logger.debug(f'Writing regular nodes to file...')
                        output_file_writer.write_normalized_nodes(regular_nodes)
                    if variant_nodes:
                        #self.logger.debug(f'Writing sequence variant nodes to file...')
                        output_file_writer.write_normalized_nodes(variant_nodes)

        self.logger.debug(f'Writing normalization map to file...')
        normalization_map_info = {'normalization_map': self.node_normalizer.node_normalization_lookup}
        with open(self.node_norm_map_file_path, "w") as node_norm_map_file:
            json.dump(normalization_map_info, node_norm_map_file, indent=4)

        # grab the list of node IDs that failed
        regular_node_norm_failures = self.node_normalizer.failed_to_normalize_ids
        variant_node_norm_failures = self.node_normalizer.failed_to_normalize_variant_ids
        if regular_node_norm_failures or variant_node_norm_failures:
            self.logger.debug(f'Writing normalization failures to file...')
            with open(self.node_norm_failures_file_path, "w") as failed_norm_file:
                for failed_node_id in regular_node_norm_failures:
                    failed_norm_file.write(f'{failed_node_id}\n')
                for failed_node_id, error_message in variant_node_norm_failures.items():
                    failed_norm_file.write(f'{failed_node_id}\t{error_message}\n')

        # grab the number of repeat writes from the file writer
        # assuming the input file contained all unique node IDs,
        # this is the number of nodes that started with different IDs but normalized to the same ID as another node
        merged_node_count = output_file_writer.repeat_node_count

        # update the metadata
        self.normalization_metadata.update({
            'regular_nodes_pre_norm': regular_nodes_pre_norm,
            'regular_node_norm_failures': len(regular_node_norm_failures),
            'regular_nodes_post_norm': regular_nodes_post_norm,
        })
        if self.has_sequence_variants:
            self.normalization_metadata.update({
                'variant_nodes_pre_norm': variant_nodes_pre_norm,
                'variant_node_norm_failures': len(variant_node_norm_failures),
                'variant_nodes_split_count': variant_nodes_split_count,
                'variant_nodes_post_norm': variant_nodes_post_norm
            })
        self.normalization_metadata.update({
            'all_nodes_post_norm': regular_nodes_post_norm + variant_nodes_post_norm,
            'merged_nodes_post_norm': merged_node_count,
            'final_normalized_nodes': regular_nodes_post_norm + variant_nodes_post_norm - merged_node_count
        })



    # given file paths to the source data edge file and an output file,
    # normalize the predicates/relations and write them to the new file
    # also write a file with the predicates that did not successfully normalize
    def normalize_edge_file(self):

        # We organize the edges into a dictionary of dictionaries of dictionaries of dictionaries, no really,
        # to merge edges with the same subject, object, predicate, and knowledge source.
        # merged_edges[subject_id][object_id][predicate][knowledge_source] = edge
        merged_edges = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

        try:
            self.logger.debug(f'Normalizing edge file {self.source_edges_file_path}...')
            with open(self.source_edges_file_path) as source_json:
                self.logger.debug(f'Parsing edge file {self.source_edges_file_path}...')
                try:
                    source_reader = jsonlines.Reader(source_json)
                    source_edges = [edge for edge in source_reader]
                except json.JSONDecodeError as e:
                    norm_error_msg = f'Error decoding json from {self.source_edges_file_path} on line number {e.lineno}'
                    raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

        except IOError as e:
            norm_error_msg = f'Error reading edges file {self.source_edges_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

        self.logger.debug(f'Parsed edges from {self.source_edges_file_path}...')

        self.logger.debug(f'Grabbing current edge norm version')
        current_edge_norm_version = EdgeNormUtils.get_current_edge_norm_version()

        self.logger.debug(f'Normalizing predicates from {self.source_edges_file_path}...')
        edge_norm_failures = self.edge_normalizer.normalize_edge_data(source_edges)
        if edge_norm_failures:
            self.logger.error(f'Edge normalization service failed to return results for {edge_norm_failures}')
        self.logger.debug(f'Predicate normalization complete. Normalizing all the edges..')

        number_of_source_edges = len(source_edges)
        normalized_edge_count = 0
        edge_mergers = 0
        edge_splits = 0
        edges_failed_due_to_nodes = 0
        edges_failed_due_to_predicates = 0

        node_norm_lookup = self.node_normalizer.node_normalization_lookup
        edge_norm_lookup = self.edge_normalizer.edge_normalization_lookup

        has_more_edges = True
        while has_more_edges:
            edges_subset = source_edges[:50000]
            if len(edges_subset) == 0:
                has_more_edges = False
            else:
                del source_edges[:50000]
                for edge in edges_subset:
                    normalized_subject_ids = None
                    normalized_object_ids = None
                    try:
                        if self.edge_subject_pre_normalized:
                            normalized_subject_ids = [edge[SUBJECT_ID]]
                        else:
                            normalized_subject_ids = node_norm_lookup[edge[SUBJECT_ID]]
                        if self.edge_object_pre_normalized:
                            normalized_object_ids = [edge[OBJECT_ID]]
                        else:
                            normalized_object_ids = node_norm_lookup[edge[OBJECT_ID]]
                    except KeyError as e:
                        self.logger.error(f"One of the node IDs from the edge file was missing from the normalizer look up, "
                                          f"it's probably not in the node file. ({e})")
                    if not (normalized_subject_ids and normalized_object_ids):
                        edges_failed_due_to_nodes += 1
                    else:
                        try:
                            edge_norm_result: EdgeNormalizationResult = edge_norm_lookup[edge[RELATION]]
                        except KeyError:
                            edge_norm_result = None

                        if not edge_norm_result:
                            edges_failed_due_to_predicates += 1
                        else:
                            edge_count = 0
                            for norm_subject_id in normalized_subject_ids:
                                for norm_object_id in normalized_object_ids:
                                    edge_count += 1
                                    # extract the normalization info
                                    normalized_predicate = edge_norm_result.identifier
                                    edge_inverted_by_normalization = edge_norm_result.inverted
                                    # edge_label = edge_norm_result.label # right now label is not used

                                    # create a new edge with the normalized values
                                    # start with the original edge to preserve other properties
                                    normalized_edge = deepcopy(edge)
                                    normalized_edge[PREDICATE] = normalized_predicate

                                    # if normalization switched the direction of the predicate, swap the nodes
                                    if edge_inverted_by_normalization:
                                        normalized_edge[OBJECT_ID] = norm_subject_id
                                        normalized_edge[SUBJECT_ID] = norm_object_id
                                        norm_subject_id = normalized_edge[SUBJECT_ID]
                                        norm_object_id = normalized_edge[OBJECT_ID]
                                    else:
                                        normalized_edge[SUBJECT_ID] = norm_subject_id
                                        normalized_edge[OBJECT_ID] = norm_object_id

                                    knowledge_source_key = f'{normalized_edge.get(ORIGINAL_KNOWLEDGE_SOURCE, "")}_' \
                                                           f'{normalized_edge.get(PRIMARY_KNOWLEDGE_SOURCE, "")}'

                                    # merge with existing similar edges and/or queue up for writing later
                                    if ((norm_subject_id in merged_edges) and
                                            (norm_object_id in merged_edges[norm_subject_id]) and
                                            (normalized_predicate in merged_edges[norm_subject_id][norm_object_id]) and
                                            (knowledge_source_key in merged_edges[norm_subject_id][norm_object_id][normalized_predicate])):
                                        previous_edge = merged_edges[norm_subject_id][norm_object_id][normalized_predicate][knowledge_source_key]
                                        edge_mergers += 1
                                        for key, value in normalized_edge.items():
                                            # TODO - make sure this is the behavior we want -
                                            # for properties that are lists append the values
                                            # otherwise overwrite them
                                            if key in previous_edge and isinstance(value, list):
                                                previous_edge[key].extend(value)
                                                if key in EDGE_PROPERTIES_THAT_SHOULD_BE_SETS:
                                                    previous_edge[key] = list(set(value))
                                            else:
                                                previous_edge[key] = value
                                    else:
                                        merged_edges[norm_subject_id][norm_object_id][normalized_predicate][knowledge_source_key] = normalized_edge
                            # this counter tracks the number of new edges created from each individual edge in the original file
                            # this could happen due to rare cases of normalization splits where one node normalizes to many
                            if edge_count > 1:
                                edge_splits += edge_count - 1

        try:
            self.logger.debug(f'Writing normalized edges to file...')
            with KGXFileWriter(edges_output_file_path=self.edges_output_file_path) as output_file_writer:
                for subject_id, object_dict in merged_edges.items():
                        for object_id, predicate_dict in object_dict.items():
                            for predicate, knowledge_source_dict in predicate_dict.items():
                                for knowledge_source_key, edge in knowledge_source_dict.items():
                                    normalized_edge_count += 1
                                    output_file_writer.write_edge(subject_id=subject_id,
                                                                  object_id=object_id,
                                                                  relation=edge[RELATION],
                                                                  predicate=predicate,
                                                                  edge_properties=edge)
        except IOError as e:
            norm_error_msg = f'Error writing edges file {self.edges_output_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

        try:
            self.logger.debug(f'Writing predicate map to file...')
            edge_norm_json = {}
            for original_predicate, edge_normalization in edge_norm_lookup.items():
                edge_norm_json[original_predicate] = edge_normalization.__dict__
            predicate_map_info = {'predicate_map': edge_norm_json,
                                  'predicate_norm_failures': edge_norm_failures}
            with open(self.edge_norm_predicate_map_file_path, "w") as predicate_map_file:
                json.dump(predicate_map_info, predicate_map_file, sort_keys=True, indent=4)
        except IOError as e:
            norm_error_msg = f'Error writing edge predicate map file {self.edge_norm_predicate_map_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

        self.normalization_metadata.update({
            'edge_norm_version': current_edge_norm_version,
            'source_edges': number_of_source_edges,
            'edges_failed_due_to_nodes': edges_failed_due_to_nodes,
            'edges_failed_due_to_predicates': edges_failed_due_to_predicates,
            # these keep track of how many edges merged into another, or split into multiple edges
            # this should be true: source_edges - failures - mergers + splits = edges post norm
            'edge_mergers': edge_mergers,
            'edge_splits': edge_splits,
            'final_normalized_edges': normalized_edge_count
        })


"""
Given a nodes file and an edges file, remove all of the nodes from the nodes file that aren't attached to edges.
"""
def remove_orphan_nodes(nodes_file_path: str, edges_file_path: str):
    utilized_nodes = set()
    with open(edges_file_path) as edges_source:
        edge_reader = jsonlines.Reader(edges_source)
        for edge in edge_reader:
            utilized_nodes.add(edge[OBJECT_ID])
            utilized_nodes.add(edge[SUBJECT_ID])

    orphan_nodes_removed = 0
    temp_nodes_file_name = f'{nodes_file_path}.temp'
    os.rename(nodes_file_path, temp_nodes_file_name)
    with open(temp_nodes_file_name) as nodes_source:
        nodes_reader = jsonlines.Reader(nodes_source)
        with KGXFileWriter(nodes_output_file_path=nodes_file_path) as kgx_file_writer:
            for node in nodes_reader:
                if node['id'] in utilized_nodes:
                    kgx_file_writer.write_normalized_node(node, uniquify=False)
                else:
                    orphan_nodes_removed += 1
    os.remove(temp_nodes_file_name)
    return orphan_nodes_removed



