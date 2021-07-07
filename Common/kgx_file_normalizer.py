import os
import json
import jsonlines
import logging
from collections import defaultdict
from Common.node_types import SEQUENCE_VARIANT
from Common.utils import LoggingUtil
from Common.utils import NodeNormUtils, EdgeNormUtils
from Common.kgx_file_writer import KGXFileWriter


class NormalizationBrokenError(Exception):
    def __init__(self, error_message: str, actual_error: str = ''):
        self.error_message = error_message
        self.actual_error = actual_error


class NormalizationFailedError(Exception):
    def __init__(self, error_message: str, actual_error: str = ''):
        self.error_message = error_message
        self.actual_error = actual_error


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
                 node_norm_failures_file_path: str,
                 source_edges_file_path: str,
                 edges_output_file_path: str,
                 edge_norm_predicate_map_file_path: str,
                 has_sequence_variants: bool = False,
                 strict_normalization: bool = True):
        self.source_nodes_file_path = source_nodes_file_path
        self.nodes_output_file_path = nodes_output_file_path
        self.node_norm_failures_file_path = node_norm_failures_file_path
        self.source_edges_file_path = source_edges_file_path
        self.edges_output_file_path = edges_output_file_path
        self.edge_norm_predicate_map_file_path = edge_norm_predicate_map_file_path
        self.has_sequence_variants = has_sequence_variants
        self.normalization_metadata = {'strict_normalization': strict_normalization}

        # instances of the normalization service wrappers
        # strict normalization flag tells normalizer to throw away any nodes that don't normalize
        self.node_normalizer = NodeNormUtils(strict_normalization=strict_normalization)
        self.edge_normalizer = EdgeNormUtils()

    def normalize_kgx_files(self,
                            edge_subject_pre_normalized: bool = False,
                            edge_object_pre_normalized: bool = False):
        self.normalization_metadata = {}
        self.normalize_node_file()
        self.normalize_edge_file(edge_subject_pre_normalized, edge_object_pre_normalized)
        return self.normalization_metadata

    # given file paths to the source data node file and an output file,
    # normalize the nodes and write them to the new file
    # also write a file with the node ids that did not successfully normalize
    def normalize_node_file(self):

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

        # if this source has sequence variants segregate them for the different norm services
        variant_nodes = []
        regular_nodes = []
        if self.has_sequence_variants:
            for node in source_nodes:
                if SEQUENCE_VARIANT in node['category']:
                    variant_nodes.append(node)
                else:
                    regular_nodes.append(node)
        else:
            # otherwise assume all the source nodes are regular nodes and continue
            regular_nodes = source_nodes

        # send the regular nodes through the normalizer - they will be normalized in place
        # the number of nodes may change if strict normalization is on
        # because nodes that fail to normalize are removed from the list
        regular_node_count_pre_norm = len(regular_nodes)
        if regular_nodes:
            node_norm_version = NodeNormUtils.get_current_node_norm_version()
            self.logger.debug(f'Normalizing {regular_node_count_pre_norm} regular nodes...')
            self.node_normalizer.normalize_node_data(regular_nodes)
        else:
            node_norm_version = "N/A"
            self.logger.debug(f'No regular nodes found for normalization...')
        regular_node_count_post_norm = len(regular_nodes)

        variant_node_count_pre_norm = len(variant_nodes)
        if self.has_sequence_variants:
            self.logger.debug(f'Normalizing {variant_node_count_pre_norm} sequence variant nodes...')
            self.node_normalizer.normalize_sequence_variants(variant_nodes)
        variant_node_count_post_norm = len(variant_nodes)

        # grab the list of node IDs that failed
        regular_node_norm_failures = self.node_normalizer.failed_to_normalize_ids
        variant_node_norm_failures = self.node_normalizer.failed_to_normalize_variant_ids

        # update the metadata
        self.normalization_metadata.update({
            'regular_node_norm_version': node_norm_version,
            'regular_nodes_pre_norm': regular_node_count_pre_norm,
            'regular_node_norm_failures': len(regular_node_norm_failures),
            'regular_nodes_post_norm': regular_node_count_post_norm
        })
        if self.has_sequence_variants:
            # variant_node_splits is a dictionary of key: variant node ID, value: list of normalized variant node IDs
            # these occur when one variant ID is ambiguous and could normalize to multiple different IDs
            variant_node_splits = self.node_normalizer.variant_node_splits
            # this takes the sum of the lengths of the values and subtracts the number of keys
            # resulting in the number of variants that were added after splitting the original IDs
            # if this makes absolutely no sense email Evan Morris and he will explain
            variant_split_count = len([variant for sublist in variant_node_splits.values() for variant in sublist]) \
                                  - len(variant_node_splits.keys())
            self.normalization_metadata.update({
                'variant_nodes_pre_norm': variant_node_count_pre_norm,
                'variant_node_norm_failures': len(variant_node_norm_failures),
                # variant nodes could split during normalization - this keeps a record of those
                'variant_nodes_split': self.node_normalizer.variant_node_splits,
                # this count represents the number of added nodes due to splits
                # ie source_variant_node_count - variant_node_norm_failures + variant_nodes_split_count
                # should equal variant_nodes_post_norm
                'variant_nodes_split_count': variant_split_count,
                'variant_nodes_post_norm': variant_node_count_post_norm
            })

        # write all of the normalized nodes
        with KGXFileWriter(nodes_output_file_path=self.nodes_output_file_path) as output_file_writer:
            if regular_nodes:
                self.logger.debug(f'Writing regular nodes to file...')
                output_file_writer.write_normalized_nodes(regular_nodes)
            if variant_nodes:
                self.logger.debug(f'Writing sequence variant nodes to file...')
                output_file_writer.write_normalized_nodes(variant_nodes)

            # grab the number of repeat writes from the file writer
            # assuming the input file contained all unique node IDs,
            # this is the number of nodes that started with different IDs but normalized to the same ID as another node
            merged_node_count = output_file_writer.repeat_node_count

        self.normalization_metadata.update({
            'all_nodes_post_norm': regular_node_count_post_norm + variant_node_count_post_norm,
            'merged_nodes_post_norm': merged_node_count,
            'final_normalized_nodes': regular_node_count_post_norm + variant_node_count_post_norm - merged_node_count
        })

        if regular_node_norm_failures or variant_node_norm_failures:
            self.logger.debug(f'Writing normalization failures to file...')
            with open(self.node_norm_failures_file_path, "w") as failed_norm_file:
                for failed_node_id in regular_node_norm_failures:
                    failed_norm_file.write(f'{failed_node_id}\n')
                for failed_node_id in variant_node_norm_failures:
                    failed_norm_file.write(f'{failed_node_id}\n')

        # self.logger.debug(json.dumps(self.normalization_metadata, indent=4))

    # given file paths to the source data edge file and an output file,
    # normalize the predicates/relations and write them to the new file
    # also write a file with the predicates that did not successfully normalize
    def normalize_edge_file(self,
                            edge_subject_pre_normalized: bool = False,
                            edge_object_pre_normalized: bool = False):

        # we organize the edges into a dictionary of dictionaries of dictionaries, no really.
        # to merge edges with the same subject object and predicate
        # merged_edges[subject_id][object_id][predicate] = edge
        merged_edges = defaultdict(lambda: defaultdict(dict))

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
        # TODO ensure that this is the current biolink version or an equivalent
        current_edge_norm_version = EdgeNormUtils.get_current_edge_norm_version()

        self.logger.debug(f'Normalizing predicates from {self.source_edges_file_path}...')
        edge_norm_failures = self.edge_normalizer.normalize_edge_data(source_edges)
        if edge_norm_failures:
            # this should rarely ever happen with the current edge norm design
            self.logger.error(f'Edge normalization service failed to return results for {edge_norm_failures}')
        self.logger.debug(f'Predicate normalization complete. Normalizing all the edges..')

        normalized_edge_count = 0
        edge_mergers = 0
        edge_splits = 0
        edges_failed_due_to_nodes = 0

        node_norm_lookup = self.node_normalizer.node_normalization_lookup
        edge_norm_lookup = self.edge_normalizer.edge_normalization_lookup
        for predicate in edge_norm_failures:
            edge_norm_lookup[predicate] = 'biolink:related_to'

        for edge in source_edges:
            try:
                if edge_subject_pre_normalized:
                    normalized_subject_ids = [edge['subject']]
                else:
                    normalized_subject_ids = node_norm_lookup[edge['subject']]
                if edge_object_pre_normalized:
                    normalized_object_ids = [edge['object']]
                else:
                    normalized_object_ids = node_norm_lookup[edge['object']]
            except KeyError as e:
                raise NormalizationBrokenError(error_message="One of the node IDs from the edge file was "
                                                             "missing from the normalizer look up, "
                                                             "it's probably not in the node file.",
                                               actual_error=f'KeyError: {e}')
            if not (normalized_subject_ids and normalized_object_ids):
                edges_failed_due_to_nodes += 1
            else:
                normalized_predicate = edge_norm_lookup[edge['relation']]
                if not normalized_predicate:
                    # this should really never happen
                    raise NormalizationBrokenError(error_message=f"Missing predicate lookup for {edge['relation']}")
                else:
                    edge_count = 0
                    for norm_subject_id in normalized_subject_ids:
                        for norm_object_id in normalized_object_ids:
                            edge_count += 1
                            # create a new edge with the normalized values
                            # start with the original edge to preserve other properties
                            normalized_edge = edge.copy()
                            normalized_edge['subject'] = norm_subject_id
                            normalized_edge['object'] = norm_object_id
                            normalized_edge['predicate'] = normalized_predicate

                            # merge with existing similar edges and/or queue up for writing later
                            if ((norm_subject_id in merged_edges) and
                                    (norm_object_id in merged_edges[norm_subject_id]) and
                                    (normalized_predicate in merged_edges[norm_subject_id][norm_object_id])):
                                previous_edge = merged_edges[norm_subject_id][norm_object_id][normalized_predicate]
                                edge_mergers += 1
                                for key, value in normalized_edge.items():
                                    # TODO - make sure this is the behavior we want -
                                    # for properties that are lists append the values
                                    # otherwise overwrite them
                                    if key in previous_edge and isinstance(value, list):
                                        previous_edge[key].extend(value)
                                    else:
                                        previous_edge[key] = value
                            else:
                                merged_edges[norm_subject_id][norm_object_id][normalized_predicate] = normalized_edge
                    if edge_count > 1:
                        edge_splits += edge_count - 1

        try:
            self.logger.debug(f'Writing normalized edges to file...')
            with KGXFileWriter(edges_output_file_path=self.edges_output_file_path) as output_file_writer:
                for subject_id, object_dict in merged_edges.items():
                        for object_id, predicate_dict in object_dict.items():
                            for predicate, edge in predicate_dict.items():
                                normalized_edge_count += 1
                                output_file_writer.write_edge(subject_id=subject_id,
                                                              object_id=object_id,
                                                              relation=edge['relation'],
                                                              predicate=predicate,
                                                              edge_properties=edge)
        except IOError as e:
            norm_error_msg = f'Error writing edges file {self.edges_output_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

        try:
            self.logger.debug(f'Writing predicate map to file...')
            with open(self.edge_norm_predicate_map_file_path, "w") as predicate_map_file:
                json.dump(edge_norm_lookup, predicate_map_file)
        except IOError as e:
            norm_error_msg = f'Error writing edge predicate map file {self.edge_norm_predicate_map_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

        self.normalization_metadata.update({
            'edge_norm_version': current_edge_norm_version,
            'source_edges': len(source_edges),
            'edges_failed_due_to_nodes': edges_failed_due_to_nodes,
            # these keep track of how many edges merged into another, or split into multiple edges
            # this should be true: source_edges - failures - mergers + splits = edges post norm
            'edge_mergers': edge_mergers,
            'edge_splits': edge_splits,
            'final_normalized_edges': normalized_edge_count
        })

        #self.logger.debug(json.dumps(self.normalization_metadata, indent=4))


