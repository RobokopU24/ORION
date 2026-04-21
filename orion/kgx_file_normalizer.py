import os
import json
import jsonlines
from collections import defaultdict

from orion.biolink_constants import (SEQUENCE_VARIANT, RETRIEVAL_SOURCES, PRIMARY_KNOWLEDGE_SOURCE, OBJECT_ID,
                                     SUBJECT_ID, PREDICATE, SUBCLASS_OF, ORIGINAL_OBJECT, ORIGINAL_SUBJECT)
from orion.normalization import NormalizationScheme, NodeNormalizer, EdgeNormalizer, EdgeNormalizationResult, \
    NormalizationFailedError
from orion.utils import chunk_iterator
from orion.logging import get_orion_logger
from orion.kgx_file_writer import KGXFileWriter


NODE_NORMALIZATION_BATCH_SIZE = 1_000_000
EDGE_NORMALIZATION_BATCH_SIZE = 1_000_000


logger = get_orion_logger("orion.kgx_file_normalizer")

# KGXFileNormalizer takes KGX jsonl files, normalizes nodes using Babel's Node Normalizer and converts edge
# predicates to biolink compliant curies according to biolink model predicate mappings, outputting normalized KGX files.
class KGXFileNormalizer:

    def __init__(self,
                 source_nodes_file_path: str,
                 nodes_output_file_path: str,
                 node_norm_map_file_path: str,
                 node_norm_failures_file_path: str,
                 source_edges_file_path: str,
                 edges_output_file_path: str,
                 edge_norm_predicate_map_file_path: str,
                 normalization_scheme: NormalizationScheme = None,
                 edge_subject_pre_normalized: bool = False,
                 edge_object_pre_normalized: bool = False,
                 has_sequence_variants: bool = False,
                 sequence_variants_pre_normalized: bool = False,
                 predicates_pre_normalized: bool = False,
                 default_provenance: str = None,
                 process_in_memory: bool = True,
                 preserve_unconnected_nodes: bool = False):
        self.normalization_scheme = normalization_scheme if normalization_scheme is not None else NormalizationScheme()
        self.source_nodes_file_path = source_nodes_file_path
        self.nodes_output_file_path = nodes_output_file_path
        self.node_norm_map_file_path = node_norm_map_file_path
        self.node_norm_failures_file_path = node_norm_failures_file_path
        self.source_edges_file_path = source_edges_file_path
        self.edges_output_file_path = edges_output_file_path
        self.edge_norm_predicate_map_file_path = edge_norm_predicate_map_file_path
        # in some cases we start with normalized nodes on one end of the edge,
        # these flags indicate we should skip normalizing those IDs
        # this is important because those IDs could be missing from the supplied nodes file
        self.edge_subject_pre_normalized = edge_subject_pre_normalized
        self.edge_object_pre_normalized = edge_object_pre_normalized
        self.predicates_pre_normalized = predicates_pre_normalized
        self.sequence_variants_pre_normalized = sequence_variants_pre_normalized
        self.has_sequence_variants = has_sequence_variants
        self.process_in_memory = process_in_memory
        self.preserve_unconnected_nodes = preserve_unconnected_nodes
        self.default_provenance = default_provenance
        self.normalization_metadata = self.normalization_scheme.get_metadata_representation()

        # instances of the normalization service wrappers
        # strict normalization flag tells normalizer to throw away any nodes that don't normalize
        try:
            self.node_normalizer = NodeNormalizer(node_normalization_version=normalization_scheme.node_normalization_version,
                                                  strict_normalization=normalization_scheme.strict,
                                                  conflate_node_types=normalization_scheme.conflation,
                                                  biolink_version=normalization_scheme.edge_normalization_version)
            self.edge_normalizer = EdgeNormalizer(edge_normalization_version=normalization_scheme.edge_normalization_version)
        except Exception as e:
            raise NormalizationFailedError(error_message=repr(e), actual_error=e)

    def normalize_kgx_files(self):
        self.normalize_node_file()
        self.normalize_edge_file()
        if not self.preserve_unconnected_nodes:
            unconnected_nodes_removed = remove_unconnected_nodes(self.nodes_output_file_path, self.edges_output_file_path)
            self.normalization_metadata['unconnected_nodes_removed'] = unconnected_nodes_removed
            self.normalization_metadata['final_normalized_nodes'] -= unconnected_nodes_removed
        else:
            self.normalization_metadata['unconnected_nodes_removed'] = 0
        return self.normalization_metadata

    # given file paths to the source data node file and an output file,
    # normalize the nodes and write them to the new file
    # also write a file with the node ids that did not successfully normalize
    def normalize_node_file(self):

        regular_nodes_pre_norm = 0
        regular_nodes_post_norm = 0
        variant_nodes_pre_norm = 0
        variant_nodes_split_count = 0
        variant_nodes_post_norm = 0

        logger.info(f'Normalizing nodes and writing to file...')
        try:
            with jsonlines.open(self.source_nodes_file_path) as source_json_reader,\
                    KGXFileWriter(nodes_output_file_path=self.nodes_output_file_path) as output_file_writer:

                # iterate through the source file
                for nodes_subset in chunk_iterator(source_json_reader, NODE_NORMALIZATION_BATCH_SIZE):

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
                        logger.debug(f'Normalizing {len(regular_nodes)} regular nodes...')
                        try:
                            self.node_normalizer.normalize_node_data(regular_nodes)
                        except Exception as e:
                            raise NormalizationFailedError(error_message='Error during node normalization.',
                                                           actual_error=e)
                    regular_nodes_post_norm += len(regular_nodes)
                    if regular_nodes:
                        logger.info(f'Normalized {regular_nodes_pre_norm} nodes so far...')

                    variant_nodes_pre_norm += len(variant_nodes)
                    if self.has_sequence_variants:
                        if not self.sequence_variants_pre_normalized:
                            logger.debug(f'Normalizing {len(variant_nodes)} sequence variant nodes...')
                            self.node_normalizer.normalize_sequence_variants(variant_nodes)
                        else:
                            # skip normalizing variants but still
                            # populate the lookup map for edge normalization
                            for node in variant_nodes:
                                self.node_normalizer.node_normalization_lookup[node['id']] = [node['id']]

                        # variant_node_splits is a dictionary of key: variant node ID, value: list of normalized variant node IDs
                        # these occur when one variant ID is ambiguous and could normalize to multiple different IDs
                        variant_node_splits = self.node_normalizer.variant_node_splits
                        # this takes the sum of the lengths of the values and subtracts the number of keys
                        # resulting in the number of variants that were added after splitting the original IDs
                        # if this makes absolutely no sense email Evan Morris and he will explain
                        variant_split_count = len([variant for sublist in variant_node_splits.values()
                                                   for variant in sublist]) - len(variant_node_splits.keys())
                        variant_nodes_split_count += variant_split_count
                    else:
                        variant_nodes_split_count = 0
                    variant_nodes_post_norm += len(variant_nodes)
                    if variant_nodes:
                        logger.info(f'Normalized {variant_nodes_pre_norm} variant nodes so far...')

                    if regular_nodes:
                        logger.debug(f'Writing nodes to file...')
                        output_file_writer.write_normalized_nodes(regular_nodes)
                    if variant_nodes:
                        logger.debug(f'Writing sequence variant nodes to file...')
                        output_file_writer.write_normalized_nodes(variant_nodes)

                # grab the number of repeat writes from the file writer
                # assuming the input file contained all unique node IDs,
                # this is the number of nodes that started with different IDs but normalized to the same ID as another node
                discarded_duplicate_node_count = output_file_writer.repeat_node_count
        except IOError as e:
            norm_error_msg = f'Error reading nodes file {self.source_nodes_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e)
        except jsonlines.InvalidLineError as e:
            norm_error_msg = f'Error decoding json from {self.source_nodes_file_path} on line number {e.lineno}: ' \
                             f'{e.line}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e)

        logger.debug(f'Writing normalization map to file...')
        normalization_map_info = {'normalization_map': self.node_normalizer.node_normalization_lookup}
        with open(self.node_norm_map_file_path, "w") as node_norm_map_file:
            json.dump(normalization_map_info, node_norm_map_file, indent=4)

        # grab the list of node IDs that failed
        regular_node_norm_failures = self.node_normalizer.failed_to_normalize_ids
        variant_node_norm_failures = self.node_normalizer.failed_to_normalize_variant_ids
        if regular_node_norm_failures or variant_node_norm_failures:
            logger.debug(f'Writing normalization failures to file...')
            with open(self.node_norm_failures_file_path, "w") as failed_norm_file:
                for failed_node_id in regular_node_norm_failures:
                    failed_norm_file.write(f'{failed_node_id}\n')
                for failed_node_id, error_message in variant_node_norm_failures.items():
                    failed_norm_file.write(f'{failed_node_id}\t{error_message}\n')

        # compute per-prefix normalization stats
        prefix_stats = self.compute_by_prefix_stats(
            node_norm_lookup=self.node_normalizer.node_normalization_lookup
        )

        # update the metadata
        self.normalization_metadata.update({
            'node_count_pre_normalization': regular_nodes_pre_norm,
            'node_count_post_normalization': regular_nodes_post_norm,
            'node_normalization_failures': len(regular_node_norm_failures),
            'normalization_by_prefix': prefix_stats,
        })
        if self.has_sequence_variants:
            self.normalization_metadata.update({
                'variant_nodes_pre_norm': variant_nodes_pre_norm,
                'variant_node_norm_failures': len(variant_node_norm_failures),
                'variant_nodes_split_count': variant_nodes_split_count,
                'variant_nodes_post_norm': variant_nodes_post_norm,
            })
        self.normalization_metadata.update({
            'discarded_duplicate_node_count': discarded_duplicate_node_count,
            'final_normalized_nodes': regular_nodes_post_norm + variant_nodes_post_norm - discarded_duplicate_node_count
        })

    # given file paths to the source data edge file and an output file,
    # normalize the predicates and write them to the new file
    # also write a file with the predicates that did not successfully normalize
    def normalize_edge_file(self):

        number_of_source_edges = 0
        normalized_edge_count = 0
        edge_splits = 0
        edges_failed_due_to_nodes = 0
        subclass_loops_removed = 0

        node_norm_lookup = self.node_normalizer.node_normalization_lookup
        edge_norm_lookup = self.edge_normalizer.edge_normalization_lookup
        edge_norm_failures = set()

        try:
            with jsonlines.open(self.source_edges_file_path) as source_json_reader, \
                    jsonlines.open(self.edges_output_file_path, 'w') as edges_out:

                for edges_subset in chunk_iterator(source_json_reader, EDGE_NORMALIZATION_BATCH_SIZE):

                    number_of_source_edges += len(edges_subset)

                    if not self.predicates_pre_normalized:
                        current_edge_norm_failures = self.edge_normalizer.normalize_edge_data(edges_subset)
                        if current_edge_norm_failures:
                            edge_norm_failures.update(current_edge_norm_failures)
                            logger.error(
                                f'Edge normalization service failed to return results for {edge_norm_failures}')

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
                            logger.error(f"One of the node IDs from the edge file was missing from the normalizer look up, "
                                              f"it's probably not in the node file. ({e})")
                        if not (normalized_subject_ids and normalized_object_ids):
                            edges_failed_due_to_nodes += 1
                        else:
                            if not self.predicates_pre_normalized:
                                try:
                                    edge_norm_result: EdgeNormalizationResult = edge_norm_lookup[edge[PREDICATE]]
                                    # extract the normalization info
                                    normalized_predicate = edge_norm_result.predicate
                                    edge_inverted_by_normalization = edge_norm_result.inverted
                                    normalized_edge_properties = edge_norm_result.properties
                                except KeyError as e:
                                    norm_error_msg = f'Edge norm lookup failure - missing {edge[PREDICATE]}!'
                                    logger.error(norm_error_msg)
                                    raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e)
                            else:
                                normalized_predicate = edge[PREDICATE]
                                edge_inverted_by_normalization = False
                                normalized_edge_properties = None

                            # a counter for the number of normalized edges coming from a single source edge
                            # it's only used to determine how many edge splits occurred
                            edge_count = 0

                            # ensure edge has a primary knowledge source
                            if RETRIEVAL_SOURCES not in edge and PRIMARY_KNOWLEDGE_SOURCE not in edge:
                                edge[PRIMARY_KNOWLEDGE_SOURCE] = self.default_provenance

                            for norm_subject_id in normalized_subject_ids:
                                for norm_object_id in normalized_object_ids:

                                    # if it's a subclass_of edge, and it's a self-loop, throw it out
                                    if normalized_predicate == SUBCLASS_OF and norm_subject_id == norm_object_id:
                                        subclass_loops_removed += 1
                                        continue

                                    edge_count += 1

                                    # create a new edge with the normalized values
                                    # start with the original edge to preserve other properties
                                    normalized_edge = edge.copy()

                                    # Keep the original subject and object IDs
                                    normalized_edge[ORIGINAL_SUBJECT] = normalized_edge[SUBJECT_ID]
                                    normalized_edge[ORIGINAL_OBJECT] = normalized_edge[OBJECT_ID]

                                    normalized_edge[PREDICATE] = normalized_predicate

                                    if normalized_edge_properties:
                                        normalized_edge.update(normalized_edge_properties)

                                    normalized_edge[SUBJECT_ID] = norm_subject_id
                                    normalized_edge[OBJECT_ID] = norm_object_id

                                    # if normalization switched the direction of the predicate,
                                    # invert the entire edge
                                    if edge_inverted_by_normalization:
                                        normalized_edge = invert_edge(normalized_edge)

                                    edges_out.write(normalized_edge)
                                    normalized_edge_count += 1

                            # this counter tracks the number of new edges created from each individual edge in the
                            # original file this could happen due to rare cases of normalization splits where one
                            # node normalizes to many
                            if edge_count > 1:
                                edge_splits += edge_count - 1

                    logger.info(f'Processed {number_of_source_edges} edges so far...')

        except OSError as e:
            norm_error_msg = f'Error normalizing edges file {self.source_edges_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e)

        try:
            logger.debug(f'Writing predicate map to file...')
            edge_norm_json = {}
            for original_predicate, edge_normalization in edge_norm_lookup.items():
                edge_norm_json[original_predicate] = edge_normalization.__dict__
            predicate_map_info = {'predicate_map': edge_norm_json,
                                  'predicate_norm_failures': list(edge_norm_failures)}
            with open(self.edge_norm_predicate_map_file_path, "w") as predicate_map_file:
                json.dump(predicate_map_info, predicate_map_file, sort_keys=True, indent=4)
        except OSError as e:
            norm_error_msg = f'Error writing edge predicate map file {self.edge_norm_predicate_map_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e)

        self.normalization_metadata.update({
            'biolink_version': self.edge_normalizer.edge_norm_version,
            'source_edges': number_of_source_edges,
            'edges_failed_due_to_nodes': edges_failed_due_to_nodes,
            # these keep track of how many edges merged into another, or split into multiple edges
            # this should be true: source_edges - failures - mergers + splits = edges post norm
            'edge_splits': edge_splits,
            'subclass_loops_removed': subclass_loops_removed,
            'final_normalized_edges': normalized_edge_count
        })

    @staticmethod
    def compute_by_prefix_stats(node_norm_lookup: dict):
        # count totals, failures, and post-normalization prefixes per original prefix
        prefix_total = defaultdict(int)
        prefix_failed = defaultdict(int)
        prefix_normalized_to = defaultdict(lambda: defaultdict(int))
        for node_id, normalized_ids in node_norm_lookup.items():
            original_prefix = node_id.split(':')[0]
            prefix_total[original_prefix] += 1
            if normalized_ids:
                for normalized_id in normalized_ids:
                    prefix_normalized_to[original_prefix][normalized_id.split(':')[0]] += 1
            else:
                prefix_failed[original_prefix] += 1

        prefix_stats = {}
        # sort the prefixes by their total count, the most common prefix is at the top of the list
        for prefix in sorted(prefix_total, key=prefix_total.get, reverse=True):
            failed = prefix_failed.get(prefix, 0)
            total = prefix_total[prefix]
            succeeded = total - failed
            percentage = int(succeeded / total * 10000) / 100  # truncate percentage to two decimal places
            prefix_stats[prefix] = {
                'succeeded': succeeded,
                'failed': failed,
                'total': total,
                'success_rate': percentage,
                # sort normalized_to by curie counts, not alphabetically
                'normalized_to': dict(sorted(prefix_normalized_to.get(prefix, {}).items(),
                                             key=lambda x: x[1], reverse=True)),
            }
            if succeeded == 0:
                logger.error(f'!!! --- ATTENTION: Curie prefix "{prefix}" failed normalization for all nodes! --- !!!')
            elif percentage < 50:
                logger.warning(f'WARNING: Curie prefix "{prefix}" only normalized {percentage}% of nodes!!!')
        return prefix_stats


def invert_edge(edge):
    inverted_edge = {}
    for key, value in edge.items():
        if SUBJECT_ID in key:
            inverted_edge[key.replace(SUBJECT_ID, OBJECT_ID)] = value
        elif OBJECT_ID in key:
            inverted_edge[key.replace(OBJECT_ID, SUBJECT_ID)] = value
        else:
            inverted_edge[key] = value
    return inverted_edge


"""
Given a nodes file and an edges file, remove all of the nodes from the nodes file that aren't attached to edges.
"""
def remove_unconnected_nodes(nodes_file_path: str, edges_file_path: str):
    utilized_nodes = set()
    with open(edges_file_path) as edges_source:
        edge_reader = jsonlines.Reader(edges_source)
        for edge in edge_reader:
            utilized_nodes.add(edge[OBJECT_ID])
            utilized_nodes.add(edge[SUBJECT_ID])

    unconnected_nodes_removed = 0
    temp_nodes_file_name = f'{nodes_file_path}.temp'
    os.rename(nodes_file_path, temp_nodes_file_name)
    with open(temp_nodes_file_name) as nodes_source:
        nodes_reader = jsonlines.Reader(nodes_source)
        with KGXFileWriter(nodes_output_file_path=nodes_file_path) as kgx_file_writer:
            for node in nodes_reader:
                if node['id'] in utilized_nodes:
                    kgx_file_writer.write_normalized_node(node, uniquify=False)
                else:
                    unconnected_nodes_removed += 1
    os.remove(temp_nodes_file_name)
    return unconnected_nodes_removed

