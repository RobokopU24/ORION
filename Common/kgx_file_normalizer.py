import os
import json
from collections import defaultdict

import Common.node_types as node_types
from Common.utils import LoggingUtil
from Common.utils import NodeNormUtils, EdgeNormUtils
from Common.kgx_file_writer import KGXFileWriter
from robokop_genetics.genetics_normalization import GeneticsNormalizer


class NormalizationBrokenError(Exception):
    def __init__(self, error_message: str, actual_error: str):
        self.error_message = error_message
        self.actual_error = actual_error


class NormalizationFailedError(Exception):
    def __init__(self, error_message: str, actual_error: str):
        self.error_message = error_message
        self.actual_error = actual_error

# This piece takes a KGX-like file and normalizes all of the node IDs and predicates.
# It then writes all of the normalized nodes and edges to new files.
#
# IMPORTANT: As of now, you must normalize a corresponding node file for the edges to work.
#
class KGXFileNormalizer:

    logger = LoggingUtil.init_logging("Data_services.Common.KGXFileNormalizer",
                                      line_format='medium',
                                      log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def __init__(self):
        # these dicts will store the results from the normalization services, they are important for future lookups
        self.cached_node_norms = {}
        self.cached_edge_norms = {}
        self.cached_sequence_variant_norms = {}

        # instances of the normalization service wrappers - lazy instantiation for sequence variants
        self.node_normalizer = NodeNormUtils()
        self.edge_normalizer = EdgeNormUtils()
        self.sequence_variant_normalizer = None

    # given file paths to the source data node file and an output file,
    # normalize the nodes and write them to the new file
    # also write a file with the node ids that did not successfully normalize
    def normalize_node_file(self,
                            source_nodes_file_path: str,
                            nodes_output_file_path: str,
                            norm_failures_output_file_path: str,
                            has_sequence_variants: bool = False):

        try:
            self.logger.debug(f'Normalizing Node File {source_nodes_file_path}...')
            regular_node_failures = []
            variant_node_failures = []
            variant_ids = []

            with open(source_nodes_file_path) as source_json, KGXFileWriter(nodes_output_file_path=nodes_output_file_path) as nodes_file_writer:
                self.logger.debug(f'Parsing Node File {source_nodes_file_path}...')
                try:
                    source_nodes = json.load(source_json)["nodes"]
                except json.JSONDecodeError as e:
                    norm_error_msg = f'Error decoding json from {source_nodes_file_path} on line number {e.lineno}'
                    raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

                # if this source has sequence variants we need to filter them to send them to the different norm services
                if has_sequence_variants:
                    regular_nodes = [node for node in source_nodes if node_types.SEQUENCE_VARIANT not in node['category']]
                else:
                    # if it doesn't we'll normalize all of them with the regular node norm service
                    regular_nodes = source_nodes

                self.logger.debug(f'Normalizing regular nodes...')
                regular_node_failures = self.node_normalizer.normalize_node_data(regular_nodes,
                                                                                 cached_node_norms=self.cached_node_norms,
                                                                                 for_json=True)

                self.logger.debug(f'Regular node norm complete.. Writing regular nodes to file...')
                for regular_node in [node for node in regular_nodes if node['id'] not in regular_node_failures]:
                    nodes_file_writer.write_normalized_node(regular_node)

                if has_sequence_variants:
                    variant_ids = [node["id"] for node in source_nodes if node not in regular_nodes]
                    if variant_ids:
                        if not self.sequence_variant_normalizer:
                            self.sequence_variant_normalizer = GeneticsNormalizer(use_cache=False)
                        self.logger.debug(f'Found {len(variant_ids)} sequence variant nodes to normalize. Normalizing...')
                        self.cached_sequence_variant_norms = self.sequence_variant_normalizer.normalize_variants(variant_ids)
                        variant_node_types = self.sequence_variant_normalizer.get_sequence_variant_node_types()
                        for variant_id in variant_ids:
                            if variant_id in self.cached_sequence_variant_norms:
                                # Sequence variants can sometimes normalize to multiple nodes (split from one to many)
                                # We take all of them and write them
                                for normalized_info in self.cached_sequence_variant_norms[variant_id]:
                                    nodes_file_writer.write_node(normalized_info["id"],
                                                                 normalized_info["name"],
                                                                 variant_node_types,
                                                                 {"equivalent_identifiers":
                                                                  normalized_info["equivalent_identifiers"]})
                            else:
                                variant_node_failures.append(variant_id)
                        variant_node_fail_count = len(variant_node_failures)
                        self.logger.debug(
                            f'Variant node norm complete - {variant_node_fail_count} nodes failed to normalize...')
                        self.logger.debug(f'Writing sequence variant nodes to file...')

            if regular_node_failures or variant_node_failures:
                self.logger.debug(f'Writing normalization failures to file...')
                with open(norm_failures_output_file_path, "w") as failed_norm_file:
                    for failed_node_id in regular_node_failures:
                        failed_norm_file.write(f'{failed_node_id}\n')
                    for failed_node_id in variant_node_failures:
                        failed_norm_file.write(f'{failed_node_id}\n')

            normalization_metadata = {
                'regular_nodes_normalized': len(source_nodes) - len(regular_node_failures),
                'regular_nodes_failed': len(regular_node_failures),
                'variant_nodes_normalized': len(variant_ids) - len(variant_node_failures),
                'variant_nodes_failed': len(variant_node_failures)
            }

            self.logger.info(json.dumps(normalization_metadata, indent=4))

            return normalization_metadata

        except IOError as e:
            norm_error_msg = f'Error reading nodes file {source_nodes_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e)

    # given file paths to the source data edge file and an output file,
    # normalize the predicates/relations and write them to the new file
    # also write a file with the predicates that did not successfully normalize
    def normalize_edge_file(self,
                            source_edges_file_path: str,
                            edges_output_file_path: str,
                            edge_failures_output_file_path: str,
                            has_sequence_variants: bool = False):
        cached_node_norms = self.cached_node_norms
        cached_edge_norms = self.cached_edge_norms
        cached_sequence_variant_norms = self.cached_sequence_variant_norms

        merged_edges = defaultdict(lambda: defaultdict(dict))

        try:
            self.logger.debug(f'Normalizing edge file {source_edges_file_path}...')
            edge_norm_failures = []
            with open(source_edges_file_path) as source_json, KGXFileWriter(edges_output_file_path=edges_output_file_path) as edges_file_writer:
                self.logger.debug(f'Parsing edge file {source_edges_file_path}...')
                try:
                    source_edges = json.load(source_json)["edges"]
                except json.JSONDecodeError as e:
                    norm_error_msg = f'Error decoding json from {source_edges_file_path} on line number {e.lineno}'
                    raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

                self.logger.debug(f'Parsed edges from {source_edges_file_path}...')

                unique_predicates = set([edge['relation'] for edge in source_edges])
                unique_predicates_count = len(unique_predicates)
                self.logger.debug(f'Normalizing {unique_predicates_count} predicates from {source_edges_file_path}...')
                edge_norm_failures = self.edge_normalizer.normalize_edge_data(source_edges,
                                                                              cached_edge_norms)
                self.logger.debug(f'Edge normalization complete {len(edge_norm_failures)} predicates failed..')
                self.logger.debug(f'Writing normalized edges..')

                normalized_edge_count = 0
                edges_failed_due_to_nodes = 0
                edges_failed_due_to_relation = 0
                if not has_sequence_variants:
                    for edge in source_edges:
                        # Look up the normalization results from before for each edge
                        subject_id = None
                        object_id = None

                        if edge['subject'] in cached_node_norms and cached_node_norms[edge['subject']] is not None:
                            subject_id = cached_node_norms[edge['subject']]['id']['identifier']

                        if edge['object'] in cached_node_norms and cached_node_norms[edge['object']] is not None:
                            object_id = cached_node_norms[edge['object']]['id']['identifier']

                        if subject_id and object_id:
                            if edge['relation'] in cached_edge_norms and cached_edge_norms[edge['relation']] is not None:
                                # write them to a file
                                edges_file_writer.write_edge(subject_id=subject_id,
                                                             object_id=object_id,
                                                             relation=edge['relation'],
                                                             predicate=cached_edge_norms[edge['relation']]['identifier'],
                                                             edge_properties=edge)
                            if edge['relation'] in cached_edge_norms:
                                predicate = cached_edge_norms[edge['relation']]['identifier']

                                # merge with existing similar edges and/or queue up for writing later
                                if ((subject_id in merged_edges) and
                                    (object_id in merged_edges[subject_id]) and
                                        (predicate in merged_edges[subject_id][object_id])):
                                    previous_edge = merged_edges[subject_id][object_id][predicate]
                                    for key, value in edge.items():
                                        if key in previous_edge and isinstance(value, list):
                                            previous_edge[key].extend(value)
                                        else:
                                            previous_edge[key] = value
                                else:
                                    merged_edges[subject_id][object_id][predicate] = edge

                                normalized_edge_count += 1
                            else:
                                edges_failed_due_to_relation += 1
                        else:
                            edges_failed_due_to_nodes += 1

                else:
                    for edge in source_edges:
                        # Look up the normalization results from before for each edge.
                        # Nodes that aren't found in the regular nodes cache, see if they're sequence variants.
                        # Sequence variants can branch after normalization,
                        # so the normalized subjects and objects need to be lists
                        subject_ids = []
                        object_ids = []

                        if edge['subject'] in cached_node_norms:
                            if cached_node_norms[edge['subject']]:
                                subject_ids = [cached_node_norms[edge['subject']]['id']['identifier']]
                        elif edge['subject'] in cached_sequence_variant_norms:
                            subject_ids = [variant['id'] for variant in cached_sequence_variant_norms[edge['subject']]]

                        if edge['object'] in cached_node_norms:
                            if cached_node_norms[edge['object']]:
                                object_ids = [cached_node_norms[edge['object']]['id']['identifier']]
                        elif edge['object'] in cached_sequence_variant_norms:
                            object_ids = [variant['id'] for variant in cached_sequence_variant_norms[edge['object']]]

                        if subject_ids and object_ids:
                            if edge['relation'] in cached_edge_norms:
                                predicate = cached_edge_norms[edge['relation']]['identifier']
                                for subject_id in subject_ids:
                                    for object_id in object_ids:

                                        # merge with existing similar edges and/or queue up for writing later
                                        if ((subject_id in merged_edges) and
                                                (object_id in merged_edges[subject_id]) and
                                                (predicate in merged_edges[subject_id][object_id])):

                                            previous_edge = merged_edges[subject_id][object_id][predicate]
                                            for key, value in edge.items():
                                                if key in previous_edge and isinstance(value, list):
                                                    previous_edge[key].extend(value)
                                                else:
                                                    previous_edge[key] = value

                                        else:
                                            merged_edges[subject_id][object_id][predicate] = edge

                                        normalized_edge_count += 1
                            else:
                                edges_failed_due_to_relation += 1
                        else:
                            edges_failed_due_to_nodes += 1

                for subject_id, object_dict in merged_edges.items():
                    for object_id, predicate_dict in object_dict.items():
                        for predicate, edge in predicate_dict.items():
                            edges_file_writer.write_edge(subject_id=subject_id,
                                                         object_id=object_id,
                                                         relation=edge['relation'],
                                                         predicate=predicate,
                                                         edge_properties=edge)

            if edge_norm_failures:
                with open(edge_failures_output_file_path, "w") as failed_edge_file:
                    for failed_edge_id in edge_norm_failures:
                        failed_edge_file.write(f'{failed_edge_id}\n')

            normalization_metadata = {
                'num_source_edges': len(source_edges),
                'num_normalized_edges': normalized_edge_count,
                'edges_failed_due_to_nodes': edges_failed_due_to_nodes,
                'edges_failed_due_to_relation': edges_failed_due_to_relation,
                'unique_predicates': list(unique_predicates),
                'unique_predicates_count': unique_predicates_count,
                'predicates_failed': len(edge_norm_failures),
            }

            self.logger.info(json.dumps(normalization_metadata, indent=4))

            return normalization_metadata

        except IOError as e:
            norm_error_msg = f'Error reading edges file {source_edges_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

