import os
import json
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


class KGXFileNormalizer:

    logger = LoggingUtil.init_logging("Data_services.Common.KGXFileNormalizer",
                                      line_format='medium',
                                      log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def __init__(self):
        self.node_normalizer = NodeNormUtils()
        self.cached_node_norms = {}
        self.edge_normalizer = EdgeNormUtils()
        self.cached_edge_norms = {}

        self.genetics_normalizer = GeneticsNormalizer(use_cache=False)

        self.testing_genetics_mode = True

    def normalize_node_file(self,
                            source_nodes_file_path: str,
                            nodes_output_file_path: str,
                            norm_failures_output_file_path: str):

        try:
            self.logger.info(f'Normalizing Node File {source_nodes_file_path}...')
            regular_node_failures = []
            with open(source_nodes_file_path) as source_json, KGXFileWriter(nodes_output_file_path) as nodes_file_writer:
                self.logger.info(f'Parsing Node File {source_nodes_file_path}...')
                try:
                    source_nodes = json.load(source_json)["nodes"]
                except json.JSONDecodeError as e:
                    norm_error_msg = f'Error decoding json from {source_nodes_file_path} on line number {e.lineno}'
                    raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

                total_nodes_count = len(source_nodes)
                self.logger.info(f'Found {total_nodes_count} nodes in {source_nodes_file_path}...')

                regular_nodes = [node for node in source_nodes if node['category'] != node_types.SEQUENCE_VARIANT]
                regular_nodes_count = len(regular_nodes)
                self.logger.info(f'Found {regular_nodes_count} regular nodes to normalize. Normalizing...')

                regular_node_failures = self.node_normalizer.normalize_node_data(regular_nodes,
                                                                                 cached_node_norms=self.cached_node_norms,
                                                                                 for_json=True)
                regular_nodes_fail_count = len(regular_node_failures)
                self.logger.info(f'Regular node norm complete - {regular_nodes_fail_count} nodes failed to normalize...')

                self.logger.info(f'Writing normalized nodes to file...')
                nodes_file_writer.write_normalized_nodes(regular_nodes)

                sequence_variant_nodes = [node for node in source_nodes if node not in regular_nodes]

                #self.genetics_normalizer.batch_normalize(sequence_variant_nodes)

            if regular_node_failures:
                with open(norm_failures_output_file_path, "w") as failed_norm_file:
                    for failed_node_id in regular_node_failures:
                        failed_norm_file.write(f'{failed_node_id}\n')

            normalization_metadata = {
                'regular_nodes_normalized': total_nodes_count - regular_nodes_fail_count,
                'regular_nodes_failed': regular_nodes_fail_count,
                'variant_nodes_normalized': len(sequence_variant_nodes),
                'variant_nodes_failed': len(sequence_variant_nodes)
            }

            self.logger.info(json.dumps(normalization_metadata, indent=4))

            return normalization_metadata

        except IOError as e:
            norm_error_msg = f'Error reading nodes file {source_nodes_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

    def normalize_edge_file(self,
                            source_edges_file_path: str,
                            edges_output_file_path: str,
                            edge_failures_output_file_path: str):
        cached_node_norms = self.cached_node_norms
        cached_edge_norms = self.cached_edge_norms
        normalization_metadata = {}
        try:
            self.logger.info(f'Normalizing edge file {source_edges_file_path}...')
            edge_norm_failures = []
            with open(source_edges_file_path) as source_json, KGXFileWriter(edges_output_file_path) as edges_file_writer:
                self.logger.info(f'Parsing edge file {source_edges_file_path}...')
                try:
                    source_edges = json.load(source_json)["edges"]
                except json.JSONDecodeError as e:
                    norm_error_msg = f'Error decoding json from {source_edges_file_path} on line number {e.lineno}'
                    raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

                total_edges_count = len(source_edges)
                self.logger.info(f'Parsed {total_edges_count} edges from {source_edges_file_path}...')

                unique_predicates = set([edge['relation'] for edge in source_edges])
                unique_predicates_count = len(unique_predicates)
                self.logger.info(f'Normalizing {unique_predicates_count} predicates from {source_edges_file_path}...')
                edge_norm_failures = self.edge_normalizer.normalize_edge_data(source_edges, cached_edge_norms, deprecated_version=False)
                edge_norm_fail_count = len(edge_norm_failures)

                self.logger.info(f'Edge normalization complete {edge_norm_fail_count} predicates failed..')
                self.logger.info(f'Writing normalized edges..')
                normalized_edges = [edge for edge in source_edges if (edge['relation'] in cached_edge_norms)
                                    and edge['subject'] in cached_node_norms and edge['object'] in cached_node_norms]

                for edge in normalized_edges:
                    # TODO this way of passing extra unspecified edge properties is pretty hacky
                    # we need to decide if all properties should be passed along
                    # or if we should specify a list of expected ones for each data source
                    edges_file_writer.write_edge(subject_id=cached_node_norms[edge['subject']]['id'],
                                                 object_id=cached_node_norms[edge['object']]['id'],
                                                 relation=edge['relation'],
                                                 predicate=edge['predicate'],
                                                 edge_properties=edge)

            if edge_norm_failures:
                with open(edge_failures_output_file_path, "w") as failed_edge_file:
                    for failed_edge_id in edge_norm_failures:
                        failed_edge_file.write(f'{failed_edge_id}\n')

            normalization_metadata = {
                'total_edges': total_edges_count,
                'unique_predicates': list(unique_predicates),
                'unique_predicates_count': unique_predicates_count,
                'predicates_failed': edge_norm_fail_count,
                'edges_failed': total_edges_count - len(normalized_edges)
            }

            self.logger.info(json.dumps(normalization_metadata, indent=4))

            return normalization_metadata

        except IOError as e:
            norm_error_msg = f'Error reading edges file {source_edges_file_path}'
            raise NormalizationFailedError(error_message=norm_error_msg, actual_error=e.msg)

