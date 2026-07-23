import os
import json
import jsonlines

from orion.biolink_constants import NODE_TYPES, SEQUENCE_VARIANT
from orion.logging import get_orion_logger

logger = get_orion_logger("orion.variant_norm_cache")

# the file names written by KGXFileNormalizer, a cache is just a normalization output directory
NORM_NODE_MAP_FILE_NAME = 'norm_node_map.json'
NORMALIZED_NODES_FILE_NAME = 'normalized_nodes.jsonl'

# error message stored for variants that are known to have failed normalization previously
CACHED_NORMALIZATION_FAILURE = 'cached_normalization_failure: this variant failed to normalize in the run ' \
                               'that produced the variant normalization cache'

# used to tell a variant that isn't in the cache apart from one cached as a normalization failure
_CACHE_MISS = object()


class VariantNormalizationCacheError(Exception):
    pass


class VariantNormalizationCache:
    """
    A lookup cache of previously normalized sequence variants, built from the KGX normalization output of a
    previous run (norm_node_map.json + normalized_nodes.jsonl). It lets NodeNormalizer skip the genetics
    normalizer (ClinGen) for variants that were already normalized, which can be very slow due to clingen
    only allowing one DBSNP to be normalized at a time. Additionally, ClinGen is updated far less regularly
    than Node Norm and is not versioned appropriately by ORION (no version is provided upstream), so caching
    even across different babel versions is not likely to be an issue.

    Normalization failures are cached along with successes - those calls are just as expensive as the ones
    that succeed, so a variant that failed to normalize previously is not tried again.

    Entries whose normalized nodes are not all present in the nodes file are not cached. That happens because
    remove_unconnected_nodes() prunes the nodes file after the map is written, so the two files are not
    guaranteed to agree, and serving a partial result would silently drop nodes of a split variant.

    A cache should come from a run that used the same strict normalization setting. Under strict
    normalization failures are recorded as None in the map, which is what this reads, but a lenient run
    records them as a variant mapped to itself, which is indistinguishable from a successful normalization
    and would be served as one.
    """

    def __init__(self, cache_directory: str):
        self.cache_directory = cache_directory
        norm_map_file_path = os.path.join(cache_directory, NORM_NODE_MAP_FILE_NAME)
        normalized_nodes_file_path = os.path.join(cache_directory, NORMALIZED_NODES_FILE_NAME)
        for file_path in (norm_map_file_path, normalized_nodes_file_path):
            if not os.path.isfile(file_path):
                raise VariantNormalizationCacheError(f'Could not initialize the variant normalization cache, '
                                                     f'{file_path} does not exist.')

        logger.info(f'Loading variant normalization cache from {cache_directory}...')

        # normalized sequence variant nodes, keyed by their normalized ID (ie CAID:CA123)
        # the category is discarded, it's the same for every variant node and is reassigned on retrieval,
        # so that cached nodes get the node types of the biolink version currently in use
        self.normalized_nodes = {}
        with jsonlines.open(normalized_nodes_file_path) as normalized_nodes_reader:
            for node in normalized_nodes_reader:
                if SEQUENCE_VARIANT in node.get(NODE_TYPES, []):
                    del node[NODE_TYPES]
                    self.normalized_nodes[node['id']] = node

        with open(norm_map_file_path) as norm_map_file:
            normalization_map = json.load(norm_map_file)['normalization_map']

        # map of original variant ID to the list of normalized IDs it became (more than one means it split),
        # or to None for variants that failed to normalize
        self.variant_id_map = {}
        self.cached_failure_count = 0
        skipped_entries = 0
        for variant_id, normalized_ids in normalization_map.items():
            if not normalized_ids:
                self.variant_id_map[variant_id] = None
                self.cached_failure_count += 1
            elif all(normalized_id in self.normalized_nodes for normalized_id in normalized_ids):
                self.variant_id_map[variant_id] = normalized_ids
            else:
                # either a regular (non-variant) node, or a variant that isn't entirely in the nodes file,
                # which happens because unconnected nodes are pruned from it after the map is written
                skipped_entries += 1

        logger.info(f'Variant normalization cache loaded: {len(self.variant_id_map)} variants '
                    f'({self.cached_failure_count} of them normalization failures), '
                    f'{len(self.normalized_nodes)} normalized nodes, '
                    f'{skipped_entries} entries skipped (not variants, or missing normalized nodes).')

    def get_normalized_variant_nodes(self, variant_id: str, node_types: list):
        """
        Look up the normalized nodes for a variant ID.

        :param variant_id: the original (pre-normalization) variant curie
        :param node_types: the categories to assign to the returned nodes
        :return: a list of normalized node dictionaries, an empty list if the variant is cached as a
                 normalization failure, or None if the variant is not in the cache at all
        """
        normalized_ids = self.variant_id_map.get(variant_id, _CACHE_MISS)
        if normalized_ids is _CACHE_MISS:
            return None
        if normalized_ids is None:
            return []
        # return copies, the caller takes ownership of the nodes and one normalized node
        # can be the result of more than one original variant ID
        return [dict(self.normalized_nodes[normalized_id], **{NODE_TYPES: node_types})
                for normalized_id in normalized_ids]

    def __len__(self):
        return len(self.variant_id_map)
