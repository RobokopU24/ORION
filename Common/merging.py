
import orjson
from xxhash import xxh64_hexdigest
from kgx.utils.kgx_utils import prepare_data_dict as kgx_dict_merge
from Common.node_types import ORIGINAL_KNOWLEDGE_SOURCE, PRIMARY_KNOWLEDGE_SOURCE, SUBJECT_ID, OBJECT_ID, PREDICATE, \
    AGGREGATOR_KNOWLEDGE_SOURCES, PUBLICATIONS, OBJECT_ID, SUBJECT_ID, PREDICATE


def quick_json_dumps(item):
    return str(orjson.dumps(item), encoding='utf-8')


def quick_json_loads(item):
    return orjson.loads(item)


EDGE_PROPERTIES_THAT_SHOULD_BE_SETS = {AGGREGATOR_KNOWLEDGE_SOURCES, PUBLICATIONS}


class GraphMerger:

    def __init__(self):
        self.nodes = {}
        self.edges = {}

    def merge_nodes(self, nodes, overwrite: bool = False):
        node_count = 0
        merge_count = 0
        for node in nodes:
            node_count += 1
            node_key = node['id']
            if node_key in self.nodes:
                merge_count += 1
                if not overwrite:
                    previous_node = quick_json_loads(self.nodes[node_key])
                    merged_node = kgx_dict_merge(node, previous_node)
                    self.nodes[node_key] = quick_json_dumps(merged_node)
            else:
                self.nodes[node_key] = quick_json_dumps(node)
        return node_count, merge_count

    def merge_edges(self, edges, overwrite: bool = False):

        def edge_key_function(edge):
            return xxh64_hexdigest(
                str(f'{edge[SUBJECT_ID]}{edge[PREDICATE]}{edge[OBJECT_ID]}' +
                    (f'{edge.get(ORIGINAL_KNOWLEDGE_SOURCE, "")}{edge.get(PRIMARY_KNOWLEDGE_SOURCE, "")}'
                     if ((ORIGINAL_KNOWLEDGE_SOURCE in edge) or (PRIMARY_KNOWLEDGE_SOURCE in edge)) else "")))

        edge_count = 0
        merge_count = 0
        for edge in edges:
            edge_count += 1
            edge_key = edge_key_function(edge)
            if edge_key in self.edges:
                merge_count += 1
                if not overwrite:
                    merged_edge = quick_json_loads(self.edges[edge_key])
                    for key, value in edge.items():
                        # TODO - make sure this is the behavior we want -
                        # for properties that are lists append the values
                        # otherwise overwrite them
                        if key in merged_edge and isinstance(value, list):
                            merged_edge[key].extend(value)
                            if key in EDGE_PROPERTIES_THAT_SHOULD_BE_SETS:
                                merged_edge[key] = list(set(value))
                        else:
                            merged_edge[key] = value
                    self.edges[edge_key] = quick_json_dumps(merged_edge)
            else:
                self.edges[edge_key] = quick_json_dumps(edge)
        return edge_count, merge_count

    def get_merged_nodes_lines(self):
        for node in self.nodes.values():
            yield f'{node}\n'

    def get_merged_edges_lines(self):
        for edge in self.edges.values():
            yield f'{edge}\n'
