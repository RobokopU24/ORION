from xxhash import xxh64_hexdigest
from kgx.utils.kgx_utils import prepare_data_dict as kgx_dict_merge
from Common.node_types import *
from Common.utils import quick_json_loads, quick_json_dumps

EDGE_PROPERTIES_THAT_SHOULD_BE_SETS = {AGGREGATOR_KNOWLEDGE_SOURCES, PUBLICATIONS}


class GraphMerger:

    def __init__(self):
        self.nodes = {}
        self.edges = {}

    # merge a list of nodes (dictionaries not kgxnode objects!) into the existing list
    # throw_out_duplicates will throw out duplicates, otherwise merge their attributes
    def merge_nodes(self, nodes, throw_out_duplicates: bool = False):
        node_count = 0
        merge_count = 0
        for node in nodes:
            node_count += 1
            node_key = node['id']
            if node_key in self.nodes:
                merge_count += 1
                if not throw_out_duplicates:
                    previous_node = quick_json_loads(self.nodes[node_key])
                    merged_node = kgx_dict_merge(node, previous_node)
                    self.nodes[node_key] = quick_json_dumps(merged_node)
            else:
                self.nodes[node_key] = quick_json_dumps(node)
        return node_count, merge_count

    # merge a list of edges (dictionaries not kgxedge objects!) into the existing list
    # throw_out_duplicates will throw out duplicates, otherwise merge their attributes
    def merge_edges(self, edges, throw_out_duplicates: bool = False):

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
                if not throw_out_duplicates:
                    merged_edge = quick_json_loads(self.edges[edge_key])
                    for key, value in edge.items():
                        # TODO - make sure this is the behavior we want -
                        # for properties that are lists append the values
                        # otherwise overwrite them
                        if key in merged_edge and isinstance(value, list):
                            merged_edge[key].extend(value)
                            if key in EDGE_PROPERTIES_THAT_SHOULD_BE_SETS:
                                merged_edge[key] = list(set(merged_edge[key]))
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
