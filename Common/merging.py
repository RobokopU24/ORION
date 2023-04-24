import os
import jsonlines
import secrets
from xxhash import xxh64_hexdigest
from Common.node_types import *
from Common.utils import quick_json_loads, quick_json_dumps, chunk_iterator

NODE_PROPERTIES_THAT_SHOULD_BE_SETS = {SYNONYMS, NODE_TYPES}
EDGE_PROPERTIES_THAT_SHOULD_BE_SETS = {AGGREGATOR_KNOWLEDGE_SOURCES, PUBLICATIONS, XREFS}


def edge_key_function(edge):
    return xxh64_hexdigest(f'{edge[SUBJECT_ID]}{edge[PREDICATE]}{edge[OBJECT_ID]}'
                           f'{edge.get(PRIMARY_KNOWLEDGE_SOURCE, "")}')


def entity_merging_function(entity_1, entity_2, properties_that_are_sets):
    for key, value in entity_2.items():
        # TODO - make sure this is the behavior we want -
        # for properties that are lists append the values
        # otherwise keep the first one
        if key in entity_1:
            if isinstance(value, list):
                entity_1[key].extend(value)
                if key in properties_that_are_sets:
                    entity_1[key] = list(set(entity_1[key]))
        else:
            entity_1[key] = value
    return entity_1


def node_key_function(node):
    return node['id']


NODE_ENTITY_TYPE = 'node'
EDGE_ENTITY_TYPE = 'edge'


class GraphMerger:

    def __init__(self):
        self.merged_node_counter = 0
        self.merged_edge_counter = 0

    def merge_nodes(self, nodes_iterable):
        raise NotImplementedError

    def merge_edges(self, edges_iterable):
        raise NotImplementedError

    def get_merged_nodes_jsonl(self):
        raise NotImplementedError

    def get_merged_edges_jsonl(self):
        raise NotImplementedError


class DiskGraphMerger(GraphMerger):

    def __init__(self, temp_directory: str = None, chunk_size: int = 5_000_000):

        super().__init__()

        self.chunk_size = chunk_size
        self.probably_unique_temp_file_key = secrets.token_hex(6)

        self.temp_node_file_paths = []
        self.current_node_chunk = 0

        self.temp_edge_file_paths = []
        self.current_edge_chunk = 0

        self.temp_directory = temp_directory

    def merge_nodes(self, nodes):
        node_counter = 0
        for chunk_of_nodes in chunk_iterator(nodes, self.chunk_size):
            self.current_node_chunk += 1
            temp_node_file = os.path.join(self.temp_directory,
                                          f'n_{self.current_node_chunk}_{self.probably_unique_temp_file_key}.temp')
            node_counter += len(chunk_of_nodes)
            self.write_sorted_entities(chunk_of_nodes, node_key_function, temp_node_file)
            self.temp_node_file_paths.append(temp_node_file)
        return node_counter

    def merge_edges(self, edges):
        edge_counter = 0
        for chunk_of_edges in chunk_iterator(edges, self.chunk_size):
            self.current_edge_chunk += 1
            temp_edge_file = os.path.join(self.temp_directory,
                                          f'e_{self.current_edge_chunk}_{self.probably_unique_temp_file_key}.temp')
            edge_counter += len(chunk_of_edges)
            self.write_sorted_entities(chunk_of_edges, edge_key_function, temp_edge_file)
            self.temp_edge_file_paths.append(temp_edge_file)
        return edge_counter

    def get_merged_nodes_jsonl(self):
        for node in self.get_merged_entities(file_paths=self.temp_node_file_paths,
                                             sorting_key_function=node_key_function,
                                             merge_function=entity_merging_function,
                                             entity_type=NODE_ENTITY_TYPE):
            yield f'{quick_json_dumps(node)}\n'
        for file_path in self.temp_node_file_paths:
            os.remove(file_path)

    def get_merged_edges_jsonl(self):
        for edge in self.get_merged_entities(file_paths=self.temp_edge_file_paths,
                                             sorting_key_function=edge_key_function,
                                             merge_function=entity_merging_function,
                                             entity_type=EDGE_ENTITY_TYPE):
            yield f'{quick_json_dumps(edge)}\n'
        for file_path in self.temp_edge_file_paths:
            os.remove(file_path)

    def get_merged_entities(self,
                            file_paths,
                            sorting_key_function,
                            merge_function,
                            entity_type):

        if not file_paths:
            print('Warning: get_next_merged_entity called but no files were available! Empty source?')
            return

        file_handlers = [open(file_path) for file_path in file_paths]
        json_readers = {i: jsonlines.Reader(file_handler) for i, file_handler in enumerate(file_handlers)}

        first_lines = {i: json_reader.read() for i, json_reader in json_readers.items()}
        next_entities = {i: (sorting_key_function(value), value) for i, value in first_lines.items()}

        # next_entities = {i: json_reader.read() for i, json_reader in json_readers.items()}

        min_key = min([key for key, entity in next_entities.values()])
        while min_key:
            merged_entity = None
            for i in list(next_entities.keys()):
                next_key, next_entity = next_entities[i]
                while next_key == min_key:
                    if merged_entity:
                        if entity_type == NODE_ENTITY_TYPE:
                            merged_entity = merge_function(merged_entity, next_entity, NODE_PROPERTIES_THAT_SHOULD_BE_SETS)
                            self.merged_node_counter += 1
                        else:
                            merged_entity = merge_function(merged_entity, next_entity, EDGE_PROPERTIES_THAT_SHOULD_BE_SETS)
                            self.merged_edge_counter += 1
                    else:
                        merged_entity = next_entity
                    try:
                        next_entity = json_readers[i].read()
                        next_key = sorting_key_function(next_entity)
                        next_entities[i] = next_key, next_entity
                    except EOFError:
                        next_key, next_entity = None, None
                        del(next_entities[i])
                        json_readers[i].close()
                        file_handlers[i].close()
            yield merged_entity
            min_key = min([key for key, entity in next_entities.values()], default=None)

    def write_sorted_entities(self, entities, sorting_function, file_path):
        entities.sort(key=sorting_function)
        with jsonlines.open(file_path, 'w', compact=True) as jsonl_writer:
            jsonl_writer.write_all(entities)


class MemoryGraphMerger(GraphMerger):

    def __init__(self):
        super().__init__()
        self.nodes = {}
        self.edges = {}

    # merge a list of nodes (dictionaries not kgxnode objects!) into the existing list
    # throw_out_duplicates will throw out duplicates, otherwise merge their attributes
    def merge_nodes(self, nodes):
        node_count = 0
        for node in nodes:
            node_count += 1
            node_key = node['id']
            if node_key in self.nodes:
                self.merged_node_counter += 1
                previous_node = self.nodes[node_key]
                merged_node = entity_merging_function(previous_node,
                                                      node,
                                                      NODE_PROPERTIES_THAT_SHOULD_BE_SETS)
                self.nodes[node_key] = merged_node
            else:
                self.nodes[node_key] = node
        return node_count

    # merge a list of edges (dictionaries not kgxedge objects!) into the existing list
    def merge_edges(self, edges):
        edge_count = 0
        for edge in edges:
            edge_count += 1
            edge_key = edge_key_function(edge)
            if edge_key in self.edges:
                self.merged_edge_counter += 1
                merged_edge = entity_merging_function(quick_json_loads(self.edges[edge_key]),
                                                      edge,
                                                      EDGE_PROPERTIES_THAT_SHOULD_BE_SETS)
                self.edges[edge_key] = quick_json_dumps(merged_edge)
            else:
                self.edges[edge_key] = quick_json_dumps(edge)
        return edge_count

    def get_merged_nodes_jsonl(self):
        for node in self.nodes.values():
            yield f'{quick_json_dumps(node)}\n'

    def get_merged_edges_jsonl(self):
        for edge in self.edges.values():
            yield f'{edge}\n'

