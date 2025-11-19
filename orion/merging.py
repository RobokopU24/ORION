import os
import jsonlines
import secrets
from xxhash import xxh64_hexdigest
from orion.biolink_utils import BiolinkUtils
from orion.biolink_constants import *
from orion.utils import quick_json_loads, quick_json_dumps, chunk_iterator, LoggingUtil


NODE_ENTITY_TYPE = 'node'
EDGE_ENTITY_TYPE = 'edge'

# TODO ideally we'd make the biolink model version configurable here
bmt = BiolinkUtils()

logger = LoggingUtil.init_logging("ORION.Common.merging",
                                  line_format='medium',
                                  log_file_path=os.getenv('ORION_LOGS'))

# Key functions for identifying duplicates during entity merging.
# Add entries to CUSTOM_KEY_FUNCTIONS to define custom matching logic for specific properties.

# Default key function: dictionaries are duplicates if they have identical JSON representation
def default_dict_merge_key(entity):
    return quick_json_dumps(entity)

# Retrieval sources are duplicates if they have the same resource id and resource role
def retrieval_sources_key(retrieval_source):
    return retrieval_source[RETRIEVAL_SOURCE_ID] + retrieval_source[RETRIEVAL_SOURCE_ROLE]

# Map property names to their custom key functions
CUSTOM_KEY_FUNCTIONS = {
    RETRIEVAL_SOURCES: retrieval_sources_key
}

def node_key_function(node):
    return node['id']


def edge_key_function(edge, custom_key_attributes=None):
    qualifiers = [f'{key}{value}' for key, value in edge.items() if bmt.is_qualifier(key)]
    standard_attributes = (f'{edge[SUBJECT_ID]}{edge[PREDICATE]}{edge[OBJECT_ID]}'
                           f'{edge.get(PRIMARY_KNOWLEDGE_SOURCE, "")}{"".join(qualifiers)}')
    if custom_key_attributes:
        custom_attributes = [edge[attr] if attr in edge else '' for attr in custom_key_attributes]
        return xxh64_hexdigest(f'{standard_attributes}{"".join(custom_attributes)}')
    else:
        return xxh64_hexdigest(standard_attributes)


def entity_merging_function(entity_1, entity_2):
    # for every property of entity 2
    for key, entity_2_value in entity_2.items():
        # if entity 1 also has the property and entity_2_value is not null/empty:
        if (key in entity_1) and entity_2_value:
            entity_1_value = entity_1[key]

            # check if one or both of them are lists so we can combine them
            entity_1_is_list = isinstance(entity_1_value, list)
            entity_2_is_list = isinstance(entity_2_value, list)
            if entity_1_is_list and entity_2_is_list:
                # if they're both lists just concat them
                entity_1_value.extend(entity_2_value)
            elif entity_1_is_list:
                # if 1 is a list and 2 isn't, append the value of 2 to the list from 1
                entity_1_value.append(entity_2_value)
            elif entity_2_is_list:
                if entity_1_value:
                    # if 2 is a list and 1 has a value, add the value of 1 to the list from 2
                    entity_1[key] = [entity_1_value] + entity_2_value
                else:
                    # if 2 is a list and 1 doesn't have a value, just use the list from 2
                    entity_1[key] = entity_2_value
            # else:
                # if neither is a list, do nothing (keep the value from 1)

            # if either was a list remove duplicate values
            if entity_1_is_list or entity_2_is_list:
                # if the list is of dictionaries
                if isinstance(entity_1[key][0], dict):
                    # Use a custom key function to determine matches if there is one
                    key_function = CUSTOM_KEY_FUNCTIONS.get(key, default_dict_merge_key)
                    # Group dictionaries by their key
                    grouped = {}
                    for item in entity_1[key]:
                        item_key = key_function(item)
                        if item_key in grouped:
                            # Recursively merge with existing item
                            grouped[item_key] = entity_merging_function(grouped[item_key], item)
                        else:
                            grouped[item_key] = item
                    entity_1[key] = list(grouped.values())
                else:
                    entity_1[key] = sorted(list(set(entity_1[key])))
        else:
            # if entity 1 doesn't have the property, add the property from entity 2
            entity_1[key] = entity_2_value
    return entity_1


class GraphMerger:

    def __init__(self):
        self.merged_node_counter = 0
        self.merged_edge_counter = 0

    def merge_nodes(self, nodes_iterable):
        raise NotImplementedError

    def merge_edges(self, edges_iterable, additional_edge_attributes=None, add_edge_id=False):
        raise NotImplementedError

    def merge_node(self, node):
        raise NotImplementedError

    def merge_edge(self, edge, additional_edge_attributes=None, add_edge_id=False):
        raise NotImplementedError

    def flush(self):
        pass

    def get_merged_nodes_jsonl(self):
        raise NotImplementedError

    def get_merged_edges_jsonl(self):
        raise NotImplementedError


class DiskGraphMerger(GraphMerger):

    def __init__(self, temp_directory: str = None, chunk_size: int = 10_000_000):

        super().__init__()

        self.chunk_size = chunk_size
        self.probably_unique_temp_file_key = secrets.token_hex(6)

        self.additional_edge_attributes = None
        self.add_edge_id = False

        self.temp_node_file_paths = []
        self.current_node_chunk = 0

        self.temp_edge_file_paths = []
        self.current_edge_chunk = 0

        self.temp_directory = temp_directory
        self.temp_file_paths = {
            NODE_ENTITY_TYPE: [],
            EDGE_ENTITY_TYPE: []
        }
        self.entity_buffers = {
            NODE_ENTITY_TYPE: [],
            EDGE_ENTITY_TYPE: []
        }

    def merge_node(self, node):
        self.entity_buffers[NODE_ENTITY_TYPE].append(node)
        if len(self.entity_buffers[NODE_ENTITY_TYPE]) >= self.chunk_size:
            self.flush_node_buffer()

    def merge_nodes(self, nodes):
        return self.merge_entities(nodes,
                                   NODE_ENTITY_TYPE,
                                   node_key_function)

    def merge_edge(self, edge, additional_edge_attributes=None, add_edge_id=False):
        self.entity_buffers[EDGE_ENTITY_TYPE].append(edge)
        if len(self.entity_buffers[EDGE_ENTITY_TYPE]) >= self.chunk_size:
            self.flush_edge_buffer()

    def merge_edges(self, edges, additional_edge_attributes=None, add_edge_id=False):
        logger.info(f'additional_edge_attributes: {additional_edge_attributes}, add_edge_id: {add_edge_id}')
        self.additional_edge_attributes = additional_edge_attributes
        self.add_edge_id = add_edge_id
        sorting_function = lambda edge: edge_key_function(edge, custom_key_attributes=additional_edge_attributes)
        return self.merge_entities(edges,
                                   EDGE_ENTITY_TYPE,
                                   sorting_function)

    def merge_entities(self, entities, entity_type, entity_sorting_function):
        entity_counter = 0
        for chunk_of_entities in chunk_iterator(entities, self.chunk_size):
            current_chunk_size = len(chunk_of_entities)
            entity_counter += current_chunk_size
            if current_chunk_size == self.chunk_size:
                # this is a full chunk of the max size, go ahead and process it
                self.sort_and_write_entities(chunk_of_entities,
                                             entity_sorting_function,
                                             entity_type)
            else:
                # this chunk is smaller than the max chunk size, add it to the buffer
                self.entity_buffers[entity_type].extend(chunk_of_entities)
                # if the buffer is full/overfull process it
                if len(self.entity_buffers[entity_type]) >= self.chunk_size:
                    self.sort_and_write_entities(self.entity_buffers[entity_type][:self.chunk_size],
                                                 entity_sorting_function,
                                                 entity_type)
                    self.entity_buffers[entity_type] = self.entity_buffers[entity_type][self.chunk_size:]
        return entity_counter

    def sort_and_write_entities(self,
                                entities,
                                entity_sorting_function,
                                entity_type):
        entities.sort(key=entity_sorting_function)
        temp_file_name = f'{entity_type}_{secrets.token_hex(6)}.temp'
        temp_file_path = os.path.join(self.temp_directory, temp_file_name)
        with jsonlines.open(temp_file_path, 'w', compact=True) as jsonl_writer:
            jsonl_writer.write_all(entities)
        self.temp_file_paths[entity_type].append(temp_file_path)

    def get_merged_nodes_jsonl(self):
        self.flush_node_buffer()
        for node in self.get_merged_entities(file_paths=self.temp_file_paths[NODE_ENTITY_TYPE],
                                             sorting_key_function=node_key_function,
                                             merge_function=entity_merging_function,
                                             entity_type=NODE_ENTITY_TYPE):
            yield f'{quick_json_dumps(node)}\n'
        for file_path in self.temp_file_paths[NODE_ENTITY_TYPE]:
            os.remove(file_path)

    def flush_node_buffer(self):
        if not self.entity_buffers[NODE_ENTITY_TYPE]:
            return
        self.sort_and_write_entities(self.entity_buffers[NODE_ENTITY_TYPE],
                                     node_key_function,
                                     NODE_ENTITY_TYPE)
        self.entity_buffers[NODE_ENTITY_TYPE] = []

    def get_merged_edges_jsonl(self):
        self.flush_edge_buffer()
        sorting_function = lambda e: edge_key_function(e, custom_key_attributes=self.additional_edge_attributes)
        for edge in self.get_merged_entities(file_paths=self.temp_file_paths[EDGE_ENTITY_TYPE],
                                             sorting_key_function=sorting_function,
                                             merge_function=entity_merging_function,
                                             entity_type=EDGE_ENTITY_TYPE,
                                             add_edge_id=self.add_edge_id):
            yield f'{quick_json_dumps(edge)}\n'
        for file_path in self.temp_file_paths[EDGE_ENTITY_TYPE]:
            os.remove(file_path)

    def flush_edge_buffer(self):
        if not self.entity_buffers[EDGE_ENTITY_TYPE]:
            return
        self.sort_and_write_entities(self.entity_buffers[EDGE_ENTITY_TYPE],
                                     edge_key_function,
                                     EDGE_ENTITY_TYPE)
        self.entity_buffers[EDGE_ENTITY_TYPE] = []

    def get_merged_entities(self,
                            file_paths,
                            sorting_key_function,
                            merge_function,
                            entity_type,
                            add_edge_id=False):

        if not file_paths:
            logger.error('get_merged_entities called but no file_paths were provided! Empty source?')
            return

        file_handlers = [open(file_path) for file_path in file_paths]
        json_readers = {i: jsonlines.Reader(file_handler) for i, file_handler in enumerate(file_handlers)}

        first_lines = {i: json_reader.read() for i, json_reader in json_readers.items()}
        next_entities = {i: (sorting_key_function(value), value) for i, value in first_lines.items()}

        min_key = min([key for key, entity in next_entities.values()], default=None)
        while min_key is not None:
            merged_entity = None
            for i in list(next_entities.keys()):
                next_key, next_entity = next_entities[i]
                while next_key == min_key:
                    if merged_entity:
                        if entity_type == NODE_ENTITY_TYPE:
                            merged_entity = merge_function(merged_entity, next_entity)
                            self.merged_node_counter += 1
                        else:
                            merged_entity = merge_function(merged_entity, next_entity)
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

            # Add the id attribute if add_edge_id is True
            if entity_type == EDGE_ENTITY_TYPE and add_edge_id and merged_entity and min_key:
                merged_entity["id"] = min_key

            yield merged_entity
            min_key = min([key for key, entity in next_entities.values()], default=None)

    def flush(self):
        self.flush_node_buffer()
        self.flush_edge_buffer()


class MemoryGraphMerger(GraphMerger):

    def __init__(self):
        super().__init__()
        self.nodes = {}
        self.edges = {}

    # merge a list of nodes (dictionaries not kgxnode objects!) into the existing set
    def merge_nodes(self, nodes):
        node_count = 0
        for node in nodes:
            node_count += 1
            self.merge_node(node)
        return node_count

    def merge_node(self, node):
        node_key = node['id']
        if node_key in self.nodes:
            self.merged_node_counter += 1
            previous_node = self.nodes[node_key]
            merged_node = entity_merging_function(previous_node,
                                                  node)
            self.nodes[node_key] = merged_node
        else:
            self.nodes[node_key] = node

    # merge a list of edges (dictionaries not kgxedge objects!) into the existing list
    def merge_edges(self, edges, additional_edge_attributes=None, add_edge_id=False):
        edge_count = 0
        for edge in edges:
            edge_count += 1
            self.merge_edge(edge, additional_edge_attributes=additional_edge_attributes, add_edge_id=add_edge_id)
        return edge_count

    def merge_edge(self, edge, additional_edge_attributes=None, add_edge_id=False):
        edge_key = edge_key_function(edge, custom_key_attributes=additional_edge_attributes)
        if edge_key in self.edges:
            self.merged_edge_counter += 1
            merged_edge = entity_merging_function(quick_json_loads(self.edges[edge_key]),
                                                  edge)
            if add_edge_id is True:
                merged_edge[EDGE_ID] = edge_key
            self.edges[edge_key] = quick_json_dumps(merged_edge)
        else:
            if add_edge_id is True:
                edge[EDGE_ID] = edge_key
            self.edges[edge_key] = quick_json_dumps(edge)

    def get_merged_nodes_jsonl(self):
        for node in self.nodes.values():
            yield f'{quick_json_dumps(node)}\n'

    def get_merged_edges_jsonl(self):
        for edge in self.edges.values():
            yield f'{edge}\n'
