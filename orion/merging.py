import heapq
import os
import secrets
from collections import defaultdict
import uuid_utils as uuid
from xxhash import xxh64_hexdigest
from orion.biolink_utils import BiolinkUtils
from orion.biolink_constants import *
import orjson
from orion.utils import quick_json_loads, quick_json_dumps
from orion.logging import get_orion_logger

ORION_UUID_NAMESPACE = uuid.UUID('e2a5b21f-4e4d-4a6e-b64a-1f3c78e2a9d0')

NODE_ENTITY_TYPE = 'node'
EDGE_ENTITY_TYPE = 'edge'

# TODO ideally we'd make the biolink model version configurable here
bmt = BiolinkUtils()

logger = get_orion_logger("orion.merging")

# mismatches where one entity has a dict for a property and another has another type.
_mismatched_dict_properties = set()
# properties where two entities had different values that could not be reconciled.
_dropped_properties = set()

# use this flush pattern so we can emit warnings after each merge, even if there are many
# (ie after each set of node/edge files)
def flush_merge_warnings():
    mismatched = sorted(_mismatched_dict_properties)
    dropped = sorted(_dropped_properties)
    if mismatched:
        logger.warning(f'Mismatched types encountered while merging properties: '
                       f'{mismatched}. entity_1 values were kept.')
    if dropped:
        logger.warning(f'Property value collisions encountered while merging properties: '
                       f'{dropped}. entity_1 values were kept, entity_2 values were dropped.')
    _mismatched_dict_properties.clear()
    _dropped_properties.clear()
    return {'mismatched_properties': mismatched, 'dropped_properties': dropped}

# Key functions for identifying duplicates during entity merging.
# Add entries to CUSTOM_KEY_FUNCTIONS to define custom matching logic for specific properties.

# Default key function: dictionaries are duplicates if they have identical JSON representation.
# Sort keys so two logically-equal dicts with different insertion order produce the same key.
def default_dict_merge_key(entity):
    return orjson.dumps(entity, option=orjson.OPT_SORT_KEYS)

# Retrieval sources are duplicates if they have the same resource id and resource role
def retrieval_sources_key(retrieval_source):
    return retrieval_source[RETRIEVAL_SOURCE_ID] + retrieval_source[RETRIEVAL_SOURCE_ROLE]

# Map property names to their custom key functions
CUSTOM_KEY_FUNCTIONS = {
    RETRIEVAL_SOURCES: retrieval_sources_key
}

def node_key_function(node):
    return node['id']


def edge_key_function(edge, custom_key_attributes=None, edge_id_type=None):
    qualifiers = sorted([f'{key}{value}' for key, value in edge.items() if bmt.is_qualifier(key)])
    primary_knowledge_source = edge.get(PRIMARY_KNOWLEDGE_SOURCE, "")
    if not primary_knowledge_source:
        for retrieval_source in edge.get(RETRIEVAL_SOURCES, []):
            if retrieval_source[RETRIEVAL_SOURCE_ROLE] == PRIMARY_KNOWLEDGE_SOURCE:
                primary_knowledge_source = retrieval_source[RETRIEVAL_SOURCE_ID]
                break
    standard_attributes = (f'{edge[SUBJECT_ID]}{edge[PREDICATE]}{edge[OBJECT_ID]}'
                           f'{primary_knowledge_source}{"".join(qualifiers)}')
    if custom_key_attributes:
        custom_attributes = []
        for attr in custom_key_attributes:
            value = edge.get(attr, "")
            if isinstance(value, dict):
                raise ValueError(f'Edge merging attribute "{attr}" has a dictionary value. '
                                 f'Dictionaries are not currently supported as edge key attributes.')
            if isinstance(value, list):
                value = str(sorted(str(v) for v in value))
            else:
                value = str(value)
            custom_attributes.append(value)
        key_input = f'{standard_attributes}{"".join(custom_attributes)}'
    else:
        key_input = standard_attributes

    if edge_id_type == 'uuid':
        return str(uuid.uuid5(ORION_UUID_NAMESPACE, key_input))
    else:
        return xxh64_hexdigest(key_input)


def entity_merging_function(entity_1, entity_2):
    # for every property of entity 2
    for key, entity_2_value in entity_2.items():
        # if entity 1 also has the property and entity_2_value is not null/empty:
        if (key in entity_1) and (entity_2_value is not None):
            entity_1_value = entity_1[key]

            # check if one or both of them are lists so we can combine them
            entity_1_is_list = isinstance(entity_1_value, list)
            entity_2_is_list = isinstance(entity_2_value, list)
            entity_1_is_dict = isinstance(entity_1_value, dict)
            entity_2_is_dict = isinstance(entity_2_value, dict)
            if entity_1_is_dict and entity_2_is_dict:
                # Dict-shaped biolink slots are id-keyed by construction, so merging by key is
                # lossless at the key level. For colliding keys whose values are themselves
                # schema-shaped dicts, recurse so nested list/dict/scalar slots merge correctly.
                for sub_key, sub_value in entity_2_value.items():
                    if sub_key in entity_1_value:
                        existing_sub_value = entity_1_value[sub_key]
                        if isinstance(existing_sub_value, dict) and isinstance(sub_value, dict):
                            entity_1_value[sub_key] = entity_merging_function(existing_sub_value, sub_value)
                        elif not existing_sub_value:
                            entity_1_value[sub_key] = sub_value
                        elif not sub_value:
                            pass  # keep entity_1; sub_value is falsy
                        elif existing_sub_value != sub_value:
                            # both truthy and differ; keep entity_1 and record the drop
                            _dropped_properties.add(key)
                    else:
                        entity_1_value[sub_key] = sub_value
            elif entity_1_is_dict or entity_2_is_dict:
                _mismatched_dict_properties.add(key)
            elif entity_1_is_list and entity_2_is_list:
                # if they're both lists just concat them
                entity_1_value.extend(entity_2_value)
            elif entity_1_is_list:
                # if 1 is a list and 2 isn't, append the value of 2 to the list from 1
                entity_1_value.append(entity_2_value)
            elif entity_2_is_list:
                if entity_1_value is not None:
                    # if 2 is a list and 1 has a value, add the value of 1 to the list from 2
                    entity_1[key] = [entity_1_value] + entity_2_value
                else:
                    # if 2 is a list and 1 doesn't have a value, just use the list from 2
                    entity_1[key] = entity_2_value
            else:
                # scalar/scalar: prefer the truthy value. Falsy values (None, 0, "", False)
                # lose to any truthy value from the other entity.
                if not entity_1_value:
                    entity_1[key] = entity_2_value
                elif not entity_2_value:
                    pass  # keep entity_1; entity_2 is falsy, nothing meaningful to drop
                elif entity_1_value != entity_2_value:
                    # both truthy and differ; keep entity_1 and record that entity_2 was dropped
                    _dropped_properties.add(key)

            # if either was a list remove duplicate values
            if entity_1_is_list or entity_2_is_list:
                # if the post-merge list is empty no need to deduplicate
                if not entity_1[key]:
                    continue
                # if the list is of dictionaries
                if isinstance(entity_1[key][0], dict):
                    # Use a custom key function to determine equivalence if there is one
                    key_function = CUSTOM_KEY_FUNCTIONS.get(key, default_dict_merge_key)
                    # Merge dictionaries with matching keys
                    grouped = {}
                    for item in entity_1[key]:
                        item_key = key_function(item)
                        if item_key in grouped:
                            # Recursively merge equivalent-by-key dictionaries
                            grouped[item_key] = entity_merging_function(grouped[item_key], item)
                        else:
                            grouped[item_key] = item
                    entity_1[key] = list(grouped.values())
                else:
                    entity_1[key] = sorted(set(entity_1[key]))
        else:
            # if entity 1 doesn't have the property, add the property from entity 2
            entity_1[key] = entity_2_value
    return entity_1


class GraphMerger:

    def __init__(self,
                 edge_merging_attributes=None,
                 add_edge_id=False,
                 edge_id_type=None,
                 overwrite_edge_ids=True):
        self.merged_node_counter = 0
        self.merged_edge_counter = 0
        self.merge_warnings = {'mismatched_properties': set(), 'dropped_properties': set()}
        self.edge_merging_attributes = edge_merging_attributes
        self.add_edge_id = add_edge_id
        self.edge_id_type = edge_id_type
        # overwrite_edge_ids only has effect when add_edge_id is true.
        # When add_edge_id is true and overwrite_edge_ids is false: existing edge ids are preserved
        # for edges not involved in a merge; a generated id is assigned only when missing or when
        # a merge actually occurs. For merged edges, a mapping from the new id to the original
        # pre-merge ids is recorded in pre_merge_edge_id_mapping.
        self.overwrite_edge_ids = overwrite_edge_ids

    def merge_nodes(self, nodes_iterable):
        raise NotImplementedError

    def merge_edges(self, edges_iterable):
        raise NotImplementedError

    def merge_node(self, node):
        raise NotImplementedError

    def merge_edge(self, edge):
        raise NotImplementedError

    def flush(self):
        pass

    def get_node_ids(self):
        raise NotImplementedError

    def get_merged_nodes_jsonl(self):
        raise NotImplementedError

    def get_merged_edges_jsonl(self):
        raise NotImplementedError

    def get_pre_merge_edge_id_mapping(self):
        raise NotImplementedError

    def _record_merge_warnings(self, warnings):
        self.merge_warnings['mismatched_properties'].update(warnings['mismatched_properties'])
        self.merge_warnings['dropped_properties'].update(warnings['dropped_properties'])


class DiskGraphMerger(GraphMerger):

    def __init__(self, temp_directory: str = None, chunk_size: int = 10_000_000,
                 edge_merging_attributes=None, add_edge_id=False, edge_id_type=None,
                 overwrite_edge_ids=True, pre_merge_mapping_file_path=None):

        super().__init__(edge_merging_attributes=edge_merging_attributes,
                         add_edge_id=add_edge_id,
                         edge_id_type=edge_id_type,
                         overwrite_edge_ids=overwrite_edge_ids)

        self.chunk_size = chunk_size
        self.temp_directory = temp_directory
        self.node_ids = set()
        self.temp_file_paths = {
            NODE_ENTITY_TYPE: [],
            EDGE_ENTITY_TYPE: []
        }
        self.entity_buffers = {
            NODE_ENTITY_TYPE: [],
            EDGE_ENTITY_TYPE: []
        }
        self.pre_merge_mapping_file_path = pre_merge_mapping_file_path
        # Transient handle opened by get_merged_edges_jsonl and written to as each merged
        # key is finalized inside get_merged_entities. Avoids buffering the full mapping
        # in memory, which would defeat the purpose of DiskGraphMerger.
        self._pre_merge_mapping_file = None

    def merge_node(self, node):
        self.node_ids.add(node['id'])
        key = node_key_function(node)
        self.entity_buffers[NODE_ENTITY_TYPE].append((key, quick_json_dumps(node)))
        if len(self.entity_buffers[NODE_ENTITY_TYPE]) >= self.chunk_size:
            self.flush_node_buffer()

    def merge_nodes(self, nodes):
        node_count = 0
        for node in nodes:
            node_count += 1
            self.merge_node(node)
        return node_count

    def merge_edge(self, edge):
        key = edge_key_function(edge, custom_key_attributes=self.edge_merging_attributes,
                                edge_id_type=self.edge_id_type)
        if self.add_edge_id and not self.overwrite_edge_ids:
            # pre_merge_id must be captured before adding a new id,
            # so that if they don't exist they aren't recorded in the merge mapping.
            pre_merge_id = edge.get(EDGE_ID)
            if pre_merge_id is None:
                edge[EDGE_ID] = key
            self.entity_buffers[EDGE_ENTITY_TYPE].append((key, quick_json_dumps(edge), pre_merge_id))
        else:
            if self.add_edge_id:
                edge[EDGE_ID] = key
            self.entity_buffers[EDGE_ENTITY_TYPE].append((key, quick_json_dumps(edge)))
        if len(self.entity_buffers[EDGE_ENTITY_TYPE]) >= self.chunk_size:
            self.flush_edge_buffer()

    def merge_edges(self, edges):
        edge_count = 0
        for edge in edges:
            edge_count += 1
            self.merge_edge(edge)
        return edge_count

    def sort_and_write_keyed_entities(self, keyed_entities, entity_type):
        keyed_entities.sort(key=lambda x: x[0])
        temp_file_name = f'{entity_type}_{secrets.token_hex(6)}.temp'
        temp_file_path = os.path.join(self.temp_directory, temp_file_name)
        with open(temp_file_path, 'w') as temp_file:
            if self.add_edge_id and not self.overwrite_edge_ids and entity_type == EDGE_ENTITY_TYPE:
                for key, entity_json, pre_merge_id in keyed_entities:
                    temp_file.write(f'{pre_merge_id or ""}\t{key}{entity_json}\n')
            else:
                for key, entity_json in keyed_entities:
                    temp_file.write(f'{key}{entity_json}\n')
        self.temp_file_paths[entity_type].append(temp_file_path)

    def get_node_ids(self):
        return self.node_ids

    def get_merged_nodes_jsonl(self):
        self.flush_node_buffer()
        try:
            for json_line in self.get_merged_entities(file_paths=self.temp_file_paths[NODE_ENTITY_TYPE],
                                                      merge_function=entity_merging_function,
                                                      entity_type=NODE_ENTITY_TYPE):
                yield json_line
            for file_path in self.temp_file_paths[NODE_ENTITY_TYPE]:
                os.remove(file_path)
        finally:
            self._record_merge_warnings(flush_merge_warnings())

    def flush_node_buffer(self):
        if not self.entity_buffers[NODE_ENTITY_TYPE]:
            return
        self.sort_and_write_keyed_entities(self.entity_buffers[NODE_ENTITY_TYPE],
                                           NODE_ENTITY_TYPE)
        self.entity_buffers[NODE_ENTITY_TYPE] = []

    def get_merged_edges_jsonl(self):
        self.flush_edge_buffer()
        track_pre_merge_ids = self.add_edge_id and not self.overwrite_edge_ids
        try:
            if track_pre_merge_ids and self.pre_merge_mapping_file_path:
                self._pre_merge_mapping_file = open(self.pre_merge_mapping_file_path, 'w')
            for json_line in self.get_merged_entities(file_paths=self.temp_file_paths[EDGE_ENTITY_TYPE],
                                                      merge_function=entity_merging_function,
                                                      entity_type=EDGE_ENTITY_TYPE,
                                                      track_pre_merge_ids=track_pre_merge_ids):
                yield json_line
            for file_path in self.temp_file_paths[EDGE_ENTITY_TYPE]:
                os.remove(file_path)
        finally:
            if self._pre_merge_mapping_file is not None:
                self._pre_merge_mapping_file.close()
                self._pre_merge_mapping_file = None
            self._record_merge_warnings(flush_merge_warnings())

    def flush_edge_buffer(self):
        if not self.entity_buffers[EDGE_ENTITY_TYPE]:
            return
        self.sort_and_write_keyed_entities(self.entity_buffers[EDGE_ENTITY_TYPE],
                                           EDGE_ENTITY_TYPE)
        self.entity_buffers[EDGE_ENTITY_TYPE] = []

    @staticmethod
    def parse_keyed_line(line):
        # Split a keyed line into (key, raw_json). The key ends at the first '{'.
        json_start = line.index('{')
        return line[:json_start], line[json_start:].rstrip('\n')

    @staticmethod
    def parse_keyed_line_with_pre_merge_id(line):
        # Split a line with format: {pre_merge_id}\t{key}{json}\n
        tab_pos = line.index('\t')
        pre_merge_id = line[:tab_pos] or None
        rest = line[tab_pos + 1:]
        json_start = rest.index('{')
        return pre_merge_id, rest[:json_start], rest[json_start:].rstrip('\n')

    def get_merged_entities(self,
                            file_paths,
                            merge_function,
                            entity_type,
                            track_pre_merge_ids=False):

        # open all the files, which are chunk_size sized files of sorted and keyed entities
        if not file_paths:
            logger.error('get_merged_entities called but no file_paths were provided! Empty source?')
            return
        file_handlers = [open(file_path) for file_path in file_paths]

        # store a string that can be used to reference the counter for the appropriate entity type
        merge_counter = 'merged_node_counter' if entity_type == NODE_ENTITY_TYPE else 'merged_edge_counter'

        # Here we use a min-heap to organize iterating through the entity files to compare their keys and merge entities
        # with matching keys. Members of the heap are tuples representing each line from a file:
        # (key, file_index, raw_json) or (key, file_index, pre_merge_id, raw_json) when tracking pre-merge IDs.
        # key is the previously calculated merging key for an entity, and raw_json is the raw json string for an entity.

        parse_line = self.parse_keyed_line_with_pre_merge_id if track_pre_merge_ids else self.parse_keyed_line

        # First start the heap with the first line from each file.
        heap = []
        for i, fh in enumerate(file_handlers):
            line = fh.readline()
            if line:
                if track_pre_merge_ids:
                    pre_merge_id, key, raw_json = parse_line(line)
                    heap.append((key, i, pre_merge_id, raw_json))
                else:
                    key, raw_json = parse_line(line)
                    heap.append((key, i, raw_json))
            else:
                fh.close()
        heapq.heapify(heap)

        # Then use the heap to iterate through all the files and merge matching entities
        while heap:
            # If we're here it means it's the first time encountering this key
            min_key = heap[0][0]
            merged_entity = None
            merged_json = None
            pre_merge_ids = [] if track_pre_merge_ids else None

            # Pop all entries with the current minimum key and merge them together
            while heap and heap[0][0] == min_key:
                if track_pre_merge_ids:
                    key, i, pre_merge_id, raw_json = heapq.heappop(heap)
                    if pre_merge_id is not None:
                        pre_merge_ids.append(pre_merge_id)
                else:
                    key, i, raw_json = heapq.heappop(heap)

                # If there's a merged entity it means we already merged entities with this key, use the same object
                if merged_entity is not None:
                    merged_entity = merge_function(merged_entity, quick_json_loads(raw_json))
                    setattr(self, merge_counter, getattr(self, merge_counter) + 1)
                # Otherwise if there is merged_json it means we encountered a matching entity but didn't merge yet
                elif merged_json is not None:
                    merged_entity = merge_function(quick_json_loads(merged_json), quick_json_loads(raw_json))
                    setattr(self, merge_counter, getattr(self, merge_counter) + 1)
                    merged_json = None
                # Otherwise this is the first time seeing this key
                else:
                    merged_json = raw_json

                # read the next line from this file
                line = file_handlers[i].readline()
                if line:
                    if track_pre_merge_ids:
                        next_pre_merge_id, next_key, next_raw_json = parse_line(line)
                        heapq.heappush(heap, (next_key, i, next_pre_merge_id, next_raw_json))
                    else:
                        next_key, next_raw_json = parse_line(line)
                        heapq.heappush(heap, (next_key, i, next_raw_json))
                else:
                    file_handlers[i].close()

            # If we did a merge we need to convert back to a json string for writing, and apply a new id if desired
            if merged_entity is not None:
                if track_pre_merge_ids:
                    # It looks like we're overwriting all edge ids, but you don't get here without a merge occurring
                    merged_entity[EDGE_ID] = min_key
                    if pre_merge_ids and self._pre_merge_mapping_file is not None:
                        self._pre_merge_mapping_file.write(
                            f'{quick_json_dumps({"post_merge_id": min_key, "pre_merge_ids": pre_merge_ids})}\n')
                yield f'{quick_json_dumps(merged_entity)}\n'
            # otherwise we can just write the raw json to file
            else:
                yield f'{merged_json}\n'

    def get_pre_merge_edge_id_mapping(self):
        # Read the mapping back from the streamed file. Primarily for test/debug use;
        # production flow writes the file directly and does not re-read it.
        if not self.pre_merge_mapping_file_path or not os.path.exists(self.pre_merge_mapping_file_path):
            return {}
        result = {}
        with open(self.pre_merge_mapping_file_path) as f:
            for line in f:
                entry = quick_json_loads(line)
                result[entry['post_merge_id']] = entry['pre_merge_ids']
        return result

    def flush(self):
        self.flush_node_buffer()
        self.flush_edge_buffer()


class MemoryGraphMerger(GraphMerger):

    def __init__(self,
                 edge_merging_attributes=None,
                 add_edge_id=False,
                 edge_id_type=None,
                 overwrite_edge_ids=True,
                 pre_merge_mapping_file_path=None):
        super().__init__(edge_merging_attributes=edge_merging_attributes,
                         add_edge_id=add_edge_id,
                         edge_id_type=edge_id_type,
                         overwrite_edge_ids=overwrite_edge_ids)
        self.nodes = {}
        self.edges = {}
        self.pre_merge_edge_id_mapping = defaultdict(list)
        self.pre_merge_mapping_file_path = pre_merge_mapping_file_path

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
    def merge_edges(self, edges):
        edge_count = 0
        for edge in edges:
            edge_count += 1
            self.merge_edge(edge)
        return edge_count

    def merge_edge(self, edge):
        edge_key = edge_key_function(edge, custom_key_attributes=self.edge_merging_attributes,
                                     edge_id_type=self.edge_id_type)
        if edge_key in self.edges:
            self.merged_edge_counter += 1
            existing_edge = quick_json_loads(self.edges[edge_key])
            if self.add_edge_id and not self.overwrite_edge_ids:
                mapping = self.pre_merge_edge_id_mapping
                if edge_key not in mapping:
                    # Capture the stored edge's original id. Skip if it equals edge_key,
                    # that means the id was generated during merging or wasn't changed and doesn't need mapping.
                    existing_id = existing_edge.get(EDGE_ID)
                    if existing_id is not None and existing_id != edge_key:
                        mapping[edge_key].append(existing_id)
                new_id = edge.get(EDGE_ID)
                if new_id is not None:
                    mapping[edge_key].append(new_id)
            merged_edge = entity_merging_function(existing_edge, edge)
            if self.add_edge_id:
                merged_edge[EDGE_ID] = edge_key
            self.edges[edge_key] = quick_json_dumps(merged_edge)
        else:
            if self.add_edge_id and (self.overwrite_edge_ids or EDGE_ID not in edge):
                edge[EDGE_ID] = edge_key
            self.edges[edge_key] = quick_json_dumps(edge)

    def get_node_ids(self):
        return set(self.nodes.keys())

    def get_merged_nodes_jsonl(self):
        for node in self.nodes.values():
            yield f'{quick_json_dumps(node)}\n'
        self._record_merge_warnings(flush_merge_warnings())

    def get_merged_edges_jsonl(self):
        for edge in self.edges.values():
            yield f'{edge}\n'
        if self.pre_merge_mapping_file_path and self.pre_merge_edge_id_mapping:
            with open(self.pre_merge_mapping_file_path, 'w') as f:
                for post_merge_id, pre_merge_ids in self.pre_merge_edge_id_mapping.items():
                    f.write(f'{quick_json_dumps({"post_merge_id": post_merge_id, "pre_merge_ids": pre_merge_ids})}\n')
        self._record_merge_warnings(flush_merge_warnings())

    def get_pre_merge_edge_id_mapping(self):
        return self.pre_merge_edge_id_mapping
