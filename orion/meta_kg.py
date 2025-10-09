import json
import jsonlines
from collections import defaultdict

from orion.biolink_constants import NODE_TYPES, SUBJECT_ID, OBJECT_ID, PREDICATE, PRIMARY_KNOWLEDGE_SOURCE, AGGREGATOR_KNOWLEDGE_SOURCES
from orion.utils import quick_jsonl_file_iterator
from orion.biolink_utils import BiolinkUtils

BL_ATTRIBUTE_MAP = {
    "equivalent_identifiers": "biolink:same_as",
    "endogenous": "aragorn:endogenous"
}

META_KG_FILENAME = 'meta_knowledge_graph.json'
TEST_DATA_FILENAME = 'testing_data.json'
EXAMPLE_DATA_FILENAME = 'example_edges.jsonl'


####
# This class is responsible for generating a meta knowledge graph, as defined by the NCATSTranslator ReasonerAPI
# It also generates sample testing data (specific examples for each edge type) for usage by the SRI testing harness.
####
class MetaKnowledgeGraphBuilder:

    def __init__(self,
                 nodes_file_path: str,
                 edges_file_path: str,
                 logger=None):
        self.logger = logger
        self.bl_utils = BiolinkUtils()

        self.node_id_to_leaf_types = None
        self.meta_kg = {
            "nodes": {},
            "edges": []
        }
        self.testing_data = {
            "source_type": "primary",
            "edges": []
        }
        self.example_edges = []
        self.analyze_nodes(nodes_file_path)
        self.analyze_edges(edges_file_path)

    ####
    # Walk through the nodes to find metadata about each node type.
    # Create a set of unique curie prefixes and attribute metadata for each node type.
    # In the process create a map of node ids to their types that are leaves in the biolink model.
    ####
    def analyze_nodes(self, nodes_file_path: str):

        core_node_attributes = {'id', 'name', NODE_TYPES}

        node_id_to_leaf_types = {}
        node_type_to_curie_prefixes = defaultdict(set)
        node_type_to_attributes = defaultdict(set)
        node_attribute_to_metadata = {}

        for node in quick_jsonl_file_iterator(nodes_file_path):

            # find the leaf node types of this node's types according to the biolink model
            try:
                leaf_types = self.bl_utils.find_biolink_leaves(frozenset(node[NODE_TYPES]))
            except TypeError:
                error_message = f'Node types were not a valid list for node: {node}'
                leaf_types = {}
                if self.logger:
                    self.logger.error(error_message)
                else:
                    print(error_message)

            # store the leaf types for this node id
            node_id_to_leaf_types[node['id']] = leaf_types

            # generate metadata for attributes on the node other than the core attributes
            node_attributes = [key for key in node.keys() if key not in core_node_attributes]
            for node_attribute in node_attributes:
                if node_attribute not in node_attribute_to_metadata:
                    node_attribute_to_metadata[node_attribute] = self.get_meta_attribute(node_attribute)

            curie_prefix = node['id'].split(":")[0]
            for node_type in leaf_types:
                # add the curie prefix from the node id to the set for each leaf node type
                node_type_to_curie_prefixes[node_type].add(curie_prefix)
                node_type_to_attributes[node_type].update(node_attributes)

        self.node_id_to_leaf_types = node_id_to_leaf_types

        self.meta_kg['nodes'] = {
            node_type: {'id_prefixes': list(node_type_to_curie_prefixes[node_type]),
                        'attributes': [node_attribute_to_metadata[attribute]
                                       for attribute in node_type_to_attributes[node_type]]}
            for node_type in node_type_to_curie_prefixes.keys()}

    def analyze_edges(self, edges_file_path: str):

        core_attributes = {SUBJECT_ID, PREDICATE, OBJECT_ID, PRIMARY_KNOWLEDGE_SOURCE, AGGREGATOR_KNOWLEDGE_SOURCES}
        node_id_to_leaf_types = self.node_id_to_leaf_types  # local reference for speed

        edge_attribute_to_metadata = {}
        edge_type_key_to_attributes = defaultdict(set)
        edge_type_key_to_qualifiers = defaultdict(lambda: defaultdict(set))
        edge_type_key_to_example = {}
        edge_types = defaultdict(lambda: defaultdict(set))  # subject_id to object_id to set of predicates

        for edge in quick_jsonl_file_iterator(edges_file_path):
            try:
                subject_types = node_id_to_leaf_types[edge[SUBJECT_ID]]
                object_types = node_id_to_leaf_types[edge[OBJECT_ID]]
            except KeyError as e:
                error_message = f'Leaf node types not found for node: {e}. '\
                                f'Make sure the node is present in the nodes file.'
                if self.logger:
                    self.logger.error(error_message)
                else:
                    print(error_message)
                continue

            edge_qualifiers = {}
            edge_attributes = []
            for key, value in edge.items():
                if key in core_attributes or value is None:
                    continue
                if self.bl_utils.is_qualifier(key):
                    edge_qualifiers[key] = value
                else:
                    edge_attributes.append(key)
                    if key not in edge_attribute_to_metadata:
                        edge_attribute_to_metadata[key] = self.get_meta_attribute(key)

            predicate = edge[PREDICATE]
            for subject_type in subject_types:
                for object_type in object_types:
                    edge_types[subject_type][object_type].add(predicate)
                    inverse_predicate = self.bl_utils.invert_predicate(predicate)
                    if inverse_predicate:
                        edge_types[object_type][subject_type].add(inverse_predicate)

                    edge_type_key = f'{subject_type}{object_type}{predicate}'
                    edge_type_key_to_attributes[edge_type_key].update(edge_attributes)
                    for qual, qual_val in edge_qualifiers.items():
                        try:
                            edge_type_key_to_qualifiers[edge_type_key][qual].add(qual_val)
                        except TypeError as e:
                            error_message = f'Type of value for qualifier not expected: {qual}: {qual_val}, '\
                                            f'ignoring for meta kg. Error: {e}'
                            if self.logger:
                                self.logger.warning(error_message)
                            else:
                                print(error_message)

                    if edge_type_key not in edge_type_key_to_example:
                        example_edge = {
                            "subject_category": subject_type,
                            "object_category": object_type,
                            "predicate": predicate,
                            "subject_id": edge[SUBJECT_ID],
                            "object_id": edge[OBJECT_ID]
                        }
                        if edge_qualifiers:
                            example_edge['qualifiers'] = [
                                {"qualifier_type_id": f"biolink:{qualifier}" if not qualifier.startswith("biolink:") else qualifier,
                                 "qualifier_value": qualifier_value}
                                for qualifier, qualifier_value in edge_qualifiers.items()
                            ]
                        edge_type_key_to_example[edge_type_key] = example_edge
                        self.example_edges.append(edge)

        for subject_node_type, object_types_to_predicates in edge_types.items():
            for object_node_type, predicates in object_types_to_predicates.items():
                for predicate in predicates:
                    edge_type_key = f'{subject_node_type}{object_node_type}{predicate}'
                    edge_metadata = {
                        'subject': subject_node_type,
                        'predicate': predicate,
                        'object': object_node_type,
                        'attributes': [edge_attribute_to_metadata[attribute_metadata]
                                       for attribute_metadata in edge_type_key_to_attributes[edge_type_key]],
                        'qualifiers': [{'qualifier_type_id': qualifier,
                                        'applicable_values': list(qual_vals)}
                                       for qualifier, qual_vals in edge_type_key_to_qualifiers[edge_type_key].items()]
                    }
                    self.meta_kg['edges'].append(edge_metadata)
                    if edge_type_key in edge_type_key_to_example:
                        self.testing_data['edges'].append(edge_type_key_to_example[edge_type_key])

    def get_meta_attribute(self, attribute_name):
        original_attribute_name = attribute_name
        # handle known edge cases by converting attributes in BL_ATTRIBUTE_MAP to preferred attribute type ids
        attribute_type_id = BL_ATTRIBUTE_MAP[attribute_name] if attribute_name in BL_ATTRIBUTE_MAP \
            else self.bl_utils.get_attribute_type_id(attribute_name)
        if not attribute_type_id:
            attribute_type_id = "biolink:Attribute"
        meta_attribute = {
            "attribute_type_id": attribute_type_id,
            "attribute_source": None,
            "original_attribute_names": [
                original_attribute_name
            ],
            "constraint_use": False,
            "constraint_name": None
        }
        return meta_attribute

    def write_meta_kg_to_file(self, output_file_path: str):
        with open(output_file_path, 'w') as meta_kg_file:
            meta_kg_file.write(json.dumps(self.meta_kg, indent=4))

    def write_test_data_to_file(self, output_file_path: str):
        with open(output_file_path, 'w') as test_data_file:
            test_data_file.write(json.dumps(self.testing_data, indent=4))

    def write_example_data_to_file(self, output_file_path: str):
        with open(output_file_path, 'w') as output_file_handler:
            with jsonlines.Writer(output_file_handler) as writer:
                writer.write_all(self.example_edges)
