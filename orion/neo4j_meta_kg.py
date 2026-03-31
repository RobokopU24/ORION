import time
import json
import os
from collections import defaultdict
from orion.neo4j_tools import Neo4jTools
from orion.biolink_constants import NAMED_THING
from orion.biolink_utils import BiolinkUtils


class Neo4jMetaKGGenerator:

    def __init__(self, logger):
        self.logger = logger
        self.bl_utils = BiolinkUtils()

    # This was mostly adapted (stolen) from Plater
    def generate_meta_kg_and_sri_test_data(self, neo4j_access: Neo4jTools, output_directory: str):

        # used to keep track of derived inverted predicates
        inverted_predicate_tracker = defaultdict(lambda: defaultdict(set))

        schema_query = """ MATCH (a)-[x]->(b) RETURN DISTINCT labels(a) as source_labels, type(x) as predicate, labels(b) as target_labels"""
        self.logger.info(f"Starting schema query {schema_query} on graph... this might take a few.")
        before_time = time.time()
        schema_query_results = neo4j_access.execute_read_cypher_query(schema_query)
        after_time = time.time()
        self.logger.info(f"Completed schema query ({after_time - before_time} seconds). Preparing initial schema.")

        schema = defaultdict(lambda: defaultdict(set))
        #  avoids adding nodes with only a NAMED_THING label (currently NamedThing)
        filter_named_thing = lambda x: frozenset(filter(lambda y: y != NAMED_THING, x))
        for schema_result in schema_query_results:
            source_labels, predicate, target_labels = \
                self.bl_utils.find_biolink_leaves(filter_named_thing(schema_result['source_labels'])), \
                schema_result['predicate'], \
                self.bl_utils.find_biolink_leaves(filter_named_thing(schema_result['target_labels']))
            for source_label in source_labels:
                for target_label in target_labels:
                    schema[source_label][target_label].add(predicate)

        # find and add the inverse for each predicate if there is one,
        # keep track of inverted predicates we added so we don't query the graph for them
        for source_label in list(schema.keys()):
            for target_label in list(schema[source_label].keys()):
                inverted_predicates = set()
                for predicate in schema[source_label][target_label]:
                    inverse_predicate = self.bl_utils.invert_predicate(predicate)
                    if inverse_predicate is not None and \
                            inverse_predicate not in schema[target_label][source_label]:
                        inverted_predicates.add(inverse_predicate)
                        inverted_predicate_tracker[target_label][source_label].add(inverse_predicate)
                schema[target_label][source_label].update(inverted_predicates)

        meta_kg_nodes = {}
        meta_kg_edges = []
        test_edges = []
        self.logger.info(f"Starting curie prefix and example edge queries...")
        before_time = time.time()
        for subject_node_type in schema:
            if subject_node_type not in meta_kg_nodes:
                curies, attributes = self.get_curie_prefixes_by_node_type(neo4j_access,
                                                                          subject_node_type)
                meta_kg_nodes[subject_node_type] = {'id_prefixes': curies, "attributes": attributes}
            for object_node_type in schema[subject_node_type]:
                if object_node_type not in meta_kg_nodes:
                    curies, attributes = self.get_curie_prefixes_by_node_type(neo4j_access,
                                                                              object_node_type)
                    meta_kg_nodes[object_node_type] = {'id_prefixes': curies, "attributes": attributes}
                for predicate in schema[subject_node_type][object_node_type]:
                    meta_kg_edges.append({
                        'subject': subject_node_type,
                        'object': object_node_type,
                        'predicate': predicate
                    })
                    if predicate not in inverted_predicate_tracker[subject_node_type][object_node_type]:
                        has_qualifiers = self.bl_utils.predicate_has_qualifiers(predicate)
                        example_edges = self.get_examples(neo4j_access=neo4j_access,
                                                          subject_node_type=subject_node_type,
                                                          object_node_type=object_node_type,
                                                          predicate=predicate,
                                                          num_examples=1,
                                                          use_qualifiers=has_qualifiers)

                        # sometimes a predicate could have qualifiers but there is not an example of one
                        if not example_edges and has_qualifiers:
                            example_edges = self.get_examples(neo4j_access=neo4j_access,
                                                              subject_node_type=subject_node_type,
                                                              object_node_type=object_node_type,
                                                              predicate=predicate,
                                                              num_examples=1,
                                                              use_qualifiers=False)

                        if example_edges:
                            neo4j_subject = example_edges[0]['subject']
                            neo4j_object = example_edges[0]['object']
                            neo4j_edge = example_edges[0]['edge']
                            test_edge = {
                                "subject_category": subject_node_type,
                                "object_category": object_node_type,
                                "predicate": predicate,
                                "subject_id": neo4j_subject['id'],
                                "object_id": neo4j_object['id']
                            }
                            if has_qualifiers:
                                qualifiers = []
                                for prop in neo4j_edge:
                                    if 'qualifie' in prop:
                                        qualifiers.append({
                                            "qualifier_type_id": f"biolink:{prop}" if not prop.startswith(
                                                "biolink:") else prop,
                                            "qualifier_value": neo4j_edge[prop]
                                        })
                                if qualifiers:
                                    test_edge["qualifiers"] = qualifiers
                            test_edges.append(test_edge)
                        else:
                            self.logger.info(f'Failed to find an example for '
                                             f'{subject_node_type}->{predicate}->{object_node_type}')

        after_time = time.time()
        self.logger.info(f"Completed curie prefix and example queries ({after_time - before_time} seconds).")
        self.logger.info \
            (f'Meta KG and SRI Testing data complete. Generated {len(test_edges)} test edges. Writing to file..')

        meta_kg = {
            "nodes": meta_kg_nodes,
            "edges": meta_kg_edges
        }
        meta_kg_file_path = os.path.join(output_directory, 'neo4j_generated_meta_kg.json')
        with open(meta_kg_file_path, 'w') as meta_kg_file:
            meta_kg_file.write(json.dumps(meta_kg, indent=4))

        testing_data = {
            "source_type": "primary",
            "edges": test_edges
        }
        testing_data_file_path = os.path.join(output_directory, 'neo4j_generated_test_data.json')
        with open(testing_data_file_path, 'w') as testing_data_file:
            testing_data_file.write(json.dumps(testing_data, indent=4))

    def get_curie_prefixes_by_node_type(self, neo4j_access: Neo4jTools, node_type: str):
        curies_query = f"""
        MATCH (n:`{node_type}`) return collect(n.id) as ids , collect(keys(n)) as attributes
        """
        self.logger.debug(f"Starting {node_type} curies query... this might take a few.")
        before_time = time.time()
        curie_query_results = neo4j_access.execute_read_cypher_query(curies_query)
        after_time = time.time()
        self.logger.debug(f"Completed {node_type} curies query ({after_time - before_time} seconds).")

        curie_prefixes = set()
        for i in curie_query_results[0]['ids']:
            curie_prefixes.add(i.split(':')[0])
        # sort according to bl model - this can throw an exception if id_prefixes are not found, default to empty
        try:
            node_bl_def = self.bl_utils.toolkit.get_element(node_type)
            id_prefixes = node_bl_def.id_prefixes
            sorted_curie_prefixes = [i for i in id_prefixes if i in curie_prefixes]  # gives precedence to what's in BL
        except Exception as e:
            sorted_curie_prefixes = []
        # add other ids even if not in BL next
        sorted_curie_prefixes += [i for i in curie_prefixes if i not in sorted_curie_prefixes]
        all_keys = set()
        for keys in curie_query_results[0]['attributes']:
            for k in keys:
                all_keys.add(k)

        attributes_as_bl_types = []
        for key in all_keys:
            attr_data = self.bl_utils.get_attribute_bl_info(key)
            if attr_data:
                attr_data['original_attribute_names'] = [key]
                attributes_as_bl_types.append(attr_data)
        return sorted_curie_prefixes, attributes_as_bl_types

    def get_examples(self,
                     neo4j_access: Neo4jTools,
                     subject_node_type,
                     object_node_type,
                     predicate=None,
                     num_examples=1,
                     use_qualifiers=False):
        """
        return example edges
        """
        qualifiers_check = " WHERE edge.qualified_predicate IS NOT NULL " if use_qualifiers else ""
        if object_node_type and predicate:
            query = f"MATCH (subject:`{subject_node_type}`)-[edge:`{predicate}`]->(object:`{object_node_type}`) " \
                    f"{qualifiers_check} return subject, edge, object limit {num_examples}"
            response = neo4j_access.execute_read_cypher_query(query)
            return response
        elif object_node_type:
            query = f"MATCH (subject:`{subject_node_type}`)-[edge]->(object:`{object_node_type}`) " \
                    f"{qualifiers_check} return subject, edge, object limit {num_examples}"
            response = neo4j_access.execute_read_cypher_query(query)
            return response