import csv
import json
import argparse
from collections import defaultdict
from orion.utils import quick_jsonl_file_iterator
from orion.biolink_constants import SUBJECT_ID, OBJECT_ID, PREDICATE


def __normalize_value(v):
    # Dicts become JSON strings
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)
    # Everything else (str, int, float, bool, None) as-is
    return v


def convert_jsonl_to_memgraph_cypher(nodes_input_file: str,
                                     edges_input_file: str,
                                     output_cypher_file: str,
                                     node_property_ignore_list=None,
                                     edge_property_ignore_list=None):
    """
    Convert nodes.jsonl and edges.jsonl into a .cypher file for Memgraph import.
    Each node becomes a CREATE statement with labels from `category`.
    Each edge becomes a CREATE statement with relationship type from `predicate`.

    Parameters
    ----------
    nodes_input_file : path to input nodes.jsonl file.
    edges_input_file : path to input edges.jsonl file.
    output_cypher_file : path to output .cypher file.
    node_property_ignore_list : set, optional, properties to ignore when writing node properties.
    edge_property_ignore_list : set, optional, properties to ignore when writing edge properties.
    """
    if not nodes_input_file or not nodes_input_file.endswith('jsonl'):
        raise Exception(f'Empty input node file or invalid file extension')
    if not edges_input_file or not edges_input_file.endswith('jsonl'):
        raise Exception(f'Empty input edge file or invalid file extension')
    if not output_cypher_file or not output_cypher_file.endswith('.cypher'):
        raise Exception(f'Empty output cypher file or invalid file extension')

    with open(output_cypher_file, "w", encoding="utf-8") as cypher_out:
        for node in quick_jsonl_file_iterator(nodes_input_file):
            if 'id' not in node:
                raise Exception('each node must include required property id')
            if 'name' not in node:
                raise Exception('each node must include required property name')
            if 'category' not in node:
                raise Exception(f'each node must include required property category')

            node_id = node["id"]
            categories = node.pop('category')
            if isinstance(categories, str):
                categories = [categories]
            # convert categories list to a labels string, add backticks to allow handling colons
            labels_str = ":".join(f"`{c}`" for c in categories) if categories else node_id

            if node_property_ignore_list:
                for ignore_key in node_property_ignore_list:
                    node.pop(ignore_key, None)

            props = {k: __normalize_value(v) for k, v in node.items()}
            props_str = "{" + ", ".join(f"{k}: {json.dumps(v, ensure_ascii=False)}" for k, v in props.items()) + "}"
            cypher_out.write(
                f"CREATE (:{labels_str} {props_str});\n"
            )
        for edge in quick_jsonl_file_iterator(edges_input_file):
            if SUBJECT_ID not in edge:
                raise Exception(f'each edge must include required property {SUBJECT_ID}')
            if OBJECT_ID not in edge:
                raise Exception(f'each edge must include required property {OBJECT_ID}')
            if PREDICATE not in edge:
                raise Exception(f'each edge must include required property {PREDICATE}')
            subj = edge[SUBJECT_ID]
            obj = edge[OBJECT_ID]
            predicate = edge[PREDICATE]

            if edge_property_ignore_list:
                for ignore_key in edge_property_ignore_list:
                    edge.pop(ignore_key, None)

            props = {k: __normalize_value(v) for k, v in edge.items() if k not in {SUBJECT_ID, OBJECT_ID, PREDICATE}}
            props_str = ", ".join(f"{k}: {json.dumps(v, ensure_ascii=False)}" for k, v in props.items())
            cypher_out.write(
                f"MATCH (a {{id: {json.dumps(subj, ensure_ascii=False)}}}), "
                f"(b {{id: {json.dumps(obj, ensure_ascii=False)}}}) "
                f"CREATE (a)-[:`{predicate}`"
                + (f" {{{props_str}}}" if props_str else "")
                + f"]->(b);\n"
            )


def convert_jsonl_to_neo4j_csv(nodes_input_file: str,
                               edges_input_file: str,
                               nodes_output_file: str = None,
                               edges_output_file: str = None,
                               output_delimiter='\t',
                               array_delimiter=chr(31),  # chr(31) = U+001F - Unit Separator
                               node_property_ignore_list=None,
                               edge_property_ignore_list=None):

    if not nodes_output_file:
        nodes_output_file = f'{nodes_input_file.rsplit(".")[0]}.csv'
    if not edges_output_file:
        edges_output_file = f'{edges_input_file.rsplit(".")[0]}.csv'

    # these will get converted into headers
    # required properties have unique/specialized types of the following instead of normal variable types
    required_node_properties = {
        'id': 'ID',
        'name': 'string',
        'category': 'LABEL'
    }
    node_properties = __determine_properties_and_types(nodes_input_file, required_node_properties)
    __convert_to_csv(input_file=nodes_input_file,
                     output_file=nodes_output_file,
                     properties=node_properties,
                     output_delimiter=output_delimiter,
                     array_delimiter=array_delimiter,
                     property_ignore_list=node_property_ignore_list)
    # __verify_conversion(nodes_output_file, node_properties, array_delimiter, output_delimiter)

    # these will get converted into headers
    # required properties have unique/specialized types of the following instead of normal variable types
    required_edge_properties = {
        SUBJECT_ID: 'START_ID',
        PREDICATE: 'TYPE',
        OBJECT_ID: 'END_ID'
    }
    edge_properties = __determine_properties_and_types(edges_input_file, required_edge_properties)
    __convert_to_csv(input_file=edges_input_file,
                     output_file=edges_output_file,
                     properties=edge_properties,
                     output_delimiter=output_delimiter,
                     array_delimiter=array_delimiter,
                     property_ignore_list=edge_property_ignore_list)
    # __verify_conversion(edges_output_file, edge_properties, array_delimiter, output_delimiter)

"""
def __verify_conversion(file_path: str,
                        properties: dict,
                        array_delimiter: str,
                        output_delimiter: str):
    counter = 0
    list_properties = [prop for prop in properties
                       if properties[prop] == "string[]"
                       or properties[prop] == "float[]"
                       or properties[prop] == "int[]"
                       or properties[prop] == 'LABEL']
    verified_properties = set()
    num_properties = len(properties.keys())
    with open(file_path, newline='') as file_handler:
        csv_reader = csv.reader(file_handler, delimiter=output_delimiter)
        next(csv_reader)
        for split_line in csv_reader:
            counter += 1
            if len(split_line) != num_properties:
                raise Exception(f'Number of fields mismatch on line {counter} - got {len(split_line)}'
                                f' expected {num_properties}: {split_line}')
            for (prop, value) in zip(properties.keys(), split_line):
                if not value:
                    continue
                if prop in list_properties:
                    value.split(array_delimiter)
                    verified_properties.add(prop)
                elif array_delimiter in value:
                    raise Exception(f'Array delimiter found in a non-list field - property {prop}: {value}')
                elif properties[prop] == 'float':
                    float(value)
                    verified_properties.add(prop)
                else:
                    str(value)
                    verified_properties.add(prop)
    if len(verified_properties) != num_properties:
        print(f'Not all properties were verified.. This should not happen..')
        print(f'Properties that were not verified: '
              f'{[prop for prop in properties.keys() if prop not in verified_properties]}')
"""

def __determine_properties_and_types(file_path: str, required_properties: dict):
    property_type_counts = defaultdict(lambda: defaultdict(int))
    for entity in quick_jsonl_file_iterator(file_path):
        for key, value in entity.items():
            if value is None:
                property_type_counts[key]["None"] += 1
                if key in required_properties and key != "name":
                    print(f'WARNING: Required property ({key}) was None: {entity.items()}')
                    raise Exception(
                        f'None found as a value for a required property (property: {key}) in line {entity.items()}')
            elif isinstance(value, bool):
                property_type_counts[key]["boolean"] += 1
            elif isinstance(value, int):
                property_type_counts[key]["int"] += 1
            elif isinstance(value, float):
                property_type_counts[key]["float"] += 1
            elif isinstance(value, list):
                has_floats = False
                has_ints = False
                has_strings = False
                for item in value:
                    if isinstance(item, float):
                        has_floats = True
                    elif isinstance(item, int):
                        has_ints = True
                    else:
                        has_strings = True
                if has_strings:
                    property_type_counts[key]["string[]"] += 1
                elif has_floats:
                    property_type_counts[key]["float[]"] += 1
                elif has_ints:
                    property_type_counts[key]["int[]"] += 1
            else:
                property_type_counts[key]["string"] += 1

    # start with the required_properties dictionary, it has the hard coded unique types for them already
    properties = required_properties.copy()
    properties_to_remove = []
    for prop, type_counts in property_type_counts.items():
        prop_types = list(type_counts.keys())
        num_prop_types = len(prop_types)

        # if 'None' in prop_types:
            # print(f'WARNING: None found as a value for property {prop}')

        if prop in required_properties and (num_prop_types > 1) and prop != "name":
            # TODO this should just enforce that required properties are the correct type,
            #  instead of trying to establish the type
            raise Exception(f'Required property {prop} had multiple conflicting types: {type_counts.items()}')
        elif prop in required_properties:
            # do nothing, already set
            pass
        elif num_prop_types == 1:
            # if the only prop type is None that means it had no values
            if prop_types[0] == "None":
                # set to remove from the properties list which means it won't be in the output files
                properties_to_remove.append(prop)
            else:
                # otherwise if only one type just set it to that
                properties[prop] = prop_types[0]
        else:
            # TODO: this probably needs more work
            # try to resolve conflicting types, attempt to pick the type that will accommodate all of the values
            # print(f'Property {prop} had conflicting types: {type_counts}')
            if 'string[]' in prop_types:
                properties[prop] = 'string[]'
            elif 'float[]' in prop_types:
                properties[prop] = 'float[]'
            elif 'int[]' in prop_types:
                properties[prop] = 'int[]'
            elif 'float' in prop_types and 'int' in prop_types and num_prop_types == 2:
                properties[prop] = 'float'
            elif 'float' in prop_types and 'None' in prop_types and num_prop_types == 2:
                properties[prop] = 'float'
            elif 'int' in prop_types and 'None' in prop_types and num_prop_types == 2:
                properties[prop] = 'int'
            else:
                properties[prop] = 'string'

        if prop not in properties and prop not in properties_to_remove:
            raise Exception(f'Property type could not be determined for: {prop}. {type_counts.items()}')

    # print(f'Found {len(properties)} properties:{properties.items()}')
    return properties


def __convert_to_csv(input_file: str,
                     output_file: str,
                     properties: dict,  # dictionary of { node/edge property: property_type }
                     array_delimiter: str,
                     output_delimiter: str,
                     property_ignore_list: set = None):

    # generate the headers which for neo4j include the property name and the type
    # for example:
    # id:ID	name:string	category:LABEL	equivalent_identifiers:string[]	information_content:float
    headers = {prop: f'{prop.removeprefix("biolink:")}:{prop_type}'
               for prop, prop_type in properties.items()}

    # if there is a property_ignore_list, remove them from the headers
    # also filter the list to include only properties that are actually present
    if property_ignore_list:
        ignored_props_present = set()
        for ignored_prop in property_ignore_list:
            if properties.pop(ignored_prop, 'PROP_NOT_FOUND') != 'PROP_NOT_FOUND':
                del headers[ignored_prop.removeprefix("biolink:")]
                ignored_props_present.add(ignored_prop)
        if not ignored_props_present:
            property_ignore_list = None
        else:
            property_ignore_list = ignored_props_present
            print(f'Properties that should be ignored were found, ignoring: {property_ignore_list}')

    properties_that_are_lists = {prop for prop, prop_type in properties.items()
                                 if prop_type in {'LABEL', 'string[]', 'float[]', 'int[]'}}
    properties_that_are_boolean = {prop for prop, prop_type in properties.items() if prop_type == 'boolean'}

    with open(output_file, 'w', newline='') as output_file_handler:
        csv_file_writer = csv.DictWriter(output_file_handler,
                                         delimiter=output_delimiter,
                                         fieldnames=properties,
                                         restval='',
                                         extrasaction='ignore',
                                         quoting=csv.QUOTE_MINIMAL)
        csv_file_writer.writerow(headers)
        for item in quick_jsonl_file_iterator(input_file):
            for key in list(item.keys()):
                if item[key] is None:
                    if key == "name":
                        item["name"] = item["id"]
                    else:
                        del item[key]
                elif property_ignore_list and key in property_ignore_list:
                    del item[key]
                else:
                    if key in properties_that_are_lists:
                        # convert lists into strings with an array delimiter
                        if isinstance(item[key], list):  # need to doublecheck for cases of properties with mixed types
                            item[key] = array_delimiter.join(str(value) for value in item[key])
                    elif key in properties_that_are_boolean:
                        # neo4j handles boolean with string 'true' being true and everything else false
                        item[key] = 'true' if item[key] is True else 'false'
            csv_file_writer.writerow(item)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert jsonl kgx files to csv neo4j import files')
    parser.add_argument('nodes', help='file with nodes in jsonl format')
    parser.add_argument('edges', help='file with edges in jsonl format')
    args = parser.parse_args()

    convert_jsonl_to_neo4j_csv(args.nodes, args.edges)
