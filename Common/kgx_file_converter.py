import csv
import argparse
from collections import defaultdict
from Common.utils import quick_jsonl_file_iterator
from Common.node_types import SUBJECT_ID, OBJECT_ID, PREDICATE
# from kgx.transformer import Transformer


def convert_jsonl_to_neo4j_csv(nodes_input_file: str,
                               edges_input_file: str,
                               nodes_output_file: str = None,
                               edges_output_file: str = None,
                               output_delimiter='\t',
                               array_delimiter=chr(31)):  # chr(31) = U+001F - Unit Separator

    if not nodes_output_file:
        nodes_output_file = f'{nodes_input_file.rsplit(".")[0]}.csv'
    if not edges_output_file:
        edges_output_file = f'{edges_input_file.rsplit(".")[0]}.csv'

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
                     array_delimiter=array_delimiter)
    __verify_conversion(nodes_output_file, node_properties, array_delimiter, output_delimiter)

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
                     array_delimiter=array_delimiter)
    __verify_conversion(edges_output_file, edge_properties, array_delimiter, output_delimiter)


def __verify_conversion(file_path: str,
                        properties: dict,
                        array_delimiter: str,
                        output_delimiter: str):
    counter = 0
    list_properties = [prop for prop in properties
                       if properties[prop] == "string[]" or properties[prop] == 'LABEL']
    verified_properties = set()
    num_properties = len(properties.keys())
    with open(file_path, newline='') as file_handler:
        csv_reader = csv.reader(file_handler, delimiter=output_delimiter)
        next(csv_reader)
        for split_line in csv_reader:
            counter += 1
            if len(split_line) != num_properties:
                raise Exception(f'Number of fields mismatch on line {counter} - got {len(split_line)}'
                                f' expected {len(property_types)}: {line}')
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
              f'{[prop for prop in property_types.keys() if prop not in verified_properties]}')
    else:
        print(f'Passed verification step: {file_path}')


def __determine_properties_and_types(file_path: str, required_properties: dict):
    property_type_counts = defaultdict(lambda: defaultdict(int))
    for entity in quick_jsonl_file_iterator(file_path):
        for key, value in entity.items():
            if value is None:
                property_type_counts[key]["None"] += 1
                if key in required_properties:
                    print(f'WARNING: Required property None: {entity.items()}')
            elif isinstance(value, bool):
                property_type_counts[key]["boolean"] += 1
            elif isinstance(value, int):
                property_type_counts[key]["int"] += 1
            elif isinstance(value, float):
                property_type_counts[key]["float"] += 1
            elif isinstance(value, list):
                property_type_counts[key]["string[]"] += 1
            else:
                property_type_counts[key]["string"] += 1

    properties = required_properties.copy()
    for prop, type_counts in property_type_counts.items():
        prop_types = list(type_counts.keys())
        num_prop_types = len(prop_types)
        has_type_conflicts = num_prop_types > 1

        if 'None' in prop_types:
            print(f'WARNING: None found as a value for property {prop}, that should not happen!')
            if prop in required_properties:
                raise Exception(f'None found as a value for a required property - {type_counts.items()}')

        if prop in required_properties and has_type_conflicts:
            raise Exception(f'Required property {prop} had multiple conflicting types: {type_counts.items()}')
        elif not has_type_conflicts:
            properties[prop] = prop_types[0]
        else:
            if 'string[]' in prop_types:
                properties[prop] = 'string[]'
            elif 'float' in prop_types and 'int' in prop_types and num_prop_types == 2:
                properties[prop] = 'float'
            elif 'float' in prop_types and 'None' in prop_types and num_prop_types == 2:
                properties[prop] = 'float'
            elif 'int' in prop_types and 'None' in prop_types and num_prop_types == 2:
                properties[prop] = 'int'
            else:
                properties[prop] = 'string'

        if prop not in properties:
            raise Exception(f'Property type could not be determined for: {prop}. {type_counts.items()}')

    print(f'Found {len(properties)} properties:{properties.items()}')
    return properties


def __convert_to_csv(input_file: str,
                     output_file: str,
                     properties: dict,  # dictionary of { node/edge property: property_type }
                     array_delimiter: str,
                     output_delimiter: str):

    headers = {prop: f'{prop}:{prop_type}' for prop, prop_type in properties.items()}
    with open(output_file, 'w', newline='') as output_file_handler:
        csv_file_writer = csv.DictWriter(output_file_handler,
                                         delimiter=output_delimiter,
                                         fieldnames=properties,
                                         restval='',
                                         quoting=csv.QUOTE_MINIMAL)
        csv_file_writer.writerow(headers)
        for item in quick_jsonl_file_iterator(input_file):
            for key in list(item.keys()):
                if item[key] is None:
                    del item[key]
                else:
                    prop_type = properties[key]
                    if prop_type == 'string[]' or prop_type == 'LABEL':
                        # convert lists into strings with an array delimiter
                        item[key] = array_delimiter.join(item[key])
                    elif prop_type == 'boolean':
                        # neo4j handles boolean with string 'true' being true and everything else false
                        item[key] = 'true' if item[key] is True else 'false'
            csv_file_writer.writerow(item)




if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Convert jsonl kgx files to csv neo4j import files')
    parser.add_argument('nodes', help='file with nodes in jsonl format')
    parser.add_argument('edges', help='file with edges in jsonl format')
    args = parser.parse_args()

    convert_jsonl_to_neo4j_csv(args.nodes, args.edges)
