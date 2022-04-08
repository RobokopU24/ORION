import csv
import argparse
from Common.utils import quick_jsonl_file_iterator
from Common.node_types import SUBJECT_ID, OBJECT_ID, PREDICATE

# from kgx.transformer import Transformer


def __verify_conversion(file_path: str,
                        property_types: dict,
                        array_delimiter: str,
                        output_delimiter: str):
    counter = 0
    list_properties = [prop for prop in property_types
                       if property_types[prop] == "string[]" or property_types[prop] == 'LABEL']
    verified_properties = set()
    num_properties = len(property_types.keys())
    with open(file_path, newline='') as file_handler:
        csv_reader = csv.reader(file_handler, delimiter=output_delimiter)
        next(csv_reader)
        for split_line in csv_reader:
            counter += 1
            if len(split_line) != num_properties:
                raise Exception(f'Number of fields mismatch on line {counter} - got {len(split_line)}'
                                f' expected {len(property_types)}: {line}')
            for (prop, value) in zip(property_types.keys(), split_line):
                if not value:
                    continue
                if prop in list_properties:
                    attempted_list_unpack = value.split(array_delimiter)
                    verified_properties.add(prop)
                elif array_delimiter in value:
                    raise Exception(f'Array delimiter found in a non-list field - property {prop}: {value}')
                elif property_types[prop] == 'float':
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


def __determine_properties_and_types(file_path: str, properties: list, property_types: dict):
    for entity in quick_jsonl_file_iterator(file_path):
        for key, value in entity.items():
            if key not in properties:
                properties.append(key)
                if isinstance(value, int) or isinstance(value, float):
                    property_types[key] = "double"
                elif isinstance(value, list):
                    property_types[key] = "string[]"
                elif isinstance(value, bool):
                    property_types[key] = "boolean"
                else:
                    property_types[key] = "string"
    print(f'Found {len(properties)} properties:{properties}')


def __convert_to_csv(input_file: str,
                     output_file: str,
                     properties: list,
                     property_types: dict,
                     array_delimiter: str,
                     output_delimiter: str):

    headers = {prop: f'{prop}:{property_types[prop]}' for prop in properties}
    with open(output_file, 'w', newline='') as output_file_handler:
        csv_file_writer = csv.DictWriter(output_file_handler,
                                         delimiter=output_delimiter,
                                         fieldnames=properties,
                                         restval='',
                                         quoting=csv.QUOTE_MINIMAL)
        csv_file_writer.writerow(headers)
        for node in quick_jsonl_file_iterator(input_file):
            for prop, prop_type in property_types.items():
                if prop_type == 'string[]' or prop_type == 'LABEL':
                    if prop in node:
                        # convert lists into strings with an array delimiter
                        node[prop] = array_delimiter.join(node[prop])
                elif prop_type == 'boolean':
                    if prop in node:
                        # neo4j handles boolean with string 'true' being true and everything else false
                        node[prop] = 'true' if node[prop] is True else 'false'
            csv_file_writer.writerow(node)


def convert_jsonl_to_neo4j_csv(nodes_input_file: str,
                               edges_input_file: str,
                               nodes_output_file: str = None,
                               edges_output_file: str = None):

    output_delimiter = '\t'
    array_delimiter = chr(31)  # U+001F - Unit Separator

    if not nodes_output_file:
        nodes_output_file = f'{nodes_input_file.rsplit(".")[0]}.csv'
    if not edges_output_file:
        edges_output_file = f'{edges_input_file.rsplit(".")[0]}.csv'

    node_properties = ['id', 'name', 'category']
    node_property_types = {
        'id': 'ID',
        'name': 'string',
        'category': 'LABEL'
    }
    __determine_properties_and_types(nodes_input_file, node_properties, node_property_types)
    __convert_to_csv(input_file=nodes_input_file,
                     output_file=nodes_output_file,
                     properties=node_properties,
                     property_types=node_property_types,
                     output_delimiter=output_delimiter,
                     array_delimiter=array_delimiter)
    __verify_conversion(nodes_output_file, node_property_types, array_delimiter, output_delimiter)

    edge_properties = [SUBJECT_ID, PREDICATE, OBJECT_ID]
    edge_property_types = {
        SUBJECT_ID: 'START_ID',
        PREDICATE: 'TYPE',
        OBJECT_ID: 'END_ID'
    }
    __determine_properties_and_types(edges_input_file, edge_properties, edge_property_types)
    __convert_to_csv(input_file=edges_input_file,
                     output_file=edges_output_file,
                     properties=edge_properties,
                     property_types=edge_property_types,
                     output_delimiter=output_delimiter,
                     array_delimiter=array_delimiter)
    __verify_conversion(edges_output_file, edge_property_types, array_delimiter, output_delimiter)

    """

    
    # if we ever want to use KGX to do it
    
    input_args = {
        'filename': [nodes_input_file, edges_input_file],
        'format': 'jsonl'
    }
    output_args = {
        'filename': nodes_output_file.rsplit(".")[0],
        'list_delimiter': '\x31',
        'format': 'tsv'
    }
    t = Transformer(stream=True)
    t.transform(input_args, output_args)
    """


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Convert jsonl kgx files to csv neo4j import files')
    parser.add_argument('nodes', help='file with nodes in jsonl format')
    parser.add_argument('edges', help='file with edges in jsonl format')
    args = parser.parse_args()

    convert_jsonl_to_neo4j_csv(args.nodes, args.edges)
