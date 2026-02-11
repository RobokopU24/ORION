import csv
import json
import argparse
from collections import defaultdict
from Common.utils import quick_jsonl_file_iterator
from Common.biolink_constants import SUBJECT_ID, OBJECT_ID, PREDICATE, NAMED_THING


def _is_cypher_primitive(x):
    return x is None or isinstance(x, (str, int, float, bool))


def _normalize_value(v):
    if _is_cypher_primitive(v):
        return v

    # lists are only allowed if all elements are primitives
    if isinstance(v, list) and all(_is_cypher_primitive(x) for x in v):
        return v

    # other non-primitive values are not allowed - need to stringify
    return json.dumps(v, ensure_ascii=False)


def convert_nodes_to_memgraph_cypher(nodes_input_file: str, output_cypher_file: str, node_property_ignore_list=None):
    """
    Convert nodes_input_file (e.g., nodes.jsonl) into a .cypher file for Memgraph import.
    Each node becomes a CREATE statement with labels from `category`.
    :param nodes_input_file: path to input nodes jsonl file
    :param output_cypher_file: path to output .cypher file
    :param node_property_ignore_list: set, optional, properties to ignore when writing node properties
    :return: all_node_labels set that includes unique categories of all nodes for later indexing
    """
    if not nodes_input_file or not nodes_input_file.endswith('jsonl'):
        raise Exception(f'Empty input node file or invalid file extension')
    if not output_cypher_file or not output_cypher_file.endswith('.cypher'):
        raise Exception(f'Empty output cypher file or invalid file extension')

    all_node_labels = set()

    with open(output_cypher_file, "w", encoding="utf-8") as cypher_out:
        for node in quick_jsonl_file_iterator(nodes_input_file):
            if "id" not in node or "name" not in node or "category" not in node:
                raise Exception("Each node must include required properties: id, name, category")

            categories = node.pop("category")
            if isinstance(categories, str):
                categories = [categories]

            for c in categories:
                all_node_labels.add(c)

            # convert categories list to a labels string, add backticks to allow handling colons
            labels_str = ":" + ":".join(f"`{c}`" for c in categories)

            if node_property_ignore_list:
                for ignore_key in node_property_ignore_list:
                    node.pop(ignore_key, None)

            props = {k: _normalize_value(v) for k, v in node.items()}
            props_str = "{" + ", ".join(f"{k}: {json.dumps(v, ensure_ascii=False)}" for k, v in props.items()) + "}"

            cypher_out.write(f"CREATE ({labels_str} {props_str});\n")

    return all_node_labels


def add_indexes_to_memgraph_cypher(all_node_labels: set, output_cypher_file: str):
    """
    add indexes to nodes names and ids for fast edge insertion and query
    :param all_node_labels: all node labels set returned from convert_nodes_to_memgraph_cypher()
    :param output_cypher_file: path to output .cypher file
    :return:
    """
    with open(output_cypher_file, "w", encoding="utf-8") as cypher_out:
        cypher_out.write(f"CREATE INDEX ON :`{NAMED_THING}`(name);\n")

        for label in sorted(all_node_labels):
            cypher_out.write(f"CREATE INDEX ON :`{label}`(id);\n")


def _to_cypher_literal(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        # producing a valid Cypher string literal
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        return "[" + ", ".join(_to_cypher_literal(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(f"{k}: {_to_cypher_literal(val)}" for k, val in v.items()) + "}"
    raise TypeError(f"Unsupported type: {type(v)}")


def convert_edges_to_memgraph_cypher(edges_input_file: str, output_cypher_file: str,
                                     edge_property_ignore_list=None,
                                     edge_batch_size=1000):
    """
    Convert edges_input_file (e.g., edges.jsonl) into a .cypher file for Memgraph import.
    Each edge becomes a CREATE statement with relationship type from `predicate`.
    :param edges_input_file: path to input edges.jsonl file.
    :param output_cypher_file: path to output .cypher file.
    :param edge_property_ignore_list: set, optional, properties to ignore when writing edge properties.
    :param edge_batch_size: the batch size for committing edge insertions in parallel for better performance
    :return:
    """
    if not edges_input_file or not edges_input_file.endswith('jsonl'):
        raise Exception(f'Empty input edge file or invalid file extension')
    if not output_cypher_file or not output_cypher_file.endswith('.cypher'):
        raise Exception(f'Empty output cypher file or invalid file extension')

    edge_batches = defaultdict(list)

    with open(output_cypher_file, "w", encoding="utf-8") as cypher_out:

        def flush_batches():
            for predicate, batch in edge_batches.items():
                if not batch:
                    continue

                batch_literal = "[" + ", ".join(_to_cypher_literal(e) for e in batch) + "]"

                cypher_out.write(
                    f"UNWIND {batch_literal} AS e\n"
                    f"WITH e.src AS src, e.dst AS dst, e.props AS props\n"
                    f"MATCH (a {{id: src}}), (b {{id: dst}})\n"
                    f"CREATE (a)-[r:`{predicate}`]->(b)\n"
                    f"SET r += props;\n\n"
                )

            edge_batches.clear()

        for edge in quick_jsonl_file_iterator(edges_input_file):
            if SUBJECT_ID not in edge or OBJECT_ID not in edge or PREDICATE not in edge:
                raise Exception(f"Edge missing required fields {SUBJECT_ID}, {OBJECT_ID}, or {PREDICATE}")

            predicate = edge[PREDICATE]

            if edge_property_ignore_list:
                for ignore_key in edge_property_ignore_list:
                    edge.pop(ignore_key, None)

            props = {
                k: _normalize_value(v) for k, v in edge.items() if k not in {SUBJECT_ID, OBJECT_ID, PREDICATE}
            }

            edge_batches[predicate].append({
                "src": edge[SUBJECT_ID], "dst": edge[OBJECT_ID], "props": props
            })

            if len(edge_batches[predicate]) >= edge_batch_size:
                flush_batches()

        flush_batches()


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
                # detect list-of-dicts and convert it into string since neo4j cannot handle dicts as properties
                if any(isinstance(v, dict) for v in value):
                    property_type_counts[key]["string"] += 1
                else:
                    has_floats = any(isinstance(v, float) for v in value)
                    has_ints = any(isinstance(v, int) for v in value)
                    has_strings = any(isinstance(v, str) for v in value)
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
                elif isinstance(item[key], dict) or (
                        isinstance(item[key], list) and any(isinstance(v, dict) for v in item[key])
                ):
                    # dump dict as compact JSON string since neo4j cannot handle dict as property
                    item[key] = json.dumps(item[key], separators=(',', ':'), ensure_ascii=False)
                else:
                    if key in properties_that_are_lists:
                        # convert lists into strings with an array delimiter
                        if isinstance(item[key], list):  # need to doublecheck for cases of properties with mixed types
                            # strip newline and \t characters to prevent neo4j import errors for input such as
                            # "publications":["\nPMID:\n    18224415\t"]
                            item[key] = array_delimiter.join(''.join([s.strip() for s in str(value).splitlines()]) for value in item[key])
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
