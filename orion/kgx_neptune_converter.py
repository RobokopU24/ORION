"""Convert KGX jsonl files into csv files for the Amazon Neptune bulk loader.

Neptune's openCypher csv format is close enough to the neo4j import format to share the property
type inference and row writing in kgx_file_converter, but it reserves different column names, uses
different type names, and fixes the delimiters that neo4j lets the caller pick. Everything Neptune
dictates lives here.

The output is loaded by POSTing to a Neptune cluster's /loader endpoint with format=opencypher,
after the files have been copied to S3. See orion.neptune_tools.
"""

from orion.biolink_constants import SUBJECT_ID, OBJECT_ID, PREDICATE, EDGE_ID
from orion.kgx_file_converter import _determine_properties_and_types, _convert_to_csv
from orion.logging import get_orion_logger

logger = get_orion_logger("orion.kgx_neptune_converter")


# These map onto Neptune's reserved column names the way REQUIRED_NODE_PROPERTIES maps onto neo4j's.
# The node id is written as "id:ID", a form Neptune reads as both the node id and a regular
# property, so n.id still resolves in queries. Relationship ids have no such form and use a bare
# ":ID" header, so they get a specialized type of their own rather than reusing 'ID'.
REQUIRED_NEPTUNE_NODE_PROPERTIES = {
    'id': 'ID',
    'name': 'string',
    'category': 'LABEL'
}

REQUIRED_NEPTUNE_EDGE_PROPERTIES = {
    SUBJECT_ID: 'START_ID',
    PREDICATE: 'TYPE',
    OBJECT_ID: 'END_ID'
}

NEPTUNE_SYSTEM_HEADERS = {
    'LABEL': ':LABEL',
    'START_ID': ':START_ID',
    'END_ID': ':END_ID',
    'TYPE': ':TYPE',
    'EDGE_ID': ':ID'
}

# Neptune fails a load when one of these columns is blank on a row, and the failure surfaces only
# after the files have been uploaded and the loader has run, so the writer checks them itself.
NEPTUNE_REQUIRED_COLUMN_TYPES = frozenset({'ID', 'EDGE_ID', 'START_ID', 'END_ID', 'TYPE'})

# Neptune type names for the property types that _determine_properties_and_types produces.
# Every array type becomes a String. Neptune has no list cardinality: an array loaded into a
# property becomes a multi-valued property, and openCypher reads one of those values arbitrarily,
# which makes the result non-deterministic. Writing the values as a single delimited String keeps
# all of them readable, and Neptune's split() turns the String back into a list at query time.
NEPTUNE_PROPERTY_TYPES = {
    'string': 'String',
    'int': 'Long',
    'float': 'Double',
    'boolean': 'Bool',
    'string[]': 'String',
    'int[]': 'String',
    'float[]': 'String'
}

# Neptune separates the labels within a :LABEL column with ";" and offers no way to configure it.
NEPTUNE_LABEL_DELIMITER = ';'

# Neptune parses its headers by splitting on these characters, so a property whose name contains
# one of them produces a column header that means something other than that property.
NEPTUNE_ILLEGAL_HEADER_CHARACTERS = (' ', ',', '\r', '\n', ':')

# The Neptune bulk loader reads comma delimited RFC 4180 files and has no delimiter setting.
NEPTUNE_OUTPUT_DELIMITER = ','

# chr(31) = U+001F - Unit Separator. Neptune sees the joined arrays as opaque String values, so this
# only has to be a character that never appears in the values themselves.
NEPTUNE_ARRAY_DELIMITER = chr(31)

# quick_jsonl_file_iterator() gzip decodes a .jsonl.gz input as it reads it.
VALID_INPUT_EXTENSIONS = ('.jsonl', '.jsonl.gz')

VALID_OUTPUT_EXTENSIONS = ('.csv', '.csv.gz')


def _neptune_header(prop: str, prop_type: str):
    """Render the Neptune csv column header for one property."""
    header_name = prop.removeprefix('biolink:')
    illegal_characters = [character for character in NEPTUNE_ILLEGAL_HEADER_CHARACTERS
                          if character in header_name]
    if illegal_characters:
        raise Exception(f'Property "{prop}" can not be written to a Neptune csv file, its name contains '
                        f'characters Neptune does not allow in a column header: {illegal_characters}. '
                        f'Rename the property or add it to the property ignore list.')
    if prop_type == 'ID':
        return f'{header_name}:ID'
    if prop_type in NEPTUNE_SYSTEM_HEADERS:
        return NEPTUNE_SYSTEM_HEADERS[prop_type]
    if prop_type not in NEPTUNE_PROPERTY_TYPES:
        raise Exception(f'Property "{prop}" has type {prop_type}, which has no Neptune equivalent.')
    return f'{header_name}:{NEPTUNE_PROPERTY_TYPES[prop_type]}'


def _label_delimiters(properties: dict):
    """The properties written to a :LABEL column, which uses Neptune's delimiter instead of ours."""
    return {prop: NEPTUNE_LABEL_DELIMITER
            for prop, prop_type in properties.items() if prop_type == 'LABEL'}


def _required_columns(properties: dict):
    return {prop for prop, prop_type in properties.items()
            if prop_type in NEPTUNE_REQUIRED_COLUMN_TYPES}


def _drop_properties_with_commas(properties: dict, entity_type: str):
    """Leave properties whose names contain a comma out of the csv files.

    TODO temporary - CHEBIProps builds its role property names out of ChEBI role labels and keeps the
     commas in them, so a graph including that source has hundreds of properties _neptune_header
     rejects. Dropping them here loads the rest of the graph until those names are sanitized at the
     source, where the same commas make the properties awkward to query in neo4j too.
    """
    properties_with_commas = [prop for prop in properties if ',' in prop]
    for prop in properties_with_commas:
        del properties[prop]
    if properties_with_commas:
        logger.warning(f'Dropped {len(properties_with_commas)} {entity_type} properties with commas in their '
                       f'names, Neptune can not write them as column headers: {properties_with_commas}')
    return properties


def _validate_file_paths(input_file: str, output_file: str):
    if not input_file or not input_file.endswith(VALID_INPUT_EXTENSIONS):
        raise Exception(f'Empty input file or invalid file extension (must be one of '
                        f'{VALID_INPUT_EXTENSIONS}): {input_file}')
    if not output_file or not output_file.endswith(VALID_OUTPUT_EXTENSIONS):
        raise Exception(f'Empty output file or invalid file extension (must be one of '
                        f'{VALID_OUTPUT_EXTENSIONS}): {output_file}')


def convert_jsonl_to_neptune_csv(nodes_input_file: str,
                                 edges_input_file: str,
                                 nodes_output_file: str,
                                 edges_output_file: str,
                                 array_delimiter: str = NEPTUNE_ARRAY_DELIMITER,
                                 node_property_ignore_list: set = None,
                                 edge_property_ignore_list: set = None):
    """Write Neptune openCypher csv files for a pair of KGX jsonl files.

    Output files named .csv.gz are gzipped, which the Neptune bulk loader reads directly.

    Returns whether the edges file has relationship ids, which determines the userProvidedEdgeIds
    setting the load has to use.
    """
    _validate_file_paths(nodes_input_file, nodes_output_file)
    _validate_file_paths(edges_input_file, edges_output_file)

    node_properties = _determine_properties_and_types(nodes_input_file,
                                                      REQUIRED_NEPTUNE_NODE_PROPERTIES)
    _drop_properties_with_commas(node_properties, 'node')
    _convert_to_csv(input_file=nodes_input_file,
                    output_file=nodes_output_file,
                    properties=node_properties,
                    output_delimiter=NEPTUNE_OUTPUT_DELIMITER,
                    array_delimiter=array_delimiter,
                    header_renderer=_neptune_header,
                    array_delimiter_overrides=_label_delimiters(node_properties),
                    required_columns=_required_columns(node_properties),
                    property_ignore_list=node_property_ignore_list)

    edge_properties = _determine_properties_and_types(edges_input_file,
                                                      REQUIRED_NEPTUNE_EDGE_PROPERTIES)
    _drop_properties_with_commas(edge_properties, 'edge')
    # Edge ids are optional in KGX, they come from the graph spec's add_edge_id option. When they
    # are present they become Neptune's relationship :ID column, which lets a failed load resume
    # rather than reloading every relationship, and lets the loader detect duplicate relationships.
    edge_ids_included = EDGE_ID in edge_properties
    if edge_ids_included:
        edge_properties[EDGE_ID] = 'EDGE_ID'
    _convert_to_csv(input_file=edges_input_file,
                    output_file=edges_output_file,
                    properties=edge_properties,
                    output_delimiter=NEPTUNE_OUTPUT_DELIMITER,
                    array_delimiter=array_delimiter,
                    header_renderer=_neptune_header,
                    array_delimiter_overrides=_label_delimiters(edge_properties),
                    required_columns=_required_columns(edge_properties),
                    property_ignore_list=edge_property_ignore_list)
    return edge_ids_included