"""Tests for the Neptune csv converter and the load files it produces.

These check the parts of Neptune's openCypher csv format that differ from the neo4j import format
the rest of the converter targets: reserved column names, capitalized type names, comma delimiting,
";" between labels, and arrays written as one delimited String instead of a real array column.
"""

import csv
import gzip
import json
import os

import pytest

from orion.kgx_neptune_converter import (NEPTUNE_ARRAY_DELIMITER,
                                         NEPTUNE_LABEL_DELIMITER,
                                         convert_jsonl_to_neptune_csv)
from orion.neptune_tools import NEPTUNE_MANIFEST_FILENAME, create_neptune_csvs, read_neptune_manifest


TEST_NODES = [
    {'id': 'CHEBI:1', 'name': 'a molecule, with a comma',
     'category': ['biolink:SmallMolecule', 'biolink:NamedThing'],
     'equivalent_identifiers': ['CHEBI:1', 'PUBCHEM.COMPOUND:9'],
     'information_content': 12.5, 'ignore_me': 'x'},
    {'id': 'NCBIGene:2', 'name': None, 'category': ['biolink:Gene'],
     'publication_count': 7, 'is_cool': True, 'nested': {'a': 1}},
]

TEST_EDGES = [
    {'id': 'edge_1', 'subject': 'CHEBI:1', 'predicate': 'biolink:affects', 'object': 'NCBIGene:2',
     'publications': ['PMID:1', 'PMID:2'], 'primary_knowledge_source': 'infores:x'},
    {'id': 'edge_2', 'subject': 'NCBIGene:2', 'predicate': 'biolink:related_to', 'object': 'CHEBI:1',
     'primary_knowledge_source': 'infores:y'},
]


def write_jsonl(filepath, entities):
    with open(filepath, 'w') as jsonl_file:
        for entity in entities:
            jsonl_file.write(f'{json.dumps(entity)}\n')


@pytest.fixture
def kgx_files(tmp_path):
    nodes_filepath = str(tmp_path / 'nodes.jsonl')
    edges_filepath = str(tmp_path / 'edges.jsonl')
    write_jsonl(nodes_filepath, TEST_NODES)
    write_jsonl(edges_filepath, TEST_EDGES)
    return nodes_filepath, edges_filepath


def read_csv(filepath):
    """Read a Neptune csv into (headers, list of row dicts keyed by header)."""
    opener = gzip.open if filepath.endswith('.gz') else open
    with opener(filepath, 'rt', newline='', encoding='utf-8') as csv_file:
        rows = list(csv.reader(csv_file, delimiter=','))
    headers = rows[0]
    return headers, [dict(zip(headers, row)) for row in rows[1:]]


def convert(tmp_path, kgx_files, **kwargs):
    nodes_filepath, edges_filepath = kgx_files
    nodes_output = str(tmp_path / 'nodes.csv')
    edges_output = str(tmp_path / 'edges.csv')
    edge_ids_included = convert_jsonl_to_neptune_csv(nodes_input_file=nodes_filepath,
                                                     edges_input_file=edges_filepath,
                                                     nodes_output_file=nodes_output,
                                                     edges_output_file=edges_output,
                                                     **kwargs)
    return nodes_output, edges_output, edge_ids_included


def test_node_headers_use_neptune_system_columns_and_types(tmp_path, kgx_files):
    nodes_output, _, _ = convert(tmp_path, kgx_files)
    headers, _ = read_csv(nodes_output)

    # the node id is written as id:ID so it is both the node id and a queryable property
    assert 'id:ID' in headers
    assert ':LABEL' in headers
    # category is only the label column, it is not also written as a property
    assert not any(header.startswith('category:') for header in headers)
    # Neptune's type names are capitalized, and arrays collapse to String
    assert 'name:String' in headers
    assert 'information_content:Double' in headers
    assert 'publication_count:Long' in headers
    assert 'is_cool:Bool' in headers
    assert 'equivalent_identifiers:String' in headers


def test_edge_headers_use_bare_system_columns(tmp_path, kgx_files):
    _, edges_output, edge_ids_included = convert(tmp_path, kgx_files)
    headers, _ = read_csv(edges_output)

    assert edge_ids_included is True
    # relationship system columns have no property name prefix, unlike the node id column
    assert ':ID' in headers
    assert ':START_ID' in headers
    assert ':END_ID' in headers
    assert ':TYPE' in headers
    assert 'subject:START_ID' not in headers
    assert 'predicate:TYPE' not in headers
    assert 'publications:String' in headers


def test_labels_are_semicolon_delimited_and_arrays_are_not(tmp_path, kgx_files):
    nodes_output, edges_output, _ = convert(tmp_path, kgx_files)
    _, node_rows = read_csv(nodes_output)
    _, edge_rows = read_csv(edges_output)

    # Neptune splits :LABEL on ";" and offers no way to configure it
    assert node_rows[0][':LABEL'] == f'biolink:SmallMolecule{NEPTUNE_LABEL_DELIMITER}biolink:NamedThing'
    # every other array is a single String value, so it keeps the array delimiter
    assert node_rows[0]['equivalent_identifiers:String'] == \
        f'CHEBI:1{NEPTUNE_ARRAY_DELIMITER}PUBCHEM.COMPOUND:9'
    assert edge_rows[0]['publications:String'] == f'PMID:1{NEPTUNE_ARRAY_DELIMITER}PMID:2'


def test_values_are_rfc_4180_quoted(tmp_path, kgx_files):
    nodes_output, _, _ = convert(tmp_path, kgx_files)
    _, node_rows = read_csv(nodes_output)

    # a comma inside a value has to survive the comma delimited format
    assert node_rows[0]['name:String'] == 'a molecule, with a comma'
    # dicts have no Neptune equivalent so they are written as json, quotes and all
    assert node_rows[1]['nested:String'] == '{"a":1}'


def test_missing_node_name_falls_back_to_id(tmp_path, kgx_files):
    nodes_output, _, _ = convert(tmp_path, kgx_files)
    _, node_rows = read_csv(nodes_output)
    assert node_rows[1]['name:String'] == 'NCBIGene:2'


def test_ignored_properties_are_not_written(tmp_path, kgx_files):
    nodes_output, _, _ = convert(tmp_path, kgx_files, node_property_ignore_list={'ignore_me'})
    headers, _ = read_csv(nodes_output)
    assert not any(header.startswith('ignore_me') for header in headers)


@pytest.fixture
def kgx_files_without_edge_ids(tmp_path):
    nodes_filepath = str(tmp_path / 'nodes.jsonl')
    edges_filepath = str(tmp_path / 'edges.jsonl')
    write_jsonl(nodes_filepath, TEST_NODES)
    write_jsonl(edges_filepath, [{key: value for key, value in edge.items() if key != 'id'}
                                 for edge in TEST_EDGES])
    return nodes_filepath, edges_filepath


def test_edges_without_ids_are_numbered_sequentially(tmp_path, kgx_files_without_edge_ids):
    _, edges_output, edge_ids_included = convert(tmp_path, kgx_files_without_edge_ids)

    assert edge_ids_included is True
    _, edge_rows = read_csv(edges_output)
    assert [row[':ID'] for row in edge_rows] == ['1', '2']


def test_provided_edge_ids_are_kept_instead_of_generated_ones(tmp_path, kgx_files):
    _, edges_output, _ = convert(tmp_path, kgx_files)
    _, edge_rows = read_csv(edges_output)
    assert [row[':ID'] for row in edge_rows] == ['edge_1', 'edge_2']


def test_edges_without_ids_omit_the_id_column_when_generation_is_off(tmp_path,
                                                                    kgx_files_without_edge_ids):
    _, edges_output, edge_ids_included = convert(tmp_path, kgx_files_without_edge_ids,
                                                 generate_edge_ids=False)

    # Neptune rejects a relationship file with an empty :ID column, so it has to be absent entirely
    # and the load has to run with userProvidedEdgeIds set to false.
    assert edge_ids_included is False
    headers, _ = read_csv(edges_output)
    assert ':ID' not in headers


def test_edge_missing_an_id_in_a_file_that_has_them_raises(tmp_path):
    nodes_filepath = str(tmp_path / 'nodes.jsonl')
    edges_filepath = str(tmp_path / 'edges.jsonl')
    write_jsonl(nodes_filepath, TEST_NODES)
    # ids are generated for a file that has none, never for part of one - a generated id could
    # collide with an id already in the file, and Neptune reads two rows sharing a relationship
    # :ID as one relationship written twice.
    write_jsonl(edges_filepath, TEST_EDGES + [{'subject': 'CHEBI:1', 'object': 'NCBIGene:2',
                                               'predicate': 'biolink:affects'}])
    with pytest.raises(Exception, match='Required columns'):
        convert(tmp_path, (nodes_filepath, edges_filepath))


def test_edge_missing_a_required_column_raises(tmp_path):
    nodes_filepath = str(tmp_path / 'nodes.jsonl')
    edges_filepath = str(tmp_path / 'edges.jsonl')
    write_jsonl(nodes_filepath, TEST_NODES)
    write_jsonl(edges_filepath, TEST_EDGES + [{'id': 'edge_3', 'subject': 'CHEBI:1',
                                               'predicate': 'biolink:affects'}])
    with pytest.raises(Exception, match='Required columns'):
        convert(tmp_path, (nodes_filepath, edges_filepath))


def test_property_name_neptune_cannot_express_raises(tmp_path):
    nodes_filepath = str(tmp_path / 'nodes.jsonl')
    edges_filepath = str(tmp_path / 'edges.jsonl')
    write_jsonl(nodes_filepath, [dict(TEST_NODES[0], **{'a bad name': 'value'})])
    write_jsonl(edges_filepath, TEST_EDGES)
    with pytest.raises(Exception, match='column header'):
        convert(tmp_path, (nodes_filepath, edges_filepath))


def test_gzipped_kgx_files_are_read_as_input(tmp_path):
    nodes_filepath = str(tmp_path / 'nodes.jsonl.gz')
    edges_filepath = str(tmp_path / 'edges.jsonl.gz')
    for filepath, entities in ((nodes_filepath, TEST_NODES), (edges_filepath, TEST_EDGES)):
        with gzip.open(filepath, 'wt') as jsonl_file:
            for entity in entities:
                jsonl_file.write(f'{json.dumps(entity)}\n')

    nodes_output, edges_output, _ = convert(tmp_path, (nodes_filepath, edges_filepath))

    for output_file in (nodes_output, edges_output):
        _, rows = read_csv(output_file)
        assert len(rows) == 2


def test_input_files_that_are_not_kgx_jsonl_are_rejected(tmp_path, kgx_files):
    nodes_filepath, _ = kgx_files
    with pytest.raises(Exception, match='invalid file extension'):
        convert(tmp_path, (nodes_filepath, str(tmp_path / 'edges.csv')))


def test_create_neptune_csvs_writes_gzipped_files_and_manifest(tmp_path, kgx_files):
    nodes_filepath, edges_filepath = kgx_files
    output_directory = tmp_path / 'output'
    output_directory.mkdir()

    assert create_neptune_csvs(nodes_filepath=nodes_filepath,
                               edges_filepath=edges_filepath,
                               output_directory=str(output_directory),
                               graph_id='TestGraph',
                               release_version='1.0.0') is True

    manifest = read_neptune_manifest(str(output_directory))
    assert manifest['format'] == 'opencypher'
    assert manifest['userProvidedEdgeIds'] is True
    # nodes and edges are listed apart because they load as two jobs from two S3 prefixes
    assert manifest['nodes'] == ['neptune_TestGraph_1.0.0_nodes.csv.gz']
    assert manifest['edges'] == ['neptune_TestGraph_1.0.0_edges.csv.gz']

    manifest_files = manifest['nodes'] + manifest['edges']
    for filename in manifest_files:
        headers, rows = read_csv(str(output_directory / filename))
        assert len(rows) == 2

    # the manifest is not a data file, the loader would try to parse it as one
    assert NEPTUNE_MANIFEST_FILENAME not in manifest_files


def test_create_neptune_csvs_records_generated_edge_ids_in_the_manifest(
        tmp_path, kgx_files_without_edge_ids):
    nodes_filepath, edges_filepath = kgx_files_without_edge_ids
    output_directory = tmp_path / 'output'
    output_directory.mkdir()

    create_neptune_csvs(nodes_filepath, edges_filepath, str(output_directory), graph_id='TestGraph')

    # the ids are in the csv either way, so the load provides them
    assert read_neptune_manifest(str(output_directory))['userProvidedEdgeIds'] is True


def test_create_neptune_csvs_skips_work_when_already_done(tmp_path, kgx_files):
    nodes_filepath, edges_filepath = kgx_files
    output_directory = tmp_path / 'output'
    output_directory.mkdir()

    create_neptune_csvs(nodes_filepath, edges_filepath, str(output_directory), graph_id='TestGraph')
    nodes_csv = output_directory / 'neptune_TestGraph_nodes.csv.gz'
    modified_time = os.path.getmtime(nodes_csv)

    create_neptune_csvs(nodes_filepath, edges_filepath, str(output_directory), graph_id='TestGraph')
    assert os.path.getmtime(nodes_csv) == modified_time


def test_create_neptune_csvs_does_the_work_when_the_manifest_describes_something_else(tmp_path,
                                                                                      kgx_files):
    nodes_filepath, edges_filepath = kgx_files
    output_directory = tmp_path / 'output'
    output_directory.mkdir()

    create_neptune_csvs(nodes_filepath, edges_filepath, str(output_directory), graph_id='GraphA')

    # a different graph in the same directory is not the work this manifest describes
    create_neptune_csvs(nodes_filepath, edges_filepath, str(output_directory), graph_id='GraphB')
    manifest = read_neptune_manifest(str(output_directory))
    assert manifest['graph_id'] == 'GraphB'
    assert manifest['nodes'] == ['neptune_GraphB_nodes.csv.gz']
    assert (output_directory / 'neptune_GraphB_nodes.csv.gz').exists()

    # neither is the same graph written with a different compression setting
    create_neptune_csvs(nodes_filepath, edges_filepath, str(output_directory), graph_id='GraphB',
                        compress=False)
    manifest = read_neptune_manifest(str(output_directory))
    assert manifest['nodes'] == ['neptune_GraphB_nodes.csv']
    assert (output_directory / 'neptune_GraphB_nodes.csv').exists()