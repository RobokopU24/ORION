from Common.merging import GraphMerger, MemoryGraphMerger, DiskGraphMerger
from Common.biolink_constants import *
import os
import json

TEMP_DIRECTORY = os.path.dirname(os.path.abspath(__file__)) + '/workspace'


def node_property_merging_test(graph_merger: GraphMerger):

    test_nodes = [{'id': 'NODE:1',
                   'name': 'Node 1',
                   NODE_TYPES: [NAMED_THING],
                   SYNONYMS: ['SYN_X', f'SYN_{i}'],
                   'testing_prop': [i]}
                  for i in range(1, 11)]

    graph_merger.merge_nodes(test_nodes)

    merged_nodes = [json.loads(node) for node in graph_merger.get_merged_nodes_jsonl()]
    assert len(merged_nodes) == 1
    merged_node = merged_nodes[0]
    assert merged_node['testing_prop'] == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert len(merged_node[SYNONYMS]) == 11
    assert 'SYN_X' in merged_node[SYNONYMS] and 'SYN_5' in merged_node[SYNONYMS]
    assert len(merged_node[NODE_TYPES]) == 1


def test_node_property_merging_in_memory():
    node_property_merging_test(MemoryGraphMerger())


def test_node_property_merging_on_disk():
    node_property_merging_test(DiskGraphMerger(temp_directory=TEMP_DIRECTORY, chunk_size=8))


def node_merging_counts_test(graph_merger: GraphMerger):

    nodes_1_20 = []
    for i in range(1, 21):
        node = {'id': f'NODE:{i}', 'name': f'Node {i}', NODE_TYPES: [NAMED_THING]}
        nodes_1_20.append(node)

    nodes_6_25 = []
    for i in range(6, 26):
        node = {'id': f'NODE:{i}', 'name': f'Node {i}', NODE_TYPES: [NAMED_THING]}
        nodes_6_25.append(node)

    input_node_counter = 0
    input_node_counter += graph_merger.merge_nodes(nodes_1_20)
    input_node_counter += graph_merger.merge_nodes(nodes_6_25)
    assert input_node_counter == 40

    merged_node_lines = list(graph_merger.get_merged_nodes_jsonl())
    assert len(merged_node_lines) == 25
    assert graph_merger.merged_node_counter == 15


def test_node_merging_counts_in_memory():
    node_property_merging_test(MemoryGraphMerger())


def test_node_merging_counts_on_disk():
    node_property_merging_test(DiskGraphMerger(temp_directory=TEMP_DIRECTORY, chunk_size=8))


def edge_property_merging_test(graph_merger: GraphMerger):
    test_edges = [{SUBJECT_ID: f'NODE:1',
                   PREDICATE: 'testing:predicate',
                   OBJECT_ID: f'NODE:2',
                   'testing_property': [i],
                   AGGREGATOR_KNOWLEDGE_SOURCES: [f'source_{i}', 'source_X'],
                   'abstract_id': f'test_abstract_id'}
                  for i in range(1, 11)]

    graph_merger.merge_edges(test_edges)
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 1
    assert len(merged_edges[0]['testing_property']) == 10

    # AGGREGATOR_KNOWLEDGE_SOURCES is one of the properties
    # for which merging should collapse into a set of distinct values instead of appending blindly
    # in this case we should've ended up with source_1 through source_10 and source_X = 11 total
    assert len(merged_edges[0][AGGREGATOR_KNOWLEDGE_SOURCES]) == 11
    assert 'id' not in merged_edges[0]

def edge_property_merging_test_edge_id_with_merging(graph_merger: GraphMerger):
    # test custom edge merging attributes with edge id addition
    test_edges = [{SUBJECT_ID: f'NODE:1',
                   PREDICATE: 'testing:predicate',
                   OBJECT_ID: f'NODE:2',
                   'testing_property': [i],
                   AGGREGATOR_KNOWLEDGE_SOURCES: [f'source_{i}', 'source_X'],
                   'abstract_id': f'test_abstract_id'}
                  for i in range(1, 11)]

    graph_merger.merge_edges(test_edges, additional_edge_attributes=['abstract_id'], add_edge_id=True)
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    print(merged_edges)
    assert len(merged_edges) == 1
    assert len(merged_edges[0]['testing_property']) == 10
    assert 'id' in merged_edges[0]
    assert merged_edges[0]['id'] is not None

def edge_property_merging_test_edge_id_without_merging(graph_merger: GraphMerger):
    # test custom edge merging attributes with edge id addition
    test_edges = [{SUBJECT_ID: f'NODE:1',
                   PREDICATE: 'testing:predicate',
                   OBJECT_ID: f'NODE:2',
                   'testing_property': [i],
                   AGGREGATOR_KNOWLEDGE_SOURCES: [f'source_{i}', 'source_X'],
                   'abstract_id': f'test_abstract_id_{i}'}
                  for i in range(1, 11)]

    graph_merger.merge_edges(test_edges, additional_edge_attributes=['abstract_id'], add_edge_id=True)
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 10
    assert 'id' in merged_edges[0]
    assert merged_edges[0]['id'] is not None

def test_edge_property_merging_in_memory():
    edge_property_merging_test(MemoryGraphMerger())

def test_edge_property_merging_on_disk():
   edge_property_merging_test(DiskGraphMerger(temp_directory=TEMP_DIRECTORY, chunk_size=8))

def test_edge_property_merging_in_memory_edge_id_with_merging():
    edge_property_merging_test_edge_id_with_merging(MemoryGraphMerger())

def test_edge_property_merging_in_memory_edge_id_without_merging():
    edge_property_merging_test_edge_id_without_merging(MemoryGraphMerger())

def test_edge_property_merging_on_disk_edge_id_with_merging():
    edge_property_merging_test_edge_id_with_merging(DiskGraphMerger(temp_directory=TEMP_DIRECTORY, chunk_size=8))

def test_edge_property_merging_on_dist_edge_id_without_merging():
    edge_property_merging_test_edge_id_without_merging(DiskGraphMerger(temp_directory=TEMP_DIRECTORY, chunk_size=8))

def edge_merging_counts_test(graph_merger: GraphMerger):

    edges_1_10_pred_1 = []
    edges_1_10_pred_2 = []
    for i in range(1, 11):
        edge = {SUBJECT_ID: f'NODE:{i}',
                PREDICATE: 'testing:predicate_1',
                OBJECT_ID: f'NODE:{i+1}',
                'testing_property': [f'{i}']}
        edges_1_10_pred_1.append(edge)
        edge = {SUBJECT_ID: f'NODE:{i}',
                PREDICATE: 'testing:predicate_2',
                OBJECT_ID: f'NODE:{i+1}',
                'testing_property': [f'{i}']}
        edges_1_10_pred_2.append(edge)
    edges_6_20_pred_1 = []
    edges_6_20_pred_2 = []
    for i in range(6, 21):
        edge = {SUBJECT_ID: f'NODE:{i}',
                PREDICATE: 'testing:predicate_1',
                OBJECT_ID: f'NODE:{i+1}',
                'testing_property': [f'{i}']}
        edges_6_20_pred_1.append(edge)
        edge = {SUBJECT_ID: f'NODE:{i}',
                PREDICATE: 'testing:predicate_2',
                OBJECT_ID: f'NODE:{i+1}',
                'testing_property': [f'{i}']}
        edges_6_20_pred_2.append(edge)

    input_edge_counter = 0
    input_edge_counter += graph_merger.merge_edges(edges_1_10_pred_1)
    input_edge_counter += graph_merger.merge_edges(edges_1_10_pred_2)
    input_edge_counter += graph_merger.merge_edges(edges_6_20_pred_1)
    input_edge_counter += graph_merger.merge_edges(edges_6_20_pred_2)
    assert input_edge_counter == 50

    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 40
    for edge in merged_edges:
        if int(edge[SUBJECT_ID].split(':')[1]) < 6:
            assert len(edge['testing_property']) == 1
        elif int(edge[SUBJECT_ID].split(':')[1]) < 11:
            assert len(edge['testing_property']) == 2
        else:
            assert len(edge['testing_property']) == 1

    assert graph_merger.merged_edge_counter == 10


def test_edge_merging_counts_in_memory():
    edge_merging_counts_test(MemoryGraphMerger())


def test_edge_merging_counts_on_disk():
    edge_merging_counts_test(DiskGraphMerger(temp_directory=TEMP_DIRECTORY, chunk_size=8))


def test_qualifier_edge_merging():

    test_edges_up = [{SUBJECT_ID: f'NODE:1',
                      PREDICATE: 'testing:predicate',
                      OBJECT_ID: f'NODE:2',
                      SUBJECT_ASPECT_QUALIFIER: f'test_aspect',
                      SUBJECT_DIRECTION_QUALIFIER: 'up',
                      'testing_prop': [i]}
                     for i in range(1, 16)]

    test_edges_down = [{SUBJECT_ID: f'NODE:1',
                        PREDICATE: 'testing:predicate',
                        OBJECT_ID: f'NODE:2',
                        SUBJECT_ASPECT_QUALIFIER: f'test_aspect',
                        SUBJECT_DIRECTION_QUALIFIER: 'down',
                        'testing_prop': [i]}
                       for i in range(1, 11)]

    test_edges_other = [{SUBJECT_ID: f'NODE:1',
                         PREDICATE: 'testing:predicate',
                         OBJECT_ID: f'NODE:2',
                         SUBJECT_ASPECT_QUALIFIER: f'test_aspect',
                         SUBJECT_DIRECTION_QUALIFIER: 'down',
                         SPECIES_CONTEXT_QUALIFIER: 'test_species',
                         'testing_prop': [i]}
                        for i in range(1, 6)]
    graph_merger = MemoryGraphMerger()
    graph_merger.merge_edges(test_edges_up)
    graph_merger.merge_edges(test_edges_down)
    graph_merger.merge_edges(test_edges_other)

    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 3

    passed_tests = 0
    for edge in merged_edges:
        if edge[SUBJECT_DIRECTION_QUALIFIER] == 'up':
            assert len(edge['testing_prop']) == 15
            assert SPECIES_CONTEXT_QUALIFIER not in edge
            passed_tests += 1
        elif edge[SUBJECT_DIRECTION_QUALIFIER] == 'down' and SPECIES_CONTEXT_QUALIFIER not in edge:
            assert len(edge['testing_prop']) == 10
            passed_tests += 1
        elif edge[SUBJECT_DIRECTION_QUALIFIER] == 'down' and SPECIES_CONTEXT_QUALIFIER in edge:
            assert len(edge['testing_prop']) == 5
            passed_tests += 1

    assert passed_tests == 3


