from Common.merging import GraphMerger, MemoryGraphMerger, DiskGraphMerger
from Common.node_types import *
import os
import json

TEMP_DIRECTORY = os.path.dirname(os.path.abspath(__file__)) + '/workspace'


def node_property_merging_test(graph_merger: GraphMerger):

    test_nodes = [{'id': 'NODE:1',
                   'name': 'Node 1',
                   'category': [NAMED_THING],
                   'testing_prop': [i]}
                  for i in range(1, 11)]

    graph_merger.merge_nodes(test_nodes)

    merged_nodes = [json.loads(node) for node in graph_merger.get_merged_nodes_jsonl()]
    assert len(merged_nodes) == 1
    assert merged_nodes[0]['testing_prop'] == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def test_node_property_merging_in_memory():
    node_property_merging_test(MemoryGraphMerger())


def test_node_property_merging_on_disk():
    node_property_merging_test(DiskGraphMerger(temp_directory=TEMP_DIRECTORY, chunk_size=8))


def node_merging_counts_test(graph_merger: GraphMerger):

    nodes_1_20 = []
    for i in range(1, 21):
        node = {'id': f'NODE:{i}', 'name': f'Node {i}', 'category': [NAMED_THING]}
        nodes_1_20.append(node)

    nodes_6_25 = []
    for i in range(6, 26):
        node = {'id': f'NODE:{i}', 'name': f'Node {i}', 'category': [NAMED_THING]}
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
                   AGGREGATOR_KNOWLEDGE_SOURCES: [f'source_{i}', 'source_X']}
                  for i in range(1, 11)]

    graph_merger.merge_edges(test_edges)
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 1
    assert len(merged_edges[0]['testing_property']) == 10

    # AGGREGATOR_KNOWLEDGE_SOURCES is one of the properties
    # for which merging should collapse into a set of distinct values instead of appending blindly
    # in this case we should've ended up with source_1 through source_10 and source_X = 11 total
    assert len(merged_edges[0][AGGREGATOR_KNOWLEDGE_SOURCES]) == 11


def test_edge_property_merging_in_memory():
    edge_property_merging_test(MemoryGraphMerger())


def test_edge_property_merging_on_disk():
    edge_property_merging_test(DiskGraphMerger(temp_directory=TEMP_DIRECTORY, chunk_size=8))


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
