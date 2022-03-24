from Common.merging import GraphMerger
from Common.utils import quick_json_loads
from Common.node_types import *


def test_node_property_merging():

    graph_merger = GraphMerger()
    throwaway_graph_merger = GraphMerger()
    for i in range(1, 11):
        node = {'id': 'NODE:1',
                'name': 'Node 1',
                'category': [NAMED_THING],
                'testing_prop': [i]}
        graph_merger.merge_nodes([node])
        throwaway_graph_merger.merge_nodes([node], throw_out_duplicates=True)

    assert len(list(graph_merger.get_merged_nodes_lines())) == 1
    node = quick_json_loads(next(graph_merger.get_merged_nodes_lines()))
    assert len(node['testing_prop']) == 10

    assert len(list(throwaway_graph_merger.get_merged_nodes_lines())) == 1
    node = quick_json_loads(next(throwaway_graph_merger.get_merged_nodes_lines()))
    assert node['testing_prop'] == [1]


def test_node_merging_counts():

    graph_merger = GraphMerger()
    nodes_1_20 = []
    for i in range(1, 21):
        node = {'id': f'NODE:{i}', 'name': f'Node {i}', 'category': [NAMED_THING]}
        nodes_1_20.append(node)

    nodes_6_25 = []
    for i in range(6, 26):
        node = {'id': f'NODE:{i}', 'name': f'Node {i}', 'category': [NAMED_THING]}
        nodes_6_25.append(node)

    node_count, merged_nodes = graph_merger.merge_nodes(nodes_1_20)
    assert (node_count, merged_nodes) == (20, 0)
    node_count, merged_nodes = graph_merger.merge_nodes(nodes_6_25)
    assert (node_count, merged_nodes) == (20, 15)
    assert len(list(graph_merger.get_merged_nodes_lines())) == 25


def test_edge_property_merging():

    graph_merger = GraphMerger()
    throwaway_graph_merger = GraphMerger()
    for i in range(1, 11):
        edge = {SUBJECT_ID: f'NODE:1',
                PREDICATE: 'testing:predicate',
                OBJECT_ID: f'NODE:2',
                'testing_property': [i],
                AGGREGATOR_KNOWLEDGE_SOURCES: [f'source_{i}', 'source_X']}
        graph_merger.merge_edges([edge])
        throwaway_graph_merger.merge_edges([edge], throw_out_duplicates=True)

    assert len(list(graph_merger.get_merged_edges_lines())) == 1
    edge = quick_json_loads(next(graph_merger.get_merged_edges_lines()))
    assert len(edge['testing_property']) == 10

    # AGGREGATOR_KNOWLEDGE_SOURCES is one of the properties
    # for which merging should collapse into a set of distinct values instead of appending blindly
    # in this case we should've ended up with source_1 through source_10 and source_X = 11 total
    assert len(edge[AGGREGATOR_KNOWLEDGE_SOURCES]) == 11

    # for the throwaway flag merger we expect only the properties from the first edge in
    assert len(list(throwaway_graph_merger.get_merged_edges_lines())) == 1
    edge = quick_json_loads(next(throwaway_graph_merger.get_merged_edges_lines()))
    assert edge['testing_property'] == [1]
    assert len(edge[AGGREGATOR_KNOWLEDGE_SOURCES]) == 2


def test_edge_merging_counts():

    graph_merger = GraphMerger()
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

    edge_count, merged_edges = graph_merger.merge_edges(edges_1_10_pred_1)
    assert (edge_count, merged_edges) == (10, 0)
    edge_count, merged_edges = graph_merger.merge_edges(edges_1_10_pred_2)
    assert (edge_count, merged_edges) == (10, 0)
    edge_count, merged_edges = graph_merger.merge_edges(edges_6_20_pred_1)
    assert (edge_count, merged_edges) == (15, 5)
    edge_count, merged_edges = graph_merger.merge_edges(edges_6_20_pred_2)
    assert (edge_count, merged_edges) == (15, 5)

    assert len(list(graph_merger.get_merged_edges_lines())) == 40

    for edge in [quick_json_loads(edge) for edge in graph_merger.get_merged_edges_lines()]:
        if int(edge[SUBJECT_ID].split(':')[1]) < 6:
            assert len(edge['testing_property']) == 1
        elif int(edge[SUBJECT_ID].split(':')[1]) < 11:
            assert len(edge['testing_property']) == 2
        else:
            assert len(edge['testing_property']) == 1


