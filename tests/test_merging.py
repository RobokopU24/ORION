from orion.merging import GraphMerger, MemoryGraphMerger, DiskGraphMerger, NODE_ENTITY_TYPE, EDGE_ENTITY_TYPE
from orion.biolink_constants import *
import os
import json
import uuid


def make_node(node_num, **properties):
    node = {'id': f'NODE:{node_num}', 'name': f'Node {node_num}', NODE_TYPES: [NAMED_THING]}
    node.update(properties)
    return node


def make_edge(subj_num, obj_num, predicate='testing:predicate', **properties):
    edge = {SUBJECT_ID: f'NODE:{subj_num}', PREDICATE: predicate, OBJECT_ID: f'NODE:{obj_num}'}
    edge.update(properties)
    return edge


def node_property_merging_test(graph_merger: GraphMerger):

    test_nodes = [make_node(1, **{SYNONYMS: ['SYN_X', f'SYN_{i}'], 'testing_prop': [i]})
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


def test_node_property_merging_on_disk(tmp_path):
    node_property_merging_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8))


def node_merging_counts_test(graph_merger: GraphMerger):

    nodes_1_20 = [make_node(i) for i in range(1, 21)]
    nodes_6_25 = [make_node(i) for i in range(6, 26)]

    input_node_counter = 0
    input_node_counter += graph_merger.merge_nodes(nodes_1_20)
    input_node_counter += graph_merger.merge_nodes(nodes_6_25)
    assert input_node_counter == 40

    merged_node_lines = list(graph_merger.get_merged_nodes_jsonl())
    assert len(merged_node_lines) == 25
    assert graph_merger.merged_node_counter == 15


def test_node_merging_counts_in_memory():
    node_merging_counts_test(MemoryGraphMerger())


def test_node_merging_counts_on_disk(tmp_path):
    node_merging_counts_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8))


def edge_property_merging_test(graph_merger: GraphMerger):
    test_edges = [make_edge(1, 2,
                            **{'testing_property': [i],
                               PUBLICATIONS: [f'PMID:{i}', 'PMID:12345']})
                  for i in range(1, 11)]

    graph_merger.merge_edges(test_edges)
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 1
    assert len(merged_edges[0]['testing_property']) == 10

    # 'PMID:12345' was included on every edge but the duplicates should've been removed resulting in 11 unique pubs
    assert len(merged_edges[0][PUBLICATIONS]) == 11
    assert 'id' not in merged_edges[0]

def edge_merging_attributes_same_value_test(graph_merger: GraphMerger):
    # edges with the same edge_merging_attributes(s) value should merge into one
    test_edges = [make_edge(1, 2,
                            **{'testing_property': [i],
                               'abstract_id': 'test_abstract_id'})
                  for i in range(1, 11)]

    graph_merger.merge_edges(test_edges)
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 1
    assert len(merged_edges[0]['testing_property']) == 10
    assert merged_edges[0]['abstract_id'] == 'test_abstract_id'


def edge_merging_attributes_different_values_test(graph_merger: GraphMerger):
    # edges with different edge_merging_attributes values should stay separate
    test_edges = [make_edge(1, 2,
                            **{'testing_property': [i],
                               'abstract_id': f'test_abstract_id_{i}'})
                  for i in range(1, 11)]

    graph_merger.merge_edges(test_edges)
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 10
    assert len(merged_edges[0]['testing_property']) == 1


def add_edge_id_test(graph_merger: GraphMerger):
    # edges should get an 'id' field when add_edge_id=True
    test_edges = [make_edge(1, 2, **{'testing_property': [i]})
                  for i in range(1, 6)]

    graph_merger.merge_edges(test_edges)
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 1
    assert merged_edges[0].get('id') is not None


def edge_id_unique_for_distinct_edges_test(graph_merger: GraphMerger):
    # distinct edges should each get a different id
    test_edges = [make_edge(i, i+1, **{'testing_property': [i]})
                  for i in range(1, 6)]

    graph_merger.merge_edges(test_edges)
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 5
    edge_ids = [edge['id'] for edge in merged_edges]
    assert all(eid is not None for eid in edge_ids)
    assert len(set(edge_ids)) == 5


def test_edge_property_merging_in_memory():
    edge_property_merging_test(MemoryGraphMerger())

def test_edge_property_merging_on_disk(tmp_path):
    edge_property_merging_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8))

def test_edge_merging_attributes_same_value_in_memory():
    edge_merging_attributes_same_value_test(MemoryGraphMerger(edge_merging_attributes=['abstract_id']))

def test_edge_merging_attributes_same_value_on_disk(tmp_path):
    edge_merging_attributes_same_value_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8,
                                                            edge_merging_attributes=['abstract_id']))

def test_edge_merging_attributes_different_values_in_memory():
    edge_merging_attributes_different_values_test(MemoryGraphMerger(edge_merging_attributes=['abstract_id']))

def test_edge_merging_attributes_different_values_on_disk(tmp_path):
    edge_merging_attributes_different_values_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8,
                                                                  edge_merging_attributes=['abstract_id']))

def test_add_edge_id_in_memory():
    add_edge_id_test(MemoryGraphMerger(add_edge_id=True))

def test_add_edge_id_on_disk(tmp_path):
    add_edge_id_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8, add_edge_id=True))

def test_edge_id_unique_for_distinct_edges_in_memory():
    edge_id_unique_for_distinct_edges_test(MemoryGraphMerger(add_edge_id=True))

def test_edge_id_unique_for_distinct_edges_on_disk(tmp_path):
    edge_id_unique_for_distinct_edges_test(DiskGraphMerger(temp_directory=str(tmp_path),
                                                           chunk_size=8,
                                                           add_edge_id=True))

def primary_knowledge_source_merging_test(graph_merger: GraphMerger):
    # edges with same subject/predicate/object but different primary_knowledge_source should NOT merge
    edge_source_a = make_edge(1, 2, **{PRIMARY_KNOWLEDGE_SOURCE: 'source_A', 'prop': [1]})
    edge_source_b = make_edge(1, 2, **{PRIMARY_KNOWLEDGE_SOURCE: 'source_B', 'prop': [2]})
    # edges with same primary_knowledge_source SHOULD merge
    edge_source_a_dup = make_edge(1, 2, **{PRIMARY_KNOWLEDGE_SOURCE: 'source_A', 'prop': [3]})

    graph_merger.merge_edges([edge_source_a, edge_source_b, edge_source_a_dup])
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 2

    for edge in merged_edges:
        if edge[PRIMARY_KNOWLEDGE_SOURCE] == 'source_A':
            assert edge['prop'] == [1, 3]
        elif edge[PRIMARY_KNOWLEDGE_SOURCE] == 'source_B':
            assert edge['prop'] == [2]


def primary_knowledge_source_from_retrieval_sources_test(graph_merger: GraphMerger):
    # when PRIMARY_KNOWLEDGE_SOURCE is not a top-level property,
    # the primary source should be extracted from RETRIEVAL_SOURCES for the merge key
    edge_1 = make_edge(1, 2, **{'prop': [1],
                                RETRIEVAL_SOURCES: [{RETRIEVAL_SOURCE_ID: 'source_A',
                                                     RETRIEVAL_SOURCE_ROLE: PRIMARY_KNOWLEDGE_SOURCE}]})
    edge_2 = make_edge(1, 2, **{'prop': [2],
                                RETRIEVAL_SOURCES: [{RETRIEVAL_SOURCE_ID: 'source_B',
                                                     RETRIEVAL_SOURCE_ROLE: PRIMARY_KNOWLEDGE_SOURCE}]})
    edge_3 = make_edge(1, 2, **{'prop': [3],
                                RETRIEVAL_SOURCES: [{RETRIEVAL_SOURCE_ID: 'source_A',
                                                     RETRIEVAL_SOURCE_ROLE: PRIMARY_KNOWLEDGE_SOURCE}]})

    graph_merger.merge_edges([edge_1, edge_2, edge_3])
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 2


def test_primary_knowledge_source_merging_in_memory():
    primary_knowledge_source_merging_test(MemoryGraphMerger())

def test_primary_knowledge_source_merging_on_disk(tmp_path):
    primary_knowledge_source_merging_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8))

def test_primary_knowledge_source_from_retrieval_sources_in_memory():
    primary_knowledge_source_from_retrieval_sources_test(MemoryGraphMerger())

def test_primary_knowledge_source_from_retrieval_sources_on_disk(tmp_path):
    primary_knowledge_source_from_retrieval_sources_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8))


def edge_merging_counts_test(graph_merger: GraphMerger):

    edges_1_10_pred_1 = [make_edge(i, i+1, predicate='testing:predicate_1',
                                   **{'string_property': f'{i}', 'string_list_property': [f'edges_1_10_{i}'],
                                      'int_property': i})
                         for i in range(1, 11)]
    edges_1_10_pred_2 = [make_edge(i, i+1, predicate='testing:predicate_2',
                                   **{'string_property': f'{i}', 'string_list_property': [f'edges_1_10_{i}'],
                                      'int_property': i})
                         for i in range(1, 11)]
    edges_6_20_pred_1 = [make_edge(i, i+1, predicate='testing:predicate_1',
                                   **{'string_property': f'{i}', 'string_list_property': [f'edges_6_21_{i}'],
                                      'int_property': 999999})
                         for i in range(6, 21)]
    edges_6_20_pred_2 = [make_edge(i, i+1, predicate='testing:predicate_2',
                                   **{'string_property': f'{i}', 'string_list_property': [f'edges_6_21_{i}'],
                                      'int_property': 999999})
                         for i in range(6, 21)]

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
            assert len(edge['string_list_property']) == 1
        elif int(edge[SUBJECT_ID].split(':')[1]) < 11:
            assert len(edge['string_list_property']) == 2
        else:
            assert len(edge['string_list_property']) == 1
        assert edge['int_property'] == int(edge[SUBJECT_ID].split(':')[1]) or edge['int_property'] == 999999

    assert graph_merger.merged_edge_counter == 10


def test_edge_merging_counts_in_memory():
    edge_merging_counts_test(MemoryGraphMerger())


def test_edge_merging_counts_on_disk(tmp_path):
    edge_merging_counts_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8))


def test_qualifier_edge_merging():

    test_edges_up = [make_edge(1, 2, **{SUBJECT_ASPECT_QUALIFIER: 'test_aspect',
                                        SUBJECT_DIRECTION_QUALIFIER: 'up',
                                        'testing_prop': [i]})
                     for i in range(1, 16)]

    test_edges_down = [make_edge(1, 2, **{SUBJECT_ASPECT_QUALIFIER: 'test_aspect',
                                          SUBJECT_DIRECTION_QUALIFIER: 'down',
                                          'testing_prop': [i]})
                       for i in range(1, 11)]

    test_edges_other = [make_edge(1, 2, **{SUBJECT_ASPECT_QUALIFIER: 'test_aspect',
                                           SUBJECT_DIRECTION_QUALIFIER: 'down',
                                           SPECIES_CONTEXT_QUALIFIER: 'test_species',
                                           'testing_prop': [i]})
                        for i in range(1, 6)]

    graph_merger = MemoryGraphMerger()
    graph_merger.merge_edges(test_edges_up)
    graph_merger.merge_edges(test_edges_down)
    graph_merger.merge_edges(test_edges_other)

    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 3
    assert graph_merger.merged_edge_counter == 27

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


def retrieval_source_merging(graph_merger: GraphMerger):

    test_edges = [
        make_edge(1, 2,
                  **{"int_property": [1, 2, 3, 4, 5],
                     "string_property": ["a", "b", "c", "d"],
                     RETRIEVAL_SOURCES: [
                      {"id": "rs1",
                       RETRIEVAL_SOURCE_ID: "source_A",
                       RETRIEVAL_SOURCE_ROLE: "primary",
                       "upstream_resource_ids": ["upstream_1", "upstream_2"]},
                      {"id": "rs2",
                       RETRIEVAL_SOURCE_ID: "source_B",
                       RETRIEVAL_SOURCE_ROLE: "supporting",
                       "upstream_resource_ids": ["upstream_3"]},
                      {"id": "rs3",
                       RETRIEVAL_SOURCE_ID: "source_C",
                       RETRIEVAL_SOURCE_ROLE: "aggregator",
                       "upstream_resource_ids": ["upstream_4", "upstream_5"]}
                  ]}),
        make_edge(1, 2,
                  **{"int_property": [4, 5, 6, 1],
                     "string_property": ["c", "d", "e", "f"],
                     RETRIEVAL_SOURCES: [
                      {"id": "rs4",  # duplicate, should merge
                       RETRIEVAL_SOURCE_ID: "source_A",
                       RETRIEVAL_SOURCE_ROLE: "primary",
                       "upstream_resource_ids": ["upstream_2", "upstream_6"]},
                      {"id": "rs5",
                       RETRIEVAL_SOURCE_ID: "source_D",
                       RETRIEVAL_SOURCE_ROLE: "aggregator",
                       "upstream_resource_ids": ["upstream_7"]},
                      {"id": "rs6",  # different role, not a duplicate
                       RETRIEVAL_SOURCE_ID: "source_B",
                       RETRIEVAL_SOURCE_ROLE: "aggregator",
                       "upstream_resource_ids": ["upstream_8"]}
                  ]}),
    ]
    graph_merger.merge_edges(test_edges)

    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 1
    assert merged_edges[0]['int_property'] == [1, 2, 3, 4, 5, 6]
    assert merged_edges[0]['string_property'] == ["a", "b", "c", "d", "e", "f"]
    # Should have 5 retrieval sources after merging (source_A+primary merged, rest unique)
    assert len(merged_edges[0][RETRIEVAL_SOURCES]) == 5

    # Find the merged source_A+primary retrieval source and verify upstream_resource_ids were merged
    source_a_primary = [rs for rs in merged_edges[0][RETRIEVAL_SOURCES]
                        if rs[RETRIEVAL_SOURCE_ID] == "source_A"
                        and rs[RETRIEVAL_SOURCE_ROLE] == "primary"][0]
    # Should have upstream_1, upstream_2, upstream_6 (upstream_2 deduplicated)
    assert len(source_a_primary["upstream_resource_ids"]) == 3
    assert set(source_a_primary["upstream_resource_ids"]) == {"upstream_1", "upstream_2", "upstream_6"}

def test_retrieval_source_merging_in_memory():
    retrieval_source_merging(MemoryGraphMerger())


def test_retrieval_source_merging_on_disk(tmp_path):
    retrieval_source_merging(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8))


def single_entity_merge_test(graph_merger: GraphMerger):
    for i in range(1, 11):
        graph_merger.merge_node(make_node(1, **{'prop': [i]}))
    for i in range(1, 6):
        graph_merger.merge_edge(make_edge(1, 2, **{'prop': [i]}))

    merged_nodes = [json.loads(n) for n in graph_merger.get_merged_nodes_jsonl()]
    assert len(merged_nodes) == 1
    assert len(merged_nodes[0]['prop']) == 10

    merged_edges = [json.loads(e) for e in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 1
    assert len(merged_edges[0]['prop']) == 5


def test_single_entity_merge_in_memory():
    single_entity_merge_test(MemoryGraphMerger())


def test_single_entity_merge_on_disk(tmp_path):
    single_entity_merge_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=4))


def empty_merge_test(graph_merger: GraphMerger):
    graph_merger.merge_nodes([])
    graph_merger.merge_edges([])
    merged_nodes = list(graph_merger.get_merged_nodes_jsonl())
    merged_edges = list(graph_merger.get_merged_edges_jsonl())
    assert len(merged_nodes) == 0
    assert len(merged_edges) == 0


def test_empty_merge_in_memory():
    empty_merge_test(MemoryGraphMerger())


def test_empty_merge_on_disk(tmp_path):
    empty_merge_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8))


def test_disk_merger_temp_file_cleanup(tmp_path):
    graph_merger = DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=4)
    nodes = [make_node(i) for i in range(1, 11)]
    edges = [make_edge(i, i+1) for i in range(1, 11)]
    graph_merger.merge_nodes(nodes)
    graph_merger.merge_edges(edges)

    # temp files should exist before reading merged results
    assert len(graph_merger.temp_file_paths[NODE_ENTITY_TYPE]) > 0
    assert len(graph_merger.temp_file_paths[EDGE_ENTITY_TYPE]) > 0
    temp_node_paths = list(graph_merger.temp_file_paths[NODE_ENTITY_TYPE])
    temp_edge_paths = list(graph_merger.temp_file_paths[EDGE_ENTITY_TYPE])
    for path in temp_node_paths + temp_edge_paths:
        assert os.path.exists(path)

    # consume the merged results (this triggers cleanup)
    list(graph_merger.get_merged_nodes_jsonl())
    list(graph_merger.get_merged_edges_jsonl())

    # temp files should be cleaned up
    for path in temp_node_paths + temp_edge_paths:
        assert not os.path.exists(path)


def uuid_edge_id_test(graph_merger: GraphMerger):
    # edges should get a valid uuid5 'id' field when edge_id_type='uuid'
    test_edges = [make_edge(1, 2, **{'testing_property': [i]})
                  for i in range(1, 6)]

    graph_merger.merge_edges(test_edges)
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 1
    edge_id = merged_edges[0].get('id')
    assert edge_id is not None
    # validate it's a proper uuid
    parsed = uuid.UUID(edge_id)
    assert parsed.version == 5


def uuid_edge_id_unique_for_distinct_edges_test(graph_merger: GraphMerger):
    # distinct edges should each get a different uuid
    test_edges = [make_edge(i, i+1, **{'testing_property': [i]})
                  for i in range(1, 6)]

    graph_merger.merge_edges(test_edges)
    merged_edges = [json.loads(edge) for edge in graph_merger.get_merged_edges_jsonl()]
    assert len(merged_edges) == 5
    edge_ids = [edge['id'] for edge in merged_edges]
    assert all(uuid.UUID(eid).version == 5 for eid in edge_ids)
    assert len(set(edge_ids)) == 5


def uuid_edge_id_deterministic_test(graph_merger_factory):
    # same edge attributes should produce the same uuid across separate mergers
    edge = make_edge(1, 2, **{'testing_property': [1]})

    merger_1 = graph_merger_factory()
    merger_1.merge_edges([edge])
    edges_1 = [json.loads(e) for e in merger_1.get_merged_edges_jsonl()]

    merger_2 = graph_merger_factory()
    merger_2.merge_edges([make_edge(1, 2, **{'testing_property': [1]})])
    edges_2 = [json.loads(e) for e in merger_2.get_merged_edges_jsonl()]

    assert edges_1[0]['id'] == edges_2[0]['id']


def test_uuid_edge_id_in_memory():
    uuid_edge_id_test(MemoryGraphMerger(add_edge_id=True, edge_id_type='uuid'))

def test_uuid_edge_id_on_disk(tmp_path):
    uuid_edge_id_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8,
                                      add_edge_id=True, edge_id_type='uuid'))

def test_uuid_edge_id_unique_for_distinct_edges_in_memory():
    uuid_edge_id_unique_for_distinct_edges_test(MemoryGraphMerger(add_edge_id=True, edge_id_type='uuid'))

def test_uuid_edge_id_unique_for_distinct_edges_on_disk(tmp_path):
    uuid_edge_id_unique_for_distinct_edges_test(DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8,
                                                                 add_edge_id=True, edge_id_type='uuid'))

def test_uuid_edge_id_deterministic_in_memory():
    uuid_edge_id_deterministic_test(lambda: MemoryGraphMerger(add_edge_id=True, edge_id_type='uuid'))

def test_uuid_edge_id_deterministic_on_disk(tmp_path):
    uuid_edge_id_deterministic_test(lambda: DiskGraphMerger(temp_directory=str(tmp_path), chunk_size=8,
                                                             add_edge_id=True, edge_id_type='uuid'))