
import os
from Common.utils import quick_jsonl_file_iterator
from Common.kgx_file_writer import KGXFileWriter
from Common.kgxmodel import kgxnode, kgxedge
from Common.node_types import *

test_workspace_dir = os.path.dirname(os.path.abspath(__file__)) + '/workspace/'
# TODO this is hacky and should be done with better design in pytest or somewhere else
if not os.path.exists(test_workspace_dir):
    os.mkdir(test_workspace_dir)
nodes_file_path = test_workspace_dir + 'test_nodes.jsonl'
edges_file_path = test_workspace_dir + 'test_edges.jsonl'

TEST_BUFFER_SIZE = 475


# TODO use kgx validation logic
def is_valid_nodes_file(file_path: str):
    for node in quick_jsonl_file_iterator(file_path):
        if not (node['id'] and node['name'] and node['category']):
            return False
    return True


def count_node_type(file_path: str, node_type: str):
    count = 0
    for node in quick_jsonl_file_iterator(file_path):
        if node_type in node['category']:
            count += 1
    return count


def is_valid_edges_file(file_path: str):
    for node in quick_jsonl_file_iterator(file_path):
        if not (node[SUBJECT_ID] and node[OBJECT_ID] and node[PREDICATE]):
            return False
    return True


def has_valid_edge_properties(file_path: str):
    counter = 0
    for edge in quick_jsonl_file_iterator(file_path):
        if not (('test_prop_bool' in edge and type(edge['test_prop_bool']) == bool) and
                ('test_prop_float' in edge and type(edge['test_prop_float']) == float) and
                ('test_prop_string' in edge and type(edge['test_prop_string']) == str)):
            return False
        if counter == 10:
            return True
    return True


def count_lines(file_path):
    line_counter = 0
    with open(file_path) as file:
        for line in file:
            line_counter += 1
    return line_counter


def remove_old_files():
    if os.path.exists(nodes_file_path):
        os.remove(nodes_file_path)
    if os.path.exists(edges_file_path):
        os.remove(edges_file_path)


def test_writing_objects():
    remove_old_files()
    with KGXFileWriter(nodes_file_path, edges_file_path, buffer_size=TEST_BUFFER_SIZE) as test_file_writer:

        # write buffer size (475) + 25 nodes = 500 nodes of different types
        for i in range(1, TEST_BUFFER_SIZE + 26):

            # set 100 each of a handful of node types
            node_categories = [NAMED_THING]
            if i <= 100:
                node_categories.append(GENE)
            elif i <= 200:
                node_categories.append(PATHWAY)
            elif i <= 300:
                node_categories.append(CHEMICAL_SUBSTANCE)
            elif i <= 400:
                node_categories.append(DRUG)
            elif i <= 500:
                node_categories.append(DISEASE_OR_PHENOTYPIC_FEATURE)

            node = kgxnode(f'TEST:{i}', f'Test Node {i}', categories=node_categories)
            test_file_writer.write_kgx_node(node)
            if i > TEST_BUFFER_SIZE:
                # attempt writing 10 duplicates
                node = kgxnode(f'TEST:{i}', f'Test Node {i}', categories=node_categories)
                test_file_writer.write_kgx_node(node)

        for i in range(1, 101):
            for j in range(i, i+5):
                edge = kgxedge(subject_id=f'TEST:{i}',
                               object_id=f'TEST:{i+j}',
                               predicate='biolink:related_to',
                               original_knowledge_source='infores:testing',
                               edgeprops={'test_prop_bool': True,
                                          'test_prop_float': 1.5,
                                          'test_prop_string': 'hey'})
                test_file_writer.write_kgx_edge(edge)

    assert is_valid_nodes_file(nodes_file_path)
    assert count_lines(nodes_file_path) == TEST_BUFFER_SIZE + 25
    assert count_node_type(nodes_file_path, GENE) == 100
    assert count_node_type(nodes_file_path, NAMED_THING) == 500
    assert test_file_writer.repeat_node_count == 25

    assert is_valid_edges_file(edges_file_path)
    assert has_valid_edge_properties(edges_file_path)
    assert count_lines(edges_file_path) == 500
    remove_old_files()


def test_writing_json_lines():

    remove_old_files()
    with KGXFileWriter(nodes_file_path, edges_file_path) as test_file_writer:

        # write buffer size (475) + 25 nodes = 500 nodes of different types
        for i in range(1, TEST_BUFFER_SIZE + 26):

            # set 100 each of a handful of node types
            node_categories = [NAMED_THING]
            if i <= 100:
                node_categories.append(GENE)
            elif i <= 200:
                node_categories.append(PATHWAY)
            elif i <= 300:
                node_categories.append(CHEMICAL_SUBSTANCE)
            elif i <= 400:
                node_categories.append(DRUG)
            elif i <= 500:
                node_categories.append(DISEASE_OR_PHENOTYPIC_FEATURE)

            node_json = {"id": f"TEST:{i}", "name": f"Test Node {i}", "category": node_categories}
            test_file_writer.write_normalized_node(node_json)
            if i > TEST_BUFFER_SIZE:
                # attempt writing 10 duplicates
                node_json = {"id": f"TEST:{i}", "name": f"Test Node {i}", "category": node_categories}
                test_file_writer.write_normalized_node(node_json)

        for i in range(1, 101):
            edges = []
            for j in range(i, i+5):
                edge_json = {SUBJECT_ID: f"TEST:{i}",
                             PREDICATE: "biolink:related_to",
                             OBJECT_ID: f"TEST:{i+j}",
                             ORIGINAL_KNOWLEDGE_SOURCE: "infores:testing",
                             "test_prop_bool": True,
                             "test_prop_float": 1.5,
                             "test_prop_string": 'hey'}
                edges.append(edge_json)
            test_file_writer.write_normalized_edges(edges)

    assert is_valid_nodes_file(nodes_file_path)
    assert count_lines(nodes_file_path) == TEST_BUFFER_SIZE + 25
    assert count_node_type(nodes_file_path, CHEMICAL_SUBSTANCE) == 100
    assert count_node_type(nodes_file_path, NAMED_THING) == 500
    assert test_file_writer.repeat_node_count == 25

    assert is_valid_edges_file(edges_file_path)
    assert has_valid_edge_properties(edges_file_path)
    assert count_lines(edges_file_path) == 500
    remove_old_files()

