
import json
import os.path
from GTEx.src.loadGTEx import GTExLoader


def test_gtex_load():
    try:
        # get a reference to the intact data processor
        gt = GTExLoader(test_data=True, use_cache=False)

        # set the test directory
        test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

        gt.load(test_dir, 'gtex_test')

        # check the results
        assert(os.path.isfile(os.path.join(test_dir, 'gtex_test_edges.json'))
               and os.path.isfile(os.path.join(test_dir, 'gtex_test_nodes.json')))

        # open the edge file list and get the lines
        with open(os.path.join(test_dir, 'gtex_test_edges.json'), 'r') as fl:
            data = json.load(fl)

        # check the line count
        assert(len(data["edges"]) == 48)

        edges_validated = 0
        for edge in data["edges"]:
            assert (len(edge['expressed_in']) == len(edge['p_value']))
            assert (len(edge['p_value']) == len(edge['slope']))
            if edge["edge_label"] == 'biolink:increases_expression_of':
                edges_validated += 1
            elif edge["edge_label"] == 'biolink:decreases_expression_of':
                edges_validated += 1
            elif edge["edge_label"] == 'biolink:affects_splicing_of':
                edges_validated += 1

        assert edges_validated == 48

        # open the node file list and get the lines
        with open(os.path.join(test_dir, 'gtex_test_nodes.json'), 'r') as fl:
            data = json.load(fl)

        # check the line count
        assert(len(data["nodes"]) == 52)

    finally:
        # remove the data files
        if os.path.isfile(os.path.join(test_dir, 'gtex_test_edges.json')):
            os.remove(os.path.join(test_dir, 'gtex_test_edges.json'))
        if os.path.isfile(os.path.join(test_dir, 'gtex_test_nodes.json')):
            os.remove(os.path.join(test_dir, 'gtex_test_nodes.json'))
