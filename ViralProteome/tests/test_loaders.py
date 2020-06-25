from src.loadUniRef2 import UniRefSimLoader
from src.loadVP import VPLoader
import os.path
import pytest


def test_load_vp():
    # get a reference to the processor
    vp = VPLoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__))

    # open the file list and turn it into a list array
    with open(os.path.join(test_dir, 'file_list.txt'), 'r') as fl:
        file_list: list = fl.readlines()

    # check the file list
    assert(file_list[0] == 'uniprot.goa')

    # check for the actual file
    assert (os.path.isfile(os.path.join(test_dir, file_list[0])))

    # load the data file and create KGX output
    vp.load(test_dir, '', file_list, 'VPLoadTest')

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'VPLoadTest_edge_file.csv')) and os.path.isfile(os.path.join(test_dir, 'VPLoadTest_node_file.csv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'VPLoadTest_edge_file.csv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 149)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'VPLoadTest_node_file.csv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 86)

    # remove the data files
    os.remove(os.path.join(test_dir, 'VPLoadTest_edge_file.csv'))
    os.remove(os.path.join(test_dir, 'VPLoadTest_node_file.csv'))


def test_load_uniref():
    # get a reference to the uniref similarity processor
    vp = UniRefSimLoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__))

    # create the file list
    test_file = 'uniref'

    # get the list of target taxon ids
    taxon_set: set = {'10493', '654924', '345201', '2219562', '2026840', '2219561', '380178', '2044919', '1887315"', '365144', '176652'}

    # load the data files and create KGX output
    vp.load(test_dir, test_file, 'taxon_file_indexes.txt', taxon_set, block_size=5000, debug_files=True)

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'uniref_Virus_edge_file.csv')) and os.path.isfile(os.path.join(test_dir, 'uniref_Virus_node_file.csv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'uniref_Virus_edge_file.csv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 7)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'uniref_Virus_node_file.csv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 7)

    # remove the data files
    os.remove(os.path.join(test_dir, 'uniref_Virus_node_file.csv'))
    os.remove(os.path.join(test_dir, 'uniref_Virus_edge_file.csv'))
