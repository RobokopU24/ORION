import os.path
from ViralProteome.src.loadUniRef import UniRefSimLoader
from ViralProteome.src.loadVP import VPLoader
from IntAct.src.loadIA import IALoader
from GOA.src.loadGOA import GOALoader


def test_vp_load():
    # get a reference to the viral proteome data processor
    vp = VPLoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

    # load the data file and create KGX output
    vp.load(test_dir, 'VPLoadTest', test_mode=True)

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'VPLoadTest_edge_file.tsv')) and os.path.isfile(os.path.join(test_dir, 'VPLoadTest_node_file.tsv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'VPLoadTest_edge_file.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 149)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'VPLoadTest_node_file.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 86)

    # remove the data files
    os.remove(os.path.join(test_dir, 'VPLoadTest_edge_file.tsv'))
    os.remove(os.path.join(test_dir, 'VPLoadTest_node_file.tsv'))


def test_uniref_load():
    # get a reference to the uniref similarity data processor
    uni = UniRefSimLoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

    # load the data file and create KGX output
    uni.load(test_dir, ['uniref'], 'taxon_file_indexes.txt', test_mode=True)

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'uniref_Virus_edge_file.tsv')))
    assert(os.path.isfile(os.path.join(test_dir, 'uniref_Virus_node_file.tsv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'uniref_Virus_edge_file.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 7)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'uniref_Virus_node_file.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 7)

    # remove the data files
    os.remove(os.path.join(test_dir, 'uniref_Virus_node_file.tsv'))
    os.remove(os.path.join(test_dir, 'uniref_Virus_edge_file.tsv'))


def test_intact_load():
    # get a reference to the intact data processor
    ia = IALoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

    # load the data files and create KGX output files
    ia.load(test_dir, 'intact', test_mode=True)

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'intact_edge_file.tsv')) and os.path.isfile(os.path.join(test_dir, 'intact_node_file.tsv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'intact_edge_file.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 34)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'intact_node_file.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 11)

    # remove the data files
    os.remove(os.path.join(test_dir, 'intact_node_file.tsv'))
    os.remove(os.path.join(test_dir, 'intact_edge_file.tsv'))


def test_goa_load():
    # get a reference to the GOA data processor
    goa = GOALoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

    # load the data file and create KGX output
    goa.load(test_dir, '/HUMAN/', 'goa_human.gaf.gz', 'Human_GOA', test_mode=True)

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'Human_GOA_edge_file.tsv')) and os.path.isfile(os.path.join(test_dir, 'Human_GOA_node_file.tsv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'Human_GOA_edge_file.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 2)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'Human_GOA_node_file.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 13)

    # remove the data files
    os.remove(os.path.join(test_dir, 'Human_GOA_node_file.tsv'))
    os.remove(os.path.join(test_dir, 'Human_GOA_edge_file.tsv'))
