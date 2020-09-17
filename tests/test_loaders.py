import os.path
import pytest

from ViralProteome.src.loadUniRef import UniRefSimLoader
from ViralProteome.src.loadVP import VPLoader
from IntAct.src.loadIA import IALoader
from GOA.src.loadGOA import GOALoader
from UberGraph.src.loadUG import UGLoader
from FooDB.src.loadFDB import FDBLoader
from GTEx.src.loadGTEx import GTExLoader
from Common.utils import GetData


def test_vp_load():
    # get a reference to the viral proteome data processor
    vp = VPLoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

    # load the data file and create KGX output
    vp.load(test_dir, 'Viral_proteome_loadtest', output_mode='tsv', test_mode=True)

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'Viral_proteome_loadtest_edges.tsv')) and os.path.isfile(os.path.join(test_dir, 'Viral_proteome_loadtest_nodes.tsv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'Viral_proteome_loadtest_edges.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 149)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'Viral_proteome_loadtest_nodes.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 86)

    # open the provenance node file and get the lines
    with open(os.path.join(test_dir, 'Viral_proteome_prov_node_file.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert (len(file_lines) == 2)

    # remove the data files
    os.remove(os.path.join(test_dir, 'Viral_proteome_loadtest_edges.tsv'))
    os.remove(os.path.join(test_dir, 'Viral_proteome_loadtest_nodes.tsv'))
    os.remove(os.path.join(test_dir, 'Viral_proteome_prov_node_file.tsv'))


def test_uniref_load():
    # get a reference to the uniref similarity data processor
    uni = UniRefSimLoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

    # load the data file and create KGX output
    uni.load(test_dir, ['uniref'], 'taxon_file_indexes.txt', output_mode='tsv', test_mode=True)

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'uniref_Virus_edges.tsv')))
    assert(os.path.isfile(os.path.join(test_dir, 'uniref_Virus_nodes.tsv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'uniref_Virus_edges.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 7)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'uniref_Virus_nodes.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 7)

    # remove the data files
    os.remove(os.path.join(test_dir, 'uniref_Virus_nodes.tsv'))
    os.remove(os.path.join(test_dir, 'uniref_Virus_edges.tsv'))


def test_intact_load():
    # get a reference to the intact data processor
    ia = IALoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

    # load the data files and create KGX output files
    ia.load(test_dir, 'intact', output_mode='tsv', test_mode=True)

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'intact_edges.tsv')) and os.path.isfile(os.path.join(test_dir, 'intact_nodes.tsv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'intact_edges.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 20)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'intact_nodes.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 11)

    # open the provenance node file and get the lines
    with open(os.path.join(test_dir, 'intact_prov_node_file.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 2)

    # remove the data files
    os.remove(os.path.join(test_dir, 'intact_nodes.tsv'))
    os.remove(os.path.join(test_dir, 'intact_edges.tsv'))
    os.remove(os.path.join(test_dir, 'intact_prov_node_file.tsv'))


def test_goa_load():
    # get a reference to the GOA data processor
    goa = GOALoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

    # load the data file and create KGX output
    goa.load(test_dir, 'goa_human.gaf.gz', 'Human_GOA', output_mode='tsv', test_mode=True)

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'Human_GOA_edges.tsv')) and os.path.isfile(os.path.join(test_dir, 'Human_GOA_nodes.tsv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'Human_GOA_edges.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 7)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'Human_GOA_nodes.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 8)

    # remove the data files
    os.remove(os.path.join(test_dir, 'Human_GOA_nodes.tsv'))
    os.remove(os.path.join(test_dir, 'Human_GOA_edges.tsv'))


def test_ubergraph_load():
    # get a reference to the intact data processor
    ug = UGLoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

    # load the data files and create KGX output files
    ug.load(test_dir, 'ubergraph_test.ttl', output_mode='tsv', test_mode=True)

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'ubergraph_test_edges.tsv')) and os.path.isfile(os.path.join(test_dir, 'ubergraph_test_nodes.tsv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'ubergraph_test_edges.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 3)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'ubergraph_test_nodes.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 7)

    # remove the data files
    os.remove(os.path.join(test_dir, 'ubergraph_test_edges.tsv'))
    os.remove(os.path.join(test_dir, 'ubergraph_test_nodes.tsv'))
    os.remove(os.path.join(test_dir, 'ubergraph_test.ttl.1'))


def test_foodb_load():
    # get a reference to the intact data processor
    fdb = FDBLoader()

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

    fdb.load(test_dir, 'foodb_test', output_mode='tsv')

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'foodb_test_edges.tsv')) and os.path.isfile(os.path.join(test_dir, 'foodb_test_nodes.tsv')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'foodb_test_edges.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 5)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'foodb_test_nodes.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 6)

    # remove the data files
    os.remove(os.path.join(test_dir, 'foodb_test_edges.tsv'))
    os.remove(os.path.join(test_dir, 'foodb_test_nodes.tsv'))


def test_gtex_load():
    # get a reference to the intact data processor
    gt = GTExLoader(test_data=True)

    # set the test directory
    test_dir = os.path.dirname(os.path.abspath(__file__)) + '/resources'

    gt.load(test_dir, 'gtex_test')

    # check the results
    assert(os.path.isfile(os.path.join(test_dir, 'gtex_test_edges.json')) and os.path.isfile(os.path.join(test_dir, 'gtex_test_nodes.json')))

    # open the edge file list and get the lines
    with open(os.path.join(test_dir, 'gtex_test_edges.json'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 54)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'gtex_test_nodes.json'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 49)

    # remove the data files
    os.remove(os.path.join(test_dir, 'gtex_test_edges.json'))
    os.remove(os.path.join(test_dir, 'gtex_test_nodes.json'))


@pytest.mark.skip(reason="Internal test only. This test requires a graph DB for result verification")
def test_swiss_prot_against_quickgo():
    from neo4j import GraphDatabase

    # get a reference to the Data_services util
    gd = GetData()

    # get the uniprot kb ids that were curated by swiss-prot
    swiss_prots: set = gd.get_swiss_prot_id_set(os.path.dirname(os.path.abspath(__file__)))

    # create a connection
    driver_qg = GraphDatabase.driver('bolt://robokopdev.renci.org:7688', auth=('neo4j', 'demo'))

    # prep for getting all unique uniprotkb ids
    param: str = 'UniProt.*'
    node_id: str = 'n1.id'
    cypher: str = f'MATCH (n1)-[e]-() WHERE not n1:Concept and any (x in n1.equivalent_identifiers where x =~ "{param}") RETURN distinct {node_id}'

    # get the data and any (x in n1.equivalent_identifiers where x =~ {param})
    result = driver_qg.session().run(cypher)

    # init the swiss prot bins
    are_swiss_prots: list = []
    not_swiss_prots: list = []

    # save the data
    for record in result:
        uniprot_id: str = record[node_id].split(':')[1]

        if uniprot_id in swiss_prots:
            are_swiss_prots.append(uniprot_id)
        else:
            not_swiss_prots.append(uniprot_id)

    # dont need this anymore
    driver_qg.close()

    print(f'{len(are_swiss_prots)} QuickGo UniProts are in Human GOA')
    print(f'{len(not_swiss_prots)} QuickGo UniProts are not in Human GOA')

    # assert we got some swiss prot vals
    assert are_swiss_prots


@pytest.mark.skip(reason="Internal test only. This test requires 2 graph DBs to compare results")
def test_compare_edge_subsets_by_uniprot():
    from neo4j import GraphDatabase

    # get a reference to the Data_services util
    gd: GetData = GetData()

    # get the uniprot kb ids that were curated by swiss-prot
    swiss_prots: set = gd.get_swiss_prot_id_set(os.path.dirname(os.path.abspath(__file__)))

    # create a connection to the QuickGO graph
    driver_qg = GraphDatabase.driver('bolt://robokopdev.renci.org:7688', auth=('neo4j', 'demo'))

    # prep for getting edges between terms
    uni_search: str = 'UniProt.*'
    output_val: str = 'uniprot_id'

    node_id1: str = 'n1.id'

    go_search: str = 'GO.*'
    node_id2: str = 'n2.id'

    # create the cypher command
    cypher: str = f'MATCH (n1)--(n2) WHERE not n1:Concept and {node_id2} =~ "{go_search}" and any (x in n1.equivalent_identifiers where x =~ "{uni_search}") RETURN distinct {node_id1}, {node_id2}, [x in n1.equivalent_identifiers where x =~ "{uni_search}" | x][0] as {output_val}'

    # get the data
    result = driver_qg.session().run(cypher)

    # init the data return
    qg_ret_val: list = []

    # for each uniprotkb id
    for record in result:
        # split the id into parts
        tmp_val = record[node_id1].split(':')

        # check for an invalid ID
        if len(tmp_val) > 2:
            print(f'{node_id1} ID error: {record[node_id1]}')
        else:
            # is the uniprotkb id in the swiss prot curation list
            if tmp_val[1] in swiss_prots:
                # save the data. uniprot id,Go id
                qg_ret_val.append(record[output_val] + ',' + record[node_id2])

                # check to see if there are uniprot ids in the QG data
                # if record['n1.id'] == record[output_val]:
                #     print(f'n1.id: {record["n1.id"]}, out_val:{record[output_val]}')

    # should have gotten something
    assert qg_ret_val

    # create a connection to the Human GOA graph
    driver_hg = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'demo'))

    # get the data
    result = driver_hg.session().run(cypher)

    # init the data return
    hg_ret_val: list = []

    # save the data. node id, GO id
    for record in result:
        hg_ret_val.append(record[output_val] + "," + record[node_id2])

    # we should have got something
    assert hg_ret_val

    # init the data bins
    are_in_hg: list = []
    not_in_hg: list = []

    # for each quick go edge
    for qg_item in qg_ret_val:
        # is the edge also in the human goa data
        if qg_item in hg_ret_val:
            are_in_hg.append(qg_item)
        # else it isn't
        else:
            not_in_hg.append(qg_item)

    print(f'There are {len(qg_ret_val)} UniProt to GO edges in QuickGO')
    print(f'There are {len(hg_ret_val)} UniProt to GO edges in Human GOA')

    print(f'There are {len(are_in_hg)} QuickGO UniProt/GO edges in Human GOA')
    print(f'There are {len(not_in_hg)} QuickGO UniProt/term edges not in Human GOA')

    # dont need these anymore
    driver_qg.close()
    driver_hg.close()

    assert are_in_hg


@pytest.mark.skip(reason="Internal test only. This test requires 2 graph DBs to compare results")
def test_compare_edge_subsets_by_gene():
    from neo4j import GraphDatabase

    # create a connection to the QuickGO graph
    driver_qg = GraphDatabase.driver('bolt://robokopdev.renci.org:7688', auth=('neo4j', 'demo'))

    # prep for getting edges between terms
    ncbi_search: str = 'NCBI.*'
    node_id1: str = 'n1.id'

    go_search: str = 'GO.*'
    node_id2: str = 'n2.id'

    # create the cypher command
    cypher: str = f'MATCH (n1)--(n2) WHERE not n1:Concept and {node_id1} =~ "{ncbi_search}" and {node_id2} =~ "{go_search}" RETURN distinct {node_id1}, {node_id2}'

    # get the data
    result = driver_qg.session().run(cypher)

    # init the data return
    qg_ret_val: list = []

    # for each uniprotkb id
    for record in result:
        # split the id into parts
        tmp_val = record[node_id1].split(':')

        # check for an invalid ID
        if len(tmp_val) > 2:
            print(f'{node_id1} ID error: {record[node_id1]}')
        else:
            # save the data. ncbi id, go id
            qg_ret_val.append(record[node_id1] + ',' + record[node_id2])

            # check to see if there are uniprot ids in the QG data
            # if record['n1.id'] == record[output_val]:
            #     print(f'n1.id: {record["n1.id"]}, out_val:{record[output_val]}')

    # should have gotten something
    assert qg_ret_val

    # create a connection to the Human GOA graph
    driver_hg = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'demo'))

    # get the data
    result = driver_hg.session().run(cypher)

    # init the data return
    hg_ret_val: list = []

    # save the data. node id, GO id
    for record in result:
        hg_ret_val.append(record[node_id1] + "," + record[node_id2])

    # we should have got something
    assert hg_ret_val

    # init the data bins
    are_in_hg: list = []
    not_in_hg: list = []

    # for each quickgo edge
    for qg_item in qg_ret_val:
        # is the edge also in the human goa data
        if qg_item in hg_ret_val:
            are_in_hg.append(qg_item)
        # else it isn't
        else:
            print(f'quickgo {qg_item} not found in human GOA')
            not_in_hg.append(qg_item)

    print(f'There are {len(qg_ret_val)} NCBI to GO edges in QuickGO')
    print(f'There are {len(hg_ret_val)} NCBI to GO edges in Human GOA')

    print(f'There are {len(are_in_hg)} QuickGO NCBI/GO edges in Human GOA')
    print(f'There are {len(not_in_hg)} QuickGO NCBI/term edges not in Human GOA')

    # dont need these anymore
    driver_qg.close()
    driver_hg.close()

    assert are_in_hg
