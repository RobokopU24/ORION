import os.path
import pytest

from neo4j import GraphDatabase
from ViralProteome.src.loadUniRef import UniRefSimLoader
from ViralProteome.src.loadVP import VPLoader
from IntAct.src.loadIA import IALoader
from GOA.src.loadGOA import GOALoader
from Common.utils import GetData


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
    assert(len(file_lines) == 1)

    # open the node file list and get the lines
    with open(os.path.join(test_dir, 'Human_GOA_node_file.tsv'), 'r') as fl:
        file_lines: list = fl.readlines()

    # check the line count
    assert(len(file_lines) == 12)

    # remove the data files
    os.remove(os.path.join(test_dir, 'Human_GOA_node_file.tsv'))
    os.remove(os.path.join(test_dir, 'Human_GOA_edge_file.tsv'))


@pytest.mark.skip(reason="This test requires 2 graph DBs to compare results")
def test_swiss_prot_against_quickgo():
    # get a reference to the Data_services util
    gd = GetData()

    # get the uniprot kb ids that were curated by swiss-prot
    swiss_prots: set = gd.get_swiss_prot_id_set(os.path.dirname(os.path.abspath(__file__)))

    # create a connection
    driver_qg = GraphDatabase.driver('bolt://robokopdev.renci.org:7688', auth=('neo4j', 'ncatsgamma'))

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


@pytest.mark.skip(reason="This test requires 2 graph DBs to compare results")
def test_compare_edge_subsets():
    # get a reference to the Data_services util
    gd: GetData = GetData()

    # get the uniprot kb ids that were curated by swiss-prot
    swiss_prots: set = gd.get_swiss_prot_id_set(os.path.dirname(os.path.abspath(__file__)))

    # create a connection to the QuickGO graph
    driver_qg = GraphDatabase.driver('bolt://robokopdev.renci.org:7688', auth=('neo4j', 'ncatsgamma'))

    # prep for getting edges between uniprotkb and GO terms
    param1: str = 'UniProt.*'
    param2: str = 'GO.*'
    # node_id1: str = 'n1.id'
    node_id2: str = 'n2.id'
    output_val: str = 'uniprot_id'

    # creat the cypher command
    cypher: str = f'MATCH (n1)--(n2) WHERE not n1:Concept and n2.id =~ "{param2}" and any (x in n1.equivalent_identifiers where x =~ "{param1}") RETURN distinct n1.id, n2.id, [x in n1.equivalent_identifiers where x =~ "{param1}" | x][0] as {output_val}'

    # get the data
    result = driver_qg.session().run(cypher)

    # init the data return
    qg_ret_val: list = []

    # for each uniprotkb id
    for record in result:
        # is the uniprotkb id in the swiss prot curation list
        if record[output_val].split(':')[1] in swiss_prots:
            # save the data. uniprot id,Go id
            qg_ret_val.append(record[output_val] + ',' + record[node_id2])

            # check to see if there are uniprot ids in the QG data
            # if record['n1.id'] == record[output_val]:
            #     print(f'n1.id: {record["n1.id"]}, out_val:{record[output_val]}')

    # should have gotten something
    assert qg_ret_val

    # create a connection to the Human GOA graph
    driver_hg = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'neo4jkp'))

    # get the data
    result = driver_hg.session().run(cypher)

    # init the data return
    hg_ret_val: list = []

    # save the data. uniprot id,Go id
    for record in result:
        hg_ret_val.append(record[output_val] + "," + record[node_id2])

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
        # else it isnt
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
