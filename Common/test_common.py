import os
import shutil
import json
import pytest

from rdflib import Graph
from Common.utils import GetData, EdgeNormUtils, NodeNormUtils


def test_get_uniprot_virus_date_stamp():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    date_stamp: str = gd.get_uniprot_virus_date_stamp(data_file_path)

    assert(date_stamp == '20201007')


def test_pull_via_http():
    from Common.utils import GetData

    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    byte_count: int = gd.pull_via_http('https://renci.org/mission-and-vision', data_file_path)

    assert byte_count

    assert(os.path.exists(os.path.join(data_file_path, 'mission-and-vision')))

    os.remove(os.path.join(data_file_path, 'mission-and-vision'))


def test_get_taxon_id_list():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set) == 190167)


def test_get_virus_files():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set) == 190167)

    file_list: list = gd.get_uniprot_virus_file_list(data_file_path, taxonid_set)

    assert(len(file_list) == 3943)


def test_get_goa_files_chain():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set) == 190167)

    file_list: list = gd.get_uniprot_virus_file_list(data_file_path, taxonid_set)

    assert(len(file_list) == 3943)

    data_file_path += '/Virus_GOA_files/'

    file_subset: list = file_list[:2]

    actual_count: int = gd.get_goa_ftp_files(data_file_path, file_subset, '/pub/databases/GO/goa', '/proteomes/')

    assert(actual_count == len(file_subset))

    # remove the test data
    shutil.rmtree(data_file_path)


def test_edge_norm():
    # get the edge norm object
    en = EdgeNormUtils()

    # create an edge list
    edge_list: list = [{'predicate': '', 'relation': 'SEMMEDDB:CAUSES', 'edge_label': ''}, {'predicate': '', 'relation': 'RO:0000052', 'edge_label': ''}]

    # normalize the data
    en.normalize_edge_data(edge_list)

    # check the return
    assert(edge_list[0]['predicate'] == 'biolink:causes')
    assert(edge_list[0]['relation'] == 'SEMMEDDB:CAUSES')
    assert(edge_list[0]['edge_label'] == 'causes')

    assert(edge_list[1]['predicate'] == 'biolink:affects')
    assert(edge_list[1]['relation'] == 'RO:0000052')
    assert(edge_list[1]['edge_label'] == 'affects')

def test_get_node_synonym():
    # get the node norm object
    nn = NodeNormUtils()

    # call to get the synonyms for this curie
    ret_val = nn.get_node_synonyms('MONDO:0018800')

    # check the count
    assert(len(ret_val) == 7)