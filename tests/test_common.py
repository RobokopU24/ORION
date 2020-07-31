import os
import shutil
from Common.utils import GetData


def test_get_uniprot_virus_date_stamp():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    date_stamp: str = gd.get_uniprot_virus_date_stamp(data_file_path)

    assert(date_stamp == '20200617')


def test_pull_via_http():
    from Common.utils import GetData
    import time

    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    byte_count: int = gd.pull_via_http('https://renci.org/mission-and-vision', data_file_path)

    assert byte_count

    time.sleep(3)

    assert(os.path.exists('mission-and-vision'))

    os.remove('mission-and-vision')


def test_get_taxon_id_list():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set) == 188973)


def test_get_virus_files():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set) == 188973)

    file_list: list = gd.get_uniprot_virus_file_list(data_file_path, taxonid_set)

    assert(len(file_list) == 3999)


def test_get_goa_files_chain():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set) == 188973)

    file_list: list = gd.get_uniprot_virus_file_list(data_file_path, taxonid_set)

    assert(len(file_list) == 3999)

    data_file_path += '/Virus_GOA_files/'

    file_subset: list = file_list[:2]

    actual_count: int = gd.get_goa_ftp_files(data_file_path, file_subset, '/pub/databases/GO/goa', '/proteomes/')

    assert(actual_count == len(file_subset))

    # remove the test data
    shutil.rmtree(data_file_path)


def test_edge_norm():
    from Common.utils import EdgeNormUtils

    # get the edge norm object
    en = EdgeNormUtils()

    # create an edge list
    edge_list: list = [{'predicate': 'SEMMEDDB:CAUSES', 'relation': '', 'edge_label': ''}, {'predicate': 'RO:0000052', 'relation': '', 'edge_label': ''}]

    # normalize the data
    ret_val = en.normalize_edge_data(edge_list)

    # check the return
    assert(ret_val[0]['predicate'] == 'SEMMEDDB:CAUSES')
    assert(ret_val[0]['relation'] == 'biolink:causes')
    assert(ret_val[0]['edge_label'] == 'causes')

    assert(ret_val[1]['predicate'] == 'RO:0000052')
    assert(ret_val[1]['relation'] == 'biolink:affects')
    assert(ret_val[1]['edge_label'] == 'affects')
