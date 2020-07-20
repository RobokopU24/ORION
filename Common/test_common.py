import os
import shutil


def test_get_taxon_id_list():
    from Common.utils import GetData

    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    date_stamp: str = gd.get_uniprot_virus_date_stamp(data_file_path)

    assert date_stamp

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set))

    file_list: list = gd.get_uniprot_virus_file_list(data_file_path, taxonid_set)

    assert(len(file_list))

    data_file_path += '/Virus_GOA_files/'

    file_subset: list = file_list[:2]

    actual_count: int = gd.get_goa_ftp_files(data_file_path, file_subset, '/pub/databases/GO/goa', '/proteomes/')

    assert(actual_count == len(file_subset))

    # remove the test data
    shutil.rmtree(data_file_path)

def test_pull_via_http():
    from Common.utils import GetData

    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    byte_count: int = gd.pull_via_http('https://renci.org/mission-and-vision', data_file_path)

    assert byte_count

    assert(os.path.exists('mission-and-vision'))

    os.remove('mission-and-vision')