import os
import shutil


def test_get_taxon_id_list():
    from Common.utils import GetData

    gd = GetData()

    data_file_path = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set))

    file_list: list = gd.get_uniprot_virus_file_list(data_file_path, type_virus, taxonid_set)

    assert(len(file_list))

    data_file_path += '/Virus_GOA_files/'

    file_subset: list = file_list[:2]

    actual_count: int = gd.get_goa_files(data_file_path, file_subset, '/pub/databases/GO/goa', '/proteomes/')

    assert(actual_count == len(file_subset))

    # remove the test data
    shutil.rmtree(data_file_path)
