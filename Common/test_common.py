import os
import shutil
import json
import pytest

from rdflib import Graph
from Common.utils import GetData, EdgeNormUtils


def test_get_uniprot_virus_date_stamp():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    date_stamp: str = gd.get_uniprot_virus_date_stamp(data_file_path)

    assert(date_stamp == '20200617')


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

    assert(len(taxonid_set) == 189019)


def test_get_virus_files():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set) == 189019)

    file_list: list = gd.get_uniprot_virus_file_list(data_file_path, taxonid_set)

    assert(len(file_list) == 3993)


def test_get_goa_files_chain():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set) == 189019)

    file_list: list = gd.get_uniprot_virus_file_list(data_file_path, taxonid_set)

    assert(len(file_list) == 3993)

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
    edge_list: list = [{'predicate': 'SEMMEDDB:CAUSES', 'relation': '', 'edge_label': ''}, {'predicate': 'RO:0000052', 'relation': '', 'edge_label': ''}]

    # normalize the data
    en.normalize_edge_data(edge_list)

    # check the return
    assert(edge_list[0]['predicate'] == 'SEMMEDDB:CAUSES')
    assert(edge_list[0]['relation'] == 'biolink:causes')
    assert(edge_list[0]['edge_label'] == 'biolink:causes')

    assert(edge_list[1]['predicate'] == 'RO:0000052')
    assert(edge_list[1]['relation'] == 'biolink:affects')
    assert(edge_list[1]['edge_label'] == 'biolink:affects')

def test2():
    BIOLINK_CONTEXT = 'context.jsonld'
    TEST_DATA = 'test_sm.ttl'
    context_json = json.load(open(BIOLINK_CONTEXT))
    context = context_json['@context']
    g = Graph().parse(TEST_DATA, format='nt')

    # print out the entire Graph in the RDF Turtle format
    print('\n' + g.serialize(format="turtle", context=context).decode("utf-8"))

#@pytest.mark.skip(reason="Not quite ready yet")
def test_get_biolink_ld_json():
    # instantiate the object that has the method to do this
    gd = GetData()

    # input_data = 'https://raw.githubusercontent.com/NCATS-Tangerine/kgx/master/tests/resources/rdf/test1.nt'
    # input_data = 'test.ttl'
    # input_data = 'test_sm.ttl'
    # input_data = 'D:/Work/Robokop/Data_services/Ubergraph_data/properties-nonredundant.ttl'

    filenames = os.listdir('D:/Work/Robokop/Data_services/Ubergraph_data/temp')

    # filenames = [input_data]

    # init a file counter
    file_counter: int = 0

    print(f'{len(filenames)} files found.')

    # open the output file
    with open('parsed.txt', 'w', encoding="utf-8") as fp:
        for filename in filenames:
            # increment the counter
            # file_counter += 1

            # print(f'Working file: {file_counter}.')

            # get the biolink json-ld data
            g: Graph = gd.get_biolink_graph(os.path.join('D:/Work/Robokop/Data_services/Ubergraph_data/temp', filename))

            # assert that we got it. more detailed interrogation to follow
            assert(isinstance(g, Graph))

            # init a list for the output data
            triples: set = set()
            triple: list = []

            # for every triple in the input data
            for t in g.triples((None, None, None)):
                # clear before use
                triple.clear()

                # get the curie for each element in the triple
                for i, n in enumerate(t):
                    try:
                        # replace the underscores to create a curie
                        val = g.compute_qname(n)[2].replace('_', ':')
                    except e:
                        print('Exception parsing rdf qname')
                        val = n

                    # add it to the group
                    triple.append(val)

                # add this the unique set
                triples.add(','.join(triple) + '\n')

