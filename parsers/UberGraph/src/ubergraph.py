import requests
import curies
import tarfile
from io import TextIOWrapper


BIOLINK_DUPLICATE_MAPPINGS = ["agrkb",
                              "OBOREL",
                              "SNOMEDCT",
                              "oboInOwl",
                              "oboformat"]

BIOLINK_MISSING_MAPPINGS = {
    'KEGG': 'http://www.kegg.jp/entry/',
    'NCBIGene': 'https://identifiers.org/ncbigene/'
}

OBO_MISSING_MAPPINGS = {
    'NCBIGene': 'http://purl.obolibrary.org/obo/NCBIGene_',
    'HGNC': 'http://purl.obolibrary.org/obo/HGNC_',
    'SGD': 'http://purl.obolibrary.org/obo/SGD_'
}


class UberGraphTools:

    def __init__(self,
                 ubergraph_archive_path: str,
                 graph_base_path: str):

        biolink_prefix_map = get_biolink_prefix_map()
        iri_to_biolink_curie_converter = curies.Converter.from_prefix_map(biolink_prefix_map)
        iri_to_obo_curie_converter = curies.get_obo_converter()

        with tarfile.open(ubergraph_archive_path, 'r') as tar_files:
            self.node_curies = {}
            with tar_files.extractfile(f'{graph_base_path}/node-labels.tsv') as node_labels_file:
                for line in TextIOWrapper(node_labels_file):
                    node_id, node_iri = tuple(line.rstrip().split('\t'))
                    node_curie = iri_to_biolink_curie_converter.compress(node_iri)
                    if node_curie is None:
                        node_curie = iri_to_obo_curie_converter.compress(node_iri)
                        if node_curie is None:
                            # print(f'Could not find prefix mapping for: {node_iri}')
                            for key, value in OBO_MISSING_MAPPINGS.items():
                                if key in node_iri:
                                    node_curie = node_iri.replace(value, f'{value}:')
                    self.node_curies[node_id] = node_curie

            self.edge_curies = {}
            with tar_files.extractfile(f'{graph_base_path}/edge-labels.tsv') as edge_labels_file:
                for line in TextIOWrapper(edge_labels_file):
                    edge_id, edge_iri = tuple(line.rstrip().split('\t'))
                    edge_curie = iri_to_biolink_curie_converter.compress(edge_iri)
                    if edge_curie is None:
                        edge_curie = iri_to_obo_curie_converter.compress(edge_iri)
                        if edge_curie is None:
                            print(f'No prefix mapping found for: {edge_iri}')
                    self.edge_curies[edge_id] = edge_curie

    def get_curie_for_node_id(self, node_id):
        return self.node_curies[node_id]

    def get_curie_for_edge_id(self, edge_id):
        return self.edge_curies[edge_id]


def get_biolink_prefix_map():
    response = requests.get \
        ('https://raw.githubusercontent.com/biolink/biolink-model/master/prefix-map/biolink-model-prefix-map.json')
    if response.status_code != 200:
        response.raise_for_status()

    biolink_prefix_map = response.json()

    for duplicate_mapping in BIOLINK_DUPLICATE_MAPPINGS:
        del (biolink_prefix_map[duplicate_mapping])
    kegg_keys = []
    for key, value in biolink_prefix_map.items():
        if 'KEGG.' in key:
            kegg_keys.append(key)
    for key in kegg_keys:
        del (biolink_prefix_map[key])

    biolink_prefix_map.update(BIOLINK_MISSING_MAPPINGS)
    return biolink_prefix_map
