import requests
import curies
import tarfile
from io import TextIOWrapper


BIOLINK_DUPLICATE_MAPPINGS = ["agrkb",
                              "OBOREL",
                              "oboInOwl",
                              "oboformat"]

BIOLINK_MAPPING_CHANGES = {
    'KEGG': 'http://identifiers.org/kegg/',
    'NCBIGene': 'https://identifiers.org/ncbigene/'
}

OBO_MISSING_MAPPINGS = {
    'NCBIGene': 'http://purl.obolibrary.org/obo/NCBIGene_',
    'HGNC': 'http://purl.obolibrary.org/obo/HGNC_',
    'SGD': 'http://purl.obolibrary.org/obo/SGD_'
}


class UberGraphTools:

    def __init__(self,
                 ubergraph_archive_path: str = None,
                 graph_base_path: str = None,
                 logger=None):

        self.node_curies = {}
        self.edge_curies = {}
        self.ubergraph_archive_path = ubergraph_archive_path
        self.graph_base_path = graph_base_path
        self.logger = logger
        self.convert_iris_to_curies()

    def convert_iris_to_curies(self):
        self.logger.info(f'Converting all Ubergraph iris to curies..')
        biolink_prefix_map = self.get_biolink_prefix_map()
        iri_to_biolink_curie_converter = curies.Converter.from_prefix_map(biolink_prefix_map)
        iri_to_obo_curie_converter = curies.get_obo_converter()
        custom_converter = curies.Converter.from_prefix_map(OBO_MISSING_MAPPINGS)
        
        # A chain combines several converters in a row
        chain_converter = curies.chain([
            iri_to_biolink_curie_converter,
            iri_to_obo_curie_converter,
            custom_converter,
        ])

        with tarfile.open(self.ubergraph_archive_path, 'r') as tar_files:
            self.node_curies = {}
            node_mapping_failures = []
            with tar_files.extractfile(f'{self.graph_base_path}/node-labels.tsv') as node_labels_file:
                for line in TextIOWrapper(node_labels_file):
                    node_id, node_iri = tuple(line.rstrip().split('\t'))
                    node_curie = chain_converter.compress(node_iri)
                    if node_curie is None:
                        node_mapping_failures.append(node_iri)
                        # print(f'Could not find prefix mapping for: {node_iri}')
                    self.node_curies[node_id] = node_curie

            self.edge_curies = {}
            edge_mapping_failures = []
            with tar_files.extractfile(f'{self.graph_base_path}/edge-labels.tsv') as edge_labels_file:
                for line in TextIOWrapper(edge_labels_file):
                    edge_id, edge_iri = tuple(line.rstrip().split('\t'))
                    edge_curie = chain_converter.compress(edge_iri)
                    if edge_curie is None:
                        edge_mapping_failures.append(edge_iri)
                        # print(f'No prefix mapping found for: {edge_iri}')
                    self.edge_curies[edge_id] = edge_curie

            if self.logger:
                self.logger.info(f'Ubergraph iri to curie conversion results:\n')
                self.logger.info(f'Nodes: {len(self.node_curies)} successfully converted, {len(node_mapping_failures)} failures.')
                if node_mapping_failures:
                    self.logger.info(f'Node conversion failure examples: {node_mapping_failures[:10]}')
                self.logger.info(f'Edges: {len(self.edge_curies)} successfully converted, {len(edge_mapping_failures)} failures.')
                if node_mapping_failures:
                    self.logger.info(f'Edge conversion failure examples: {edge_mapping_failures[:10]}')

    def get_curie_for_node_id(self, node_id):
        return self.node_curies[node_id]

    def get_curie_for_edge_id(self, edge_id):
        return self.edge_curies[edge_id]

    @staticmethod
    def get_biolink_prefix_map():
        # TODO - ideally this would be a specific version of the biolink model, that's not supported by parsers yet
        response = requests.get \
            ('https://raw.githubusercontent.com/biolink/biolink-model/master/prefix-map/biolink-model-prefix-map.json')
        if response.status_code != 200:
            response.raise_for_status()

        biolink_prefix_map = response.json()

        for duplicate_mapping in BIOLINK_DUPLICATE_MAPPINGS:
            if duplicate_mapping in biolink_prefix_map:
                del (biolink_prefix_map[duplicate_mapping])
        kegg_keys = []
        for key, value in biolink_prefix_map.items():
            if 'KEGG.' in key:
                kegg_keys.append(key)
        for key in kegg_keys:
            del (biolink_prefix_map[key])

        biolink_prefix_map.update(BIOLINK_MAPPING_CHANGES)
        return biolink_prefix_map

    @staticmethod
    def get_latest_source_version(ubergraph_url: str):
        sparql_url = f'{ubergraph_url}/sparql'
        sparql_query = 'PREFIX dcterms: <http://purl.org/dc/terms/> SELECT DISTINCT * WHERE { <http://reasoner.renci.org/ontology> dcterms:created ?date . }'
        headers = {'Accept': 'text/tab-separated-values'}
        payload = {'query': sparql_query}
        response = requests.get(sparql_url, headers=headers, params=payload)
        if response.status_code != 200:
            response.raise_for_status()

        for response_line in response.content.decode('utf-8').splitlines():
            if 'dateTime' in response_line:
                date_time = response_line.split("^^")[0]
                date_time = date_time.strip('"')
                date_time = date_time.split('T')[0]
                return date_time

        raise Exception('Could not establish version from sparql query')
