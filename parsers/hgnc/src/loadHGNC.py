import argparse
import csv
import os
import requests

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge
from Common.prefixes import HGNC, HGNC_FAMILY
from Common.biolink_constants import *


##############
# Class: HGNC loader
#
# By: Phil Owen
# Date: 3/31/2021
# Desc: Class that loads/parses the HGNC data.
##############
class HGNCLoader(SourceDataLoader):

    source_id: str = HGNC
    provenance_id: str = 'infores:hgnc'
    parsing_version: str = '1.3'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.source_db = 'HUGO Gene Nomenclature Committee'
        self.complete_set_file_name = 'hgnc_complete_set.txt'
        self.data_file = self.complete_set_file_name
        self.data_url = "https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/"

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return: the data version
        """
        headers = {"Accept": "application/json"}
        info_response = requests.get('https://www.genenames.org/rest/info', headers=headers)
        if info_response.ok:
            info_json = info_response.json()
            modified_date = info_json['lastModified']
            latest_version = modified_date.split('T')[0]
            return latest_version
        else:
            info_response.raise_for_status()

    def get_data(self) -> int:
        """
        Gets the HGNC data from two sources.

        """
        gd: GetData = GetData()
        data_file_url = self.data_url + self.data_file
        gd.pull_via_http(url=data_file_url, data_dir=self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        :return: ret_val: metadata about the parsing
        """
        # get the path to the data file
        infile_path: str = os.path.join(self.data_path, self.complete_set_file_name)

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        with open(infile_path, 'r', encoding="utf-8") as fp:
            # get the data columns
            cols = ['hgnc_id', 'symbol', 'name', 'locus_group', 'locus_type', 'status', 'location', 'location_sortable', 'alias_symbol', 'alias_name', 'prev_symbol',
                    'prev_name', 'gene_family', 'gene_family_id', 'date_approved_reserved', 'date_symbol_changed', 'date_name_changed', 'date_modified', 'entrez_id',
                    'ensembl_gene_id', 'vega_id', 'ucsc_id', 'ena', 'refseq_accession', 'ccds_id', 'uniprot_ids', 'pubmed_id', 'mgd_id', 'rgd_id', 'lsdb', 'cosmic',
                    'omim_id', 'mirbase', 'homeodb', 'snornabase', 'bioparadigms_slc', 'orphanet', 'pseudogene.org', 'horde_id', 'merops', 'imgt', 'iuphar',
                    'kznf_gene_catalog', 'mamit-trnadb', 'cd', 'lncrnadb', 'enzyme_id', 'intermediate_filament_db', 'rna_central_ids', 'lncipedia', 'gtrnadb', 'agr']

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '?', fp), delimiter='\t', fieldnames=cols)

            # init the first record flag
            first: bool = True

            # for each record
            for r in data:
                # first record is the column header
                if first:
                    # set the flag and skip this record
                    first = False
                    continue

                # increment the counter
                record_counter += 1

                # did we get a valid record
                if len(r['gene_family_id']) > 0:
                    # create the gene node
                    gene_id = r['hgnc_id']
                    gene_name = r['name']
                    gene_props = {'locus_group': r['locus_group'], 'symbol': r['symbol'], 'location': r['location']}
                    gene_node = kgxnode(gene_id, name=gene_name, nodeprops=gene_props)
                    self.final_node_list.append(gene_node)

                    # split the gene family ids and create node/edges for each
                    for idx, gene_family_id in enumerate(r['gene_family_id'].split('|')):
                        # split the gene family name
                        gene_family = r['gene_family'].split('|')

                        # save the gene family curie
                        gene_family_curie = f'{HGNC_FAMILY}:' + gene_family_id

                        # create the gene family node
                        gene_family_node = kgxnode(gene_family_curie, name=gene_family[idx])
                        self.final_node_list.append(gene_family_node)

                        # get the baseline properties
                        props = {KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                                 AGENT_TYPE: MANUAL_AGENT}

                        # were there publications
                        if len(r['pubmed_id']) > 0:
                            props[PUBLICATIONS] = ['PMID:' + v for v in r['pubmed_id'].split('|')]

                        # create the gene to gene family edge
                        new_edge = kgxedge(gene_family_curie,
                                           gene_id,
                                           predicate='BFO:0000051',
                                           primary_knowledge_source=self.provenance_id,
                                           edgeprops=props)
                        self.final_edge_list.append(new_edge)
                else:
                    skipped_record_counter += 1

        # TODO parse the hgnc genes in group file?, gene_groups_file_name: str

        self.logger.debug(f'Parsing data file complete.')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # return to the caller
        return load_metadata
