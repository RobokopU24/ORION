import csv
import re
import requests

from pathlib import Path

from orion.utils import GetData, GetDataPullError
from orion.loader_interface import SourceDataLoader
from orion.kgxmodel import kgxnode, kgxedge
from orion.prefixes import HGNC, HGNC_FAMILY
from orion.biolink_constants import AGENT_TYPE, MANUAL_AGENT, KNOWLEDGE_ASSERTION, KNOWLEDGE_LEVEL, PUBLICATIONS


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
    parsing_version: str = '1.6'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.source_db = 'HUGO Gene Nomenclature Committee'
        self.complete_set_file_name = 'hgnc_complete_set.txt'
        self.data_file = self.complete_set_file_name

        # HGNC publishes dated monthly snapshots and frequent/daily updates to files.
        # We used to use the daily builds but they caused unnecessary updates too frequently,
        # here we use a (bi)monthly build.
        self.archive_prefix = 'hgnc/archive/archive/monthly/tsv/hgnc_complete_set'
        self.archive_listing_url = f'https://storage.googleapis.com/storage/v1/b/public-download-files/o' \
                                   f'?prefix={self.archive_prefix}&fields=items(name)'
        self.data_url = f'https://storage.googleapis.com/public-download-files/{self.archive_prefix}'

        # lazy load the fetched date/version
        self.latest_version = None

        self.member_of_predicate = "RO:0002350"

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data, the release date of the most recent monthly archive

        :return: the data version
        """
        if self.latest_version:
            return self.latest_version

        listing_response = requests.get(self.archive_listing_url)
        listing_response.raise_for_status()

        archive_file_pattern = re.compile(rf'{re.escape(self.archive_prefix)}_(\d{{4}}-\d{{2}}-\d{{2}})\.txt$')
        release_dates = [match.group(1) for item in listing_response.json().get('items', [])
                         if (match := archive_file_pattern.match(item['name']))]
        if not release_dates:
            raise GetDataPullError(f'Could not find any HGNC monthly archive releases at {self.archive_listing_url}')

        # the dates sort correctly as strings, the most recent one is the latest release
        self.latest_version = max(release_dates)
        return self.latest_version

    def get_data(self) -> int:
        """
        Gets the HGNC data.

        """
        gd: GetData = GetData()
        data_file_url = f'{self.data_url}_{self.get_latest_source_version()}.txt'
        gd.pull_via_http(url=data_file_url,
                         data_dir=self.data_path,
                         saved_file_name=self.complete_set_file_name)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        :return: ret_val: metadata about the parsing
        """
        record_counter: int = 0
        skipped_record_counter: int = 0
        data_file = Path(self.data_path) / self.complete_set_file_name
        with data_file.open('r') as file:
            dict_reader = csv.DictReader(file, delimiter='\t')
            for r in dict_reader:
                if not r["gene_group_id"]:
                    skipped_record_counter += 1
                    continue

                # extract gene node information and make a node
                gene_id = r['hgnc_id']
                gene_name = r['name']
                gene_props = {'locus_group': r['locus_group'], 'symbol': r['symbol'], 'location': r['location']}
                gene_node = kgxnode(gene_id, name=gene_name, nodeprops=gene_props)
                self.output_file_writer.write_kgx_node(gene_node)

                # split the gene group ids and names and iterate through them
                gene_group_ids = r['gene_group_id'].split('|')
                gene_group_names = r['gene_group'].split('|')
                for gene_group_id, gene_group_name in zip(gene_group_ids, gene_group_names):

                    # "gene group" is the hgnc family id, make nodes for them
                    gene_family_id = f'{HGNC_FAMILY}:{gene_group_id}'
                    gene_family_node = kgxnode(gene_family_id, name=gene_group_name)
                    self.output_file_writer.write_kgx_node(gene_family_node)

                    # make a gene family to gene edge
                    # include publications as an edge property if there are any
                    edge_props = {KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                                  AGENT_TYPE: MANUAL_AGENT}
                    if r['pubmed_id']:
                        edge_props[PUBLICATIONS] = [f'PMID:{pmid}' for pmid in r['pubmed_id'].split('|')]
                    new_edge = kgxedge(subject_id=gene_id,
                                       object_id=gene_family_id,
                                       predicate=self.member_of_predicate,
                                       primary_knowledge_source=self.provenance_id,
                                       edgeprops=edge_props)
                    self.output_file_writer.write_kgx_edge(new_edge)
                    record_counter += 1

        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }
        return load_metadata
