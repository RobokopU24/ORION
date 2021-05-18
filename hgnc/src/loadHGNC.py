import argparse
import logging
import datetime
import csv
import os

import xml.etree.cElementTree as E_Tree
from Common.utils import LoggingUtil, GetData
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader


##############
# Class: HGNC metabolites loader
#
# By: Phil Owen
# Date: 3/31/2021
# Desc: Class that loads/parses the HGNC data.
##############
class HGNCLoader(SourceDataLoader):
    # the final output lists of nodes and edges
    final_node_list: list = []
    final_edge_list: list = []

    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super(SourceDataLoader, self).__init__()

        # set global variables
        self.data_path: str = os.environ['DATA_SERVICES_STORAGE']
        self.data_file: list = ['hgnc_complete_set.txt', 'hgnc_genes_in_groups.txt']
        self.test_mode: bool = test_mode
        self.source_id: str = 'HGNC'
        self.source_db: str = 'HUGO Gene Nomenclature Committee'

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.HGNC.HGNCLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return datetime.datetime.now().strftime("%m/%d/%Y")

    def write_to_file(self, nodes_output_file_path: str, edges_output_file_path: str) -> None:
        """
        sends the data over to the KGX writer to create the node/edge files

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return: Nothing
        """
        # get a KGX file writer
        with KGXFileWriter(nodes_output_file_path, edges_output_file_path) as file_writer:
            # for each node captured
            for node in self.final_node_list:
                # write out the node
                file_writer.write_node(node['id'], node_name=node['name'], node_types=[], node_properties=None)

            # for each edge captured
            for edge in self.final_edge_list:
                # write out the edge data
                file_writer.write_edge(subject_id=edge['subject'], object_id=edge['object'], relation=edge['relation'], edge_properties=edge['properties'], predicate='')

    def get_HGNC_data(self) -> int:
        """
        Gets the HGNC data from two sources.

        """
        # get a reference to the data gathering class
        gd: GetData = GetData(self.logger.level)

        # do the real thing if we arent in debug mode
        if not self.test_mode:
            # get the complete data set
            file_count: int = gd.pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/genenames/hgnc/tsv/', [self.data_file[0]], self.data_path)

            # did we get the file
            if file_count > 0:
                # get the gene groups dataset
                byte_count: int = gd.pull_via_http('https://www.genenames.org/cgi-bin/genegroup/download-all/' + self.data_file[1], self.data_path)

                # did we get the data
                if byte_count > 0:
                    file_count += 1
        else:
            file_count: int = 1

        # return the file count to the caller
        return file_count

    def load(self, nodes_output_file_path: str, edges_output_file_path: str) -> dict:
        """
        parses the HGNC data file gathered from https://HGNC.ca/system/downloads/current/HGNC_metabolites.zip

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return the parsed metadata stats
        """

        self.logger.info(f'HGNCloader - Start of HGNC data processing.')

        # get the list of taxons to process
        file_count = self.get_HGNC_data()

        # init the return
        load_metadata: dict = {}

        # did we get the archives
        if file_count == 2:
            self.logger.debug(f'{self.data_file} archive retrieved. Parsing HGNC data.')

            # parse the data
            load_metadata = self.parse_data_file(self.data_path, self.data_file[0], self.data_file[1])

            self.logger.info(f'HGNCLoader - {self.data_file} Processing complete.')

            # write out the data
            self.write_to_file(nodes_output_file_path, edges_output_file_path)

            self.logger.info(f'HGNCLoader - Processing complete.')
        else:
            self.logger.error(f'Error: Retrieving HGNC archive failed.')

        # return the metadata to the caller
        return load_metadata

    def parse_data_file(self, data_file_path: str, complete_set_file_name: str, gene_groups_file_name: str) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        :param data_file_path: the path to the HGNC data
        :param complete_set_file_name: the name of the HGNC complete set file name
        :param gene_groups_file_name: the name of the HGNC gene groups file name
        :return: ret_val: record counts
        """
        # get the path to the data file
        infile_path: str = os.path.join(data_file_path, complete_set_file_name)

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
            first : bool = True

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
                    self.final_node_list.append({'id': r['hgnc_id'], 'name': r['name'], 'properties': {'locus_group': r['locus_group'], 'symbol': r['symbol'], 'location': r['location']}})

                    # split the gene family ids and create node/edges for each
                    for idx, gene_family_id in enumerate(r['gene_family_id'].split('|')):
                        # split the gene family name
                        gene_family = r['gene_family'].split('|')

                        # save the gene family curie
                        gene_family_curie = f'HGNC.FAMILY:' + gene_family_id

                        # create the gene family node
                        self.final_node_list.append({'id': gene_family_curie, 'name': gene_family[idx]})

                        # get the baseline properties
                        props = {'source_database': 'hgnc'}

                        # were there publications
                        if len(r['pubmed_id']) > 0:
                            props.update({'publications': ['PMID:' + v for v in r['pubmed_id'].split('|')]})

                        # create the gene to gene family edge
                        self.final_edge_list.append({'subject': gene_family_curie, 'relation': 'BFO:0000051', 'object': r['hgnc_id'], 'properties': props})
                else:
                    skipped_record_counter += 1

        # TODO parse the hgnc genes in group file?

        self.logger.debug(f'Parsing XML data file complete.')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # return to the caller
        return load_metadata


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load HGNC data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the HGNC data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    HGNC_data_dir: str = args['data_dir']

    # get a reference to the processor
    HGNC = HGNCLoader()

    # load the data files and create KGX output
    HGNC.load(HGNC_data_dir, HGNC_data_dir)
