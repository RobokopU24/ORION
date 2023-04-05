import argparse
import csv
import os

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge
from Common.prefixes import HGNC, HGNC_FAMILY


##############
# Class: HGNC metabolites loader
#
# By: Phil Owen
# Date: 3/31/2021
# Desc: Class that loads/parses the HGNC data.
##############
class HGNCLoader(SourceDataLoader):

    source_id: str = HGNC
    provenance_id: str = 'infores:hgnc'
    description = "The HUGO Gene Nomenclature Committee (HGNC) database provides open access to HGNC-approved unique symbols and names for human genes, gene groups, and associated resources, including links to genomic, proteomic and phenotypic information."
    source_data_url = "ftp://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/archive/"
    license = "https://www.genenames.org/about/"
    attribution = "https://www.genenames.org/about/"
    parsing_version: str = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.complete_set_file_name = 'hgnc_complete_set.txt'
        self.data_files: list = [self.complete_set_file_name, 'hgnc_genes_in_groups.txt']
        self.test_mode: bool = test_mode
        self.source_db: str = 'HUGO Gene Nomenclature Committee'

        self.ftp_site = 'ftp.ebi.ac.uk'
        self.ftp_dir = '/pub/databases/genenames/hgnc/tsv/'

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return: the data version
        """

        data_puller = GetData()
        data_file_date = data_puller.get_ftp_file_date(self.ftp_site, self.ftp_dir, self.data_files[0])
        return data_file_date

    def get_data(self) -> int:
        """
        Gets the HGNC data from two sources.

        """
        # get a reference to the data gathering class
        gd: GetData = GetData(self.logger.level)

        # TODO
        # if self.test_mode:
        #   set up test data instead
        # else:
        # get the complete data set
        file_count: int = gd.pull_via_ftp(self.ftp_site, self.ftp_dir, [self.data_files[0]], self.data_path)

        # did we get the file
        if file_count > 0:
            # get the gene groups dataset
            byte_count: int = gd.pull_via_http('https://www.genenames.org/cgi-bin/genegroup/download-all/' + self.data_files[1], self.data_path)

            # did we get the data
            if byte_count > 0:
                file_count += 1

        # return the file count to the caller
        return file_count

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
                        props = {}

                        # were there publications
                        if len(r['pubmed_id']) > 0:
                            props.update({'publications': ['PMID:' + v for v in r['pubmed_id'].split('|')]})

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
