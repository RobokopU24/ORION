import os
import csv
import argparse
import tarfile

from csv import reader
from Common.utils import GetData
from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from parsers.GOA.src.loadGOA import get_goa_predicate, get_goa_edge_properties, get_goa_subject_props, DATACOLS


##############
# Class: Virus Proteome loader
#
# By: Phil Owen
# Date: 4/21/2020
# Desc: Class that loads the Virus Proteome data and creates KGX files for importing into a Neo4j graph.
##############
class VPLoader(SourceDataLoader):

    source_id = 'ViralProteome'
    provenance_id = 'infores:goa'
    description = "The Gene Ontology (GO) Consortium’s Viral Proteome resource provides open access to curated assignment of GO terms to proteins and proteome relationships derived from the UniProt KnowledgeBase for all NCBI Taxa considered viruses."
    source_data_url = "https://www.ebi.ac.uk/GOA/proteomes"
    license = "https://www.ebi.ac.uk/about/terms-of-use"
    attribution = "https://www.ebi.ac.uk/about/terms-of-use"
    parsing_version: str = '1.1'

    # organism types
    TYPE_BACTERIA: str = '0'
    TYPE_VIRUS: str = '9'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.ncbi_taxon_file = 'nodes.dmp'
        self.ncbi_taxon_archive = 'taxdump.tar.gz'
        self.proteome_to_taxon_file = 'proteome2taxid'
        self.uniprot_sars_cov_2_file = 'uniprot_sars-cov-2.gaf'

        self.data_files = [self.ncbi_taxon_archive,
                           self.uniprot_sars_cov_2_file,
                           self.proteome_to_taxon_file]

        self.uniprot_file_list = []

        self.current_organism_type = self.TYPE_VIRUS

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        # leaving here - should the sars-cov file date be included in the version? probably not
        # sars_version: str = self.gd.get_ftp_file_date('ftp.ebi.ac.uk', '/pub/contrib/goa/', self.uniprot_sars_cov_2_file)
        # ret_val = f'{sars_version}_{proteome_version}'

        gd = GetData()
        proteomes_version = gd.get_ftp_file_date('ftp.ebi.ac.uk', '/pub/databases/GO/goa/proteomes/', 'datestamp')
        latest_version = f'{proteomes_version}'
        return latest_version

    def get_data(self):
        gd = GetData()

        # fetch the taxon to organism type mapping file
        success = gd.pull_via_ftp('ftp.ncbi.nih.gov',
                        '/pub/taxonomy',
                        [self.ncbi_taxon_archive],
                        self.data_path)

        # fetch the proteome to taxon mapping file
        success_2 = gd.pull_via_ftp('ftp.ebi.ac.uk',
                                  '/pub/databases/GO/goa/proteomes',
                                  [self.proteome_to_taxon_file],
                                  self.data_path)
        if not success and success_2:
            return False

        self.uniprot_file_list = self.get_uniprot_file_list()

        # get the relevant proteome files
        file_count = gd.pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/GO/goa/proteomes/', self.uniprot_file_list, self.data_path)

        # get the 1 sars-cov-2 file
        file_count_2 = gd.pull_via_ftp('ftp.ebi.ac.uk', '/pub/contrib/goa/', [self.uniprot_sars_cov_2_file], self.data_path)

        return file_count and file_count_2

    def get_uniprot_file_list(self):
        """
        :return: the set of file names to get
        """
        target_taxa_set = self.get_taxon_ids_for_organism_type(self.current_organism_type)
        file_list: list = []
        with open(os.path.join(self.data_path, self.proteome_to_taxon_file), 'r') as fp:
            csv_reader: reader = csv.reader(fp, delimiter='\t')
            for line in csv_reader:
                if line[1] in target_taxa_set:
                    file_list.append(line[2])
                    if self.test_mode and len(file_list) == 100:
                        break
        self.logger.info(f'Finished Uniprot proteome to taxon file parsing.. '
                         f'found {len(file_list)} files (for {len(target_taxa_set)} taxa)')
        return file_list

    def get_taxon_ids_for_organism_type(self, organism_type: str) -> set:
        """
        gets the taxon ids associated with the organism_type

        :param: the organism type
        :return: a list of file indexes
        """
        taxon_ids: set = set()
        tar_file_path = os.path.join(self.data_path, self.ncbi_taxon_archive)
        tar_file = tarfile.open(tar_file_path, 'r:gz')
        fp = tar_file.extractfile(self.ncbi_taxon_file)
        for line in fp.readlines():
            # remove the trailing \t and split by the line delimiter
            new_line = line.decode('utf-8').strip().split('\t|\t')
            parts = [x.strip() for x in new_line]
            if parts[4] == organism_type:
                taxon_ids.add(parts[0])
        fp.close()
        tar_file.close()
        self.logger.info(f'Finished NCBI taxon parsing.. '
                         f'found {len(taxon_ids)} taxon ids for organism type {organism_type}')
        return taxon_ids

    def parse_data(self):
        """
        loads goa and gaf associated data gathered from ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/

        :param: nodes_output_file_path - path to node file
        :param: edges_output_file_path - path to edge file
        :return: dict of load statistics
        """
        self.logger.debug(f'VPLoader - Start of viral proteome data processing.')
        if not self.uniprot_file_list:
            self.uniprot_file_list = self.get_uniprot_file_list()

        all_files = self.uniprot_file_list
        all_files.append(self.uniprot_sars_cov_2_file)
        extractor = Extractor(file_writer=self.output_file_writer)
        file_count = 0
        for f in all_files:
            file_count += 1
            with open(os.path.join(self.data_path, f), 'r', encoding="utf-8") as fp:
                extractor.csv_extract(fp,
                                      lambda line: f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}',
                                      # extract subject id,
                                      lambda line: f'{line[DATACOLS.GO_ID.value]}',  # extract object id
                                      lambda line: get_goa_predicate(line),  # predicate extractor
                                      lambda line: get_goa_subject_props(line),  # subject props
                                      lambda line: {},  # object props
                                      lambda line: get_goa_edge_properties(line),  # edge props
                                      comment_character="!", delim='\t')

        return extractor.load_metadata

if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB viral proteome data files and create KGX import files.')

    # command line should be like: python loadVP.py -p /projects/stars/Data_services/UniProtKB_data
    ap.add_argument('-p', '--data_path', required=True, help='The location of the VP data files')

    # parse the arguments
    args = vars(ap.parse_args())

    UniProtKB_data_dir = args['data_dir']
    vp = VPLoader()

    # load the data files and create KGX output
    vp.load(UniProtKB_data_dir, UniProtKB_data_dir)
