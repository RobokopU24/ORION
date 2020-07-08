import os
import argparse
from ViralProteome.src.loadVP import VPLoader
from ViralProteome.src.loadUniRef import UniRefSimLoader
from Common.utils import LoggingUtil, GetData
from pathlib import Path

# create a logger
logger = LoggingUtil.init_logging("Data_services.ViralProteome.load_all", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[0], 'logs'))


if __name__ == '__main__':
    """ Parses both the UniProtKB viral proteome and UniRef data and creates KGX import files for each. """
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB viral proteome and UniRef data files and create KGX import files.')

    # command line should be like: python load_all.py -p /projects/stars/Data_services/UniProtKB_data -u /projects/stars/Data_services/UniRef_data -r uniref50,uniref90,uniref100
    ap.add_argument('-p', '--uniprot_dir', required=True, help='The directory of the UniProtKB data files')
    ap.add_argument('-u', '--uniref_dir', required=True, help='The directory of the UniRef data files')
    ap.add_argument('-f', '--uniref_files', required=True, help='Comma separated UniRef data file(s) to parse')

    # parse the arguments
    args = vars(ap.parse_args())

    # assign the uniprot directory
    UniProtKB_data_dir = args['uniprot_dir']

    # assign the uniref directory
    UniRef_data_dir = args['uniref_dir']

    # assign the uniref files
    UniRef_files = args['uniref_files'].split(',')

    # get a reference to the processor
    vp = VPLoader()

    # and get a reference to the data gatherer
    gd = GetData()

    # get the list of target taxa
    target_taxa_set: set = gd.get_ncbi_taxon_id_set(UniProtKB_data_dir, vp.TYPE_VIRUS)

    # get the list of files that contain those taxa
    file_list: list = gd.get_uniprot_virus_file_list(UniProtKB_data_dir, vp.TYPE_VIRUS, target_taxa_set)

    # manually add in the sars cov2 data file
    file_list.append('uniprot_sars-cov-2.gaf')

    # assign the data directory
    goa_data_dir = UniProtKB_data_dir + '/Virus_GOA_files/'

    # get the data files
    actual_count: int = gd.get_goa_files(goa_data_dir, file_list, '/pub/databases/GO/goa', '/proteomes/')

    # did we get all the files
    if len(file_list) == actual_count:
        # load the data files and create KGX output
        vp.load(UniProtKB_data_dir, 'Virus_GOA_files/', file_list, 'Viral_proteome_GOA')
    else:
        logger.error('Error: Did not receive all the UniProtKB GOA files.')

    # get a reference to the processor
    vp = UniRefSimLoader()

    # load the data files and create KGX output
    vp.load(UniRef_data_dir, UniRef_files, 'taxon_file_indexes.txt', block_size=10000, debug_files=False)

    logger.info(f'UniRef data parsing and KGX file creation complete.')
