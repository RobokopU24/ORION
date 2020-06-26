import os
import argparse
from src.loadVP import VPLoader
from src.loadUniRef2 import UniRefSimLoader

from Common.utils import LoggingUtil
from pathlib import Path

# create a logger
logger = LoggingUtil.init_logging("Data_services.ViralProteome.load_all", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))


if __name__ == '__main__':
    """ Parses both the UniProtKB viral proteome and UniRef data and creates KGX import files for each. """
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB viral proteome and UniRef data files and create KGX import files.')

    # command line should be like: python load_all.py -p /projects/stars/VP_data/UniProtKB_data -u /projects/stars/VP_data/UniRef_data -r uniref50,uniref90,uniref100
    ap.add_argument('-p', '--uniprot_dir', required=True, help='The directory of the UniProtKB data files')
    ap.add_argument('-u', '--uniref_dir', required=True, help='The directory of the UniRef data files')
    ap.add_argument('-r', '--uniref_files', required=True, help='Name(s) of input UniRef files (comma delimited)')

    # parse the arguments
    args = vars(ap.parse_args())

    # get a reference to the processor
    vp = VPLoader()

    # assign the uniprot directory
    UniProtKB_data_dir = args['uniprot_dir']

    # open the file list and turn it into a list array
    with open(UniProtKB_data_dir + '/GOA_virus_file_list.txt', 'r') as fl:
        file_list: list = fl.readlines()

    # strip off the trailing '\n'
    file_list = [line[:-1] for line in file_list]

    # load the data files and create KGX output
    vp.load(UniProtKB_data_dir, 'Virus_GOA_files/', file_list, 'VP_Virus')

    logger.info(f'UniProtKB viral proteome processing complete. Starting UniRef processing.\n')

    # assign the file list
    file_list: list = args['uniref_files'].split(',')

    # assign the uniref directory
    UniProtKB_data_dir = args['uniref_dir']

    # get a reference to the processor
    vp = UniRefSimLoader()

    # load the data files and create KGX output
    vp.load(UniProtKB_data_dir, file_list, 'taxon_file_indexes.txt', block_size=10000, debug_files=False)

    logger.info(f'UniRef data parsing and KGX file creation complete.\n')
