import os
import argparse
from ViralProteome.src.loadVP import VPLoader
from ViralProteome.src.loadUniRef import UniRefSimLoader
from IntAct.src.loadIA import IALoader
from GOA.src.loadGOA import GOALoader
from Common.utils import LoggingUtil
from pathlib import Path

# create a logger
logger = LoggingUtil.init_logging("Data_services.ViralProteome.load_all", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[0], 'logs'))


if __name__ == '__main__':
    """ 
    Parses both the UniProtKB viral proteome and UniRef data and creates KGX import files for each. 

    Example command lines:    
    -p E:/Data_services/UniProtKB_data 
    -r E:/Data_services/UniRef_data
    -f uniref100,uniref90,uniref50
    -i E:/Data_services/IntAct_data
    -g E:/Data_services/UniProtKB_data
    
    """
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB viral proteome and UniRef data files and create KGX import files.')

    # declare command line arguments
    ap.add_argument('-p', '--uniprot_dir', required=False, help='The directory of the UniProtKB data files.')
    ap.add_argument('-r', '--uniref_dir', required=False, help='The directory of the UniRef data files.')
    ap.add_argument('-f', '--uniref_files', required=False, help='Comma separated UniRef data file(s) to parse.')
    ap.add_argument('-i', '--intact_dir', required=False, help='The directory of the IntAct data files.')
    ap.add_argument('-g', '--goa_dir', required=False, help='The directory of the GOA data files.')

    # parse the arguments
    args = vars(ap.parse_args())

    # assign the uniprot dir
    UniProtKB_data_dir = args['uniprot_dir']

    if UniProtKB_data_dir is not None:
        # get a reference to the processor
        vp = VPLoader()

        # load the data files and create KGX output
        vp.load(UniProtKB_data_dir, 'Viral_proteome_GOA')

    # assign the uniref directory and target files
    UniRef_data_dir = args['uniref_dir']
    UniRef_files = args['uniref_files']

    if UniRef_data_dir is not None and UniRef_files is not None:
        # get a reference to the processor
        uni = UniRefSimLoader()

        # load the data files and create KGX output
        uni.load(UniRef_data_dir, UniRef_files.split(','), 'taxon_file_indexes.txt')

    # assign the uniref directory
    IntAct_data_dir = args['intact_dir']

    if IntAct_data_dir is not None:
        # get a reference to the processor
        ia = IALoader()

        # load the data files and create KGX output files
        ia.load(IntAct_data_dir, 'intact')

    # assign the uniref directory
    GOA_data_dir = args['goa_dir']

    if GOA_data_dir is not None:
        # get a reference to the processor
        goa = GOALoader()

        # load the data files and create KGX output files
        goa.load(GOA_data_dir, '/HUMAN/', 'goa_human.gaf.gz', 'Human_GOA')
