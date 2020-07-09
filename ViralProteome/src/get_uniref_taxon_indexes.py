import os
import argparse
from ViralProteome.src.loadUniRef import UniRefSimLoader
from Common.utils import LoggingUtil, GetData
from pathlib import Path

# create a logger
logger = LoggingUtil.init_logging("Data_services.ViralProteome.get_uniref_taxon_indexes", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Index UniRef data files for faster parsing.')

    # command line should be like: python get_uniref_taxon_targets.py -d /projects/stars/Data_services/UniRef_data -f uniref50,uniref90,uniref100
    ap.add_argument('-d', '--data_dir', required=True, help='The location of the UniRef data files')
    ap.add_argument('-f', '--UniRef_files', required=True, help='Name(s) of input UniRef files (comma delimited)')

    # parse the arguments
    args = vars(ap.parse_args())

    # load the utility class to get the virus taxa id list
    gd = GetData()

    # uniref_data_dir: str = 'E:/Data_services/UniRef_data'
    # uniref_data_dir = '/projects/stars/Data_services/UniRef_data'
    # uniref_data_dir = '/e/Data_services/UniRef_data'
    uniref_data_dir = args['data_dir']

    # the files to process
    # in_file_list: list = ['UniRef50']  # , 'UniRef100' UniRef90 UniRef50
    in_file_list: list = args['UniRef_files'].split(',')

    logger.info('Getting taxon id list.')

    # get the list of target taxa
    target_taxa_set: set = gd.get_ncbi_taxon_id_set(uniref_data_dir, UniRefSimLoader().TYPE_VIRUS)

    logger.info('Creating taxon search file.')

    # the path to the file that contains the list of taxa to search for
    search_file_path = os.path.join(uniref_data_dir, 'taxon_list.txt')

    # write out the list of taxa to search for
    with open(search_file_path, 'w') as wfp:
        for item in target_taxa_set:
            wfp.write(f'<property type="common taxon ID" value="{item}"\n')

    logger.info('Executing dos2unix command.')

    # optional: execute the dos2unix command on the target taxon file to get the line endings correct
    os.system(f'dos2unix "{search_file_path}."')

    # for each uniref file type
    for file in in_file_list:
        logger.info(f'Working input file: {file}.')

        # get the path to the file with taxon indexes
        index_file_path = os.path.join(uniref_data_dir, f'{file.lower()}_taxon_file_indexes.txt')

        # get the in and out file paths
        uniref_infile_path: str = os.path.join(uniref_data_dir, f'{file.lower()}.xml')

        logger.info(f'Executing grep command: grep -F -b -f "{search_file_path}" "{uniref_infile_path}" >> "{index_file_path}"')

        # execute the grep command using the target taxon list
        # Note: you must use the latest version of grep for this to work
        # grep -F -b -f "/e/Data_services/UniRef_data/taxon_list.txt" "/e/Data_services/UniRef_data/uniref100.xml" >> "/e/Data_services/UniRef_data/uniref100_taxon_file_indexes.txt"
        # grep -F -b -f "/e/Data_services/UniRef_data/taxon_list.txt" "/e/Data_services/UniRef_data/uniref90.xml" >> "/e/Data_services/UniRef_data/uniref90_taxon_file_indexes.txt"
        # grep -F -b -f "/e/Data_services/UniRef_data/taxon_list.txt" "/e/Data_services/UniRef_data/uniref50.xml" >> "/e/Data_services/UniRef_data/uniref50_taxon_file_indexes.txt"
        os.system(f'grep -F -b -f "{search_file_path}" "{uniref_infile_path}" >> "{index_file_path}"')

    # do not remove the file if in debug mode
    # if logger.level != logging.DEBUG:
    #     # remove the original list of taxon ids
    #     os.remove(search_file_path)
