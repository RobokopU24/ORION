from ftplib import FTP
import os
import argparse
import csv
from csv import reader
from pathlib import Path
from src.loadVP import VPLoader

# get a reference to the processor
vp = VPLoader()

# organism types
TYPE_BACTERIA: str = '0'
TYPE_VIRUS: str = '9'


def pull_via_ftp(ftp_site: str, ftp_dir: str, file_filter: str, file_data_path: str, files: list) -> (int, int):
    """
    gets the requested files from UniProtKB ftp directory

    :param ftp_site: url of the ftp site
    :param ftp_dir: the directory in the site
    :param file_filter: what files to gather
    :param file_data_path: the destination of the captured file
    :param files: the list of files to capture
    :return: the expected and discovered file counts
    """
    # start a counter
    counter: int = 0

    try:
        # open the FTP connection and go to the directory
        ftp: FTP = FTP(ftp_site)
        ftp.login()
        ftp.cwd(ftp_dir)

        # for each .goa file
        for f in files:
            # does the file exist and has data in it
            try:
                size: int = os.path.getsize(file_data_path + f)
            except FileNotFoundError:
                size: int = 0

            # if we have a size we done need to get the file
            if size == 0:
                # open the file
                with open(file_data_path + f, 'wb') as fp:
                    # get the file data into a file
                    ftp.retrbinary(f'RETR {f}', fp.write)

            # inform user of progress
            counter += 1

            if counter % 50 == 0:
                print(f'{counter} {file_filter} files processed, {len(files) - counter} to go.')

        # close the ftp object
        ftp.quit()

        print(f'{counter} total {file_filter} files processed.')

    except Exception as e2:
        print(f'Exception: {e2}')
        return len(files), counter

    # return the number of files expected and the actual number processed to the caller
    return len(files), counter


def get_taxon_id_list(taxon_data_dir: str, infile_name: str, organism_type: str) -> list:
    """
    gets the files associated with viruses and/or bacteria
    the nodes.dmp file can be found in the archive: ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz

    :param taxon_data_dir: the location of the nodes.dump file
    :param infile_name: the proteome to taxon id file name
    :param organism_type: the organism type
    :return: a list of file indexes
    """
    ret_val: list = []

    with open(os.path.join(taxon_data_dir, infile_name), 'r') as fp:
        # create a csv reader for it
        csv_reader: reader = csv.reader(fp, delimiter='\t')

        # for the rest of the lines in the file. type 0 = TYPE_BACTERIA, type 9 = TYPE_VIRUS
        for line in csv_reader:
            if line[8] == organism_type:  # in [TYPE_BACTERIA, TYPE_VIRUS]
                ret_val.append(line[0])

    # return the list
    return ret_val


def create_file_list(proteome_data_dir: str, infile_name: str, organism_type: str, taxon_ids: list, file_data_path=None) -> list:
    """
    gets the list of file names that will be downloaded
    the proteome2taxid file can be found in the ftp directory at: ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/

    :param proteome_data_dir: the location of the proteome to taxon id file
    :param infile_name: the proteome to taxon id file name
    :param organism_type: one of two types supported
    :param taxon_ids: the taxon ids
    :param file_data_path: the path to where the data files
    :return: the list of file names to get
    """
    files: list = []

    with open(os.path.join(proteome_data_dir, infile_name), 'r') as fp:
        # create a csv reader for it
        csv_reader: reader = csv.reader(fp, delimiter='\t')

        # spin through the list and get the file name
        for line in csv_reader:
            # is this file in the list
            if line[1] in taxon_ids:
                # are we checking file existence
                if file_data_path is not None:
                    # get the location of the file
                    file_path = Path(file_data_path + line[2])

                    # check if the file exists
                    if not file_path.is_file():
                        print(f'{line[2]} not found.')

                # save the file in the list
                files.append(line[2])

    # sort the file list
    ret_val: list = sorted(files)

    # get the file name based on organism type
    if organism_type == TYPE_VIRUS:
        out_file_name: str = 'file_list_virus.txt'

        # add the sars cov-2 file manually
        ret_val.append('uniprot_sars-cov-2.gaf')
    else:
        out_file_name: str = 'file_list_bacteria.txt'

    print(f'{len(ret_val)} total files found.')

    # write out the file list
    with open(os.path.join('../src/', out_file_name), 'w') as fp2:
        # create the file with the list
        for f in ret_val:
            fp2.write(f + '\n')

    # return the list to the caller
    return ret_val


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Gather UniProtKB viral proteome data files.')

    # command line should be like: python get_vp_files.py -d /projects/stars/VP_data/UniProtKB_data
    ap.add_argument('-d', '--data_dir', required=True, help='The location of the UniProtKB data files')

    # parse the arguments
    args = vars(ap.parse_args())

    # assign the goa proteome data directory.
    # data_dir = '\\\\nuc2\\renci\\Work\\Robokop\\VP_data\\UniProtKB_data'
    # data_dir = '/projects/stars/VP_data/UniProtKB_data'
    # data_dir = 'D:/Work/Robokop/VP_data/UniProtKB_data'
    data_dir = args['data_dir']

    # init local and return variables
    attempts: int = 0
    target_count: int = 0
    actual_count: int = 0

    vp.print_debug_msg('Parsing taxon id list')

    # open the nodes.dmp file to get a list of tax ids (col 0) with divisions (col 6) TYPE_BACTERIA or TYPE_VIRUS
    taxonid_list: list = get_taxon_id_list(data_dir, 'nodes.dmp', TYPE_VIRUS)

    vp.print_debug_msg('Creating input file list')

    # open the proteome to tax id file (proteome2taxid) and get the list of file names using the organism type and taxon id
    # add the fil_data_dir to the call (Virus_GOA_files/) if you also want to detect if the file has not already been captured
    file_list: list = create_file_list(data_dir, 'proteome2taxid', TYPE_VIRUS, taxonid_list)

    # append the path to have where the GOA files are going to be
    data_dir += 'Virus_GOA_files/'

    vp.print_debug_msg('Getting files from FTP site.')

    # this FTP site is not very resilient
    while attempts < 25:
        try:
            # get the 1 sars-cov-2 file
            pull_via_ftp('ftp.ebi.ac.uk', '/pub/contrib/goa/', 'uniprot_sars-cov-2.gaf', data_dir, ['uniprot_sars-cov-2.gaf'])

            # get the rest of the files
            target_count, actual_count = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/GO/goa/proteomes/', '*.goa', data_dir, file_list)

            # if we got all the files
            if target_count != 0 and (target_count == actual_count):
                break
        # handle issues in file retrieval
        except Exception as e:
            vp.print_debug_msg(f'target: {target_count}, actual: {actual_count}, attempts: {attempts}, {e}')
            attempts += 1
        finally:
            vp.print_debug_msg('Archiving directory...')

            # import tarfile
            # open a new tar file for writing in compressed mode
            # with tarfile.open('goa_files.tgz', 'w:gz') as tar_out:
            #     # add the file to the tar ball
            #     tar_out.add(data_path, arcname='.')
