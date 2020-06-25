import os
import argparse
from src.loadUniRef2 import UniRefSimLoader

# get a new similarity loader object so we can use the function to get the taxon list
usl = UniRefSimLoader()


def get_entry_element(index_path: str, uniref_path: str, uniref_outfile_path: str):
    """
    gets the uniref "entry" element from the data file

    :param index_path: the path to the index file
    :param uniref_path: the path to the uniref data files
    :param uniref_outfile_path: the location of the output files
    :return:
    """
    line_counter: int = 0

    # open read the index file, uniref file and output file
    with open(index_path, 'r') as index_fp, open(uniref_path, 'rb+') as uniref_fp, open(uniref_outfile_path, 'w') as out_fp:
        # add the xml preamble
        for i in range(0, 5):
            out_fp.write(uniref_fp.readline().decode("utf-8"))

        # for all entry indexes found on the call to grep
        for line in index_fp:
            # increment the line counter
            line_counter += 1

            # output a status indicator
            if line_counter % 10000 == 0:
                usl.print_debug_msg(f'Completed {line_counter} indexes')

            # start looking a bit before the location grep found
            taxon_index = int(line.split(':')[0]) - 150

            # backup up to 500 characters max to look for the "<entry>" start
            for i in range(1, 500):
                # goto the last checked position
                uniref_fp.seek(taxon_index)

                # read 6 characters to see if it is the start of the entry
                uniref_line = uniref_fp.read(6)
                # print(f'{uniref_fp.tell()}: {uniref_line1}')

                # did we find the entry start
                if uniref_line.decode("utf-8") == "<entry":
                    # go back to the start of the 'entry'
                    uniref_fp.seek(taxon_index)

                    # start writing out data until we see the end of the entry
                    while True:
                        # get the line form the uniref file
                        uniref_line = uniref_fp.readline().decode("utf-8")

                        # no need to save the DNA sequence data
                        if uniref_line.startswith('  <seq'):
                            continue

                        # write out the line
                        out_fp.write(uniref_line)

                        # print(f'{uniref_fp.tell()}: {uniref_line1}')

                        # did we find the end of the entry
                        if uniref_line.startswith('</entr'):
                            break
                    break
                else:
                    # move up a character and recheck
                    taxon_index -= 1

        usl.print_debug_msg(f'Completed {line_counter} indexes')


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Index UniRef data files for faster parsing.')

    # command line should be like: python get_uniref_taxon_targets.py -d /projects/stars/VP_data/UniRef_data -f uniref50,uniref90,uniref100
    ap.add_argument('-d', '--data_dir', required=True, help='The location of the UniRef data files')
    ap.add_argument('-f', '--UniRef_files', required=True, help='Name(s) of input UniRef files (comma delimited)')

    # parse the arguments
    args = vars(ap.parse_args())

    # data_dir: str = 'D:/Work/Robokop/VP_data/UniRef_data'
    # data_dir = '/projects/stars/VP_data/UniRef_data'
    # data_dir = '/d/Work/Robokop/VP_data/UniRef_data'
    data_dir = args['data_dir']

    # the files to process
    # in_file_list: list = ['UniRef50']  # , 'UniRef100' UniRef90 UniRef50
    in_file_list: list = args['UniRef_files'].split(',')

    usl.print_debug_msg('Getting taxon id list')

    # get the list of target uniprot ids from the viral proteome node list
    taxa: set = usl.get_virus_taxon_id_set(data_dir, 'nodes.dmp', usl.TYPE_VIRUS)

    usl.print_debug_msg('Creating taxon search file')

    # the path to the file that contains the list of taxa to search for
    search_file_path = data_dir + f'/taxon_list.txt'

    # write out the list of taxa to search for
    with open(search_file_path, 'w') as wfp:
        for item in taxa:
            wfp.write(f'<property type="common taxon ID" value="{item}"\n')

    usl.print_debug_msg('Executing dos2unix command')

    # optional: execute the dos2unix command on the target taxon file to get the line endings correct
    os.system(f"dos2unix \"{search_file_path}\"")

    # for each uniref file type
    for file in in_file_list:
        usl.print_debug_msg(f'Working input file: {file}')

        # get the path to the file with taxon indexes
        index_file_path = data_dir + f'/{file.lower()}_taxon_file_indexes.txt'

        # get the in and out file paths
        uniref_infile_path: str = data_dir + f'/{file.lower()}.xml'

        usl.print_debug_msg('Executing grep command')

        # execute the grep command using the target taxon list
        # Note: you must use the latest version of grep for this to work
        os.system(f"grep -F -b -f \"{search_file_path}\" \"{uniref_infile_path}\" >> \"{index_file_path}\"")

        # optional: create/load the file that contains the entry nodes
        # print_debug_msg('Opening files for index/read/write')
        # uniref_outfile_path: str = data_dir + f'/{file.lower()}_p.xml'
        #
        # get_entry_element(index_file_path, uniref_infile_path, uniref_outfile_path)
