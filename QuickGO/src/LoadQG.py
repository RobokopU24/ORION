import os
import hashlib
import argparse
import requests
import enum
import pandas as pd
from io import TextIOBase, TextIOWrapper
from csv import reader
from zipfile import ZipFile
from ftplib import FTP


# data column enumerators
class DataCols(enum.IntEnum):
    placeholder = 0

##############
# Class: QuickGo data loader
#
# By: Phil Owen
# Date: 6/23/2020
# Desc: Class that loads/parses the QuickGO data and creates KGX files for importing into a Neo4j graph.
##############
class QGLoader:

    # storage for cached node normalizations
    cached_node_norms: dict = {}

    # storage for experiment groups to write to file.
    experiment_grp_list: list = []

    def load(self, data_path: str, out_name: str, data_file_name: str):
        """
        Loads/parsers the QuickGO data file to produce node/edge KGX files for importation into a graph database.

        :param data_path: the directory that contains the data file
        :param out_name: The output file name prefix
        :return: None
        """
        self.print_debug_msg(f'Start of QuickGO data processing. Getting data archive.')

        # get the Intact zip file
        if self.pull_via_ftp('ftp.ebi.ac.uk', '/pub/contrib/goa/QuickGO/full/', data_path, data_file_name):
            self.print_debug_msg(f'{data_file_name} archive retrieved. Parsing QuickGO data.')

            with open(os.path.join(data_path, f'{out_name}_node_file.csv'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_path, f'{out_name}_edge_file.csv'), 'w', encoding="utf-8") as out_edge_f:
                # write out the node and edge data headers
                out_node_f.write(f'id,name,category,equivalent_identifiers,taxon\n')
                out_edge_f.write(f'id,subject,relation_label,edge_label,publications,detection_method,object\n')

                # parse the data
                self.parse_data_file(os.path.join(data_path, data_file_name), out_node_f, out_edge_f)
            self.print_debug_msg(f'File parsing complete.')
        else:
            self.print_debug_msg(f'Error getting the IntAct archive. Exiting.')

    def parse_data_file(self, infile_path: str, infile_name: str, out_node_f: TextIOBase, out_edge_f: TextIOBase):
        """
        Parses the data file for graph nodes/edges and writes them out the KGX csv files.

        :param infile_path: the the data file path
        :param infile_path: the name of the data file to process
        :param out_edge_f: the edge file pointer
        :param out_node_f: the node file pointer
        :return: ret_val: the node list
        """
        with ZipFile(infile_path) as zf:
            # open the taxon file indexes and the uniref data file
            with zf.open(infile_name, 'r') as fp:
                # create a csv parser
                lines = reader(TextIOWrapper(fp, "utf-8"), delimiter='\t')

                # while there are lines in the csv file
                for line in lines:
                    # did we get something usable back

                    # prime the experiment group tracker if this is the first time in
                    if first:
                        first = False

    def write_out_data(self, out_edge_f: TextIOBase, out_node_f: TextIOBase):
        """
        writes out the data collected from the QuickGO file to KGX node and edge files

        :param out_edge_f: the edge file
        :param out_node_f: the node file
        :return:
        """

        # write out the edges
        self.write_edge_data(out_edge_f, self.experiment_grp_list)

    def normalize_node_data(self, node_list: list) -> list:
        """
        This method calls the NodeNormalization web service to get the normalized name, category and equivalent identifiers for the node.
        the data comes in as a node list and we will normalize the only the taxon nodes.

        :param node_list: A list with items to normalize
        :return:
        """

        # node list index counter
        node_idx: int = 0

        # save the node list count to avoid grabbing it over and over
        node_count: int = len(node_list)

        # init a list to identify genes that have not been node normed
        tmp_normalize: set = set()

        # iterate through the data and get the keys to normalize

        # convert the set to a list so we can iterate through it
        to_normalize: list = list(tmp_normalize)

        # define the chuck size
        chunk_size: int = 1000

        # init the indexes
        start_index: int = 0

        # get the last index of the list
        last_index: int = len(to_normalize)

        # grab chunks of the data frame
        while True:
            if start_index < last_index:
                # define the end index of the slice
                end_index: int = start_index + chunk_size

                # force the end index to be the last index to insure no overflow
                if end_index >= last_index:
                    end_index = last_index

                # self.print_debug_msg(f'Working block indexes {start_index} to {end_index} of {last_index}.')

                # collect a slice of records from the data frame
                data_chunk: list = to_normalize[start_index: end_index]

                # get the data
                resp: requests.models.Response = requests.get('https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=' + '&curie='.join(data_chunk))

                # did we get a good status code
                if resp.status_code == 200:
                    # convert to json
                    rvs: dict = resp.json()

                    # merge this list with what we have gotten so far
                    merged = {**self.cached_node_norms, **rvs}

                    # save the merged list
                    self.cached_node_norms = merged
                else:
                    # the 404 error that is trapped here means that the entire list of nodes didnt get normalized.
                    # self.print_debug_msg(f'response code: {resp.status_code}')

                    # since they all failed to normalize add to the list so we dont try them again
                    for item in data_chunk:
                        self.cached_node_norms.update({item: None})

                # move on down the list
                start_index += chunk_size
            else:
                break

        # reset the node index
        node_idx: int = 0

        # for each row in the slice add the new id and name
        # iterate through all the node groups
        while node_idx < node_count:
            # get the node list item
            rv = node_list[node_idx]

            # loop through the A/B interactors and taxons in the interaction
            for prefix in ['u_', 't_']:
                for suffix in ['a', 'b']:
                    try:
                        # did we find a normalized value
                        if self.cached_node_norms[rv[prefix + suffix]] is not None:
                            cached_val = self.cached_node_norms[rv[prefix + suffix]]

                            # find the name and replace it with label
                            if 'label' in cached_val['id'] and cached_val['id']['label'] != '':
                                node_list[node_idx][prefix + 'alias_' + suffix + ''] = cached_val['id']['label']

                            # get the categories
                            if 'type' in cached_val:
                                node_list[node_idx][prefix + 'category_' + suffix] = '|'.join(cached_val['type'])
                            else:
                                node_list[node_idx][prefix + 'category_' + suffix] = 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing'

                            # get the equivalent identifiers
                            if 'equivalent_identifiers' in cached_val and len(cached_val['equivalent_identifiers']) > 0:
                                node_list[node_idx][prefix + 'equivalent_identifiers_' + suffix] = '|'.join(list((item['identifier']) for item in cached_val['equivalent_identifiers']))
                            else:
                                node_list[node_idx][prefix + 'equivalent_identifiers_' + suffix] = node_list[node_idx][prefix + suffix]

                            # find the id and replace it with the normalized value
                            node_list[node_idx][prefix + suffix] = cached_val['id']['identifier']
                        else:
                            # put in the defaults
                            if node_list[node_idx][prefix + 'alias_' + suffix] == '':
                                node_list[node_idx][prefix + 'alias_' + suffix] = node_list[node_idx][prefix + suffix]

                            node_list[node_idx][prefix + 'category_' + suffix] = 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing'
                            node_list[node_idx][prefix + 'equivalent_identifiers_' + suffix] = node_list[node_idx][prefix + suffix]
                    except KeyError:
                        # put in the defaults
                        if node_list[node_idx][prefix + 'alias_' + suffix] == '':
                            node_list[node_idx][prefix + 'alias_' + suffix] = node_list[node_idx][prefix + suffix]

                        node_list[node_idx][prefix + 'category_' + suffix] = 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing'
                        node_list[node_idx][prefix + 'equivalent_identifiers_' + suffix] = node_list[node_idx][prefix + suffix]
                        self.print_debug_msg(f"Error finding node normalization {node_list[node_idx][prefix + suffix]}")

            # go to the next index
            node_idx += 1

        # return the updated list to the caller
        return node_list

    @staticmethod
    def write_edge_data(out_edge_f, experiment_grp):
        """
        writes edges for the experiment group list passed

        :param out_edge_f: the edge file
        :param experiment_grp: single experiment data
        :return: nothing
        """

        # self.print_debug_msg(f'Creating edges for {len(node_list)} nodes.')

        # self.print_debug_msg(f'   {node_idx} Entry member edges created.')

    def pull_via_ftp(self, ftp_site: str, ftp_dir: str, file_data_path: str, file: str) -> bool:
        """
        gets the requested files from UniProtKB ftp directory

        :param ftp_site: url of the ftp site
        :param ftp_dir: the directory in the site
        :param file: the name of the file to capture
        :param file_data_path: the destination of the captured file
        :return: None
        """

        # init the return value
        ret_val: bool = False

        try:
            # open the FTP connection and go to the directory
            ftp: FTP = FTP(ftp_site)
            ftp.login()
            ftp.cwd(ftp_dir)

            # does the file exist and has data in it
            try:
                size: int = os.path.getsize(os.path.join(file_data_path, file))
            except FileNotFoundError:
                size: int = 0

            # if we have a size we done need to get the file
            if size == 0:
                # open the file
                with open(os.path.join(file_data_path, file), 'wb') as fp:
                    # get the file data into a file
                    ftp.retrbinary(f'RETR {file}', fp.write)
            else:
                self.print_debug_msg(f'Archive retrieval complete.')

            # close the ftp object
            ftp.quit()

            # set the return value
            ret_val = True
        except Exception as e:
            print(f'Pull_via_ftp() failed. Exception: {e}')

        # retuen pass/fail to the caller
        return ret_val


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Loads the QuickGO data file and creates KGX import files.')

    # command line should be like: python loadQG.py -d /projects/stars/QuickGO/
    ap.add_argument('-d', '--data_dir', required=True, help='The QuickGO data file directory')

    # parse the arguments
    args = vars(ap.parse_args())

    # QuickGO_data_dir = ''
    # QuickGO_data_dir = ''
    # QuickGO_data_dir = 'D:\Work\Robokop\QuickGO'
    QuickGO_data_dir = args['data_dir']

    # get a reference to the processor
    qg = QGLoader()

    # load the data files and create KGX output files
    qg.load(QuickGO_data_dir, 'quickgo', '2017-11-01-04-00-22.zip')
