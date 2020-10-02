import os
import logging
import tarfile
import csv
import gzip
import requests
import pandas as pd

from rdflib import Graph
from urllib.request import urlopen
from csv import reader, DictReader
from ftplib import FTP
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


class LoggingUtil(object):
    """
    creates and configures a logger
    """
    @staticmethod
    def init_logging(name, level=logging.INFO, line_format='short', log_file_path=None):
        """
            Logging utility controlling format and setting initial logging level
        """
        # get a new logger
        logger = logging.getLogger(__name__)

        # is this the root
        if not logger.parent.name == 'root':
            return logger

        # define the various output formats
        format_type = {
            "minimum": '%(message)s',
            "short": '%(funcName)s(): %(message)s',
            "medium": '%(asctime)-15s - %(funcName)s(): %(message)s',
            "long": '%(asctime)-15s  - %(filename)s %(funcName)s() %(levelname)s: %(message)s'
        }[line_format]

        # create a stream handler (default to console)
        stream_handler = logging.StreamHandler()

        # create a formatter
        formatter = logging.Formatter(format_type)

        # set the formatter on the console stream
        stream_handler.setFormatter(formatter)

        # get the name of this logger
        logger = logging.getLogger(name)

        # set the logging level
        logger.setLevel(level)

        # if there was a file path passed in use it
        if log_file_path is not None:
            # create a rotating file handler, 100mb max per file with a max number of 10 files
            file_handler = RotatingFileHandler(filename=os.path.join(log_file_path, name + '.log'), maxBytes=100000000, backupCount=10)

            # set the formatter
            file_handler.setFormatter(formatter)

            # set the log level
            file_handler.setLevel(level)

            # add the handler to the logger
            logger.addHandler(file_handler)

        # add the console handler to the logger
        logger.addHandler(stream_handler)

        # return to the caller
        return logger

    @staticmethod
    def print_debug_msg(msg: str):
        """
        Adds a timestamp to a printed message

        :param msg: the message that gets appended onto a timestamp and output to console
        :return: None
        """

        # get the timestamp
        now: datetime = datetime.now()

        # output the text
        print(f'{now.strftime("%Y/%m/%d %H:%M:%S")} - {msg}')


class NodeNormUtils:
    """
    Class that contains methods relating to node normalization of KGX data.

    the input node list should be KGX compliant and have the following columns that may be
    changed during the normalization:

        id: the id value to be normalized upon
        name: the name of the node
        category: the semantic type(s)
        equivalent_identifiers: the list of synonymous ids
    """

    def __init__(self, log_level=logging.INFO):
        """
        constructor
        :param log_level - overrides default log level
        """
        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.Common.NodeNormUtils", level=log_level, line_format='medium', log_file_path=os.path.join(Path(__file__).parents[1], 'logs'))

    def normalize_node_data(self, node_list: list, cached_node_norms: dict = None, for_json: bool = False, block_size: int = 2500) -> list:
        """
        This method calls the NodeNormalization web service to get the normalized identifier and name of the taxon node.
        the data comes in as a node list.

        :param node_list: A list with items to normalize
        :param cached_node_norms: dict of previously captured normalizations
        :param for_json: flag to indicate json output
        :param block_size: the number of curies in the request
        :return:
        """

        self.logger.debug(f'Start of normalize_node_data. items: {len(node_list)}')

        # init the cache list if it wasn't passed in
        if cached_node_norms is None:
            cached_node_norms: dict = {}

        # init the node index counter
        node_idx: int = 0

        # save the node list count to avoid grabbing it over and over
        node_count: int = len(node_list)

        # init a set to hold taxa that have not yet been node normed
        tmp_normalize: set = set()

        # iterate through node groups and get only the taxa records.
        while node_idx < node_count:
            # check to see if this one needs normalization data from the website
            if not node_list[node_idx]['id'] in cached_node_norms:
                tmp_normalize.add(node_list[node_idx]['id'])
            else:
                self.logger.debug(f"Cache hit: {node_list[node_idx]['id']}")

            # increment to the next node array element
            node_idx += 1

        # convert the set to a list so we can iterate through it
        to_normalize: list = list(tmp_normalize)

        # init the array index lower boundary
        start_index: int = 0

        # get the last index of the list
        last_index: int = len(to_normalize)

        self.logger.debug(f'{last_index} unique nodes found in this group.')

        # grab chunks of the data frame
        while True:
            if start_index < last_index:
                # define the end index of the slice
                end_index: int = start_index + block_size

                # force the end index to be the last index to insure no overflow
                if end_index >= last_index:
                    end_index = last_index

                self.logger.debug(f'Working block {start_index} to {end_index}.')

                # collect a slice of records from the data frame
                data_chunk: list = to_normalize[start_index: end_index]

                # self.logger.info(f'Calling node norm service. request size is {len("&curie=".join(data_chunk))} bytes')

                # get the data
                resp: requests.models.Response = requests.get('https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=' + '&curie='.join(data_chunk))

                # did we get a good status code
                if resp.status_code == 200:
                    # convert json to dict
                    rvs: dict = resp.json()

                    # merge this list with what we have gotten so far
                    merged = {**cached_node_norms, **rvs}

                    # save the merged list
                    cached_node_norms = merged
                else:
                    # the error that is trapped here means that the entire list of nodes didnt get normalized.
                    self.logger.error(f'Node norm response code: {resp.status_code}')

                    # since they all failed to normalize add to the list so we dont try them again
                    for item in data_chunk:
                        cached_node_norms.update({item: None})

                # move on down the list
                start_index += block_size
            else:
                break

        # reset the node index
        node_idx = 0

        # storage for items that failed to normalize
        failed_to_normalize: list = []

        # for each row in the slice add the new id and name
        # iterate through node groups and get only the taxa records.
        while node_idx < node_count:
            # get a reference to the node list
            rv = node_list[node_idx]

            # did we find a normalized value
            if cached_node_norms[rv['id']] is not None:
                # find the name and replace it with label
                if 'label' in cached_node_norms[rv['id']]['id']:
                    node_list[node_idx]['name'] = cached_node_norms[rv['id']]['id']['label']

                # find the type and use it as a category
                if 'type' in cached_node_norms[rv['id']]:
                    if for_json:
                        node_list[node_idx]['category'] = cached_node_norms[rv['id']]['type']
                    else:
                        node_list[node_idx]['category'] = '|'.join(cached_node_norms[rv['id']]['type'])

                # get the equivalent identifiers
                if 'equivalent_identifiers' in cached_node_norms[rv['id']] and len(cached_node_norms[rv['id']]['equivalent_identifiers']) > 0:
                    if for_json:
                        node_list[node_idx]['equivalent_identifiers'] = list(item['identifier'] for item in cached_node_norms[rv['id']]['equivalent_identifiers'])
                    else:
                        node_list[node_idx]['equivalent_identifiers'] = '|'.join(list((item['identifier']) for item in cached_node_norms[rv['id']]['equivalent_identifiers']))

                # find the id and replace it with the normalized value
                node_list[node_idx]['id'] = cached_node_norms[rv['id']]['id']['identifier']
            else:
                # add for display purposes
                failed_to_normalize.append(rv['id'])

            # go to the next node index
            node_idx += 1

        # if something failed to normalize output it
        if len(failed_to_normalize) > 0:
            # remove all nodes that dont have a category as they cant have an edge if they dont
            node_list[:] = [d for d in node_list if d['category'] != '']

            self.logger.debug(f'Of {len(node_list)} nodes, {len(failed_to_normalize)} failed to normalize and were removed: {", ".join(failed_to_normalize)}')

        self.logger.debug(f'End of normalize_node_data.')

        # return the failed list to the caller
        return failed_to_normalize

    def synomymize_node_data(self, node_list: list) -> list:
        # for each node list item
        for idx, item in enumerate(node_list):
            # these types normally get worked in node normalization
            if not item['id'].startswith('NCBIGene:') and not item['id'].startswith('MONDO:'):
                # generate the request url
                url: str = f"https://onto.renci.org/synonyms/{item['id']}"

                # make the call to the synonymizer
                resp: requests.models.Response = requests.get(url)

                # did we get a good status code
                if resp.status_code == 200:
                    # convert json to dict
                    rvs: dict = resp.json()

                    # get the synonyms into a list. this data could potentially have double quotes in it
                    synonyms: list = [x['desc'].replace('"', '\\"') for x in rvs]

                    # did we get anything
                    if len(synonyms) > 0:
                        # save the values to the list
                        node_list[idx]['synonyms'] = '|'.join(synonyms)
                else:
                    self.logger.debug(f'Could not find synonym of r: {item["id"]}')

        # return the list to the caller
        return node_list


class EdgeNormUtils:
    """
    Class that contains methods relating to edge normalization of KGX data.

    the input predicate list should be KGX compliant and have the following columns that may be
    changed during the normalization:

        predicate: the name of the predicate
        relation: the biolink label curie
        edge_label: label of the predicate

    """
    def __init__(self, log_level=logging.INFO):
        """
        constructor
        :param log_level - overrides default log level
        """
        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.Common.EdgeNormUtils", level=log_level, line_format='medium', log_file_path=os.path.join(Path(__file__).parents[1], 'logs'))

    def normalize_edge_data(self, edge_list: list, cached_edge_norms: dict = None, block_size: int = 2500) -> list:
        """
        This method calls the EdgeNormalization web service to get the normalized identifier and labels.
        the data comes in as a edge list.

        :param edge_list: A list with items to normalize
        :param cached_edge_norms: dict of previously captured normalizations
        :param block_size: the number of curies to process in a single call
        :return:
        """

        self.logger.debug(f'Start of normalize_edge_data. items: {len(edge_list)}')

        # init the cache list if it wasn't passed in
        if cached_edge_norms is None:
            cached_edge_norms: dict = {}

        # init the edge index counter
        edge_idx: int = 0

        # save the edge list count to avoid grabbing it over and over
        edge_count: int = len(edge_list)

        # init a set to hold edge relations that have not yet been normed
        tmp_normalize: set = set()

        # iterate through node groups and get only the taxa records.
        while edge_idx < edge_count:
            # check to see if this one needs normalization data from the website
            if not edge_list[edge_idx]['predicate'] in cached_edge_norms:
                tmp_normalize.add(edge_list[edge_idx]['predicate'])
            else:
                self.logger.debug(f"Cache hit: {edge_list[edge_idx]['predicate']}")

            # increment to the next node array element
            edge_idx += 1

        # convert the set to a list so we can iterate through it
        to_normalize: list = list(tmp_normalize)

        # init the array index lower boundary
        start_index: int = 0

        # get the last index of the list
        last_index: int = len(to_normalize)

        self.logger.debug(f'{last_index} unique edges will be normalized.')

        # grab chunks of the data frame
        while True:
            if start_index < last_index:
                # define the end index of the slice
                end_index: int = start_index + block_size

                # force the end index to be the last index to insure no overflow
                if end_index >= last_index:
                    end_index = last_index

                self.logger.debug(f'Working block {start_index} to {end_index}.')

                # collect a slice of records from the data frame
                data_chunk: list = to_normalize[start_index: end_index]

                self.logger.debug(f'Calling edge norm service. request size is {len("&predicate=".join(data_chunk))} bytes')

                # get the data
                resp: requests.models.Response = requests.get('https://edgenormalization-sri.renci.org/resolve_predicate?version=latest&predicate=' + '&predicate='.join(data_chunk))

                self.logger.debug(f'End calling edge norm service.')

                # did we get a good status code
                if resp.status_code == 200:
                    # convert json to dict
                    rvs: dict = resp.json()

                    # merge this list with what we have gotten so far
                    merged = {**cached_edge_norms, **rvs}

                    # save the merged list
                    cached_edge_norms = merged
                else:
                    # the error that is trapped here means that the entire list of nodes didnt get normalized.
                    self.logger.debug(f'Edge norm response code: {resp.status_code}')

                    # since they all failed to normalize add to the list so we dont try them again
                    for item in data_chunk:
                        cached_edge_norms.update({item: None})

                # move on down the list
                start_index += block_size
            else:
                break

        # reset the node index
        edge_idx = 0

        # storage for items that failed to normalize
        failed_to_normalize: list = list()

        # for each row in the slice add the new id and name
        while edge_idx < edge_count:
            # get a reference to the edge list
            rv = edge_list[edge_idx]

            # did we find a normalized value
            if rv['predicate'] in cached_edge_norms and rv['predicate'] != '':
                # find the identifier and make it the relation label
                if 'identifier' in cached_edge_norms[rv['predicate']]:
                    edge_list[edge_idx]['relation'] = cached_edge_norms[rv['predicate']]['identifier']

                # get the label and turn it into a curie
                if 'label' in cached_edge_norms[rv['predicate']]:
                    edge_list[edge_idx]['edge_label'] = f'biolink:{cached_edge_norms[rv["predicate"]]["label"]}'
            else:
                failed_to_normalize.append(rv['predicate'])

            # go to the next edge index
            edge_idx += 1

        # if something failed to normalize output it
        if len(failed_to_normalize) > 0:
            self.logger.debug(f'Failed to normalize: {", ".join(failed_to_normalize)}')

        self.logger.debug(f'End of normalize_edge_data.')

        # return the failed list to the caller
        return failed_to_normalize


class GetData:
    """
    Class that contains methods that can be used to get various data sets.
    """

    def __init__(self, log_level=logging.INFO):
        """
        constructor
        :param log_level - overrides default log level
        """
        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.Common.GetData", level=log_level, line_format='short', log_file_path=os.path.join(Path(__file__).parents[1], 'logs'))

    def pull_via_ftp(self, ftp_site: str, ftp_dir: str, ftp_files: list, data_file_path: str) -> int:
        """
        gets the requested files from UniProtKB ftp directory

        :param ftp_site: url of the ftp site
        :param ftp_dir: the directory in the site
        :param ftp_files: the name of the file to capture
        :param data_file_path: the destination of the captured file
        :return: boolean pass/fail
        """

        # init a retrieved file counter for the return
        file_counter: int = 0

        try:
            # open the FTP connection and go to the directory
            ftp: FTP = FTP(ftp_site)
            ftp.login()
            ftp.cwd(ftp_dir)

            # if the target directory doesnt exist, create it
            if not os.path.exists(data_file_path):
                os.makedirs(data_file_path)

            # for each file requested
            for f in ftp_files:
                self.logger.debug(f'Retrieving {ftp_site}{ftp_dir}{f} -> {data_file_path}')

                # does the file exist and has data in it
                try:
                    size: int = os.path.getsize(os.path.join(data_file_path, f))
                except FileNotFoundError:
                    size: int = 0

                # if we have a size we done need to get the file
                if size == 0:
                    # open the file
                    with open(os.path.join(data_file_path, f), 'wb') as fp:
                        # get the file data into a file
                        ftp.retrbinary(f'RETR {f}', fp.write)

                # inform user of progress
                file_counter += 1

                if file_counter % 50 == 0:
                    self.logger.debug(f'{file_counter} files retrieved, {len(ftp_files) - file_counter} to go.')

            self.logger.debug(f'{file_counter} file(s) retrieved of {len(ftp_files)} requested.')

            # close the ftp object
            ftp.quit()
        except Exception as e:
            self.logger.error(f'Error: pull_via_ftp() failed. Exception: {e}')

        # return pass/fail to the caller
        return file_counter

    def pull_via_http(self, url: str, data_dir: str) -> int:
        """
        gets the file from an http stream.

        :param url:
        :param data_dir:
        :return: the number of bytes read
        """

        # get the filename
        data_file: str = url.split('/')[-1]

        # init the byte counter
        byte_counter: int = 0

        # get the file if its not there
        if not os.path.exists(os.path.join(data_dir, data_file)):
            self.logger.debug(f'Retrieving {url} -> {data_dir}')

            # get the file
            file_data = urlopen(url)

            # open a file for the data
            with open(os.path.join(data_dir, data_file), 'wb') as fp:
                # init the retrieve bytes by block size
                block = 8192

                # until all bytes read
                while True:
                    # get some bytes
                    buffer = file_data.read(block)

                    # did we run out of data
                    if not buffer:
                        break

                    # keep track of the number of bytes transferred
                    byte_counter += len(buffer)

                    # output the data to the file
                    fp.write(buffer)
        else:
            byte_counter = 1

        # return the number of bytes read
        return byte_counter

    def get_swiss_prot_id_set(self, data_dir: str, debug_mode=False) -> set:
        """
        gets/parses the swiss-prot listing file and returns a set of uniprot kb ids from
        ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.dat.gz.

        :param data_dir: the directory to place the file temporarily
        :param debug_mode: flag it indicate debug mode
        :return: a set of uniprot kb ids
        """

        self.logger.debug('Start of swiss-prot curated uniprot id retrieval')

        # init the return value
        ret_val: set = set()

        # the name of the tar file that has the target data file
        data_file_name = 'uniprot_sprot.dat.gz'

        # get the tar file that has the taxon id data
        self.pull_via_ftp('ftp.uniprot.org', '/pub/databases/uniprot/current_release/knowledgebase/complete/', [data_file_name], data_dir)

        # open the tar file
        with gzip.open(os.path.join(data_dir, data_file_name), 'r') as zf:
            # for each line in the file
            for line in zf:
                # turn the read value into a string
                line = line.decode("utf-8")

                # is this one we are looking for
                if line.startswith('AC'):
                    # split the line to separate out the uniprot ids
                    ids = line.split('   ')[1].split('; ')

                    # save each protein item listed
                    for item in ids:
                        # save it
                        ret_val.add(item.strip(';\n'))

        # do not remove the file if in debug mode
        if self.logger.level != logging.DEBUG and not debug_mode:
            # remove the target file
            os.remove(os.path.join(data_dir, data_file_name))

        self.logger.debug(f'End of swiss-prot uniprot id retrieval. {len(ret_val)} retrieved.')

        # return the list
        return ret_val

    def get_ncbi_taxon_id_set(self, taxon_data_dir, organism_type: str) -> set:
        """
        gets the files associated with viruses (and/or maybe bacteria)
        the nodes.dmp file can be found in the archive: ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz

        :param: the organism type
        :return: a list of file indexes
        """

        self.logger.debug(f'Start of NCBI taxon retrieval.')

        # init the return value
        ret_val: set = set()

        # the name of the tar file that has the target data file
        data_file_name = 'taxdump.tar.gz'

        # get the tar file that has the taxon id data
        self.pull_via_ftp('ftp.ncbi.nih.gov', '/pub/taxonomy', [data_file_name], taxon_data_dir)

        # open the tar file
        tar_file = tarfile.open(os.path.join(taxon_data_dir, data_file_name), 'r:gz')

        # get a reference to the file
        fp = tar_file.extractfile('nodes.dmp')

        # read in the file lines
        lines = fp.readlines()

        # for each line in the file
        for line in lines:
            # remove the trailing \t and split by the line delimiter
            new_line = line.decode('utf-8').strip().split('\t|\t')

            # get all the elements in the line without \t
            parts = [x.strip() for x in new_line]

            # is this one we are looking for
            if parts[4] == organism_type:
                # save it
                ret_val.add(parts[0])

        # close the files
        fp.close()
        tar_file.close()

        # do not remove the file if in debug mode
        if self.logger.level != logging.DEBUG:
            # remove the target file
            os.remove(os.path.join(taxon_data_dir, data_file_name))

        self.logger.debug(f'Start of NCBI taxon retrieval. {len(ret_val)} retrieved.')

        # return the list
        return ret_val

    def get_uniprot_virus_date_stamp(self, data_dir):
        """
        retrieves and reads the only line in the uniprokb file "datestamp" that indicates the date
        the data in the directory was created.

        :return: the date stamp string
        """

        # get the date stamp file
        self.pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/GO/goa/proteomes', ['datestamp'], data_dir)

        # open the file and read the first line
        with open(os.path.join(data_dir, 'datestamp'), 'r') as fp:
            # get the line of text that is the date stamp
            ret_val: str = fp.readline().strip('\n')

        os.remove(os.path.join(data_dir, 'datestamp'))

        # return to the caller
        return ret_val

    def get_uniprot_virus_file_list(self, proteome_data_dir: str, taxa_id_set: set) -> list:
        """
        gets the list of virus proteome file names that will be downloaded
        the proteome2taxid file can be found in the ftp directory at: ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/

        :param proteome_data_dir: the location of the proteome to taxon id conversion file
        :param taxa_id_set: the set of taxa ids
        :return: the set of file names to get
        """
        self.logger.debug(f'Start of uniprot virus file list retrieval.')

        # init the return value
        ret_val: list = []

        # storage for the final file list
        files: list = []

        # set the data file name
        data_file_name = 'proteome2taxid'

        # get the proteome to taxon id file
        file_count: int = self.pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/GO/goa/proteomes', [data_file_name], proteome_data_dir)

        # did we get the file
        if file_count == 1:
            # open the file
            with open(os.path.join(proteome_data_dir, data_file_name), 'r') as fp:
                # create a csv reader for it
                csv_reader: reader = csv.reader(fp, delimiter='\t')

                # spin through the list and get the file name
                for line in csv_reader:
                    # is this file in the list of target taxa
                    if line[1] in taxa_id_set:
                        # save the file in the list
                        files.append(line[2])

            # add the sars cov-2 file manually
            files.append('uniprot_sars-cov-2.gaf')

            # sort the file list
            ret_val = sorted(files)

            # close the file
            fp.close()

            # do not remove the file if in debug mode
            if self.logger.level != logging.DEBUG:
                # remove the data file
                os.remove(os.path.join(proteome_data_dir, data_file_name))

            self.logger.debug(f'End of uniprot virus file list retrieval. {len(ret_val)} retrieved.')
        else:
            self.logger.error(f'Error: {data_file_name} as not retrieved.')

        # return the list to the caller
        return ret_val

    def get_goa_http_file(self, data_dir: str, data_file: str):
        """
        gets the GOA file via HTTP.

        :param data_dir: the location where the data should be saved
        :param data_file: the name of the file to get
        :return int: the number of bytes read
        """
        self.logger.debug(f'Start of GOA file retrieval.')

        # get the rest of the files
        byte_count: int = self.pull_via_http(f'http://current.geneontology.org/annotations/{data_file}', data_dir)

        # return to the caller
        return byte_count

    def get_goa_ftp_files(self, data_dir: str, file_list: list, ftp_parent_dir: str, ftp_sub_dir: str) -> int:
        """
        gets the uniprot GOA data file(s).

        :param data_dir: the data file(s) destination
        :param file_list: the list of files
        :param ftp_parent_dir: the ftp data parent directory
        :param ftp_sub_dir: the ftp data sub directory
        :return: the retrieved file count
        """
        self.logger.debug(f'Start of GOA file retrieval.')

        # init some counters
        attempts: int = 0
        target_count: int = len(file_list)
        file_count: int = 0

        # a connection to this FTP site is not reliable
        while attempts < 25:
            try:
                # get the rest of the files
                file_count = self.pull_via_ftp('ftp.ebi.ac.uk', ftp_parent_dir + ftp_sub_dir, file_list, data_dir)

                # if we got all the files
                if target_count != 0 and (target_count == file_count):
                    break
            # handle issues in file retrieval
            except Exception as e:
                self.logger.error(f'Error: target: {target_count}, actual: {file_count}, attempts: {attempts}, {e}')
                attempts += 1

        self.logger.debug(f'End of GOA file retrieval. {file_count} retrieved.')

        # return the number of files captured
        return file_count

    @staticmethod
    def format_normalization_failures(data_set_name: str, node_norm_failures: list, edge_norm_failures: list):
        """
        outputs the nodes/edges that failed normalization

        :param data_set_name: the name of the data source that produced these results
        :param node_norm_failures: set of node curies
        :param edge_norm_failures: set of edge predicates
        :return:
        """
        the_logger = LoggingUtil.init_logging(f"Data_services.Common.NormFailures.{data_set_name}", level=logging.INFO, line_format='minimum', log_file_path=os.path.join(Path(__file__).parents[1], 'logs'))

        # get the list into a dataframe group
        df = pd.DataFrame(node_norm_failures, columns=['curie'])
        df_node_grp = df.groupby('curie').size() \
            .reset_index(name='count') \
            .sort_values('count', ascending=False)

        # iterate through the groups and create the edge records.
        for row_index, row in df_node_grp.iterrows():
            the_logger.info(f'{row["curie"]}\t{data_set_name}')
            # self.logger.info(f'Failed node CURIE: {row["curie"]}, count: {row["count"]}')

            # get the list into a dataframe group
        df = pd.DataFrame(edge_norm_failures, columns=['curie'])
        df_edge_grp = df.groupby('curie').size() \
            .reset_index(name='count') \
            .sort_values('count', ascending=False)

        # iterate through the groups and create the edge records.
        for row_index, row in df_edge_grp.iterrows():
            the_logger.info(f'{row["curie"]}\t{data_set_name}')
            # self.logger.info(f'Failed edge predicate: {row["curie"]}, count: {row["count"]}')

    @staticmethod
    def get_biolink_graph(data_uri: str) -> Graph:
        """
        Gets the passed URI into turtle format

        :return: A RDF Graph of the ttl data file passed in
        """

        # create a RDF graph of the json-ld
        ret_val = Graph().parse(data_uri, format='turtle')

        # return the data to the caller
        return ret_val

    @staticmethod
    def split_file(data_file_path: str, data_file_name: str, lines_per_file: int = 150000) -> list:
        """
        splits a file into numerous smaller files.

        :param data_file_path: the path to where the input file is and where the split files go
        :param data_file_name: the name of the input data file
        :param lines_per_file: the number of lines for each split file
        :return: a list of file names that were created
        """

        # init the return
        ret_val: list = []

        # declare file name prefix
        file_prefix: str = data_file_name + '.'

        # init a file and line counter
        file_counter: int = 1
        line_counter: int = 0

        # init storage for a group of lines
        lines: list = []

        # get all the data lines
        with open(os.path.join(data_file_path, data_file_name), 'r') as fp:
            while True:
                # read the line
                line = fp.readline()

                # save the line if there is one
                if line:
                    lines.append(line)
                    line_counter += 1
                else:
                    break

                # did we hit the write threshold
                if line_counter >= lines_per_file:
                    # loop through the lines
                    # create the output file
                    file_name = os.path.join(data_file_path, file_prefix + str(file_counter))

                    # add the file name to the output list
                    ret_val.append(file_name)

                    # open the file
                    with open(file_name, 'w') as of:
                        # write the lines
                        of.write(''.join(lines))

                    # increment the file counter
                    file_counter += 1

                    # reset the line counter
                    line_counter = 0

                    # clear out for the next cycle
                    lines.clear()

        # output any not yet written
        # create the output file
        file_name = os.path.join(data_file_path, file_prefix + str(file_counter))

        # add the file name to the output list
        ret_val.append(file_name)

        # open the file
        with open(file_name, 'w') as of:
            # write the lines
            of.write('\n'.join(lines))

        # return the file name list
        return ret_val

    @staticmethod
    def get_list_from_csv(in_file: str, sort_by: str) -> list:
        """
        Opens the CSV file passed and turns it into a sorted list of dicts

        :param in_file: the path to the file to be parsed
        :param sort_by: the sort by column name
        :return: a list of sorted dicts
        """
        # init the return
        ret_val: list = []

        # open the input file
        with open(in_file, 'r', encoding='latin-1') as data:
            # chunk through the line in the file
            for item in DictReader(data):
                # save the item
                ret_val.append(item)

        # sort the list
        ret_val = sorted(ret_val, key=lambda i: (i[sort_by]))

        # return to the caller
        return ret_val


class DatasetDescription:
    @staticmethod
    def create_description(data_path: str, prov_data: dict, out_name: str):
        """
        creates a graph node that contains detailed information on a parsed/loaded dataset

        Biolink model specs:
            https://biolink.github.io/biolinkml/

            https://biolink.github.io/biolink-model/docs/DataSet.html
            https://biolink.github.io/biolink-model/docs/DataSetVersion.html
            https://biolink.github.io/biolink-model/docs/DataSetSummary.html
            https://biolink.github.io/biolink-model/docs/DistributionLevel.html

            https://biolink.github.io/biolink-model/docs/DataFile.html
            https://biolink.github.io/biolink-model/docs/SourceFile.html

        Expected parameters/values passed in are:
            data_set_name
            data_set_title
            data_set_web_site
            data_set_download_url
            data_set_version
            data_set_retrieved_on

        :return:
        """
        # open the output node files
        with open(os.path.join(data_path, f'{out_name}_prov_node_file.tsv'), 'w', encoding="utf-8") as out_node_f:
            # write out the node and edge data headers
            out_node_f.write(f'id\tname\tcategory\ttitle\tsource_web_page\tdownloadURL\tsource_version\tretrievedOn\n')

            # write out the node data
            out_node_f.write(f'{prov_data["data_set_version"]}_{prov_data["data_set_name"]}\t{prov_data["data_set_name"]}\tdataset|named_thing\t{prov_data["data_set_title"]}\t{prov_data["data_set_web_site"]}\t{prov_data["data_set_download_url"]}\t{prov_data["data_set_version"]}\t{prov_data["data_set_retrieved_on"]}\n')
