import os
import logging
import tarfile
import csv
import gzip
import requests
import pandas as pd

from zipfile import ZipFile
from io import TextIOWrapper
from io import BytesIO
from rdflib import Graph
import urllib
from csv import reader, DictReader
from ftplib import FTP
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from robokop_genetics.genetics_normalization import GeneticsNormalizer
from Common.node_types import ROOT_ENTITY


class LoggingUtil(object):
    """
    creates and configures a logger
    """
    @staticmethod
    def init_logging(name, level=logging.INFO, line_format='minimum', log_file_path=None):
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

    def __init__(self, log_level=logging.INFO, strict_normalization: bool = True):
        """
        constructor
        :param log_level - overrides default log level
        """
        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.Common.NodeNormUtils", level=log_level, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])
        # storage for regular nodes that failed to normalize
        self.failed_to_normalize_ids = set()
        # storage for variant nodes that failed to normalize
        self.failed_to_normalize_variant_ids = set()
        # flag that determines whether nodes that failed to normalize should be included or thrown out
        self.strict_normalization = strict_normalization
        # storage for variant nodes that split into multiple new nodes in normalization
        self.variant_node_splits = {}
        # normalization map for future look up of all normalized node IDs
        self.node_normalization_lookup = {}

    def normalize_node_data(self, node_list: list, block_size: int = 5000) -> list:
        """
        This method calls the NodeNormalization web service to get the normalized identifier and name of the node.
        the data comes in as a node list.

        :param node_list: A list with items to normalize
        :param cached_node_norms: dict of previously captured normalizations
        :param block_size: the number of curies in the request
        :return:
        """

        self.logger.debug(f'Start of normalize_node_data. items: {len(node_list)}')

        # init the cache list - this accumulates all of the results from the node norm service
        cached_node_norms: dict = {}

        # save the node list count to avoid grabbing it over and over
        node_count: int = len(node_list)

        # create a unique set of node ids
        tmp_normalize: set = set([node['id'] for node in node_list])

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
                resp: requests.models.Response = requests.post('https://nodenormalization-sri.renci.org/get_normalized_nodes', json={'curies': data_chunk})

                # did we get a good status code
                if resp.status_code == 200:
                    # convert json to dict
                    rvs: dict = resp.json()

                    # merge this list with what we have gotten so far
                    cached_node_norms.update(**rvs)
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

        # node ids that failed to normalize
        failed_to_normalize: list = []

        # for each node update the node with normalized information
        # store the normalized IDs for later look up
        while node_idx < node_count:
            # get the next node list item by index
            current_node_id = node_list[node_idx]['id']

            # did we find a normalized value
            if cached_node_norms[current_node_id] is not None:

                # update the node with the normalized info
                current_node = node_list[node_idx]
                normalized_id = cached_node_norms[current_node_id]['id']['identifier']
                current_node['id'] = normalized_id
                current_node['category'] = cached_node_norms[current_node_id]['type']
                current_node['equivalent_identifiers'] = list(item['identifier'] for item in cached_node_norms[current_node_id]['equivalent_identifiers'])
                # set the name as the label if it exists
                if 'label' in cached_node_norms[current_node_id]['id']:
                    current_node['name'] = cached_node_norms[current_node_id]['id']['label']

                self.node_normalization_lookup[current_node_id] = [normalized_id]
            else:
                # we didn't find a normalization - add it to the failure list
                failed_to_normalize.append(current_node_id)
                if self.strict_normalization:
                    # if strict normalization is on we set that index to None so that it is later removed
                    node_list[node_idx] = None
                    # store None in the normalization map so we know it didn't normalize
                    self.node_normalization_lookup[current_node_id] = None
                else:
                    #  if strict normalization is off we set a default node type
                    node_list[node_idx]['category'] = [ROOT_ENTITY]
                    if not node_list[node_idx]['name']:
                        node_list[node_idx]['name'] = node_list[node_idx]['id']
                    # if strict normalization is off set its previous id in the normalization map
                    self.node_normalization_lookup[current_node_id] = [current_node_id]

            # go to the next node index
            node_idx += 1

        # if something failed to normalize - log it and optionally remove it from the node list
        if len(failed_to_normalize) > 0:
            self.failed_to_normalize_ids.update(failed_to_normalize)

            # if strict remove all nodes that failed normalization
            if self.strict_normalization:
                node_list[:] = [d for d in node_list if d is not None]

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

    def normalize_sequence_variants(self, variant_nodes: list):

        sequence_variant_normalizer = GeneticsNormalizer(use_cache=False)
        variant_ids = [node['id'] for node in variant_nodes]
        sequence_variant_norms = sequence_variant_normalizer.normalize_variants(variant_ids)
        variant_node_types = sequence_variant_normalizer.get_sequence_variant_node_types()

        variant_nodes.clear()
        for variant_id, variant_norms in sequence_variant_norms.items():
            if variant_norms:
                for normalized_info in variant_norms:
                    normalized_node = {
                        'id': normalized_info["id"],
                        'name':  normalized_info["name"],
                        # as long as sequence variant types are all the same we can skip this assignment
                        # 'category': normalized_info["type"],
                        'category': variant_node_types,
                        'equivalent_identifiers': normalized_info['equivalent_identifiers']
                    }
                    variant_nodes.append(normalized_node)
                if len(variant_norms) > 1:
                    split_ids = [node['id'] for node in variant_norms]
                    self.variant_node_splits[variant_id] = split_ids
                    self.node_normalization_lookup[variant_id] = split_ids
                else:
                    self.node_normalization_lookup[variant_id] = [variant_norms[0]['id']]
            else:
                self.failed_to_normalize_variant_ids.add(variant_id)
                self.node_normalization_lookup[variant_id] = None
                if not self.strict_normalization:
                    self.node_normalization_lookup[variant_id] = variant_id
                    # TODO for now we dont preserve other properties on variant nodes that didnt normalize
                    # the splitting makes that complicated and doesnt seem worth it until we have a good use case
                    fake_normalized_node = {
                        'id': variant_id,
                        'name': variant_id,
                        'category': variant_node_types,
                        'equivalent_identifiers': []
                    }
                    variant_nodes.append(fake_normalized_node)

        return variant_nodes


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
        self.logger = LoggingUtil.init_logging("Data_services.Common.EdgeNormUtils", level=log_level, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])
        # normalization map for future look up of all normalized predicates
        self.edge_normalization_lookup = {}

    def normalize_edge_data(self,
                            edge_list: list,
                            cached_edge_norms: dict = None,
                            block_size: int = 2500) -> list:
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
            if not edge_list[edge_idx]['relation'] in cached_edge_norms:
                tmp_normalize.add(edge_list[edge_idx]['relation'])
            else:
                self.logger.debug(f"Cache hit: {edge_list[edge_idx]['relation']}")

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
                    cached_edge_norms.update(**rvs)
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

        # storage for items that failed to normalize
        failed_to_normalize: list = list()

        # walk through the unique relations and extract the normalized predicate for the lookup map
        for relation in to_normalize:
            success = False
            # did the service return a value
            if relation in cached_edge_norms:
                if 'identifier' in cached_edge_norms[relation]:
                    # store it in the look up map
                    self.edge_normalization_lookup[relation] = cached_edge_norms[relation]['identifier']
                    success = True
            if not success:
                # if no result for whatever reason add it to the fail list
                failed_to_normalize.append(relation)

        # if something failed to normalize output it
        if failed_to_normalize:
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
        self.logger = LoggingUtil.init_logging("Data_services.Common.GetData", level=log_level, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    @staticmethod
    def pull_via_ftp_binary(ftp_site, ftp_dir, ftp_file):
        """
        Gets the ftp file in binary mode

        :param ftp_site: the URL of the ftp site
        :param ftp_dir: the directory in the ftp site
        :param ftp_file: the name of the file to retrieve
        :return:
        """
        # create the FTP object
        ftp = FTP(ftp_site)

        # log into the FTP site
        ftp.login()

        # change to the correct directory on the ftp site
        ftp.cwd(ftp_dir)

        # for each data byte retreived
        with BytesIO() as data:
            # capture the data and put it in the buffer
            ftp.retrbinary(f'RETR {ftp_file}', data.write)

            # get the data in a stream
            binary = data.getvalue()

        # close the connection to the ftp site
        ftp.quit()

        # return the data stream
        return binary

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
                    # get the size of the file
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

                # progress output
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

            hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'}
            req = urllib.request.Request(url, headers=hdr)

            # Read the file inside the .gz archive located at url
            with urllib.request.urlopen(req) as response:
                with gzip.GzipFile(fileobj=response) as uncompressed:
                    file_content = uncompressed.read()

                    # strip off the .gz if exists
                    data_file = data_file.replace('.gz', '')

                    # open a file for the data
                    with open(os.path.join(data_dir, data_file), 'wb') as fp:
                        # output the data to the file
                        byte_counter = fp.write(file_content)
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
        # if self.logger.level != logging.DEBUG and not debug_mode:
        #     # remove the target file
        #     os.remove(os.path.join(data_dir, data_file_name))

        self.logger.debug(f'End of swiss-prot uniprot id retrieval. {len(ret_val)} retrieved.')

        # return the list
        return ret_val

    def get_foodb_files(self, data_dir: str, data_file_name: str, file_list: list) -> int:
        """
        gets the food db files in the specified list from:
        https://foodb.ca/public/system/downloads/foodb_2020_4_7_csv.tar.gz.

        :param data_dir: the directory to place the file temporarily
        :param data_file_name: the name of the target file archive
        :param file_list: list of files to get
        :return:
        """

        self.logger.debug('Start of foodb file retrieval')

        # init the file counter
        file_count: int = 0

        # init the extraction directory
        foodb_dir: str = ''

        # get the tar file that has the taxon id data
        self.pull_via_http('https://foodb.ca/public/system/downloads/' + data_file_name + '.tar.gz', data_dir)

        # open the tar file
        tar = tarfile.open(os.path.join(data_dir, data_file_name + '.tar.gz'), "r")

        # for each member of the tar fiule
        for member in tar.getmembers():
            # get the name
            name = member.name.split('/')

            # if a valid name was found
            if len(name) > 1:
                # is the name in the target list
                if name[1] in file_list:
                    # save the file
                    tar.extract(member, data_dir)

                    # save the extraction directory
                    foodb_dir = name[0]

                    # increment the file counter
                    file_count += 1

        self.logger.debug(f'End of foodb file retrieval. {file_count} files retrieved.')

        # return the list
        return file_count, foodb_dir

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

    def get_ctd_http_files(self, data_dir: str, file_list: list) -> int:
        """
        gets the CTD file via HTTP.

        :param data_dir: the location where the data should be saved
        :param file_list: the list of files to get
        :return int: the number of files retrieved
        """
        self.logger.debug(f'Start of CTD file retrieval.')

        # unit a file counter
        file_counter: int = 0

        for data_file in file_list:
            if os.path.isfile(os.path.join(data_dir, data_file)):
                byte_count = 1
            else:
                # get the rest of the files
                byte_count: int = self.pull_via_http(f'http://ctdbase.org/reports/{data_file}.gz', data_dir)

            # did re get some good file data
            if byte_count > 0:
                file_counter += 1
            else:
                self.logger.error(f'Failed to get {data_file}.')

        # return to the caller
        return file_counter

    def get_gtopdb_http_files(self, data_dir: str, file_list: list) -> int:
        """
        gets the gtopdb files via HTTP.

        :param data_dir: the location where the data should be saved
        :param file_list: the files to get
        :return int: the number of files retrieved
        """
        self.logger.debug(f'Start of GtoPdb file retrieval.')

        # unit a file counter
        file_counter: int = 0

        for data_file in file_list:
            if os.path.isfile(os.path.join(data_dir, data_file)):
                byte_count = 1
            else:
                # get the rest of the files
                byte_count: int = self.pull_via_http(f'https://www.guidetopharmacology.org/DATA/{data_file}', data_dir)

            # did re get some good file data
            if byte_count > 0:
                file_counter += 1
            else:
                self.logger.error(f'Failed to get {data_file}.')

        # return to the caller
        return file_counter

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
        the_logger = LoggingUtil.init_logging(f"Data_services.Common.NormFailures.{data_set_name}", level=logging.INFO, line_format='medium', log_file_path=os.path.join(Path(__file__).parents[1], 'logs'))

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
    def split_file(infile_path, data_file_path: str, data_file_name: str, lines_per_file: int = 150000) -> list:
        """
        splits a file into numerous smaller files.

        : infile_path: the path to the input file
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

        # open the zip file
        with ZipFile(os.path.join(infile_path)) as zf:
            # open the taxon file indexes and the uniref data file
            with TextIOWrapper(zf.open(data_file_name), encoding="utf-8") as fp:
                while True:
                    # read the line
                    line = fp.readline()

                    # save the line
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
