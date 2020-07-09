import os
import requests
import logging
import tarfile
import csv
import gzip
from csv import reader
from ftplib import FTP
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


class LoggingUtil(object):
    """
    creates and configures a logger
    """
    @staticmethod
    def init_logging(name, level=logging.INFO, line_format='short', log_file_path=None, log_file_level=None):
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

            # if a log level for the file was passed in use it
            if log_file_level is not None:
                level = log_file_level

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
    # create a logger
    logger = LoggingUtil.init_logging("Data_services.Common.NodeNormUtils", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[1], 'logs'))

    def normalize_node_data(self, node_list: list, cached_node_norms: dict = None) -> list:
        """
        This method calls the NodeNormalization web service to get the normalized identifier and name of the taxon node.
        the data comes in as a node list and we will normalize the only the taxon nodes.

        :param node_list: A list with items to normalize
        :param cached_node_norms: dict of previously captured normalizations
        :return:
        """

        # init the cache list if it wasnt passed in
        if cached_node_norms is None:
            cached_node_norms: dict = {}

        # init the node index counter
        node_idx: int = 0

        # save the node list count to avoid grabbing it over and over
        node_count: int = len(node_list)

        # init a list to identify taxa that has not yet been node normed
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

        # define the chuck size for normalization batches
        chunk_size: int = 1000

        # init the array index lower boundary
        start_index: int = 0

        # get the last index of the list
        last_index: int = len(to_normalize)

        self.logger.debug(f'{last_index} unique nodes will be normalized.')

        # grab chunks of the data frame
        while True:
            if start_index < last_index:
                # define the end index of the slice
                end_index: int = start_index + chunk_size

                # force the end index to be the last index to insure no overflow
                if end_index >= last_index:
                    end_index = last_index

                self.logger.debug(f'Working block {start_index} to {end_index}.')

                # collect a slice of records from the data frame
                data_chunk: list = to_normalize[start_index: end_index]

                # get the data
                resp: requests.models.Response = requests.get('https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=' + '&curie='.join(data_chunk))

                # did we get a good status code
                if resp.status_code == 200:
                    # convert to json
                    rvs: dict = resp.json()

                    # merge this list with what we have gotten so far
                    merged = {**cached_node_norms, **rvs}

                    # save the merged list
                    cached_node_norms = merged
                else:
                    # the 404 error that is trapped here means that the entire list of nodes didnt get normalized.
                    self.logger.debug(f'Response code: {resp.status_code}')

                    # since they all failed to normalize add to the list so we dont try them again
                    for item in data_chunk:
                        cached_node_norms.update({item: None})

                # move on down the list
                start_index += chunk_size
            else:
                break

        # reset the node index
        node_idx = 0

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

                if 'type' in cached_node_norms[rv['id']]:
                    node_list[node_idx]['category'] = '|'.join(cached_node_norms[rv['id']]['type'])

                # get the equivalent identifiers
                if 'equivalent_identifiers' in cached_node_norms[rv['id']] and len(cached_node_norms[rv['id']]['equivalent_identifiers']) > 0:
                    node_list[node_idx]['equivalent_identifiers'] = '|'.join(list((item['identifier']) for item in cached_node_norms[rv['id']]['equivalent_identifiers']))

                # find the id and replace it with the normalized value
                node_list[node_idx]['id'] = cached_node_norms[rv['id']]['id']['identifier']
            else:
                self.logger.debug(f"{rv['id']} has no normalized value")

            # go to the next node index
            node_idx += 1

        # return the updated list to the caller
        return node_list


class GetData:
    """
    Class that contains methods that can be used to get various data sets.
    """
    # create a logger
    logger = LoggingUtil.init_logging("Data_services.Common.GetData", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[1], 'logs'))

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

    def get_swiss_prot_id_set(self, data_dir: str) -> set:
        """
        gets/parses the swiss-prot listing file and returns a set of uniprot kb ids from
        ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.dat.gz.

        :param data_dir: the directory to place the file temporarily
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

                    # save each item listed
                    for item in ids:
                        # save it
                        ret_val.add(item.strip(';\n'))

        # do not remove the file if in debug mode
        if self.logger.level != logging.DEBUG:
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

    def get_uniprot_virus_file_list(self, proteome_data_dir: str, taxa_id_set: set) -> list:
        """
        gets the list of virus proteome file names that will be downloaded
        the proteome2taxid file can be found in the ftp directory at: ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/

        :param proteome_data_dir: the location of the proteome to taxon id conversion file
        :param taxa_id_set: the set of taxa ids
        :return: the set of file names to get
        """
        self.logger.debug(f'Start of uniprot virus file list retrieval.')

        # storage for the final file list
        files: list = []

        # set the data file name
        data_file_name = 'proteome2taxid'

        # get the proteome to taxon id file
        self.pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/GO/goa/proteomes', [data_file_name], proteome_data_dir)

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
        ret_val: list = sorted(files)

        # close the file
        fp.close()

        # do not remove the file if in debug mode
        if self.logger.level != logging.DEBUG:
            # remove the data file
            os.remove(os.path.join(proteome_data_dir, data_file_name))

        self.logger.debug(f'End of uniprot virus file list retrieval. {len(ret_val)} retrieved.')

        # return the list to the caller
        return ret_val

    def get_goa_files(self, data_dir: str, file_list: list, ftp_parent_dir: str, ftp_sub_dir: str) -> int:
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
                # get the 1 sars-cov-2 file
                self.pull_via_ftp('ftp.ebi.ac.uk', '/pub/contrib/goa/', ['uniprot_sars-cov-2.gaf'], data_dir)

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
            out_node_f.write(f'{prov_data["data_set_name"]}\t{prov_data["data_set_name"]}\tdataset|named_thing\t{prov_data["data_set_title"]}\t{prov_data["data_set_web_site"]}\t{prov_data["data_set_download_url"]}\t{prov_data["data_set_version"]}\t{prov_data["data_set_retrieved_on"]}\n')
