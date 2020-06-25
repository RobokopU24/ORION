import os
import requests
import logging
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
            "short": '%(funcName)s: %(message)s',
            "medium": '%(funcName)s: %(asctime)-15s %(message)s',
            "long": '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
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
            file_handler = RotatingFileHandler(filename=os.path.join(log_file_path, name + '.log'), maxBytes=1000000, backupCount=10)

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
    # create a logger
    logger = LoggingUtil.init_logging("Data_services.Common.NodeNormUtils", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[1], 'logs'))

    def normalize_node_data(self, cached_node_norms, node_list: list) -> list:
        """
        This method calls the NodeNormalization web service to get the normalized identifier and name of the taxon node.
        the data comes in as a node list and we will normalize the only the taxon nodes.

        :param cached_node_norms: list of previously captured normalizations
        :param node_list: A list with items to normalize
        :return:
        """

        # loop through the list and only save the NCBI taxa nodes
        node_idx: int = 0

        # save the node list count to avoid grabbing it over and over
        node_count: int = len(node_list)

        # init a list to identify taxa that has not been node normed
        tmp_normalize: set = set()

        # iterate through node groups and get only the taxa records.
        while node_idx < node_count:
            # is this a NCBI taxon
            if node_list[node_idx]['id'].startswith('N'):
                # check to see if this one needs normalization data from the website
                if not node_list[node_idx]['id'] in cached_node_norms:
                    tmp_normalize.add(node_list[node_idx]['id'])
                # else:
                #     self.print_debug_msg(f"Cache hit: {node_list[node_idx]['id']}")

            node_idx += 1

        # convert the set to a list so we can iterate through it
        to_normalize: list = list(tmp_normalize)

        # define the chuck size
        chunk_size: int = 1000

        # init the indexes
        start_index: int = 0

        # get the last index of the list
        last_index: int = len(to_normalize)

        # self.print_debug_msg(f'{last_index} unique nodes will be normalized.')

        # grab chunks of the data frame
        while True:
            if start_index < last_index:
                # define the end index of the slice
                end_index: int = start_index + chunk_size

                # force the end index to be the last index to insure no overflow
                if end_index >= last_index:
                    end_index = last_index

                # self.print_debug_msg(f'Working block {start_index} to {end_index}.')

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
                    # self.print_debug_msg(f'response code: {resp.status_code}')

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
            # is this a NCBI taxon
            if node_list[node_idx]['id'].startswith('N'):
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
                    self.logger.error(f"{rv['id']} has no normalized value")

            # go to the next index
            node_idx += 1

        # return the updated list to the caller
        return node_list


class GetData:
    # create a logger
    logger = LoggingUtil.init_logging("Data_services.Common.GetData", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[1], 'logs'))

    def pull_via_ftp(self, ftp_site: str, ftp_dir: str, file_data_path: str, file: str) -> bool:
        """
        gets the requested files from UniProtKB ftp directory

        :param ftp_site: url of the ftp site
        :param ftp_dir: the directory in the site
        :param file_data_path: the destination of the captured file
        :param file: the name of the file to capture
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
                self.logger.info(f'Archive retrieval complete.')

            # close the ftp object
            ftp.quit()

            # set the return value
            ret_val = True
        except Exception as e:
            self.logger.error(f'Pull_via_ftp() failed. Exception: {e}')

        # return pass/fail to the caller
        return ret_val
