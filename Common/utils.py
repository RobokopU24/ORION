import os
import logging
import tarfile
import csv
import gzip
import requests
import pandas as pd
import orjson
from dateutil import parser as dp
from itertools import islice

from urllib import request
from zipfile import ZipFile
from io import TextIOWrapper
from io import BytesIO
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
    def init_logging(name, level=logging.INFO, line_format='minimum', log_file_path=None):
        """
            Logging utility controlling format and setting initial logging level
        """
        # get a new logger
        logger = logging.getLogger(__name__)

        # is this the root
        if not logger.parent.name == 'root':
            return logger

        # get the logger with the specified name
        logger = logging.getLogger(name)

        # if it already has handlers, it was already instantiated - return it
        if logger.hasHandlers():
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

        # set the logging level
        if 'DATA_SERVICES_TEST_MODE' in os.environ and os.environ['DATA_SERVICES_TEST_MODE']:
            level = logging.DEBUG
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


class GetDataPullError(Exception):
    def __init__(self, error_message: str):
        self.error_message = error_message


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

        try:
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

        except Exception as e:
            error_message = f'GetDataPullError pull_via_ftp_binary() failed for {ftp_site}. Exception: {e}'
            raise GetDataPullError(error_message)

        # return the data stream
        return binary

    def get_ftp_file_date(self, ftp_site, ftp_dir, ftp_file) -> str:
        """
        gets the modified date of the file from the ftp site

        :param ftp_site:
        :param ftp_dir:
        :param ftp_file:
        :return:
        """
        # init the return value
        ret_val: str = 'Not found'

        try:
            # open the FTP connection and go to the directory
            ftp: FTP = FTP(ftp_site)
            ftp.login()
            ftp.cwd(ftp_dir)

            # get the date of the file
            date_val = ftp.voidcmd(f'MDTM {ftp_file}').split(' ')

            # did we get something
            if len(date_val) > 0:
                # return the parsed date
                return dp.parse(date_val[1]).strftime('%-m_%-d_%Y')
        except Exception as e:
            error_message = f'Error getting modification date for ftp file: {ftp_site}{ftp_dir}{ftp_file}. {e}'
            self.logger.error(error_message)
            raise GetDataPullError(error_message)

        return ret_val

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
            error_message = f'GetDataPullError pull_via_ftp() failed for {ftp_site}. Exception: {e}'
            self.logger.error(error_message)
            raise GetDataPullError(error_message)

        # return pass/fail to the caller
        return file_counter

    def get_http_file_modified_date(self, file_url: str):
        try:
            r = requests.head(file_url)
            url_time = r.headers['last-modified']
            return dp.parse(url_time).strftime("%-m_%-d_%Y")
        except Exception as e:
            error_message = f'Error getting modification date for http file: {file_url}. {repr(e)}-{e}'
            self.logger.error(error_message)
            raise GetDataPullError(error_message)

    def pull_via_http(self, url: str, data_dir: str, is_gzip=False) -> int:
        """
        gets the file from an http stream.

        :param url:
        :param data_dir:
        :param is_gzip:
        :return: the number of bytes read
        """

        # get the filename
        data_file: str = url.split('/')[-1]

        # init the byte counter
        byte_counter: int = 0

        # get the file if its not there
        if not os.path.exists(os.path.join(data_dir, data_file)):

            self.logger.debug(f'Retrieving {url} -> {data_dir}')
            try:
                hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'}
                req = request.Request(url, headers=hdr)

                # get the the file data handle
                file_data = request.urlopen(req)

                # is this a gzip file
                if is_gzip:
                    # get a handle to the data
                    file_data = gzip.GzipFile(fileobj=file_data)

                    # strip off the .gz if exists
                    data_file = data_file.replace('.gz', '')

                with open(os.path.join(data_dir, data_file), 'wb') as fp:
                    # specify the buffered data block size
                    block = 131072

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
            except Exception as e:
                error_message = f'GetDataPullError pull_via_http() failed. URL: {url}. Exception: {e}'
                self.logger.error(error_message)
                raise GetDataPullError(error_message)

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

    def get_foodb_files(self, full_url: str, data_dir: str, data_file_name: str, file_list: list) -> (int, str, str):
        """
        gets the food db files

        :param full_url: the URL to the data file
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

        # get the tar file that has the foodb data
        self.pull_via_http(full_url, data_dir)

        # open the tar file
        tar = tarfile.open(os.path.join(data_dir, data_file_name), "r")

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
        return file_count, foodb_dir, name[0]

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
                error_message = f'GetDataPullError get_gtopdb_http_files() failed to download file {data_file}.'
                self.logger.error(error_message)
                raise GetDataPullError(error_message)

        # return to the caller
        return file_counter

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
    def split_file(archive_file_path: str, output_dir: str, data_file_name: str, lines_per_file: int = 500000) -> list:
        """
        splits a file into numerous smaller files.

        :param archive_file_path: the path to the zipped archive file
        :param output_dir: the path where the split files go
        :param data_file_name: the name of the data file inside of the archive
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
        with ZipFile(archive_file_path) as zf:
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
                        file_name = os.path.join(output_dir, file_prefix + str(file_counter))

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

        # write any remaining lines to the last split file
        if lines:

            # create the output file
            file_name = os.path.join(output_dir, file_prefix + str(file_counter))

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


def quick_json_dumps(item):
    return str(orjson.dumps(item), encoding='utf-8')


def quick_json_loads(item):
    return orjson.loads(item)


def quick_jsonl_file_iterator(json_file):
    with open(json_file, 'r', encoding='utf-8') as stream:
        for line in stream:
            yield orjson.loads(line)


def chunk_iterator(iterable, chunk_size):
    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, chunk_size))
        if chunk:
            yield chunk
        else:
            break


def snakify(text):
    lowercase_text = text.lower()  # make lowercase
    snakified_text = lowercase_text.replace(',', '_').replace('-', '_')  # replace commas and dashes with underscores
    snakified_text = '_'.join(snakified_text.split())  # replace whitespace with underscores
    return snakified_text
