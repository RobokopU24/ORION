import logging
import os
from Common.kgx_file_writer import KGXFileWriter
from Common.kgx_file_normalizer import remove_orphan_nodes
from Common.utils import LoggingUtil


class SourceDataLoader:

    parsing_version = "1.0"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """Initialize with the option to run in testing mode."""
        self.test_mode: bool = test_mode
        self.filter_field = None

        if source_data_dir:
            self.data_path = os.path.join(source_data_dir, "source")
            if not os.path.exists(self.data_path):
                os.mkdir(self.data_path)
        else:
            self.data_path = os.environ["DATA_SERVICES_STORAGE"]

        # the final output lists of nodes and edges
        self.final_node_list: list = []
        self.final_edge_list: list = []

        # placeholder for lazy instantiation
        self.output_file_writer: KGXFileWriter = None

        # create a logger
        self.logger = LoggingUtil.init_logging(f"Data_services.parsers.{self.get_name()}",
                                               level=logging.INFO,
                                               line_format='medium',
                                               log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_latest_source_version(self):
        """Determine and return the latest source version ie. a unique identifier associated with the latest version."""
        raise NotImplementedError

    def get_data(self):
        """Download the source data"""
        raise NotImplementedError

    def get_latest_parsing_version(self):
        # implementations of parsers should override and increment this whenever they change
        return self.parsing_version

    def parse_data(self):
        """Parse the downloaded data into kgx files"""
        raise NotImplementedError

    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        """
        :param edges_output_file_path: path to the new nodes output file
        :param nodes_output_file_path: path to the new edges output file
        :return: dict of metadata about the loading
        """
        source_name = self.get_name()
        self.logger.info(f'{source_name}: Processing beginning')

        try:
            # download the data files if needed
            if self.needs_data_download():
                source_data_downloaded = False
            else:
                source_data_downloaded = True
                self.logger.debug(f'{source_name}: Source data previously retrieved, parsing now...')

            # did we get the files successfully
            if source_data_downloaded:

                # create a KGX file writer, parsers may use this
                self.output_file_writer = KGXFileWriter(nodes_output_file_path,
                                                        edges_output_file_path)

                # if so parse the data
                load_metadata = self.parse_data()
                if 'errors' in load_metadata and load_metadata['errors']:
                    self.logger.error(f'{source_name}: Experienced {len(load_metadata["errors"])} errors while parsing... examples: {load_metadata["errors"][:10]}')
                    load_metadata['parsing_error_examples'] = load_metadata.pop('errors')[:10]
                self.logger.info(f'{source_name}: File parsing complete. Writing to file...')

                # if nodes or edges were queued, write them to file
                if self.final_node_list or self.final_edge_list:
                    self.write_to_file()

                load_metadata['repeat_nodes'] = self.output_file_writer.repeat_node_count
                load_metadata['source_nodes'] = self.output_file_writer.nodes_written
                load_metadata['source_edges'] = self.output_file_writer.edges_written

                # TODO we should extract information from the writer to add to the metadata here (ie node type counts)
                # orphan_nodes_removed = remove_orphan_nodes(nodes_output_file_path, edges_output_file_path)
                # load_metadata['orphan_nodes_removed'] = orphan_nodes_removed
            else:
                error_message = f'{source_name}: Error - Retrieving files failed.'
                self.logger.error(error_message)
                raise SourceDataFailedError(error_message)

        except Exception:
            raise

        finally:
            if self.output_file_writer:
                self.output_file_writer.close()

            # remove the temp data files or do any necessary clean up
            self.clean_up()

        self.logger.info(f'{self.get_name()}:Processing complete')

        return load_metadata

    def needs_data_download(self):
        try:
            # some implementations will have one data_file
            if self.data_file:
                downloaded_data = os.path.join(self.data_path, self.data_file)
                # check if the one file already exists - if it does return false, does not need a download
                if os.path.exists(downloaded_data):
                    return False
                return True
        except AttributeError:
            pass
        try:
            # and some may have many
            if self.data_files:
                # for many files - if any of them do not exist return True to download them
                for data_file_name in self.data_files:
                    downloaded_data = os.path.join(self.data_path, data_file_name)
                    if not os.path.exists(downloaded_data):
                        return True
                return False
        except AttributeError:
            pass

    def clean_up(self):
        # as of now we decided to not remove source data after parsing
        # this function could still be overridden by parsers to remove temporary files or workspace clutter
        pass
        """
        try:
            # some implementations will have one data_file
            if self.data_file:
                file_to_remove = os.path.join(self.data_path, self.data_file)
                if os.path.exists(file_to_remove):
                    os.remove(file_to_remove)
        except AttributeError:
            pass
        try:
            # and some may have many
            if self.data_files:
                for data_file_name in self.data_files:
                    file_to_remove = os.path.join(self.data_path, data_file_name)
                    if os.path.exists(file_to_remove):
                        os.remove(file_to_remove)
        except AttributeError:
            pass
        """

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_filter_set(self):
        """
        Could be overwritten with a set of terms or identifiers to match against and filter by.
        Used in conjuction with filter_field, currently for csv_extract only.
        :return: set - the set of terms
        """
        return None

    def write_to_file(self) -> None:
        """
        sends the data over to the KGX writer to create the node/edge files

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        """

        # for each node captured
        for node in self.final_node_list:
            # write out the node
            self.output_file_writer.write_kgx_node(node)

        # for each edge captured
        for edge in self.final_edge_list:
            # write out the edge data
            self.output_file_writer.write_kgx_edge(edge)

    def has_sequence_variants(self):
        return False


class SourceDataWithVariantsLoader(SourceDataLoader):

    def has_sequence_variants(self):
        return True


class SourceDataBrokenError(Exception):
    def __init__(self, error_message: str):
        self.error_message = error_message


class SourceDataFailedError(Exception):
    def __init__(self, error_message: str):
        self.error_message = error_message
