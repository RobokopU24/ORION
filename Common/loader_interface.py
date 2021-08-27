from Common.kgx_file_writer import KGXFileWriter
import abc
import os


class SourceDataLoader(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'load') and
                callable(subclass.load) and
                hasattr(subclass, 'get_latest_source_version') and
                callable(subclass.get_latest_source_version) and
                hasattr(subclass, '__init__') and
                callable(subclass.__init__) or
                NotImplemented)

    def __init__(self):
        """Initialize with the option to run in testing mode."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest_source_version(self):
        """Determine and return the latest source version ie. a unique identifier associated with the latest version."""
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
                success = self.get_data()
                self.logger.info(f'{source_name}: Source data retrieved, parsing now...')
            else:
                success = True
                self.logger.info(f'{source_name}: Source data previously retrieved, parsing now...')

            # did we get the files successfully
            if success:
                # if so parse the data
                load_metadata = self.parse_data()
                if 'errors' in load_metadata and load_metadata['errors']:
                    self.logger.error(f'{source_name}: Experienced {len(load_metadata["errors"])} errors while parsing... examples: {load_metadata["errors"][:10]}')
                    load_metadata['parsing_error_examples'] = load_metadata.pop('errors')[:10]
                self.logger.info(f'{source_name}: File parsing complete. Writing to file...')

                # write the output files
                writing_metadata = self.write_to_file(nodes_output_file_path, edges_output_file_path)
                load_metadata.update(writing_metadata)
            else:
                error_message = f'{source_name}: Error - Retrieving files failed.'
                self.logger.error(error_message)
                raise SourceDataFailedError(error_message)

        except Exception:
            raise

        finally:
            # remove the temp data files or do any necessary clean up
            pass
            #self.clean_up()

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

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def write_to_file(self, nodes_output_file_path: str, edges_output_file_path: str) -> None:
        """
        sends the data over to the KGX writer to create the node/edge files

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return: writing_metadata: a dict with metadata about the file writing process
        """
        writing_metadata = {}

        # get a KGX file writer
        with KGXFileWriter(nodes_output_file_path,
                           edges_output_file_path,
                           ignore_orphan_nodes=True) as file_writer:

            # using ignore_orphan_nodes in the file writer so we have to write the edges first

            # for each edge captured
            for edge in self.final_edge_list:
                # write out the edge data
                file_writer.write_kgx_edge(edge)

            # for each node captured
            for node in self.final_node_list:
                # write out the node
                file_writer.write_kgx_node(node)

            if file_writer.orphan_node_count > 0:
                writing_metadata['orphan_nodes_skipped'] = file_writer.orphan_node_count

        return writing_metadata

    def has_sequence_variants(self):
        return False


class SourceDataWithVariantsLoader(SourceDataLoader):

    def __init__(self, test_mode: bool):
        raise NotImplementedError

    def has_sequence_variants(self):
        return True


class SourceDataBrokenError(Exception):
    def __init__(self, error_message: str):
        self.error_message = error_message


class SourceDataFailedError(Exception):
    def __init__(self, error_message: str):
        self.error_message = error_message
