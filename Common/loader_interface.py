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

    @abc.abstractmethod
    def __init__(self, test_mode: bool):
        """Initialize with the option to run in testing mode."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest_source_version(self):
        """Determine and return the latest source version ie. a unique identifier associated with the latest version."""
        raise NotImplementedError

    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        """
        :param edges_output_file_path:
        :param nodes_output_file_path:
        :return:
        """
        self.logger.info(f'{self.get_name()}:Processing beginning')

        # init the return
        load_metadata: dict = {}

        # get the human goa data
        byte_count = self.get_data()

        # did we get all the files
        if byte_count > 0:
            # parse the data
            load_metadata = self.parse_data_file()

            self.logger.debug(f'File parsing complete.')

            # write the output files
            self.write_to_file(nodes_output_file_path, edges_output_file_path)
        else:
            self.logger.error(f'Error: Retrieving file {self.data_file} failed.')

        # remove the temp data files or do any necessary clean up
        self.clean_up()

        self.logger.info(f'{self.get_name()}:Processing complete')

        return load_metadata

    def clean_up(self):
        if isinstance(self.data_file, list):
            for data_file_name in self.data_file:
                file_to_remove = os.path.join(self.data_path, data_file_name)
                if os.path.exists(file_to_remove):
                    os.remove(file_to_remove)
        else:
            file_to_remove = os.path.join(self.data_path, self.data_file)
            if os.path.exists(file_to_remove):
                os.remove(file_to_remove)

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
        :return: Nothing
        """
        # get a KGX file writer
        with KGXFileWriter(nodes_output_file_path, edges_output_file_path) as file_writer:
            # for each node captured
            for node in self.final_node_list:
                # write out the node
                file_writer.write_node(node.identifier,
                                       node_name=node.name,
                                       node_types=node.categories,
                                       node_properties=node.properties)

            # for each edge captured
            for edge in self.final_edge_list:
                # write out the edge data
                file_writer.write_edge(subject_id=edge.subjectid,
                                       object_id=edge.objectid,
                                       relation=edge.relation,
                                       edge_properties=edge.properties)


    def has_sequence_variants(self):
        return False


class SourceDataWithVariantsLoader(SourceDataLoader):

    def __init__(self, test_mode: bool):
        raise NotImplementedError

    def get_latest_source_version(self):
        raise NotImplementedError

    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        raise NotImplementedError

    def has_sequence_variants(self):
        return True


class SourceDataBrokenError(Exception):
    def __init__(self, error_message: str):
        self.error_message = error_message


class SourceDataFailedError(Exception):
    def __init__(self, error_message: str):
        self.error_message = error_message
