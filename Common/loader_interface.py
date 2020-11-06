
import abc


class SourceDataLoader(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'load') and
                callable(subclass.load_data_source) and
                hasattr(subclass, 'get_latest_source_version') and
                callable(subclass.extract_text) and
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

    @abc.abstractmethod
    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        """Load the source data and write it to kgx files in the specified locations."""
        raise NotImplementedError


class SourceDataBrokenError(Exception):
    def __init__(self, error_message: str):
        self.error_message = error_message


class SourceDataFailedError(Exception):
    def __init__(self, error_message: str):
        self.error_message = error_message
