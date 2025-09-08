
import os
import enum
import gzip

from orion.extractor import Extractor
from orion.loader_interface import SourceDataLoader
from orion.biolink_constants import PRIMARY_KNOWLEDGE_SOURCE
from orion.prefixes import HGNC  # only an example, use existing curie prefixes or add your own to the prefixes file
from orion.utils import GetData


# if parsing a tsv or csv type file with columns, use a enum to represent each field
class GENERICDATACOLS(enum.IntEnum):
    COLUMN_1 = 0
    COLUMN_2 = 1
    COLUMN_3 = 2
    COLUMN_4 = 3


##############
# Class: XXXX source loader
#
# Desc: Class that loads/parses the XXXX data.
##############
class ParserTemplate(SourceDataLoader):

    source_id: str = 'SourceID'
    # this should be a valid infores curie from the biolink infores catalog
    provenance_id: str = 'infores:provenance'
    # increment parsing_version whenever changes are made to the parser that would result in changes to parsing output
    parsing_version: str = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.example_url = 'https://example.com/'
        self.example_data_file = 'name_of_data_file.tsv.gz'
        self.data_files = [self.example_data_file]

    def get_latest_source_version(self) -> str:
        # if possible go to the source and retrieve a string that is the latest version of the source data
        latest_version = 'v1.0'
        return latest_version

    def get_data(self) -> bool:
        # get_data is responsible for fetching the files in self.data_files and saving them to self.data_path
        source_data_url = f'{self.example_url}{self.example_data_file}'
        data_puller = GetData()
        data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        # This is a made up example of how one might extract nodes and edges from a tsv file
        # In this case it's taking the subject ID from column 1 and the object ID from column 3,
        # prepending them with a curie prefix. The predicate comes from column 3. The value in column 4
        # is set as a property on the edge.
        extractor = Extractor(file_writer=self.output_file_writer)
        example_file: str = os.path.join(self.data_path, self.example_data_file)
        with gzip.open(example_file, 'rt') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f'{HGNC}:{line[GENERICDATACOLS.COLUMN_1.value]}',  # subject id
                                  lambda line: f'{HGNC}:{line[GENERICDATACOLS.COLUMN_3.value]}',  # object id
                                  lambda line: line[GENERICDATACOLS.COLUMN_2.value],  # predicate extractor
                                  lambda line: {},  # subject properties
                                  lambda line: {},  # object properties
                                  lambda line: {PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id,
                                                'column_4': line[GENERICDATACOLS.COLUMN_4.value]},  # edge properties
                                  comment_character='#',
                                  delim='\t',
                                  has_header_row=True)
        return extractor.load_metadata
