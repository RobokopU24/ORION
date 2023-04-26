
import os
import enum
import gzip

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import PRIMARY_KNOWLEDGE_SOURCE


# the data header columns for the orthologs tsv file:
class ORTHOLOGDATACOLS(enum.IntEnum):
    GENE_1_ID = 0
    GENE_1_LABEL = 1
    GENE_2_ID = 4
    GENE_2_LABEL = 5

##############
# Class: Genome Alliance data source loader
#
# Desc: Class that loads/parses the cord19 model data.
##############
class GenomeAllianceOrthologLoader(SourceDataLoader):

    source_id: str = 'GenomeAllianceOrthologs'
    provenance_id: str = 'infores:agrkb'
    parsing_version: str = '1.2'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.latest_version = None
        self.latest_version = self.get_latest_source_version()
        self.genome_alliance_url = f'https://download.alliancegenome.org/{self.get_latest_source_version()}/ORTHOLOGY-ALLIANCE/COMBINED/'
        self.genome_alliance_ortholog_file = 'ORTHOLOGY-ALLIANCE_COMBINED_25.tsv.gz'
        self.data_files = [self.genome_alliance_ortholog_file]

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        if not self.latest_version:
            self.latest_version = '5.3.0'
        return self.latest_version

    def get_data(self) -> int:
        """
        Gets the data.

        """
        sources_to_pull = [
            f'{self.genome_alliance_url}{self.genome_alliance_ortholog_file}'
        ]
        data_puller = GetData()
        for source_url in sources_to_pull:
            data_puller.pull_via_http(source_url, self.data_path)

        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor(file_writer=self.output_file_writer)
        orthologs_file: str = os.path.join(self.data_path, self.genome_alliance_ortholog_file)
        with gzip.open(orthologs_file, 'rt') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[ORTHOLOGDATACOLS.GENE_1_ID.value].replace('_', ''),  # subject id
                                  lambda line: line[ORTHOLOGDATACOLS.GENE_2_ID.value].replace('_', ''),  # object id
                                  lambda line: 'biolink:orthologous_to',  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id}, #edgeprops
                                  comment_character='#',
                                  delim='\t',
                                  has_header_row=True)

        return extractor.load_metadata
