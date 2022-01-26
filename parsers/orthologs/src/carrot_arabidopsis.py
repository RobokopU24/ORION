import argparse
import os
import gzip
import enum

from Common.loader_interface import SourceDataLoader
# from Common.utils import GetData
from Common.extractor import Extractor
from io import TextIOWrapper

# the data columns are:
class DATACOLS(enum.IntEnum):
    carrot_gene_id = 0
    arabidopsis_gene_id = 1
    unnormalized_score = 2
    normalized_score = 3


class CarrotArabidopsisOrthlogsLoader(SourceDataLoader):

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_file = "DCv3_Arabidopsis_orthologs_test.txt" # "DCv3_Arabidopsis_orthologs.txt"
        self.data_file_path = os.path.join(self.data_path, self.data_file)


    # get latest source version
    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        # init the return
        # Not needed, locally stored file.
        ret_val: str = 'curr_version'
        return ret_val

    # get data
    def get_data(self) -> int:
        """
        Locally stored file
        Download not needed
        :return:
        """
        # To Do: Store it somewhere and download from that url
        ret_val = 1
        return ret_val

    # parse data
    def parse_data(self) -> dict:
        """
        Parses the data file for nodes/edges

        :return: dict of parsing metadata results
        """
        # self.data_file_path <- path to carrot to arabidopsis gene ortholog file

        extractor = Extractor()
        # file = open(self.data_file_path)
        # for i, line in enumerate(file, start=1):
        #     print(i, line)


        with (gzip.open if self.data_file_path.endswith(".gz") else open)(self.data_file_path) as ortholog_file:
            extractor.csv_extract(ortholog_file,
                                  lambda  line: f'{line[DATACOLS.carrot_gene_id.value]}', # extract subject id
                                  lambda line: f'{line[DATACOLS.arabidopsis_gene_id.value]}', # extract object id
                                  lambda line: f'{line[DATACOLS.normalized_score]}', # extract predicate
                                  lambda line: {}, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {}, # edge props
                                  has_header_row=True)

            # return to the caller
            self.final_node_list = extractor.nodes
            self.final_edge_list = extractor.edges
            return extractor.load_metadata





if __name__ == '__main__':
    # create command line parser
    ap = argparse.ArgumentParser(description='Load carrot arabidopsis orthologs file')

    # command: python carrotArabidopsis.py -p storage_path
    ap.add_argument('-p', '--data_dir', required=True, help='The location to save the KGX files')

    # parse arguments
    args = vars(ap.parse_args())

    # get the params
    data_dir = args['data_dir']

    orthologLoader = CarrotArabidopsisOrthlogsLoader(False)
    # orthologLoader.parse_data()

    # load data files and create KGX output
    orthologLoader.load(f"{data_dir}/nodes", f"{data_dir}/edges")




