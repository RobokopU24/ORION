import argparse
import os

from Common.loader_interface import SourceDataLoader
from Common.utils import GetData


class CarAraOrthLoader(SourceDataLoader):

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        data_file = "DCv3_Arabidopsis_orthologs.txt"
        data_file_path = os.path.join(self.data_path, data_file)


    # get latest source version
    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        # init the return
        ret_val: str = 'Not needed, locally stored file.'
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




if __name__ == '__main__':
    # create command line parser
    ap = argparse.ArgumentParser(description='Load carrot arabidopsis orthologs file')

    # command: python carrotArabidopsis.py -p storage_path
    ap.add_argument('-p', '--data_dir', required=True, help='The location to save the KGX files')

    # parse arguments
    args = vars(ap.parse_args())

    # get the params
    data_dir = args['data_dir']

    carOrtholog = CarAraOrthLoader(False)

    # load data files and create KGX output
    carOrtholog.load(f"{data_dir}/nodes", f"{data_dir}/edges")




