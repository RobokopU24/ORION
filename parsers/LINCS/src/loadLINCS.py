import os
import zipfile

import polars as pl
import yaml
from yaml import SafeLoader

from Common.kgxmodel import kgxnode, kgxedge
from Common.loader_interface import SourceDataLoader
from Common.biolink_constants import *
from Common.prefixes import PUBCHEM_COMPOUND
from Common.utils import GetData


class LINCSLoader(SourceDataLoader):

    source_id: str = 'LINCS'
    provenance_id: str = 'infores:lincs'
    parsing_version: str = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_url = 'https://cfde-drc.s3.amazonaws.com/LINCS/KG%20Assertions'
        self.data_file = "LINCS.zip"

        with open('/ORION/cfde-config.yml', 'r') as file:
            yaml_data = list(yaml.load_all(file, Loader=SafeLoader))
        self.config = list(filter(lambda x: x["name"] == self.source_id, yaml_data))[0]

    def get_latest_source_version(self) -> str:
        return self.config['version']

    def get_data(self) -> bool:
        data_puller = GetData()
        source_data_url = f'{self.data_url}/{self.get_latest_source_version()}/{self.data_file}'
        data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        zip_file = os.path.join(self.data_path, self.data_file)

        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(self.data_path)

        return self.parse_cfde_source(self.config, self.data_path)
