import os
import argparse
import enum
import requests
import yaml

from Common.utils import GetData
from Common.kgxmodel import kgxnode, kgxedge
from Common.node_types import ROOT_ENTITY, XREFS
from Common.loader_interface import SourceDataLoader

from gzip import GzipFile


# The CAM KP triplet file:
class CAMDATACOLS(enum.IntEnum):
    SUBJECT_ID = 0
    PREDICATE = 1
    OBJECT_ID = 2
    PROVENANCE_URL = 3
    PROVENANCE_ID = 4


##############
# Class: CAM-KP loader
# Desc: Class that loads/parses the CAM-KP data.
##############
class CAMKPLoader(SourceDataLoader):

    source_id: str = "CAMKP"
    provenance_id: str = "infores:go-cam"
    description = "CAMs (Causal Activity Models) are small knowledge graphs built using the Web Ontology Language (OWL). The CAM database combines many CAM graphs along with a large merged bio-ontology containing the full vocabulary of concepts referenced within the individual CAMs. Each CAM describes an instantiation of some of those concepts in a particular context, modeling the interactions between those instances as an interlinked representation of a complex biological or environmental process."
    source_data_url = "https://github.com/ExposuresProvider/cam-kp-api"
    license = "https://github.com/ExposuresProvider/cam-kp-api/blob/master/LICENSE"
    attribution = "https://github.com/ExposuresProvider/cam-kp-api"
    parsing_version = "1.0"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.source_db: str = 'CAM KP'
        self.data_url: str = 'https://stars.renci.org/var/cam-kp/'
        self.data_file: str = 'cam-kg.tsv.gz'
        self.version_file = 'cam-kg.yaml'

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data
        :return:
        """
        version_file_url = f"{self.data_url}{self.version_file}"
        r = requests.get(version_file_url)
        version_yaml = yaml.full_load(r.text)
        build_version = str(version_yaml['build'])
        return build_version

    def get_data(self) -> int:
        """
        Gets the TextMiningKP data.
        """
        data_puller = GetData()
        source_url = f"{self.data_url}{self.data_file}"
        data_puller.pull_via_http(source_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        :return: ret_val: record counts
        """

        record_counter = 0
        skipped_record_counter = 0
        
        node_file_path: str = os.path.join(self.data_path, self.data_file)
        with GzipFile(node_file_path) as zf:
            for bytesline in zf:
                lines = bytesline.decode('utf-8')
                line = lines.strip().split('\t')

                subject_id = self.sanitize_cam_node_id(line[CAMDATACOLS.SUBJECT_ID.value])
                subject_node = kgxnode(subject_id,
                                       name='',
                                       categories=[ROOT_ENTITY],
                                       nodeprops=None)
                self.output_file_writer.write_kgx_node(subject_node)

                object_id = self.sanitize_cam_node_id(line[CAMDATACOLS.OBJECT_ID.value])
                object_node = kgxnode(object_id,
                                      name='',
                                      categories=[ROOT_ENTITY],
                                      nodeprops=None)
                self.output_file_writer.write_kgx_node(object_node)

                predicate = line[CAMDATACOLS.PREDICATE.value]
                edge_provenance_id = line[CAMDATACOLS.PROVENANCE_ID.value]
                edge_provenance_url = line[CAMDATACOLS.PROVENANCE_URL.value]
                edge_properties = {XREFS: [edge_provenance_url]}
                new_edge = kgxedge(subject_id=subject_id,
                                   object_id=object_id,
                                   predicate=predicate,
                                   primary_knowledge_source=edge_provenance_id,
                                   edgeprops=edge_properties)
                self.output_file_writer.write_kgx_edge(new_edge)

                record_counter += 1
        
        self.logger.debug(f'Parsing data file complete.')
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
            }

        return load_metadata


    def sanitize_cam_node_id(self, node_id):
        if node_id.startswith("MGI:"):
            node_id = node_id[4:]
        return node_id


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load CAMKP data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the TextMiningKP data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    data_dir: str = args['data_dir']

    # get a reference to the processor
    ldr = CAMKPLoader()

    # load the data files and create KGX output
    ldr.load(data_dir, data_dir)
