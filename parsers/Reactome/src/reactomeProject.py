import os
import enum
import argparse
from gzip import GzipFile
import requests
from bs4 import BeautifulSoup
import re
from neo4j import GraphDatabase
drr = '/Users/olawumiolasunkanmi/Library/CloudStorage/OneDrive-UniversityofNorthCarolinaatChapelHill/FALL2022/BACKUPS/ARAGORN/Data_services_root/Data_services'
os.sys.path.insert(0, drr)

from collections import defaultdict
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge
from Common.prefixes import REACTOME


PREDICATE_PROP_COLUMN = 1
SUBJECT_COLUMN = 0
OBJECT_COLUMN = -1
##############
# Class: Reactome loader
#
# By: 
# Date: 
# Desc: Class that loads/parses the reactome data.
##############
class ReactomeLoader(SourceDataLoader):

    # Setting the class level variables for the source ID and provenance
    source_id: str = 'ReactomeProperties'
    provenance_id: str = 'infores:reactome-properties'
    description = ""
    source_data_url = ""
    license = ""
    attribution = ""
    parsing_version = '1.1'
    preserve_unconnected_nodes = True

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.version_url: str = 'https://reactome.org/about/news'
        self.data_url: str = 'bolt://localhost:7688/'
        self.data_user: str = 'neo4j'
        self.database: str = 'reactome'
        self.password: str = 'qwerty1234'
        # self.data_files = [self.nodes_file,
        #                    self.relation_file]
        self.query = "MATCH (a:Reaction)-[r:precedingEvent]->(b:Reaction) RETURN a, r, b"
        self.driver = GraphDatabase.driver(self.data_url, auth=(self.data_user, self.password), database=self.database)

    def get_latest_source_version(self) -> str:
        """
        gets the latest available version of the data

        :return:
        """
        response = requests.get(self.version_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            for tag in soup.find_all():
                match = re.search(r'V\d+', tag.get_text())
                if match:
                    version  = f'{match.group(0)!r}'
                    date_tag = tag.find('time').get('datetime')
                
                    break 
            return (str(version + date_tag))
        else:
            return (f"Error: {response.status_code}; last Known Version: (V84)")

    def get_data(self) -> bool:
        """
        Gets the chebi-properties data.

        """
        # get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)
        for dt_file in self.data_files:
            gd.pull_via_http(f'{self.data_url}{dt_file}',
                             self.data_path)
        
        return True
    
    def get_triple(self) -> list:
        cypher_query = self.query
        # Execute the query and retrieve the results
        with self.driver.session() as session:
            result = session.run(cypher_query)
            results = list(result)
        
        # Extract the node properties and relation information
        nodes = defaultdict(dict)
        relations = []

        for record in results:
            a_id = record["a"]["stId"]
            b_id = record["b"]["stId"]
            a_id = f'REACT:{a_id}'
            b_id = f'REACT:{b_id}'
            # Add the subject and object nodes to the dictionary if they don't exist
            if not nodes[a_id]:
                nodes[a_id].update(dict(record["a"].items()))
            if not nodes[b_id]:
                nodes[b_id].update(dict(record["b"].items()))

            # Add relation properties to the corresponding dictionaries
            rl = list(dict(record["r"].items()).values())
            rll = [a_id, rl, b_id]
            relations.append(rll)

        return nodes, relations, (record["r"].type)


    def parse_data(self):
        """
        Parses the data file for graph nodes/edges
        """

        nodes, predicate_props, predicate = self.get_triple()

        # iterate through the compounds file and create a dictionary of chebi_id -> name
        
        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # walk through the chebi IDs and names from the compounds file and create the chebi property nodes
        for node, props in nodes.items():

            # increment the record counter
            record_counter += 1
            if self.test_mode and record_counter == 2000:
                break

                # create a node with the properties
            output_node = kgxnode(node,
                                    nodeprops=props)
            if not output_node:
                continue
            self.final_node_list.append(output_node)
        
        for rel in predicate_props:
            subjectid = rel[SUBJECT_COLUMN]
            objectid = rel[OBJECT_COLUMN]
            properties = rel[PREDICATE_PROP_COLUMN]
            predicate = predicate
            kgxedge(
                subject_id=subjectid,
                object_id=objectid,
                predicate=predicate,
                edgeprops = properties
            )

        self.logger.debug(f'Parsing data file complete.')
        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
            }

        return load_metadata


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load Reactome data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the KGs data file')

    # # parse the arguments
    args = vars(ap.parse_args())

    # # this is the base directory for data files and the resultant KGX files.
    rct_data_dir: str = args['data_dir']

    # get a reference to the processor
    ch_ldr = ReactomeLoader()

    # load the data files and create KGX output
    ch_ldr.load(rct_data_dir + '/nodes.jsonl', rct_data_dir + '/edges.jsonl')

    # unique_nodes_edges()
    # nodes = get_nodes(node_label=None)
    # relationships = get_edges(edge_label=None)
    # get_node_edge_json(nodes, relationships)
