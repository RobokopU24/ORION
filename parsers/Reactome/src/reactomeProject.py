import os
import enum
import argparse
import requests
from collections import defaultdict
from bs4 import BeautifulSoup
import re
from neo4j import GraphDatabase
# drr = '/Users/olawumiolasunkanmi/Data_services_root/Data_services'
# os.sys.path.insert(0, drr)

from collections import defaultdict
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge
from Common.prefixes import REACTOME


SUBJECT_COLUMN = 0
PREDICATE_COLUMN = 1
OBJECT_COLUMN = 2
INCLUDE_COLUMN = 3


PREDICATE_PROP_COLUMN = -1
##############
# Class: Reactome loader
#
# By: 
# Date: 
# Desc: Class that loads/parses the reactome data.
##############
class ReactomeLoader(SourceDataLoader):

    # Setting the class level variables for the source ID and provenance
    source_id: str = 'Reactome'
    provenance_id: str = 'infores:reactome-properties'
    description = "Reactome is a free, open-source, curated and peer-reviewed pathway database"
    source_data_url = "https://reactome.org/"
    license = "https://reactome.org/license"
    attribution = "https://academic.oup.com/nar/article/50/D1/D687/6426058?login=false"
    parsing_version = 'V8'
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
        self.driver = GraphDatabase.driver(self.data_url, auth=(self.data_user, self.password), database=self.database)
        self.triple_file: str = 'reactomeContents - triple.csv'
        

    def get_latest_source_version(self) -> str:
        """
        gets the latest available version of the data

        :return:
        """

        # load the web page for CTD
        html_page: requests.Response = requests.get(self.version_url)
        
        if html_page.status_code == 200:
            # get the html into a parsable object
            resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')

            # set the search text

            # find the version tag
            a_tag: BeautifulSoup.Tag = resp.find_all()

            for tag in a_tag:
                match = re.search(r'V\d+', tag.get_text())
                if match:
                    version  = f'{match.group(0)!r}'
                    date_tag = tag.find('time').get('datetime')
                
                    break 
            return (f'Version: {str(version + date_tag)}')
        else:
            return (f"Last Known Version: {self.parsing_version}")

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
 
    def parse_data(self):
        """
        Parses the data file for graph nodes/edges
        """

        nodes, relations = self.get_triple()

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
                skipped_record_counter += 1
                continue
            self.final_node_list.append(output_node)
        
        for rel in relations:
            subjectid = rel[SUBJECT_COLUMN]
            objectid = rel[OBJECT_COLUMN]
            predicate = rel[PREDICATE_COLUMN]
            property = rel[PREDICATE_PROP_COLUMN]
        
            output_edge = kgxedge(
                subject_id=subjectid,
                object_id=objectid,
                predicate=predicate,
                primary_knowledge_source=self.provenance_id,
                edgeprops=property
            )
            
            if not output_edge:
                continue
            self.final_edge_list.append(output_edge)

        self.logger.debug(f'Parsing data file complete.')
        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
            }

        return load_metadata
    
    def filterProps(self, record):
        supposeProperties = ["CatalystActivity", "Summation"]
        record = {'a': {k: v for k, v in record['a'].items()},
                'b': {k: v for k, v in record['b'].items()}}
        a_id = None
        b_id = None

        if 'stId' in record["a"]:
            a_id = f'{REACTOME}:{record["a"]["stId"]}'
        
        elif 'databaseName' in record["a"] and 'accession' in record["a"]:
            a_id = f'{record["a"]["databaseName"]}:{record["a"]["accession"]}'
        
        elif record["a"]['schemaClass'] in supposeProperties:
                new_key = record["a"]["schemaClass"].lower()
                record["b"][new_key] = record["a"]['displayName']
                a_id = None
        else:
            a_id = record["a"]['schemaClass']

        if 'stId' in record["b"]:
            b_id = f'{REACTOME}:{record["b"]["stId"]}'
        elif 'databaseName' in record["b"] and 'accession' in record["b"]:
            b_id = f'{record["b"]["databaseName"]}:{record["b"]["accession"]}'
        elif record["b"]["schemaClass"] in supposeProperties:
                new_key = record["b"]["schemaClass"].lower()
                record["a"][new_key] = record["b"]["displayName"]
                b_id = None 
        else:
            a_id = record["b"]['schemaClass']

        return record, a_id, b_id

    

    def get_triple(self) -> list:
        queries_to_include = []
        lines_to_process = []

        with open(self.triple_file, 'r') as inf:
            lines = inf.readlines()
            lines_to_process = lines[1:]
        
        for line in lines_to_process:
            line = line.strip().split(',')
            if line[INCLUDE_COLUMN] == 'Include':
                cypher_query = f"MATCH (a:{line[SUBJECT_COLUMN]})-[r:{line[PREDICATE_COLUMN]}]->(b:{line[OBJECT_COLUMN]}) RETURN a, r, b"
                queries_to_include.append(cypher_query)

        # Execute the queries and retrieve the results
        with self.driver.session() as session:
            results = []
            for cypher_query in queries_to_include:
                result = session.run(cypher_query)
                results.append(list(result))


        # Extract the node properties and relation information
        nodes = defaultdict(dict)
        relations = []

        for result in results:
            for record in result:
                records, a_id, b_id = self.filterProps(record)
                
                if not a_id or a_id in nodes[a_id]:
                    continue
                nodes[a_id].update(dict(records["a"].items()))

                if not b_id or b_id in nodes[b_id]:
                    continue
                nodes[b_id].update(dict(records["b"].items()))

                if a_id and b_id:
                    rl = dict(record["r"])
                    rll = [a_id, record["r"].type, b_id, rl]
                    relations.append(rll)

        return nodes, relations
    

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
