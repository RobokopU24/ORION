import os
import enum
import json
import argparse
import requests
from collections import defaultdict
from bs4 import BeautifulSoup
import re
from neo4j import GraphDatabase
drr = '/Users/olawumiolasunkanmi/Data_services_root/Data_services'
os.sys.path.insert(0, drr)

from collections import defaultdict
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader, SourceDataFailedError
from Common.kgxmodel import kgxnode, kgxedge
from Common.prefixes import REACTOME, NCBITAXON, GTOPDB


SUBJECT_COLUMN = 0
PREDICATE_COLUMN = 1
OBJECT_COLUMN = 2
INCLUDE_COLUMN = 3


PREDICATE_PROP_COLUMN = -1

PREDICATE_MAPPING = {"compartment":"biolink:occurs_in","output":"biolink:has_output","input":"biolink:has_input","hasEvent":"biolink:contains_process","precedingEvent":"biolink:precedes","activeUnit":"biolink:actively_involves","hasComponent":"biolink:has_part","catalystActivity":"biolink:actively_involves","cellType":"biolink:located_in","goBiologicalProcess":"biolink:same_as"}
SUPPOSE_PROPERTIES = ("Summation", "EntityFunctionalStatus", "GeneticallyModifiedResidue", "AbstractModifiedResidue")


# Normalized node are normalized once formatted as curie
NORMALIZED_NODES = ("ReactionLikeEvent",)#("Pathway", "Event", "BlackboxEvent", "FailedReaction", "Depolymerisation", "Polymerisation")

# These nodes has no reactome stID, however their ID is the combination of two attributes on them
ON_NODE_MAPPING = ("GO_Term", "Species", "ExternalOntology", "ReferenceTherapeutic",)#databaseName+:+identifier
                    # GO_Term: "Compartment", "GO_BiologicalProcess", "GO_MolecularFunction", "GO_CellularComponent",
                    # ExternalOntology: "Disease", "CellType"

ON_NODE_ID_MAPPING = ("ReferenceMolecule",  "ReferenceSequence") #databaseName+KB:+identifier or databaseName+:+identifier
ALL_ON_NODE_MAPPING = NORMALIZED_NODES + ON_NODE_MAPPING + ON_NODE_ID_MAPPING


# ReferenceIsoform is the reference ID for EntityWITHACCESSIONEDSEQUENCE(PROTEIN)
# GenomeEncodedEntity: EntityWITHACCESSIONEDSEQUENCE(Protein), EntityWITHACCESSIONEDSEQUENCE(Gene and transcript), EntityWITHACCESSIONEDSEQUENCE(DNA), EntityWITHACCESSIONEDSEQUENCE(RNA)
CROSS_MAPPING = ('GenomeEncodedEntity', 'SimpleEntity', 'Drug')

TO_WRITE = ('Provenance/Include', 'Attribute/Include') #Descriptive features of other existing nodes eg Summation
TO_MAP = ('IDMapping/Include', ) # Maps the external identifier of a node from another node
TO_INCLUDE = ('Include',)
RDF_EDGES_TO_INCLUDE = ('RDF_edges/Include',)

NODE_NOT_WORKING = set()
EDGE_NOT_WORKING = set()
##############
# Class: Reactome loader
#
# By: 
# Date: 
# Desc: Class that loads/parses the reactome data.
##############
class ReactomeLoader(SourceDataLoader):

    # Setting the class level variables for the source ID and provenance
    source_id: str = 'REACTOME-DB'
    provenance_id: str = 'infores:reactome'
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
        self.driver = None
        self.data_url: str = 'bolt://localhost:7688/'#"bolt://127.0.0.1:7688" #
        self.data_user: str = 'neo4j'
        self.password: str = 'reactomepleasebegood'
        # self.dbase: str = 'reactomes'
        self.driver = GraphDatabase.driver(self.data_url, auth=(self.data_user, self.password))
        self.triple_file: str = 'reactomeContents_CriticalTriples.csv'
        self.triple_path = os.path.dirname(os.path.abspath(__file__))
        self.data_files = []


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
        # gd: GetData = GetData(self.logger.level)
        # for dt_file in self.data_files:
        #     gd.pull_via_http(f'{self.data_url}{dt_file}',
        #                      self.data_path)

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
                NODE_NOT_WORKING.update((node, props))
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
                predicate=PREDICATE_MAPPING.get(predicate, predicate),
                primary_knowledge_source=self.provenance_id,
                edgeprops=property
            )
            
            if not output_edge:
                EDGE_NOT_WORKING.update((subjectid, predicate, objectid))
                continue
            self.final_edge_list.append(output_edge)

        self.logger.debug(f'Parsing data file complete.')
        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
            }
        # if EDGE_NOT_WORKING:
        #     with open('No_edges.txt', 'w') as nw:
        #         nw.write(json.dumps(list(EDGE_NOT_WORKING), indent=4))
        # if NODE_NOT_WORKING:
        #     with open('No_nodes.txt', 'w') as nw:
        #         nw.write(json.dumps(list(NODE_NOT_WORKING), indent=4))

        return load_metadata
    
    
    def get_triple(self) -> list:
        #These Mapping files contains the Ids that normalizes, so instead of the triples alone, we include the mapping too
        queries_to_include = []
        queries_to_map = []
        queries_to_write = []

        with open(os.path.join(self.triple_path, self.triple_file), 'r') as inf:
            lines = inf.readlines()
            lines_to_process = lines[1:]
        
        for line in lines_to_process:
            line = line.strip().split(',')
            if line[INCLUDE_COLUMN] in RDF_EDGES_TO_INCLUDE:
              # Not in use now
                queries_to_include.append(
                    self.rdf_edge_mapping(line[SUBJECT_COLUMN], line[PREDICATE_COLUMN], line[OBJECT_COLUMN]))
              
            elif line[INCLUDE_COLUMN] in TO_INCLUDE:
                cypher_query = f"MATCH (a:{line[SUBJECT_COLUMN]})-[r:{line[PREDICATE_COLUMN]}]->(b:{line[OBJECT_COLUMN]}) RETURN a, r, b"
                queries_to_include.append(cypher_query)
              
            elif line[INCLUDE_COLUMN] in TO_MAP:
              # Not in use now
                queries_to_map.append(self.map_ids(line[SUBJECT_COLUMN], line[PREDICATE_COLUMN], line[OBJECT_COLUMN]))
            else:
              # Not in use now
                if line[INCLUDE_COLUMN] in TO_WRITE:
                    queries_to_write.append(self.write_properties(line[SUBJECT_COLUMN], line[PREDICATE_COLUMN], line[OBJECT_COLUMN]))

        # Temporary saves to know what kind of cypher queries are being run
        import json
        with open('Mapqueries.txt', 'w') as qw:
            qw.write(json.dumps(queries_to_map, indent=4))
        with open('includequeries.txt', 'w') as qw:
            qw.write(json.dumps(queries_to_include, indent=4))
        with open('Writequeries.txt', 'w') as qw:
            qw.write(json.dumps(queries_to_write, indent=4))


        with self.driver.session() as session:
            results = []

            #Map the id from the nodes databaseName+:+identifier
            for node in ALL_ON_NODE_MAPPING:
                session.run(self.on_node_mapping(node))

            for node in CROSS_MAPPING:
                session.run(self.cross_map_ids(node))

            #Map other propersties from the nodes summation ...
            # PS: Not currently in use since the content file contains only 'include'
            for cypher_query in queries_to_write:
                if not cypher_query:
                    continue
                session.run(cypher_query)

            #Map the id from the other nodes respectively
            # PS: Not currently in use since the content file contains only 'include'
            for cypher_query in queries_to_map:
                if not cypher_query:
                    continue
                session.run(cypher_query)

            #Finally, run the cypher to get results - nodes and edges
            for cypher_query in queries_to_include:
                result = session.run(cypher_query)
                results.append(list(result))
       
        # # Extract the node properties and relation information
        nodes = defaultdict(dict)
        relations = []

        for result in results:
            for records in result:
                a_id = records.get('a', {}).get('ids')
                b_id = records.get('b', {}).get('ids')

                if not a_id:
                    NODE_NOT_WORKING.add(records.get('a', {}))
                    continue
                nodes[a_id].update(dict(records["a"].items()))

                if not b_id:
                    NODE_NOT_WORKING.add(records.get('b', {}))
                    continue
                nodes[b_id].update(dict(records["b"].items()))

                if (a_id and b_id):
                    rl = dict(records["r"])
                    rll = [a_id, records["r"].type, b_id, rl]
                    relations.append(rll)
                else:
                    EDGE_NOT_WORKING.update(dict(records))


        return nodes, relations


    def write_properties(self, s, p, o):
        def repl_func(match):
            return match.group(1).lower()

        cypher = None
        #Cross Property Mapping eg Summation
        mapp = {'a':s, 'b': o}
        if o in SUPPOSE_PROPERTIES:
            cypher = f"MATCH (a:{s})-[r:{p}]->(b:{o}) SET a.{re.sub(r'^(.)', repl_func, mapp.get('b'))} = b.displayName"

        elif s in SUPPOSE_PROPERTIES:
            cypher = f"MATCH (a:{s})-[r:{p}]->(b:{o}) SET b.{re.sub(r'^(.)', repl_func, mapp.get('a'))} = a.displayName"
        
        return cypher
    
    def on_node_mapping(self, node):
        #On-node/Same node ID Mapping eg GO, Disease ...
        if node =="Species":
            cypher = f"MATCH (a:{node}) SET a.ids ='{NCBITAXON}:'+a.taxId"
        elif node == "ReferenceTherapeutic":
            cypher = f"MATCH (a:{node}) SET  a.ids ='{GTOPDB}:'+a.identifier"
        elif node in ON_NODE_ID_MAPPING or node =="ExternalOntology":
            cypher = f"MATCH (a:{node}) SET a.ids = CASE WHEN SIZE(SPLIT(a.databaseName, ' ')) >= 2 THEN SPLIT(a.databaseName, ' ')[0] + 'Gene' + ':' + SPLIT(a.identifier, '.')[0] WHEN a.databaseName = 'UniProt' THEN REPLACE(a.databaseName, ' ', '') + 'KB:' + SPLIT(a.identifier, '.')[0] ELSE REPLACE(a.databaseName, ' ', '') + ':' + SPLIT(a.identifier, '.')[0] END"
            # Some are NCBI Entrez Gene or NCBI Nucleotide or uniprot or ensembl etc
            # The split is necessary because of some funny looking curies:
            # NCBI Gene:324=>NCBIGene:324; KEGG Gene (Homo sapiens):123=>KEGGGene:123
        elif node in NORMALIZED_NODES:
            #Biolink normalized reactome ids eg pathways, events ......
            cypher = f"MATCH (a:{node}) SET a.ids ='{REACTOME}:'+a.stId"
        else:
            # GO .......
            cypher = f"MATCH (a:{node}) SET a.ids = a.databaseName+':'+a.accession"
        return cypher

    def map_ids(self, s, p, o):
        # ReferenceID Mappings eg EntityWithAccessionedSequence ->ReferenceSequence ......
        cypher = None
        # OBJECT
        if s in CROSS_MAPPING:
            # Cross Property Mapping eg Summation
            cypher = f"MATCH (a:{s})-[r:referenceEntity]->(b:{o}) SET a.ids = b.ids"
        elif o in CROSS_MAPPING:
            cypher = f"MATCH (a:{s})-[r:{p}]->(b:{o}) SET b.ids = a.ids"
        if not cypher:
            # None of the s or object has external mapping file
            cypher = f"MATCH (a:{s})-[r:{p}]->(b:{o}) SET a.ids ='{REACTOME}:'+a.stId, b.ids='{REACTOME}:'+b.stId"
        return cypher

    def cross_map_ids(self, node):
        # Cross Property Mapping eg  ReferenceID Mappings for EntityWithAccessionedSequence ->ReferenceSequence ......
        cypher = f"MATCH (a:{node})-[r:referenceEntity]->(b) SET a.ids = b.ids"
        return cypher

    def rdf_edge_mapping(self, s, p, o):
        cypher = None
        # For any triple like
            # CatalystActivity-[activity]->Go_MolecularFunction and,
            # Reaction-[catalystActivity]->CatalystActivity
                # Collapse the 2 hops and get: Reaction-[activity]-Go_MolecularFunction
        # CatalystActivity to catalystActivity
        if s =='CatalystActivity':
            cypher= f"MATCH (x)-[rx:catalystActivity]->(a:{s})-[r:activity]->(b:{o}) return x, r, b"
        elif o =='CatalystActivity':
            cypher = f"MATCH (a:{s})-[r:{p}]-(b:{o})-[rx:catalystActivity]-(x) return a, r, x"

        return cypher

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

