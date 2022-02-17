
import argparse
import time
import docker
from neo4j import GraphDatabase, Neo4jDriver
from kgx.transformer import Transformer


# ATTENTION - currently KGX does not support neo4j 4.3 officially yet,
# this is on hold until it does.. in the meantime you need to use neo4j 3 or this version of kgx:
# git+https://github.com/biolink/kgx.git@neo4j-client-upgrade


class GraphDBTools:

    def __init__(self,
                 graph_id: str,
                 graph_db_password: str = 'bad_password',
                 http_port: int = 7474,
                 https_port: int = 7473,
                 bolt_port: int = 7687):
        self.graph_id = graph_id if graph_id else "default"
        self.graph_http_port = http_port
        self.graph_https_port = https_port
        self.graph_bolt_port = bolt_port
        self.graph_db_container_name = f'data_services_graph_db_{self.graph_id}'
        self.graph_db_uri = f'neo4j://localhost:{bolt_port}'
        # TODO use the container instead of localhost
        # self.graph_uri = f'neo4j://{self.graph_db_container_name}:{self.graph_http_port}'
        self.graph_db_password = graph_db_password

        self.graph_db_container = None

    def init_graph_db_container(self):
        docker_client = docker.from_env()
        for container in docker_client.containers.list(all=True):
            if container.name == self.graph_db_container_name:
                print(f'Found previous container for {container.name}.')
                if container.status == 'exited':
                    container.remove()
                    print(f'Removed previous container for {container.name}.')
                else:
                    return
                    # raise Exception(f'Error: Graph DB Container named {self.graph_db_container_name} already exists!')

        # TODO - make memory settings configurable
        environment = [
            f'NEO4J_AUTH=neo4j/{self.graph_db_password}',
            'NEO4J_dbms_memory_heap_max__size=4G',
            'NEO4J_dbms_memory_heap_initial__size=4G',
            'NEO4J_dbms_memory_pagecache_size=4G'
        ]

        ports = {
            self.graph_http_port: self.graph_http_port,
            self.graph_https_port: self.graph_https_port,
            self.graph_bolt_port: self.graph_bolt_port
        }
        print(f'Creating Neo4j docker container named {self.graph_db_container_name}...')
        self.graph_db_container = docker_client.containers.run("neo4j:4.3",
                                                               name=self.graph_db_container_name,
                                                               environment=environment,
                                                               ports=ports,
                                                               # network='data_services_network',
                                                               auto_remove=True,
                                                               detach=True)

    def wait_for_container_initialization(self):
        try:
            http_driver: Neo4jDriver = GraphDatabase.driver(
                self.graph_db_uri, auth=('neo4j', self.graph_db_password)
            )
            with http_driver.session() as session:
                print(f'Accessed neo4j container!')
        except Exception as e:
            print(f'Waiting for Neo4j container to finish initialization... {repr(e)}{e}')
            time.sleep(10)
            self.wait_for_container_initialization()

    def load_graph(self,
                   nodes_input_file: str,
                   edges_input_file: str,
                   input_file_format: str = 'jsonl'):

        if not self.graph_db_container:
            self.init_graph_db_container()
        self.wait_for_container_initialization()

        if input_file_format != 'jsonl':
            raise Exception(f'File format {input_file_format} not supported by GraphDBTools.')

        # prepare the parameters required by KGX
        input_args = {
            'filename': [nodes_input_file, edges_input_file],
            'format': input_file_format
        }
        output_args = {
            'uri': self.graph_db_uri,
            'username':  'neo4j',
            'password': self.graph_db_password,
            'format': 'neo4j'
        }
        # use KGX to load the files into neo4j
        print(f'Loading data from {nodes_input_file} and {edges_input_file} into {self.graph_db_container_name}...')
        t = Transformer(stream=True)
        t.transform(input_args, output_args)


if __name__ == '__main__':

    """
    parser = argparse.ArgumentParser(description='Load edges and nodes into Neo4j')
    parser.add_argument('graph_id', help='ID of the graph', default='default')
    parser.add_argument('nodes', help='file with nodes in jsonl format')
    parser.add_argument('edges', help='file with edges in jsonl format')
    # these are generated automatically for now
    # parser.add_argument('--uri', help='URI/URL for Neo4j (including port)', default='localhost:7474')
    # parser.add_argument('--username', help='username', default='neo4j')
    # parser.add_argument('--password', help='password', default='default')
    args = parser.parse_args()

    graph_db_tools = GraphDBTools(args.graph_id)
    graph_db_tools.load_graph(args.nodes,
                              args.edges)
    """

    # ATTENTION - currently KGX does not support neo4j 4.3 officially yet,
    # this is on hold until it does.. in the meantime you need to use neo4j 3 or this version of kgx:
    # git+https://github.com/biolink/kgx.git@neo4j-client-upgrade

    graph_db_tools = GraphDBTools("Example_Graph_ID")
    nodes_file = '/Example/nodes.jsonl'
    edges_file = '/Example/edges.jsonl'
    graph_db_tools.load_graph(nodes_file,
                              edges_file)
