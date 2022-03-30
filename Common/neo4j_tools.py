import argparse
import time
import docker
import csv
import os
from neo4j import GraphDatabase, Neo4jDriver
from kgx.transformer import Transformer
from Common.node_types import SUBJECT_ID, OBJECT_ID, PREDICATE
from Common.utils import quick_jsonl_file_iterator


class GraphDBTools:

    def __init__(self,
                 graph_id: str,
                 graph_db_password: str = 'default',
                 http_port: int = 7474,
                 https_port: int = 7473,
                 bolt_port: int = 7687,
                 neo4j_host: str = 'localhost',
                 use_docker_network: bool = False,
                 available_gb_memory: int = 8):
        self.graph_id = graph_id if graph_id else "default"
        self.graph_http_port = http_port
        self.graph_https_port = https_port
        self.graph_bolt_port = bolt_port
        self.graph_db_password = graph_db_password
        self.available_gb_memory = available_gb_memory
        if use_docker_network:
            self.graph_db_host = f'data_services_graph_db_{self.graph_id}'
            self.graph_db_uri = f'bolt://{self.graph_db_host}:{bolt_port}'
        else:
            self.graph_db_host = neo4j_host
            self.graph_db_uri = f'bolt://{neo4j_host}:{bolt_port}'

    def init_graph_db_container(self,
                                use_csv: bool = True,
                                csv_nodes_file: str = None,
                                csv_edges_file: str = None):
        docker_client = docker.from_env()
        container = self.get_container(container_name=self.graph_db_host,
                                       docker_client=docker_client)
        if container:
            if container.status == 'exited':
                container.remove()
                print(f'Removed previous container for {container.name}.')
            else:
                raise Exception(f'Error: Graph DB Container named {self.graph_db_host} already exists!')

        heap_size = self.available_gb_memory * .75
        pagecache_size = self.available_gb_memory * .25
        environment = [
            f'NEO4J_AUTH=neo4j/{self.graph_db_password}',
            f'NEO4J_dbms_memory_heap_max__size={heap_size}G',
            f'NEO4J_dbms_memory_heap_initial__size={heap_size}G',
            f'NEO4J_dbms_memory_pagecache_size={pagecache_size}G',
            'NEO4J_dbms_default__listen__address=0.0.0.0'
        ]
        ports = {
            self.graph_http_port: self.graph_http_port,
            self.graph_https_port: self.graph_https_port,
            self.graph_bolt_port: self.graph_bolt_port
        }
        if use_csv:
            current_graph_dir = csv_nodes_file.rsplit('/', 1)[0]
            neo4j_data_dir_relative_path = current_graph_dir + '/neo4j_data'
            os.mkdir(neo4j_data_dir_relative_path)
            if neo4j_data_dir_relative_path.startswith('/Data_services_graphs'):
                # If the path starts with /Data_services_graphs we are probably in a docker container.
                # The following will replace the docker relative directory path with the real one from the host,
                # so that we may mount the volume in the new docker container.
                neo4j_data_dir_real_path = f'{os.environ["HOST_GRAPHS_DIR"]}' \
                                           f'{neo4j_data_dir_relative_path.split("/Data_services_graphs/", 1)[1]}'
            else:
                neo4j_data_dir_real_path = neo4j_data_dir_relative_path

            volumes = [
                f'{os.environ["HOST_GRAPHS_DIR"]}:/Data_services_graphs',
                f'{neo4j_data_dir_real_path}:/data'  # neo4j data directory - necessary for persistence after import
            ]
            print(f'Creating container and importing csv files to neo4j...')
            neo4j_cmd = f'neo4j-admin import --nodes={csv_nodes_file} --relationships={csv_edges_file}'
            docker_client.containers.run("neo4j:4.3",
                                         name=self.graph_db_host,
                                         command=neo4j_cmd,
                                         environment=environment,
                                         ports=ports,
                                         network='data_services_network',
                                         volumes=volumes)

            # wait for the container to finish importing and exit
            print(f'Import complete. Waiting for container to exit...')
            import_complete = False
            while not import_complete:
                container = self.get_container(container_name=self.graph_db_host,
                                               docker_client=docker_client)
                print(f'Waiting... got container {container.name} with status {container.status}')
                if container.status == 'exited':
                    container.remove()
                    import_complete = True
                else:
                    time.sleep(10)

            print(f'Creating a backup dump of the neo4j...')
            neo4j_cmd = f'neo4j-admin dump --to={current_graph_dir}/graph.db.dump'
            docker_client.containers.run("neo4j:4.3",
                                         name=self.graph_db_host,
                                         command=neo4j_cmd,
                                         environment=environment,
                                         ports=ports,
                                         auto_remove=True,
                                         network='data_services_network',
                                         volumes=volumes,
                                         detach=False)
            print(f'Backup dump complete.')
            # os.remove(csv_nodes_file)
            # os.remove(csv_edges_file)
            # os.remove(neo4j_data_dir_relative_path)
        else:
            docker_client.containers.run("neo4j:4.3",
                                         name=self.graph_db_host,
                                         environment=environment,
                                         ports=ports,
                                         auto_remove=False,
                                         network='data_services_network',
                                         detach=True)

    def get_container(self, docker_client: docker.DockerClient, container_name: str):
        for container in docker_client.containers.list(all=True):
            if container.name == container_name:
                return container
        return None

    def wait_for_container_initialization(self):
        try:
            http_driver: Neo4jDriver = GraphDatabase.driver(
                self.graph_db_uri,
                auth=('neo4j', self.graph_db_password)
            )
            with http_driver.session() as session:
                session.run("match (c) return count(c)")
                print(f'Accessed neo4j container!')
        except Exception as e:
            print(f'Waiting for Neo4j container to finish initialization... {repr(e)}{e}')
            time.sleep(10)
            self.wait_for_container_initialization()

    def load_graph(self,
                   nodes_input_file: str,
                   edges_input_file: str,
                   input_file_format: str = 'jsonl',
                   start_neo4j: bool = False,
                   use_csv: bool = False):

        if input_file_format != 'jsonl':
            raise Exception(f'File format {input_file_format} not supported by GraphDBTools.')

        if use_csv:
            print(f'Converting kgx to csv files...')
            csv_nodes_file, csv_edges_file = self.convert_kgx_to_csv(nodes_input_file, edges_input_file)
            self.init_graph_db_container(use_csv=use_csv,
                                         csv_nodes_file=csv_nodes_file,
                                         csv_edges_file=csv_edges_file)
            return

        if start_neo4j:
            print(f'Creating Neo4j docker container named {self.graph_db_host}...')
            self.init_graph_db_container()
        else:
            print(f'Looking for existing Neo4j instance..')
        self.wait_for_container_initialization()

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
        print(f'Loading data from {nodes_input_file} and {edges_input_file} into {self.graph_db_uri}...')
        t = Transformer(stream=True)
        t.transform(input_args, output_args)

    def convert_kgx_to_csv(self,
                           nodes_file: str,
                           edges_file: str):
        '''
        node_properties = set()
        for node in quick_jsonl_file_iterator(nodes_file):
            for key, value in node.items():
                node_properties.add(key)
        '''
        nodes_csv_file_name = nodes_file + '.csv'
        with open(nodes_csv_file_name, 'w', newline='') as nodes_csv_file:
            nodes_writer = csv.writer(nodes_csv_file)
            nodes_writer.writerow([':ID', 'name', ':LABEL'])
            for node in quick_jsonl_file_iterator(nodes_file):
                csv_node = [node['id'], node['name'], '|'.join(node['category'])]
                nodes_writer.writerow(csv_node)

        '''
        edge_properties = set()
        for edge in quick_jsonl_file_iterator(edges_file):
            for key, value in edge.items():
                edge_properties.add(key)
        '''
        edges_csv_file_name = edges_file + '.csv'
        with open(edges_csv_file_name, 'w', newline='') as edges_csv_file:
            edges_writer = csv.writer(edges_csv_file)
            edges_writer.writerow([':START_ID', ':TYPE', ':END_ID'])
            for edge in quick_jsonl_file_iterator(edges_file):
                csv_edge = [edge[SUBJECT_ID], edge[PREDICATE], edge[OBJECT_ID]]
                edges_writer.writerow(csv_edge)

        return nodes_csv_file_name, edges_csv_file_name


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Load edges and nodes into Neo4j')
    parser.add_argument('graph_id', help='ID of the graph', default='default')
    parser.add_argument('nodes', help='file with nodes in jsonl format')
    parser.add_argument('edges', help='file with edges in jsonl format')
    # these are generated automatically for now
    parser.add_argument('--neo4j-host', help='Host address for Neo4j', default='localhost')
    parser.add_argument('--neo4j-http-port', help='neo4j http port', default=7474)
    parser.add_argument('--neo4j-https-port', help='neo4j https port', default=7473)
    parser.add_argument('--neo4j-bolt-port', help='neo4j https port', default=7687)
    parser.add_argument('--username', help='username', default='neo4j')
    parser.add_argument('--password', help='password', default='default')
    parser.add_argument('--start-neo4j', help='starts neo4j as a docker container', action="store_true")
    parser.add_argument('--use-docker-network', help='use a local docker network', action="store_true")
    args = parser.parse_args()

    graph_db_tools = GraphDBTools(
        graph_id=args.graph_id,
        graph_db_password=args.password,
        http_port=args.neo4j_http_port,
        https_port=args.neo4j_https_port,
        bolt_port=args.neo4j_bolt_port,
        neo4j_host=args.neo4j_host,
        use_docker_network=args.use_docker_network
    )
    graph_db_tools.load_graph(args.nodes,
                              args.edges,
                              start_neo4j=args.start_neo4j,
                              use_csv=True)
    """
    
    graph_db_tools = GraphDBTools("Example_Graph_ID")
    nodes_file = '/Example/nodes.jsonl'
    edges_file = '/Example/edges.jsonl'
    graph_db_tools.load_graph(nodes_file,
                              edges_file)
    """
