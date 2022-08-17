import argparse
import time
import docker
import os
import neo4j
import subprocess
from Common.node_types import NAMED_THING
from Common.utils import LoggingUtil


class Neo4jTools:

    def __init__(self,
                 graph_id: str,
                 graph_version: str,
                 neo4j_host: str = '0.0.0.0',
                 http_port: int = 7474,
                 https_port: int = 7473,
                 bolt_port: int = 7687):
        self.graph_id = graph_id
        self.graph_version = graph_version
        self.host = neo4j_host
        self.http_port = http_port
        self.https_port = https_port
        self.bolt_port = bolt_port
        self.graph_db_uri = f'bolt://{neo4j_host}:{bolt_port}'
        self.graph_db_auth = ("neo4j", os.environ['DATA_SERVICES_NEO4J_PASSWORD'])
        self.neo4j_driver = neo4j.GraphDatabase.driver(self.graph_db_uri, auth=self.graph_db_auth)
        self.logger = LoggingUtil.init_logging("Data_services.Common.neo4j_tools",
                                               line_format='medium',
                                               log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def import_csv_files(self,
                         graph_directory: str,
                         csv_nodes_filename: str = None,
                         csv_edges_filename: str = None):
        self.logger.info(f'Importing csv files to neo4j...')
        neo4j_import_cmd = ["neo4j-admin", "import", f"--nodes={csv_nodes_filename}",
                            f"--relationships={csv_edges_filename}",
                            f'--delimiter=TAB',
                            '--array-delimiter=U+001F',
                            '--ignore-empty-strings=true']
        import_results: subprocess.CompletedProcess = subprocess.run(neo4j_import_cmd,
                                                                     cwd=graph_directory,
                                                                     stderr=subprocess.PIPE)
        import_results_return_code = import_results.returncode
        if import_results_return_code != 0:
            error_message = f'Neo4j import subprocess error (ExitCode {import_results_return_code}): ' \
                            f'{import_results.stderr.decode("UTF-8")}'
            self.logger.error(error_message)
        return import_results_return_code

    def create_backup_dump(self,
                           dump_file_path: str = None):
        self.logger.info(f'Creating a backup dump of the neo4j...')
        neo4j_dump_cmd = ['neo4j-admin', 'dump', f'--to={dump_file_path}']
        dump_results: subprocess.CompletedProcess = subprocess.run(neo4j_dump_cmd,
                                                                   stderr=subprocess.PIPE)
        dump_results_return_code = dump_results.returncode
        if dump_results_return_code != 0:
            error_message = f'Neo4j dump subprocess error (ExitCode {dump_results_return_code}): ' \
                            f'{dump_results.stderr.decode("UTF-8")}'
            self.logger.error(error_message)
        return dump_results_return_code

    def start_neo4j(self):
        self.logger.info(f'Starting Neo4j DB...')
        return self.__issue_neo4j_command('start')

    def stop_neo4j(self):
        self.logger.info(f'Stopping Neo4j DB...')
        return self.__issue_neo4j_command('stop')

    def __issue_neo4j_command(self, command: str):
        neo4j_cmd = ['neo4j', f'{command}']
        neo4j_results: subprocess.CompletedProcess = subprocess.run(neo4j_cmd,
                                                                    stderr=subprocess.PIPE)
        neo4j_results_return_code = neo4j_results.returncode
        if neo4j_results_return_code != 0:
            error_message = f'Neo4j {command} subprocess error (ExitCode {neo4j_results_return_code}): ' \
                            f'{neo4j_results.stderr.decode("UTF-8")}'
            self.logger.error(error_message)
        return neo4j_results_return_code

    def set_initial_password(self):
        neo4j_cmd = ['neo4j-admin', 'set-initial-password', os.environ['DATA_SERVICES_NEO4J_PASSWORD']]
        neo4j_results: subprocess.CompletedProcess = subprocess.run(neo4j_cmd,
                                                                    stderr=subprocess.PIPE)
        neo4j_results_return_code = neo4j_results.returncode
        if neo4j_results_return_code != 0:
            error_message = f'Neo4j {neo4j_cmd} subprocess error (ExitCode {neo4j_results_return_code}): ' \
                            f'{neo4j_results.stderr.decode("UTF-8")}'
            self.logger.error(error_message)
        return neo4j_results_return_code

    def add_db_indexes(self):
        self.logger.info('Adding indexes to neo4j db...')
        indexes_added = 0
        index_names = []
        try:
            with self.neo4j_driver.session() as session:

                # edge id index
                cypher_result = list(session.run("CALL db.relationshipTypes()"))
                rel_types = [result['relationshipType'] for result in cypher_result]
                self.logger.info(f'Adding edge indexes for rel types: {rel_types}')
                for i, rel_type in enumerate(rel_types):
                    index_name = f'edge_id_{i}'
                    edge_id_index_cypher = f'CREATE INDEX {index_name} FOR ()-[r:`{rel_type}`]-() ON (r.id)'
                    session.run(edge_id_index_cypher).consume()
                    indexes_added += 1
                    index_names.append(index_name)

                # node name index
                node_name_index_cypher = f'CREATE INDEX node_name_index FOR (n:`{NAMED_THING}`) on (n.name)'
                self.logger.info(f'Adding node name index on {NAMED_THING}.name')
                session.run(node_name_index_cypher).consume()
                indexes_added += 1
                index_names.append("node_name_index")

                # node id indexes
                cypher_result = list(session.run("CALL db.labels()"))
                node_labels = [result['label'] for result in cypher_result]
                node_labels.remove(NAMED_THING)
                self.logger.info(f'Adding node id indexes for node labels: {node_labels}')
                for node_label in node_labels:
                    node_label_index = f'node_id_{node_label.replace(":", "_")}'
                    node_name_index_cypher = f'CREATE CONSTRAINT {node_label_index} ON (n:`{node_label}`) ' \
                                             f'ASSERT n.id IS UNIQUE'
                    session.run(node_name_index_cypher).consume()
                    indexes_added += 1
                    index_names.append(node_label_index)

                # wait for indexes
                self.logger.info(f'Waiting for indexes to be created...')
                await_indexes_cypher = f'CALL db.awaitIndexes()'
                session.run(await_indexes_cypher).consume()

                self.logger.info(f'Waiting for indexes to be created...')
                retrieve_indexes_cypher = f'SHOW INDEXES'
                retrieve_indexes_results = session.run(retrieve_indexes_cypher)
                index_count = 0
                existing_indexes = 0
                for result in retrieve_indexes_results:
                    if result['name'] not in index_names:
                        existing_indexes += 1
                    else:
                        if result['state'] == 'ONLINE':
                            index_count += 1
                        else:
                            self.logger.error(f"Oh No. Index {result['name']} has state {result['state']} "
                                              f"but should be online.")
                if indexes_added != existing_indexes + index_count:
                    self.logger.error(f"Oh No. Tried to add {indexes_added} indexes but only {index_count} were added.")
                    return 1

        except neo4j.exceptions.ClientError as e:
            self.logger.error(f"Adding indexes failed: {e}")
            return 1

        self.logger.info(f"Adding indexes successful. {indexes_added} indexes created.")
        return 0

    def wait_for_neo4j_initialization(self, counter: int = 1):
        try:
            with self.neo4j_driver.session() as session:
                session.run("return 1")
                self.logger.info(f'Neo4j ready at {self.host}:{self.http_port}, {self.graph_db_uri}.')
                return 0
        except neo4j.exceptions.AuthError as e:
            raise e
        except Exception as e:
            if counter > 8:
                self.logger.error(f'Waited too long for Neo4j initialization... giving up..')
                return 1
            self.logger.info(f'Waiting for Neo4j container to finish initialization... {repr(e)}')
            time.sleep(10)
            return self.wait_for_neo4j_initialization(counter + 1)

    def close(self):
        self.neo4j_driver.close()


# this version only works if you can access the docker.sock - soon to be removed
class GraphDBTools:

    def __init__(self,
                 graph_id: str = 'default',
                 graph_db_password: str = 'default',
                 neo4j_host: str = None,
                 http_port: int = 7474,
                 https_port: int = 7473,
                 bolt_port: int = 7687,
                 available_gb_memory: int = 8,
                 unix_user_id: str = '0:0'):

        self.graph_id = graph_id
        self.graph_http_port = http_port
        self.graph_https_port = https_port
        self.graph_bolt_port = bolt_port
        self.graph_db_password = graph_db_password
        self.available_gb_memory = available_gb_memory
        self.unix_user_id = unix_user_id
        if neo4j_host is None:
            self.container_name = f'data_services_graph_db_{self.graph_id}'
            self.neo4j_ports = {
                self.graph_http_port: self.graph_http_port,
                self.graph_https_port: self.graph_https_port,
                self.graph_bolt_port: self.graph_bolt_port
            }
            heap_size = self.available_gb_memory * .75
            pagecache_size = self.available_gb_memory * .25
            self.neo4j_env_vars = [
                f'NEO4J_AUTH=neo4j/{self.graph_db_password}',
                f'NEO4J_dbms_memory_heap_max__size={heap_size}G',
                f'NEO4J_dbms_memory_heap_initial__size={heap_size}G',
                f'NEO4J_dbms_memory_pagecache_size={pagecache_size}G',
                'NEO4J_dbms_default__listen__address=0.0.0.0'  # ,
                # 'SECURE_FILE_PERMISSIONS=no'
            ]
            self.graph_db_uri = f'bolt://{self.container_name}:{bolt_port}'
            self.docker_client: docker.DockerClient = docker.from_env(timeout=80)
            self.neo4j_volumes = None

        else:
            self.container_name = None
            self.neo4j_ports = None
            self.neo4j_env_vars = None
            self.docker_client = None
            self.graph_db_uri = f'bolt://{neo4j_host}:{bolt_port}'

    def neo4j_import_csv_files(self,
                               csv_nodes_file: str = None,
                               csv_edges_file: str = None,
                               output_dir: str = None):
        self.check_for_existing_container()
        self.establish_neo4j_volumes(output_dir=output_dir)

        print(f'Creating container and importing csv files to neo4j...')
        neo4j_import_cmd = f'neo4j-admin import --nodes={csv_nodes_file} --relationships={csv_edges_file} ' \
                           f'--delimiter="\t" --array-delimiter="U+001F" --ignore-empty-strings=true'

        import_container = self.docker_client.containers.run("neo4j:4.3",
                                                             name=self.container_name,
                                                             command=neo4j_import_cmd,
                                                             environment=self.neo4j_env_vars,
                                                             ports=self.neo4j_ports,
                                                             network='data_services_network',
                                                             volumes=self.neo4j_volumes,
                                                             user=self.unix_user_id,
                                                             auto_remove=True,
                                                             detach=True)
        import_logs = import_container.attach(stdout=True, stderr=True, stream=True, logs=True)
        for log_line in import_logs:
            print(log_line.decode("utf-8").strip())

        # wait for the container to finish importing and exit
        result = import_container.wait(condition='removed')
        exit_code = result['StatusCode']
        return exit_code

    def neo4j_create_backup_dump(self,
                                 output_dir: str = None):
        self.check_for_existing_container()
        self.establish_neo4j_volumes(output_dir=output_dir)

        print(f'Creating a backup dump of the neo4j...')
        dump_file_path = os.path.join(output_dir, 'graph.db.dump')
        neo4j_dump_cmd = f'neo4j-admin dump --to={dump_file_path}'
        dump_container = self.docker_client.containers.run("neo4j:4.3",
                                                           name=self.container_name,
                                                           command=neo4j_dump_cmd,
                                                           environment=self.neo4j_env_vars,
                                                           ports=self.neo4j_ports,
                                                           network='data_services_network',
                                                           volumes=self.neo4j_volumes,
                                                           user=self.unix_user_id,
                                                           auto_remove=True,
                                                           detach=True)
        dump_logs = dump_container.attach(stdout=True, stderr=True, stream=True, logs=True)
        for log_line in dump_logs:
            print(log_line.decode("utf-8").strip())
        print(f'Dump complete. Waiting for container to exit...')
        # wait for the container to finish importing and exit
        result = dump_container.wait(condition='removed')
        exit_code = result['StatusCode']
        return exit_code

    def establish_neo4j_volumes(self,
                                output_dir: str = None):
        neo4j_data_dir_relative_path = os.path.join(output_dir, 'neo4j_data')
        if not os.path.exists(neo4j_data_dir_relative_path):
            os.mkdir(neo4j_data_dir_relative_path)
        if neo4j_data_dir_relative_path.startswith('/Data_services_graphs'):
            # If the path starts with /Data_services_graphs we are probably in a docker container.
            # The following will replace the docker relative directory path with the real one from the host,
            # so that we may mount the volume in the new docker container.
            neo4j_data_dir_real_path = os.path.join(
                f'{os.environ["HOST_GRAPHS_DIR"]}',
                f'{neo4j_data_dir_relative_path.split("/Data_services_graphs/", 1)[1]}'
            )
        else:
            neo4j_data_dir_real_path = neo4j_data_dir_relative_path

        self.neo4j_volumes = [
            f'{neo4j_data_dir_real_path}:/data',  # neo4j data directory - necessary for persistence after import
            f'{os.environ["HOST_GRAPHS_DIR"]}:/Data_services_graphs'
        ]

    def neo4j_add_db_indexes(self,
                             output_dir: str = None):
        print('Adding indexes to neo4j DB...')
        deploy_success = self.deploy_neo4j(output_dir=output_dir)
        if not deploy_success:
            print(f'Error standing up Neo4j, could not add indexes.')
            return 1
        try:
            with self.get_neo4j_driver_session() as session:

                # edge id index
                cypher_result = list(session.run("CALL db.relationshipTypes()"))
                rel_types = [result['relationshipType'] for result in cypher_result]
                print(f'Adding edge indexes for rel types: {rel_types}')
                for i, rel_type in enumerate(rel_types):
                    edge_id_index_cypher = f'CREATE INDEX edge_id_{i} FOR ()-[r:`{rel_type}`]-() ON (r.id)'
                    session.run(edge_id_index_cypher)

                # node name index
                node_name_index_cypher = f'CREATE INDEX node_name_index FOR (n:`{NAMED_THING}`) on (n.name)'
                print(f'Adding node name index on {NAMED_THING}.name')
                session.run(node_name_index_cypher)

                # node id indexes
                cypher_result = list(session.run("CALL db.labels()"))
                node_labels = [result['label'] for result in cypher_result]
                node_labels.remove(NAMED_THING)
                print(f'Adding node id indexes for node labels: {node_labels}')
                for node_label in node_labels:
                    node_index_id = f'node_id_{node_label.replace(":", "_")}'
                    node_name_index_cypher = f'CREATE CONSTRAINT {node_index_id} ON (n:`{node_label}`) ' \
                                             f'ASSERT n.id IS UNIQUE'
                    session.run(node_name_index_cypher)

            self.get_container().stop()

            # wait for the container to finish importing and exit
            result = self.get_container().wait()
            exit_code = result['StatusCode']

        except neo4j.exceptions.ClientError as e:
            print(e)
            self.get_container().stop()
            exit_code = 1

        if exit_code != 0:
            print("Adding indexes failed. Aborting.")
        else:
            print("Adding indexes successful.")
        return exit_code

    def wait_for_container_to_exit(self):
        container_exited = False
        while not container_exited:
            container = self.get_container()
            if container:
                if container.status == 'exited':
                    print(f'Container exited.. Removing container...')
                    container.remove()
                    container_exited = True
                else:
                    print(f'Waiting... container {container.name} has status {container.status}')
                    time.sleep(10)
            else:
                container_exited = True
                print(f'Container not found.. Possibly auto-removed..')

    def deploy_neo4j(self,
                     output_dir: str,
                     include_apoc: bool = False):

        self.check_for_existing_container()
        self.establish_neo4j_volumes(output_dir=output_dir)
        apoc_string = 'NEO4JLABS_PLUGINS=["apoc", "graph-data-science"]'
        if include_apoc:
            self.neo4j_env_vars.append(apoc_string)

        print(f'Starting up Neo4j...')
        self.docker_client.containers.run("neo4j:4.3",
                                          name=self.container_name,
                                          environment=self.neo4j_env_vars,
                                          ports=self.neo4j_ports,
                                          network='data_services_network',
                                          volumes=self.neo4j_volumes,
                                          user=self.unix_user_id,
                                          detach=True)
        deploy_success = self.wait_for_neo4j_initialization()
        if include_apoc:
            self.neo4j_env_vars.remove(apoc_string)
        return deploy_success

    def get_container(self):
        try:
            return self.docker_client.containers.get(self.container_name)
        except docker.errors.NotFound:
            return None

    def check_for_existing_container(self):
        container = self.get_container()
        if container:
            if container.status == 'exited':
                container.remove()
                print(f'Removed previous container for {self.container_name}.')
            else:
                raise Exception(f'Error: Graph DB Container named {self.container_name} already exists!')

    def wait_for_neo4j_initialization(self, counter: int=1):
        try:
            with self.get_neo4j_driver_session() as session:
                session.run("return 1")
                print(f'Neo4j ready at {self.graph_db_uri}.')
                return True
        except Exception as e:
            if counter > 8:
                print(f'Waited too long for Neo4j initialization... giving up..')
                return False
            print(f'Waiting for Neo4j container to finish initialization...')
            time.sleep(10)
            return self.wait_for_neo4j_initialization(counter + 1)

    def get_neo4j_driver_session(self):
        http_driver: neo4j.Neo4jDriver = neo4j.GraphDatabase.driver(
            self.graph_db_uri,
            auth=('neo4j', self.graph_db_password)
        )
        return http_driver.session()

    """
    # neo4j_load_using_kgx is deprecated and probably broken - saving for possible use later on
    def __neo4j_load_using_kgx(self,
                             nodes_input_file: str,
                             edges_input_file: str,
                             input_file_format: str = 'jsonl'):

        output_directory = nodes_input_file.rsplit('/', 1)[0]
        self.establish_neo4j_volumes(output_dir=output_directory)
        if self.docker_client:
            self.docker_client.containers.run("neo4j:4.3",
                                         name=self.container_name,
                                         environment=self.neo4j_env_vars,
                                         ports=self.ports,
                                         auto_remove=False,
                                         network='data_services_network',
                                         user=self.unix_user_id,
                                         detach=True)

        self.wait_for_neo4j_initialization()

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
    """


def run_neo4j_pipeline(args):
    nodes_file = args.nodes
    edges_file = args.edges
    output_directory = nodes_file.rsplit('/', 1)[0]

    graph_db_tools = GraphDBTools(
        graph_id=args.graph_id,
        graph_db_password=args.password,
        http_port=args.neo4j_http_port,
        https_port=args.neo4j_https_port,
        bolt_port=args.neo4j_bolt_port,
        neo4j_host=args.neo4j_host,
        available_gb_memory=int(args.memory),
        unix_user_id=args.uid
    )

    exit_code = graph_db_tools.neo4j_import_csv_files(csv_nodes_file=nodes_file,
                                                      csv_edges_file=edges_file,
                                                      output_dir=output_directory)
    if exit_code != 0:
        return

    exit_code = graph_db_tools.neo4j_add_db_indexes(output_dir=output_directory)
    if exit_code != 0:
        return

    exit_code = graph_db_tools.neo4j_create_backup_dump(output_dir=output_directory)
    if exit_code != 0:
        return

    graph_db_tools.deploy_neo4j(output_dir=output_directory, include_apoc=True)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Load edges and nodes into Neo4j')
    parser.add_argument('graph_id', help='ID of the graph', default='default')
    parser.add_argument('nodes', help='file with nodes in jsonl format')
    parser.add_argument('edges', help='file with edges in jsonl format')
    # these are generated automatically for now
    parser.add_argument('--neo4j-host', help='Host address for Neo4j', default=None)
    parser.add_argument('--neo4j-http-port', help='neo4j http port', default=7474)
    parser.add_argument('--neo4j-https-port', help='neo4j https port', default=7473)
    parser.add_argument('--neo4j-bolt-port', help='neo4j https port', default=7687)
    # parser.add_argument('--username', help='username', default='neo4j')
    parser.add_argument('--password', help='neo4j password', default='default')
    parser.add_argument('--memory', help='available RAM', default=8)
    parser.add_argument('--uid', help='unix user id', default='0:0')
    args = parser.parse_args()

    run_neo4j_pipeline(args)


