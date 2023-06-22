import time
import os
import neo4j
import subprocess
import Common.kgx_file_converter as kgx_file_converter
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

    def create_neo4j_dump(self,
                          graph_id: str,
                          graph_version: str,
                          graph_directory: str,
                          nodes_filename: str = 'nodes.jsonl',
                          edges_filename: str = 'edges.jsonl'):

        graph_nodes_file_path = os.path.join(graph_directory, nodes_filename)
        graph_edges_file_path = os.path.join(graph_directory, edges_filename)
        nodes_csv_filename = 'nodes.temp_csv'
        edges_csv_filename = 'edges.temp_csv'
        csv_nodes_file_path = os.path.join(graph_directory, nodes_csv_filename)
        csv_edges_file_path = os.path.join(graph_directory, edges_csv_filename)
        if os.path.exists(csv_nodes_file_path) and os.path.exists(csv_edges_file_path):
            self.logger.info(f'CSV files were already created for {graph_id}({graph_version})')
        else:
            self.logger.info(f'Creating CSV files for {graph_id}({graph_version})...')
            kgx_file_converter.convert_jsonl_to_neo4j_csv(nodes_input_file=graph_nodes_file_path,
                                                          edges_input_file=graph_edges_file_path,
                                                          nodes_output_file=csv_nodes_file_path,
                                                          edges_output_file=csv_edges_file_path)
            self.logger.info(f'CSV files created for {graph_id}({graph_version})...')

        graph_dump_file_path = os.path.join(graph_directory, f'graph_{graph_version}.db.dump')
        if os.path.exists(graph_dump_file_path):
            self.logger.info(f'Neo4j dump already exists for {graph_id}({graph_version})')
            return True

        neo4j_access = Neo4jTools(graph_id=graph_id, graph_version=graph_version)
        try:
            password_exit_code = neo4j_access.set_initial_password()
            if password_exit_code != 0:
                return False

            import_exit_code = neo4j_access.import_csv_files(graph_directory=graph_directory,
                                                             csv_nodes_filename=nodes_csv_filename,
                                                             csv_edges_filename=edges_csv_filename)
            if import_exit_code != 0:
                return False

            start_exit_code = neo4j_access.start_neo4j()
            if start_exit_code != 0:
                return False

            waiting_exit_code = neo4j_access.wait_for_neo4j_initialization()
            if waiting_exit_code != 0:
                return False

            indexes_exit_code = neo4j_access.add_db_indexes()
            if indexes_exit_code != 0:
                return False

            stop_exit_code = neo4j_access.stop_neo4j()
            if stop_exit_code != 0:
                return False

            dump_exit_code = neo4j_access.create_backup_dump(graph_dump_file_path)
            if dump_exit_code != 0:
                return False

        finally:
            neo4j_access.close()

        self.logger.info(f'Success! Neo4j dump created with indexes for {graph_id}({graph_version})')
        return True

    def import_csv_files(self,
                         graph_directory: str,
                         csv_nodes_filename: str = None,
                         csv_edges_filename: str = None):
        self.logger.info(f'Importing csv files to neo4j...')
        neo4j_import_cmd = ["neo4j-admin", "import", f"--nodes={csv_nodes_filename}",
                            f"--relationships={csv_edges_filename}",
                            '--delimiter=TAB',
                            '--array-delimiter=U+001F',
                            '--force=true']
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

    @staticmethod
    def do_cypher_tx(tx, cypher):
        neo4j_result = tx.run(cypher)
        result = []
        for record in neo4j_result:
            result.append({key: value for key, value in record.items()})
        return result

    def execute_read_cypher_query(self, c_query):
        with self.neo4j_driver.session() as session:
            neo4j_result = session.execute_read(self.do_cypher_tx, c_query)
            return neo4j_result

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

                self.logger.info(f'Confirming index creation...')
                retrieve_indexes_cypher = f'SHOW INDEXES'
                retrieve_indexes_results = session.run(retrieve_indexes_cypher)
                confirmed_index_count = 0
                for result in retrieve_indexes_results:
                    if result['name'] in index_names:
                        if result['state'] == 'ONLINE':
                            confirmed_index_count += 1
                        else:
                            self.logger.error(f"Oh No. Index {result['name']} has state {result['state']} "
                                              f"but should be online.")
                if indexes_added != confirmed_index_count:
                    self.logger.error(f"Oh No. Tried to add {indexes_added} indexes "
                                      f"but only {confirmed_index_count} were added.")
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


