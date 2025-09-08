import time
import os
import neo4j
import subprocess
import orion.kgx_file_converter as kgx_file_converter
from orion.biolink_constants import NAMED_THING
from orion.utils import LoggingUtil


class Neo4jTools:

    def __init__(self,
                 neo4j_host: str = '0.0.0.0',
                 http_port: int = 7474,
                 https_port: int = 7473,
                 bolt_port: int = 7687,
                 password: str = None):
        self.host = neo4j_host
        self.http_port = http_port
        self.https_port = https_port
        self.bolt_port = bolt_port
        self.password = password if password else os.environ.get('ORION_NEO4J_PASSWORD', 'orion-password')
        self.graph_db_uri = f'bolt://{neo4j_host}:{bolt_port}'
        self.graph_db_auth = ("neo4j", self.password)
        self.neo4j_driver = neo4j.GraphDatabase.driver(self.graph_db_uri, auth=self.graph_db_auth)
        self.logger = LoggingUtil.init_logging("ORION.Common.neo4j_tools",
                                               line_format='medium',
                                               log_file_path=os.getenv('ORION_LOGS'))

    def import_csv_files(self,
                         graph_directory: str,
                         csv_nodes_filename: str = None,
                         csv_edges_filename: str = None):

        password_exit_code = self.set_initial_password()
        if password_exit_code != 0:
            return password_exit_code

        self.logger.info(f'Importing csv files to neo4j...')
        neo4j_import_cmd = ['neo4j-admin', 'database', 'import', 'full',
                            f'--nodes={csv_nodes_filename}',
                            f'--relationships={csv_edges_filename}',
                            '--delimiter=TAB',
                            '--array-delimiter=U+001F',
                            '--overwrite-destination=true']
        import_results: subprocess.CompletedProcess = subprocess.run(neo4j_import_cmd,
                                                                     cwd=graph_directory,
                                                                     capture_output=True)
        self.logger.info(import_results.stdout)
        import_results_return_code = import_results.returncode
        if import_results_return_code != 0:
            error_message = f'Neo4j import subprocess error (ExitCode {import_results_return_code}): ' \
                            f'{import_results.stderr.decode("UTF-8")}'
            self.logger.error(error_message)
        return import_results_return_code

    def load_backup_dump(self,
                         dump_file_path: str = None):
        password_exit_code = self.set_initial_password()
        if password_exit_code != 0:
            return password_exit_code

        self.logger.info(f'Loading a neo4j backup dump {dump_file_path}...')
        neo4j_load_cmd = ['neo4j-admin', 'database', 'load', f'--from-path={dump_file_path}', '--overwrite-destination=true', 'neo4j']
        load_results: subprocess.CompletedProcess = subprocess.run(neo4j_load_cmd,
                                                                   capture_output=True)
        self.logger.info(load_results.stdout)
        load_results_return_code = load_results.returncode
        if load_results_return_code != 0:
            error_message = f'Neo4j load subprocess error (ExitCode {load_results_return_code}): ' \
                            f'{load_results.stderr.decode("UTF-8")}'
            self.logger.error(error_message)
        return load_results_return_code

    def migrate_dump_to_neo4j_5(self):
        self.logger.info(f'Migrating db dump to neo4j 5...')
        neo4j_migrate_cmd = ['neo4j-admin', 'database', 'migrate', '--force-btree-indexes-to-range', 'neo4j']
        migrate_results: subprocess.CompletedProcess = subprocess.run(neo4j_migrate_cmd,
                                                                      capture_output=True)
        self.logger.info(migrate_results.stdout)
        results_return_code = migrate_results.returncode
        if results_return_code != 0:
            error_message = f'Neo4j migrate subprocess error (ExitCode {results_return_code}): ' \
                            f'{migrate_results.stderr.decode("UTF-8")}'
            self.logger.error(error_message)
        return results_return_code

    def create_backup_dump(self,
                           dump_directory: str = None):
        self.logger.info(f'Creating a backup dump of the neo4j...')
        neo4j_dump_cmd = ['neo4j-admin', 'database', 'dump', 'neo4j', f'--to-path={dump_directory}']
        dump_results: subprocess.CompletedProcess = subprocess.run(neo4j_dump_cmd,
                                                                   capture_output=True)
        self.logger.info(dump_results.stdout)
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
                                                                    capture_output=True)
        self.logger.info(neo4j_results.stdout)
        neo4j_results_return_code = neo4j_results.returncode
        if neo4j_results_return_code != 0:
            error_message = f'Neo4j {command} subprocess error (ExitCode {neo4j_results_return_code}): ' \
                            f'{neo4j_results.stderr.decode("UTF-8")}'
            self.logger.error(error_message)
        return neo4j_results_return_code

    def set_initial_password(self):
        self.logger.info('Setting initial password for Neo4j...')
        neo4j_cmd = ['neo4j-admin', 'dbms', 'set-initial-password', self.password]
        neo4j_results: subprocess.CompletedProcess = subprocess.run(neo4j_cmd,
                                                                    capture_output=True)
        self.logger.info(neo4j_results.stdout)
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

                # node name index
                node_name_index_cypher = f'CREATE INDEX node_name_index FOR (n:`{NAMED_THING}`) ON (n.name)'
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
                    node_name_index_cypher = f'CREATE CONSTRAINT {node_label_index} FOR (n:`{node_label}`) ' \
                                             f'REQUIRE n.id IS UNIQUE'
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

    def wait_for_neo4j_initialization(self, counter: int = 1, max_retries: int = 10):
        try:
            with self.neo4j_driver.session() as session:
                session.run("return 1")
                self.logger.info(f'Neo4j ready at {self.host}:{self.http_port}, {self.graph_db_uri}.')
                return 0
        except neo4j.exceptions.AuthError as e:
            raise e
        except Exception as e:
            if counter > max_retries:
                self.logger.error(f'Waited too long for Neo4j initialization... giving up..')
                return 1
            self.logger.info(f'Waiting for Neo4j container to finish initialization... {repr(e)}')
            time.sleep(10)
            return self.wait_for_neo4j_initialization(counter + 1)

    def close(self):
        self.neo4j_driver.close()


def create_neo4j_dump(nodes_filepath: str,
                      edges_filepath: str,
                      output_directory: str,
                      graph_id: str = 'graph',
                      graph_version: str = '',
                      node_property_ignore_list: set = None,
                      edge_property_ignore_list: set = None,
                      logger=None):
    nodes_csv_filename = 'nodes.temp_csv'
    edges_csv_filename = 'edges.temp_csv'
    csv_nodes_file_path = os.path.join(output_directory, nodes_csv_filename)
    csv_edges_file_path = os.path.join(output_directory, edges_csv_filename)
    if os.path.exists(csv_nodes_file_path) and os.path.exists(csv_edges_file_path):
        if logger:
            logger.info(f'CSV files were already created for {graph_id}({graph_version})')
    else:
        if logger:
            logger.info(f'Creating CSV files for {graph_id}({graph_version})...')
        kgx_file_converter.convert_jsonl_to_neo4j_csv(nodes_input_file=nodes_filepath,
                                                      edges_input_file=edges_filepath,
                                                      nodes_output_file=csv_nodes_file_path,
                                                      edges_output_file=csv_edges_file_path,
                                                      node_property_ignore_list=node_property_ignore_list,
                                                      edge_property_ignore_list=edge_property_ignore_list)
        if logger:
            logger.info(f'CSV files created for {graph_id}({graph_version})...')

    # would like to do the following, but apparently you can't specify a custom name for the dump now
    # graph_dump_name = f'graph_{graph_version}.neo4j5.db.dump' if graph_version else 'graph.neo4j5.db.dump'
    # graph_dump_file_path = os.path.join(output_directory, graph_dump_name)
    graph_dump_name = 'neo4j.dump'
    graph_dump_file_path = os.path.join(output_directory, graph_dump_name)
    if os.path.exists(graph_dump_file_path):
        if logger:
            logger.info(f'Neo4j dump already exists for {graph_id}({graph_version})')
        return True

    neo4j_access = Neo4jTools()
    try:
        import_exit_code = neo4j_access.import_csv_files(graph_directory=output_directory,
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

        dump_exit_code = neo4j_access.create_backup_dump(output_directory)
        if dump_exit_code != 0:
            return False

    finally:
        neo4j_access.close()

    if logger:
        logger.info(f'Success! Neo4j dump created with indexes for {graph_id}({graph_version})')
    return True
