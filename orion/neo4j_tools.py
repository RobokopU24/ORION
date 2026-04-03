import time
import os
import neo4j
import subprocess
import orion.kgx_file_converter as kgx_file_converter
from orion.biolink_constants import NAMED_THING
from orion.logging import get_orion_logger


logger = get_orion_logger("orion.neo4j_tools")


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

    @staticmethod
    def __run_command(cmd: list, log_message: str, **kwargs) -> int:
        logger.info(f'{log_message}...')
        result = subprocess.run(cmd, capture_output=True, **kwargs)
        stdout = result.stdout.decode("UTF-8").strip()
        if stdout:
            logger.info(stdout)
        if result.returncode != 0:
            stderr = result.stderr.decode("UTF-8").strip()
            logger.error(f'{log_message} failed (ExitCode {result.returncode}): {stderr}')
        return result.returncode

    def import_csv_files(self,
                         graph_directory: str,
                         csv_nodes_filename: str = None,
                         csv_edges_filename: str = None):

        password_exit_code = self.set_initial_password()
        if password_exit_code != 0:
            return password_exit_code

        return self.__run_command(
            ['neo4j-admin', 'database', 'import', 'full',
             f'--nodes={csv_nodes_filename}',
             f'--relationships={csv_edges_filename}',
             '--delimiter=TAB',
             '--array-delimiter=U+001F',
             '--overwrite-destination=true'],
            log_message='Importing csv files to neo4j',
            cwd=graph_directory)

    def load_backup_dump(self,
                         dump_file_path: str = None):
        password_exit_code = self.set_initial_password()
        if password_exit_code != 0:
            return password_exit_code

        return self.__run_command(
            ['neo4j-admin', 'database', 'load',
             f'--from-path={dump_file_path}',
             '--overwrite-destination=true', 'neo4j'],
            log_message=f'Loading neo4j backup dump {dump_file_path}')

    def migrate_dump_to_neo4j_5(self):
        return self.__run_command(
            ['neo4j-admin', 'database', 'migrate',
             '--force-btree-indexes-to-range', 'neo4j'],
            log_message='Migrating db dump to neo4j 5')

    def create_backup_dump(self,
                           dump_directory: str = None):
        return self.__run_command(
            ['neo4j-admin', 'database', 'dump', 'neo4j',
             f'--to-path={dump_directory}'],
            log_message='Creating a backup dump of neo4j')

    def start_neo4j(self):
        return self.__run_command(
            ['neo4j', 'start', '--verbose'],
            log_message='Starting Neo4j DB')

    def stop_neo4j(self):
        logger.info('Stopping Neo4j DB...')
        # We would prefer to stop the neo4j using "neo4j stop" it's not working with the current docker image.
        # Instead, we can find and kill the neo4j process using the pid.
        #
        # We could attempt "stop neo4j" and only kill the process as a fallback as follows, but it takes 2 whole minutes
        # to time out. Seeing as that always happens with the current docker image that's slow and not helpful.
        # exit_code = self.__run_command(['neo4j', 'stop', '--verbose'], log_message='Stopping Neo4j DB')
        # if exit_code != 0:
        #     logger.warning(f'neo4j stop failed (exit code {exit_code}), falling back to process kill...')
        exit_code = self.__kill_neo4j_process()
        return exit_code

    def __kill_neo4j_process(self):
        # See notes in the stop_neo4j() function. Using a custom docker image broke "stop neo4j", probably due to a
        # mismatch in pids occurring the way we're running neo4j commands with subprocesses. This is pretty hacky and
        # not guaranteed to work on setups that don't use the docker container, but it does seem to work so far.
        try:
            # Try the PID file first
            pid_file = os.path.join(os.environ.get('NEO4J_HOME', '/var/lib/neo4j'), 'run', 'neo4j.pid')
            pid = None
            if os.path.exists(pid_file):
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())

            if pid:
                logger.info(f'Sending SIGTERM to Neo4j process (PID {pid})...')
                os.kill(pid, 15)  # SIGTERM
            else:
                logger.error(f'Neo4j PID was not found and Neo4j could not be stopped.')
                return 1

            # Wait for Neo4j to shut down gracefully
            for i in range(30):
                check = subprocess.run(['pgrep', '-f', 'org.neo4j'], capture_output=True)
                if check.returncode != 0:  # Process gone
                    logger.info('Neo4j process stopped successfully.')
                    return 0
                time.sleep(1)

            logger.error('Neo4j process did not stop within 30 seconds.')
            return 1
        except Exception as e:
            logger.error(f'Error killing Neo4j process: {e}')
            return 1

    def set_initial_password(self):
        return self.__run_command(
            ['neo4j-admin', 'dbms', 'set-initial-password', self.password],
            log_message='Setting initial password for Neo4j')

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
        logger.info('Adding indexes to neo4j db...')
        indexes_added = 0
        index_names = []
        try:
            with self.neo4j_driver.session() as session:

                # node name index
                node_name_index_cypher = f'CREATE INDEX node_name_index FOR (n:`{NAMED_THING}`) ON (n.name)'
                logger.info(f'Adding node name index on {NAMED_THING}.name')
                session.run(node_name_index_cypher).consume()
                indexes_added += 1
                index_names.append("node_name_index")

                # node id indexes
                cypher_result = list(session.run("CALL db.labels()"))
                node_labels = [result['label'] for result in cypher_result]
                logger.info(f'Adding node id indexes for node labels: {node_labels}')
                for node_label in node_labels:
                    node_label_index = f'node_id_{node_label.replace(":", "_")}'
                    node_name_index_cypher = f'CREATE CONSTRAINT {node_label_index} FOR (n:`{node_label}`) ' \
                                             f'REQUIRE n.id IS UNIQUE'
                    session.run(node_name_index_cypher).consume()
                    indexes_added += 1
                    index_names.append(node_label_index)

                # wait for indexes
                logger.info(f'Waiting for indexes to be created...')
                await_indexes_cypher = f'CALL db.awaitIndexes()'
                session.run(await_indexes_cypher).consume()

                logger.info(f'Confirming index creation...')
                retrieve_indexes_cypher = f'SHOW INDEXES'
                retrieve_indexes_results = session.run(retrieve_indexes_cypher)
                confirmed_index_count = 0
                for result in retrieve_indexes_results:
                    if result['name'] in index_names:
                        if result['state'] == 'ONLINE':
                            confirmed_index_count += 1
                        else:
                            logger.error(f"Oh No. Index {result['name']} has state {result['state']} "
                                              f"but should be online.")
                if indexes_added != confirmed_index_count:
                    logger.error(f"Oh No. Tried to add {indexes_added} indexes "
                                      f"but only {confirmed_index_count} were added.")
                    return 1

        except neo4j.exceptions.ClientError as e:
            logger.error(f"Adding indexes failed: {e}")
            return 1

        logger.info(f"Adding indexes successful. {indexes_added} indexes created.")
        return 0

    def wait_for_neo4j_initialization(self, counter: int = 1, max_retries: int = 10):
        try:
            with self.neo4j_driver.session() as session:
                session.run("return 1")
                logger.info(f'Neo4j ready at {self.host}:{self.http_port}, {self.graph_db_uri}.')
                return 0
        except neo4j.exceptions.AuthError as e:
            raise e
        except Exception as e:
            if counter > max_retries:
                logger.error(f'Waited too long for Neo4j initialization... giving up..')
                return 1
            logger.info(f'Waiting for Neo4j container to finish initialization... {repr(e)}')
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
                      edge_property_ignore_list: set = None):
    nodes_csv_filename = 'nodes.temp_csv'
    edges_csv_filename = 'edges.temp_csv'
    csv_nodes_file_path = os.path.join(output_directory, nodes_csv_filename)
    csv_edges_file_path = os.path.join(output_directory, edges_csv_filename)
    if os.path.exists(csv_nodes_file_path) and os.path.exists(csv_edges_file_path):
        logger.info(f'CSV files were already created for {graph_id}({graph_version})')
    else:
        logger.info(f'Creating CSV files for {graph_id}({graph_version})...')
        kgx_file_converter.convert_jsonl_to_neo4j_csv(nodes_input_file=nodes_filepath,
                                                      edges_input_file=edges_filepath,
                                                      nodes_output_file=csv_nodes_file_path,
                                                      edges_output_file=csv_edges_file_path,
                                                      node_property_ignore_list=node_property_ignore_list,
                                                      edge_property_ignore_list=edge_property_ignore_list)
        logger.info(f'CSV files created for {graph_id}({graph_version}).')

    # would like to do the following, but apparently you can't specify a custom name for the dump now
    # graph_dump_name = f'graph_{graph_version}.neo4j5.db.dump' if graph_version else 'graph.neo4j5.db.dump'
    # graph_dump_file_path = os.path.join(output_directory, graph_dump_name)
    graph_dump_name = 'neo4j.dump'
    graph_dump_file_path = os.path.join(output_directory, graph_dump_name)
    if os.path.exists(graph_dump_file_path):
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

    # remove the temp csv files we made to do the neo4j import
    os.remove(csv_nodes_file_path)
    os.remove(csv_edges_file_path)
    # remove the import.report neo4j generates, if successful it's typically empty
    import_report_path = os.path.join(output_directory, 'import.report')
    if os.path.exists(import_report_path):
        os.remove(import_report_path)
    logger.info(f'Success! Neo4j dump created with indexes for {graph_id}({graph_version})')
    return True
