# import docker
# import time
# import psycopg2
# import os
# import mysql.connector


# !!!! READ ME !!!!
# This module is basically obsolete but I'm leaving it here just in case we ever want to use it again.
# It provides an interface for creating and using docker containers to run mysql or postgres databases,
# but it requires the ability for the docker driver to access the docker daemon (usually root access)
"""
class DSContainerError(Exception):
    def __init__(self, error_message: str, actual_error: str = ''):
        self.error_message = error_message
        self.actual_error = actual_error


class DataServicesContainer:

    def __init__(self, container_name: str, logger):

        self.logger = logger
        self.logger.debug(f'Connecting to docker..')
        self.docker_client = docker.from_env(timeout=120)
        self.logger.debug(f'Success connecting to docker..')
        self.container_name = container_name
        self.remove_old_containers()

        # placeholders for overriding
        self.default_image = None
        self.default_ports = None
        self.default_volumes = [f"{os.environ['HOST_STORAGE_DIR']}:/ORION_storage"]
        self.environment_vars = {}

    def run(self,
            image: str = None,
            ports: dict = None,
            volumes: list = None):
        if image is None:
            image = self.default_image
        if ports is None:
            ports = self.default_ports
        if volumes is None:
            volumes = self.default_volumes
        self.logger.info(f'Running container with image {image}, name {self.container_name}, ports {ports}.')
        self.docker_client.containers.run(image,
                                          name=self.container_name,
                                          network='data_services_network',
                                          ports=ports,
                                          volumes=volumes,
                                          environment=self.environment_vars,
                                          auto_remove=True,
                                          detach=True)
        self.wait_for_container_to_be_ready()
        self.logger.info(f'Container {self.container_name} ready.')

    def wait_for_container_to_be_ready(self, retries: int = 1):
        service_available = False
        try:
            docker_container_status = self.get_container().status
            if docker_container_status == 'running':
                self.logger.info(f'Container {self.container_name} running.. pinging service..')
                service_available = self.ping_service()
            else:
                self.logger.info(f'Waiting on container.. status: {docker_container_status}')
        except Exception as e:
            print(f'On retry {retries} exception: {e}')

        if not service_available:
            if retries < 6:
                time.sleep(15)
                return self.wait_for_container_to_be_ready(retries + 1)
            raise DSContainerError(f'Waited a while but container {self.container_name} is not responding.')

    def ping_service(self):
        raise NotImplementedError('DataServicesContainer ping_service '
                                  'should be overridden by specific container implementations')

    def remove_old_containers(self):
        # see if a container with the same name already exists
        docker_container = self.get_container()
        if docker_container:
            # if so get rid of it
            self.logger.info(f'Removing old container with the same name..')
            # TODO: prevent potential conflicts between multiple users on the same docker cluster
            docker_container.remove(force=True)

    def get_container(self):
        try:
            return self.docker_client.containers.get(self.container_name)
        except docker.errors.NotFound:
            return None

    def stop_container(self):
        self.get_container().stop()

    # given a list of source files move them inside of the container
    # this could be useful if sharing volumes is not possible
    #
    def move_files_to_container(self, source_file_paths: list):
        self.logger.info(f'Moving files into container: {source_file_paths}')
        with self.convert_to_tar(source_file_paths) as archive_to_move:
            self.get_container().put_archive('/', archive_to_move)

    def convert_to_tar(self, source_file_paths: list):
        f = tempfile.NamedTemporaryFile()
        t = tarfile.open(mode='w', fileobj=f)
        for file_path in source_file_paths:
            abs_path = os.path.abspath(file_path)
            file_basename = os.path.basename(file_path)
            t.add(abs_path, arcname=file_basename, recursive=False)
        t.close()
        f.seek(0)
        return f


class PostgresContainer(DataServicesContainer):

    def __init__(self, container_name: str, postgres_version: str, logger):
        super().__init__(container_name=container_name, logger=logger)
        self.db_connection_port = 5432
        self.default_image = f"postgres:{postgres_version}"
        self.default_ports = {'5432/tcp': self.db_connection_port}

    def load_db_dump(self, dump_file_path: str):
        self.logger.info(f'Restoring db dump with psql...')
        docker_container = self.get_container()
        exit_code, response = docker_container.exec_run(f"/bin/bash -c 'gunzip -c {dump_file_path} | "
                                                         f"psql -U postgres postgres'")
        if exit_code == 0:
            self.logger.info(f'Database dump restored... {response}')
        else:
            error_message = f'Database dump restoration failed. Exit code {exit_code}:{response}'
            self.logger.error(error_message)
            raise DSContainerError(error_message)

    # returns a psycopg2 connection object
    def get_db_connection(self):
        return psycopg2.connect(user='postgres', host=self.container_name, port=self.db_connection_port)

    def ping_service(self):
        db_conn = self.get_db_connection()
        cur = db_conn.cursor()
        cur.execute("SELECT 1")
        return True


class MySQLContainer(DataServicesContainer):

    def __init__(self,
                 container_name: str,
                 mysql_version: str,
                 database_name: str,
                 logger):
        super().__init__(container_name=container_name, logger=logger)
        self.default_image = f"mysql:{mysql_version}"
        self.db_connection_port = 3306
        self.default_ports = {'3306/tcp': self.db_connection_port}
        self.database_name = database_name
        # self.database_user = 'default_user'
        self.database_password = 'default_password'
        self.database_root_password = 'default_root_password'
        self.environment_vars["MYSQL_ROOT_PASSWORD"] = self.database_root_password
        # self.environment_vars["MYSQL_USER"] = self.database_user
        # self.environment_vars["MYSQL_PASSWORD"] = self.database_password
        self.environment_vars["MYSQL_DATABASE"] = self.database_name
        self.environment_vars["MYSQL_ROOT_HOST"] = "%"

        self.mysql_connection_config = {
            # 'user': self.database_user,
            'user': 'root',
            'password': self.database_root_password,
            'host': self.container_name,
            'port': self.db_connection_port,
            'database': self.database_name
        }

    def load_db_dump(self, dump_file_path: str):
        self.logger.info(f'Restoring db dump...')
        docker_container = self.get_container()
        exit_code, response = docker_container.exec_run(f"/bin/bash -c 'gunzip -c {dump_file_path} | "
                                       f"mysql -uroot -p{self.database_root_password} {self.database_name}'")
        if exit_code == 0:
            self.logger.info(f'Database dump restored... {response}')
        else:
            error_message = f'Database dump restoration failed. Exit code {exit_code}:{response}'
            self.logger.error(error_message)
            raise DSContainerError(error_message)

    # returns a mysql connection object
    def get_db_connection(self):
        try:
            return mysql.connector.connect(**self.mysql_connection_config)
        except mysql.connector.Error as err:
            # we could be more specific here
            raise err

    def ping_service(self):
        db_conn = self.get_db_connection()
        cur = db_conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        db_conn.close()
        return True
"""

