import docker
import time
import psycopg2
import os
import tempfile
import tarfile
import mariadb

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

        self.docker_container = None
        # placeholders for overriding
        self.default_image = None
        self.default_ports = None
        self.default_volumes = [f"{os.environ['HOST_STORAGE_DIR']}:/Data_services_storage"]
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
        self.docker_container = self.docker_client.containers.run(image,
                                                                  name=self.container_name,
                                                                  network='data_services_network',
                                                                  ports=ports,
                                                                  volumes=volumes,
                                                                  environment=self.environment_vars,
                                                                  auto_remove=True,
                                                                  detach=True)
        self.logger.debug(f'Container {self.container_name} created.. pinging for availability..')
        self.wait_for_container_to_be_ready()
        self.logger.info(f'Container {self.container_name} ready.')

    def wait_for_container_to_be_ready(self, retries: int = 1):
        try:
            service_available = self.ping_service()
        except Exception as e:
            service_available = False
            print(f'On retry {retries} exception: {e}')

        if not service_available:
            self.logger.debug(f'Waiting for container {self.container_name}.. retry {retries}')
            if retries < 6:
                time.sleep(15)
                return self.wait_for_container_to_be_ready(retries + 1)
            raise DSContainerError(f'Waited a while but container {self.container_name} is not responding.')

    def ping_service(self):
        raise NotImplementedError('DataServicesContainer ping_service '
                                  'should be overridden by specific container implementations')

    def remove_old_containers(self):
        # see if a container with the same name already exists
        docker_container = self.get_container_object()
        if docker_container:
            # if so get rid of it
            self.logger.debug(f'Removing old container..')
            docker_container.remove(force=True)
            # TODO: prevent potential conflicts between multiple users on the same docker cluster

    def get_container_object(self):
        try:
            return self.docker_client.containers.get(self.container_name)
        except docker.errors.NotFound:
            return None

    # given a list of source files to move
    # create a tar archive of the files and use docker.put_archive to move them to the container
    # def move_files_to_container(self, source_file_paths: list):
    #    self.logger.info(f'Moving files into container: {source_file_paths}')
    #    with self.convert_to_tar(source_file_paths) as archive_to_move:
    #        self.docker_container.put_archive('/', archive_to_move)

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
        self.default_image = f"postgres:{postgres_version}"
        self.default_ports = {'5432/tcp': 5432}

    def load_db_dump(self, dump_file_path: str):
        self.logger.info(f'Restoring db dump with psql...')
        # used this with the move_files function
        # dump_file_path = f'/{dump_file_name}'
        exit_code, response = self.docker_container.exec_run(f"/bin/bash -c 'gunzip -c {dump_file_path} | "
                                                             f"psql -U postgres postgres'")
        if exit_code == 0:
            self.logger.info(f'Database dump restored... {response}')
        else:
            self.logger.info(f'Database dump restoration failed. Exit code {exit_code}:{response}')

    # returns a psycopg2 connection object
    def get_db_connection(self):
        return psycopg2.connect(user='postgres', host=self.container_name, port=5432)

    def ping_service(self):
        db_conn = self.get_db_connection()
        cur = db_conn.cursor()
        cur.execute("SELECT 1")
        return True


class MariaDBContainer(DataServicesContainer):

    def __init__(self,
                 container_name: str,
                 mariadb_version: str,
                 database_name: str,
                 logger):
        super().__init__(container_name=container_name, logger=logger)
        self.default_image = f"mariadb:{mariadb_version}"
        self.default_ports = {'3307/tcp': 3307}
        self.mariadb_db_name = database_name
        self.mariadb_password = 'default_mariadb_password'
        self.environment_vars["MARIADB_ROOT_PASSWORD"] = self.mariadb_password
        self.environment_vars["MARIADB_DATABASE"] = self.mariadb_db_name

    def load_db_dump(self, dump_file_path: str):
        self.logger.info(f'Restoring db dump...')
        # dump_file_path = f'/{dump_file_name}'
        exit_code, response = self.docker_container.exec_run(f"/bin/bash -c 'gunzip -c {dump_file_path} | "
                                       f"mysql -uroot -p{self.mariadb_password} {self.mariadb_db_name}'")
        if exit_code == 0:
            self.logger.info(f'Database dump restored... {response}')
        else:
            self.logger.info(f'Database dump restoration failed. Exit code {exit_code}:{response}')

    # returns a mariadb connection object
    def get_db_connection(self):
        return mariadb.connect(user='root',
                               database=self.mariadb_db_name,
                               password=self.mariadb_password,
                               host=self.container_name,
                               port=3306)

    def ping_service(self):
        db_conn = self.get_db_connection()
        cur = db_conn.cursor()
        cur.execute("SELECT 1")
        return True
