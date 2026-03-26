import mysql.connector
import psycopg2

# right now we don't maintain a mysql db connection across multiple queries, a new one is established per query
# fine for current usage, but if a source required many queries we may want to add that capability
class MySQLConnector:

    def __init__(self,
                 db_host: str,
                 db_user: str,
                 db_password: str,
                 db_name: str,
                 db_port: str,
                 logger):
        self.logger = logger
        self.mysql_connection_config = {
            'host': db_host,
            'user': db_user,
            'password': db_password,
            'database': db_name,
            'port': db_port,
        }

    # returns a mysql connection object
    def get_db_connection(self):
        try:
            return mysql.connector.connect(**self.mysql_connection_config)
        except mysql.connector.Error as err:
            # we could be more specific here
            raise err

    def ping_db(self):
        try:
            db_conn = self.get_db_connection()
            cur = db_conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            db_conn.close()
            return True
        except mysql.connector.Error as err:
            self.logger.error(err)
            return False

    def query(self, sql_query: str) -> dict:
        """
        executes a sql statement

        :param sql_query:
        :return dict of results:
        """
        db_connection = self.get_db_connection()
        cursor = db_connection.cursor(dictionary=True, buffered=True)
        cursor.execute(sql_query)

        ret_val: dict = cursor.fetchall()

        cursor.close()
        db_connection.close()
        return ret_val


class PostgresConnector:

    def __init__(self,
                 db_host: str,
                 db_user: str,
                 db_password: str,
                 db_name: str,
                 db_port: str,
                 logger):
        self.logger = logger
        self.host = db_host
        self.user = db_user
        self.password = db_password
        self.dbname = db_name
        self.port = db_port

    # returns a psycopg2 connection object
    def get_db_connection(self):
        return psycopg2.connect(user=self.user,
                                password=self.password,
                                dbname=self.dbname,
                                host=self.host,
                                port=self.port)

    def ping_service(self):
        try:
            db_conn = self.get_db_connection()
            cur = db_conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            db_conn.close()
            return True
        except psycopg2.Error as e:
            self.logger.error(f'Could not connect to postgres database at {self.host}:{self.port} - {repr(e)}')
            return False
