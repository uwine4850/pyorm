from enum import Enum
from pyorm.connect.connectors.mysql_c import MysqlConnect
from pyorm.connect.connectors.sqlite_c import SqliteConnect


class DbType(Enum):
    SQLITE = 'sqlite'
    MYSQL = 'mysql'


class Connect:
    """
    Connecting to databases.
    """
    def __init__(self, db_type: DbType, **kwargs):
        self.db_type = db_type
        self.connect_data = kwargs

    def get_database_name(self):
        return self.connect_data['database']

    def query(self, query_string, dictionary=False):
        """
        Execute a request and return its data.
        """
        match self.db_type:
            case DbType.MYSQL:
                return MysqlConnect(self.connect_data['user'], self.connect_data['password'], self.connect_data['host'],
                                    self.connect_data['database']).query(query_string, dictionary)
            case DbType.SQLITE:
                return SqliteConnect(self.connect_data['database']).query(query_string, dictionary)
