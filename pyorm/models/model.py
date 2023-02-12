from pyorm.models.columns import ColumnMixin, Column, ColumnType, PrimaryKey
from pyorm.connect.connection import Connect, DbType
from pyorm.models.queries.sql_query import get_current_query, QueryMiddleware
from abc import abstractmethod
from typing import Type
from pyorm.models.db.mysql import MysqlModel
from pyorm.models.db.sqlite import SqliteModel


class CreateModel:
    """
    Model view of a specific table for migrations only.
    """
    def __init__(self, table_name, connect: Connect):
        self.table_name = table_name
        self.connect = connect
        self._columns = []
        self.query = get_current_query(self.connect)
        self._primary_key = None
        self.query_mddl = QueryMiddleware(print_log=True)

        self.mysql_model = None
        self.sqlite_model = None

    def init_columns(self, *args: list[Column]):
        self._columns = list(args)

    def primary_key(self, name):
        self._primary_key = PrimaryKey(name)

    def migrate(self):
        self.init_model(ColumnMixin)
        self._columns.insert(0, self._primary_key)
        self.check_primary_key()
        self.mysql_model = MysqlModel(self.query, self.table_name, self._columns, self.query_mddl, self._primary_key)
        self.sqlite_model = SqliteModel(self.query, self.table_name, self._columns, self.query_mddl, self._primary_key)

        match self.connect.db_type:
            case DbType.MYSQL:
                self.mysql_model.delete_columns()
                self.mysql_model.add_columns()
                self.mysql_model.update_columns()
            case DbType.SQLITE:
                self.sqlite_model.delete_columns()
                self.sqlite_model.add_columns()
                self.sqlite_model.update_columns()

    def check_primary_key(self):
        if not self._primary_key:
            raise 'PRIMARY KEY not set'

    def create_table_if_not_exist(self):
        pk = self._get_current_pk_sql()
        db_tables = [i['Name'] for i in self.query_mddl.query(self.query.show_tables())]
        if not self.table_name in db_tables:
            self.query_mddl.query(self.query.create_table(self.table_name, [pk]), log=True)

    def _get_current_pk_sql(self) -> str:
        pk = None
        match self.connect.db_type:
            case DbType.MYSQL:
                pk = self._primary_key.as_sql_for_mysql()
            case DbType.SQLITE:
                pk = self._primary_key.as_sql_for_sqlite()
        return pk

    @abstractmethod
    def init_model(self, column: Type[ColumnMixin]):
        pass


class ManageMigrate:
    def __init__(self, models, connection: Connect):
        self.connection = connection
        self.models = models

    def _create_tables(self):
        for model in self.models:
            m = model(self.connection)
            m.init_model(ColumnMixin)
            m.create_table_if_not_exist()

    def migrate(self):
        self._create_tables()
        for model in self.models:
            model(self.connection).migrate()