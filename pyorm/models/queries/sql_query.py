from pyorm.connect.connection import Connect, DbType
from abc import abstractmethod


class Query:
    def __init__(self, connect: Connect):
        self.conn = connect

    def custom_query(self, query: str, dictionary=False):
        return self.conn.query(query, dictionary)

    @abstractmethod
    def create_table(self, table_name, columns: list[str]):
        pass

    @abstractmethod
    def show_tables(self):
        pass

    @abstractmethod
    def show_columns(self, table_name):
        pass

    @abstractmethod
    def add_columns_by_sql(self, table_name, sql_columns: list[str]):
        pass

    @abstractmethod
    def delete_columns(self, table_name, columns_name: list[str]):
        pass

    @abstractmethod
    def add_foreign_key(self, table_name, column_name, related_table, related_column, on_delete, on_update):
        pass

    @abstractmethod
    def show_fk_name(self, column_name):
        pass

    @abstractmethod
    def delete_foreign_key(self, table_name, fk_name):
        pass

    @abstractmethod
    def update_columns(self, table_name, columns_name_and_slq: list[dict]):
        """
        :param columns_name_and_slq: The key in the form of a column name and the value is in the form of its sql code.
        All of these are included in the list.
        """
        pass

    @abstractmethod
    def show_pk_name(self, table_name):
        pass

    @abstractmethod
    def show_additional_fk_info(self, table_name, column_name):
        pass

    @abstractmethod
    def rename_column(self, table_name, old_name, new_name):
        pass

    @abstractmethod
    def select_all(self, select_name, from_table, dictionary=False):
        pass

    @abstractmethod
    def select_where(self, select_name, from_table, where, dictionary=False):
        pass

    @abstractmethod
    def insert(self, table_name, **kwargs):
        pass

    @abstractmethod
    def delete(self, table_name, where):
        pass

    @abstractmethod
    def update_field(self, table_name, where: dict, **kwargs):
        pass


class QueryMiddleware:
    def __init__(self, print_log=False):
        self.print_log = print_log

    def query(self, query: list, log=False, info=''):
        query_res, query_str = query
        if self.print_log and log:
            print('--', query_str, '|', info)
        return query_res


def update_kwargs_to_sql(**kwargs):
    upd = ''

    for x, i in enumerate(kwargs):
        if x == len(kwargs)-1:
            upd += f"`{i}` = '{kwargs[i]}'"
        else:
            upd += f"`{i}` = '{kwargs[i]}', "
    return upd


def kwargs_to_sql_where(**kwargs):
    where = ''
    for x, i in enumerate(kwargs):
        if x == len(kwargs)-1:
            where += f"{i}='{kwargs[i]}'"
        else:
            where += f"{i}='{kwargs[i]}' AND "
    return where


def insert_kwargs_to_sql(**kwargs):
    cols = '('
    values = 'VALUES ('

    for x, i in enumerate(kwargs):
        if x == len(kwargs)-1:
            cols += f"`{i}`) "
            values += f"'{kwargs[i]}')"
        else:
            cols += f"`{i}`, "
            values += f"'{kwargs[i]}', "

    res = cols + values
    return res


def column_list_to_str(columns: list[str]):
    str_columns = ''
    for x, col in enumerate(columns):
        if x == len(columns)-1:
            str_columns += col
        else:
            str_columns += col + ', '
    return str_columns


def column_list_to_deletecolumn_str(columns_name: list[str]):
    str_columns = ''
    for x, col in enumerate(columns_name):
        if x == len(columns_name)-1:
            str_columns += f"DROP `{col}`"
        else:
            str_columns += f"DROP `{col}`, "
    return str_columns


def get_current_query(connection: Connect):
    from pyorm.models.queries.mysql_query import MysqlQuery
    from pyorm.models.queries.sqlite_query import SqliteQuery
    match connection.db_type:
        case DbType.MYSQL:
            return MysqlQuery(connection)
        case DbType.SQLITE:
            return SqliteQuery(connection)
        case _:
            raise 'query not found'


def get_update_columns(columns_name_and_slq: list[dict]):
    """
    :param columns_name_and_slq: The key in the form of a column name and the value is in the form of its sql code.
    All of these are included in the list.
    """
    update_columns_str = ''
    for x, col in enumerate(columns_name_and_slq):
        key_col = list(col.keys())[0]
        if x == len(columns_name_and_slq)-1:
            update_columns_str += f"CHANGE `{key_col}` {col[key_col]}"
        else:
            update_columns_str += f"CHANGE `{key_col}` {col[key_col]}, "
    return update_columns_str
