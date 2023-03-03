from pyorm.models.queries.sql_query import Query, column_list_to_str, column_list_to_deletecolumn_str, \
    insert_kwargs_to_sql, kwargs_to_sql_where, update_kwargs_to_sql
from pyorm.connect.connection import Connect, DbType
from pyorm.models.queries.normalizer import QueryResultNormalizer


class SqliteQuery(Query):
    def __init__(self, connect: Connect):
        self.normalizer = QueryResultNormalizer(DbType.SQLITE)
        super().__init__(connect)

    def create_table(self, table_name, columns: list[str]):
        return self.custom_query(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({column_list_to_str(columns)})")

    def show_columns(self, table_name):
        return self.normalizer.show_columns(self.custom_query(f"PRAGMA table_info({table_name})", dictionary=True))

    def show_tables(self):
        return self.normalizer.show_tables(self.custom_query("SELECT name FROM sqlite_master WHERE type='table'",
                                                             dictionary=True))

    def add_columns_by_sql(self, table_name, sql_columns: list[str]):
        for sql_col in sql_columns:
            self.custom_query(f"ALTER TABLE `{table_name}` {column_list_to_str([sql_col])};")
        return [[], f"ALTER TABLE `{table_name}` {column_list_to_str(sql_columns)};"]

    def delete_columns(self, table_name, columns_name: list[str]):
        for col_name in columns_name:
            self.custom_query(f"ALTER TABLE `{table_name}` {column_list_to_deletecolumn_str([col_name])};")
        return [[], f"ALTER TABLE `{table_name}` {column_list_to_deletecolumn_str(columns_name)};"]

    def add_foreign_key(self, table_name, column_name, related_table, related_column, on_delete, on_update):
        return self.custom_query(f"ALTER TABLE {table_name} ADD COLUMN {column_name} INTEGER REFERENCES "
                          f"{related_table}({related_column}) ON DELETE {on_delete} ON UPDATE {on_update};")

    def show_fk_name(self, table_name):
        res = []
        q = self.custom_query(f"PRAGMA foreign_key_list('{table_name}');", dictionary=True)[0]
        if q:
            for i in q:
                res.append(i['from'])
        return [res, f"PRAGMA foreign_key_list('{table_name}');"]

    def show_additional_fk_info(self, table_name, column_name):
        return self.normalizer.show_additional_fk_info(
            self.custom_query(f"SELECT * FROM pragma_foreign_key_list('{table_name}') WHERE `from` = '{column_name}';",
                              dictionary=True)
        )

    def delete_foreign_key(self, table_name, fk_name):
        return self.custom_query(f"ALTER TABLE {table_name} DROP {fk_name};")

    def update_columns(self, table_name, columns_name_and_slq: list[dict]):
        col_names = [list(i.keys())[0] for i in columns_name_and_slq]
        self.delete_columns(table_name, col_names)
        for col in columns_name_and_slq:
            for key in col:
                self.add_columns_by_sql(table_name, ['ADD' + col[key]])
        return [[], f"Updated columns {col_names}"]

    def show_pk_name(self, table_name):
        return self.custom_query(f'SELECT l.name FROM pragma_table_info("{table_name}") as l WHERE l.pk <> 0;')

    def rename_column(self, table_name, old_name, new_name):
        return self.custom_query(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} to {new_name};")

    def select_all(self, select_name, from_table, dictionary=False):
        return self.custom_query(f"SELECT {select_name} FROM {from_table}")

    def select_where(self, select_name, from_table, dictionary=False, **where):
        return self.custom_query(f"SELECT {select_name} FROM {from_table} WHERE {kwargs_to_sql_where(**where)}",
                                 dictionary=dictionary)

    def insert(self, table_name, **kwargs):
        return self.custom_query(f"INSERT INTO `{table_name}` {insert_kwargs_to_sql(**kwargs)}")

    def delete(self, table_name, **where):
        return self.custom_query(f"DELETE FROM {table_name} WHERE {kwargs_to_sql_where(**where)}")

    def update_field(self, table_name, where: dict, **kwargs):
        return self.custom_query(f"UPDATE `{table_name}` SET {update_kwargs_to_sql(**kwargs)} "
                                 f"WHERE {kwargs_to_sql_where(**where)}")
