from pyorm.models.queries.sql_query import Query, column_list_to_str, column_list_to_deletecolumn_str, get_update_columns
from pyorm.connect.connection import Connect, DbType
from pyorm.models.queries.normalizer import QueryResultNormalizer


class MysqlQuery(Query):
    def __init__(self, connect: Connect):
        self.normalizer = QueryResultNormalizer(DbType.MYSQL)
        super().__init__(connect)

    def create_table(self, table_name, columns: list[str]):
        return self.custom_query(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({column_list_to_str(columns)})")

    def show_columns(self, table_name):
        return self.normalizer.show_columns(self.custom_query(f'SHOW COLUMNS FROM {table_name}', dictionary=True))

    def show_tables(self):
        return self.normalizer.show_tables(self.custom_query('SHOW TABLES;', dictionary=True))

    def add_columns_by_sql(self, table_name, sql_columns: list[str]):
        return self.custom_query(f"ALTER TABLE `{table_name}` {column_list_to_str(sql_columns)};")

    def delete_columns(self, table_name, columns_name: list[str]):
        return self.custom_query(f"ALTER TABLE `{table_name}` {column_list_to_deletecolumn_str(columns_name)};")

    def add_foreign_key(self, table_name, column_name, related_table, related_column, on_delete, on_update):
        return self.custom_query(f"ALTER TABLE {table_name} ADD FOREIGN KEY (`{column_name}`) REFERENCES"
                          f" `{related_table}`(`{related_column}`) ON DELETE {on_delete} ON UPDATE {on_update};")

    def show_fk_name(self, column_name):
        return self.custom_query(f"SELECT `CONSTRAINT_NAME` FROM `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` "
                                 f"WHERE `REFERENCED_TABLE_NAME` IS NOT NULL AND `COLUMN_NAME` = '{column_name}';")

    def show_additional_fk_info(self, table_name, column_name):
        constraint_name = self.show_fk_name(column_name)[0][0]
        return self.normalizer.show_additional_fk_info(
            self.custom_query(f"SELECT `DELETE_RULE`, `UPDATE_RULE`, `REFERENCED_TABLE_NAME` FROM "
                              f"`INFORMATION_SCHEMA`.`REFERENTIAL_CONSTRAINTS` "
                              f"WHERE `CONSTRAINT_NAME` = '{constraint_name}' AND `TABLE_NAME` = '{table_name}';",
                              dictionary=True)
        )

    def delete_foreign_key(self, table_name, fk_name):
        return self.custom_query(f"ALTER TABLE {table_name} DROP FOREIGN KEY {fk_name};")

    def update_columns(self, table_name, columns_name_and_slq: list[dict]):
        return self.custom_query(f"ALTER TABLE `{table_name}` {get_update_columns(columns_name_and_slq)}")

    def show_pk_name(self, table_name):
        res = self.custom_query(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY';", dictionary=True)[0][0]['Column_name']
        return [[res], f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY';"]

    def rename_column(self, table_name, old_name, new_name):
        return self.custom_query(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} to {new_name};")
