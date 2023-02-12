from pyorm.models.columns import Column, ColumnType
from pyorm.models.db.model import DbModel


class SqliteModel(DbModel):
    def __init__(self, query, table_name, columns, query_mddl, primary_key):
        super().__init__(query, table_name, columns, query_mddl, primary_key)
        self.sqlite_manager = _SqliteManager(self.table_name, self.query, self.query_mddl)

    def add_fk(self, table_name, fk_columns: list[Column]):
        self.sqlite_manager.add_fk(table_name, fk_columns)

    def upd_fk(self, fk_list: list[Column]):
        # To add foreign keys for sqlite, you need to create a duplicate of the selected table with already
        # updated foreign keys, insert data from the old table,
        # delete the old table and rename the new table with the old table's name.

        sql_columns = []
        fk_columns = []

        # Separation of columns into foreign keys and the rest.
        for col in self.columns:
            match col.ftype:
                case ColumnType.PRIMARY_KEY:
                    sql_columns.append(col.as_sql_for_sqlite())
                case ColumnType.FOREIGN_KEY:
                    fk_columns.append(col)
                case _:
                    sql_columns.append(col.as_sql())

        temp_name = f"{self.table_name}__TEMP__"

        # cretae new table
        self.query_mddl.query(self.query.create_table(temp_name, sql_columns))
        self.add_fk(temp_name, fk_columns)

        # insert old data to new table
        self.query_mddl.query(self.query.custom_query(f"INSERT INTO {temp_name} SELECT * FROM {self.table_name};"))

        # delete old table
        self.query_mddl.query(self.query.custom_query(f"DROP TABLE {self.table_name};"))

        # rename new table
        self.query_mddl.query(self.query.custom_query(f"ALTER TABLE {temp_name} RENAME TO {self.table_name};"))


class _SqliteManager:
    def __init__(self, table_name, query, query_mddl):
        self.table_name = table_name
        self.query = query
        self.query_mddl = query_mddl

    def add_fk(self, table_name, fk_columns):
        for q in fk_columns:
            try:
                self.query_mddl.query(self.query.add_foreign_key(table_name, q.name, q.related_table,
                                                                 q.related_column, q.on_delete.value, q.on_update.value,
                                                                 ), log=True)
            except Exception as e:
                print(e)
