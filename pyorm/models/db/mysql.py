from pyorm.models.columns import Column
from pyorm.models.db.model import DbModel


class MysqlModel(DbModel):
    def __init__(self, query, table_name, columns, query_mddl, primary_key):
        super().__init__(query, table_name, columns, query_mddl, primary_key)
        self.mysql_manager = _MysqlManager(self.table_name, self.query, self.query_mddl)

    def add_fk(self, table_name, fk_columns: list[Column]):
        self.mysql_manager.add_fk(table_name, fk_columns)

    def upd_fk(self, fk_list: list[Column]):
        for fk in fk_list:
            fk_name = self.query_mddl.query(self.query.show_fk_name(fk.name))[0]
            print(self.query_mddl.query(self.query.show_fk_name(fk.name)))

            # delete and add new foreign key
            self.query_mddl.query(self.query.delete_foreign_key(self.table_name, fk_name))
            self.query_mddl.query(
                self.query.add_foreign_key(self.table_name, fk.name, fk.related_table, fk.related_column,
                                           fk.on_delete.value, fk.on_update.value), log=True
            )


class _MysqlManager:
    def __init__(self, table_name, query, query_mddl):
        self.table_name = table_name
        self.query = query
        self.query_mddl = query_mddl

    def add_fk(self, table_name, fk_columns):
        for q in fk_columns:
            # First, a column is added that will be the foreign key.
            self.query_mddl.query(self.query.add_columns_by_sql(table_name, ['ADD' + q.as_sql()]), log=False)
            try:
                # A foreign key appears to the newly created column.
                self.query_mddl.query(self.query.add_foreign_key(table_name, q.name, q.related_table,
                                                                 q.related_column, q.on_delete.value, q.on_update.value
                                                                 ), log=True)
            except Exception as e:
                print(e)
                # If an error occurs, delete the newly created column.
                self.query_mddl.query(self.query.delete_columns(self.table_name, [q]), log=False)
