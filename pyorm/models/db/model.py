from abc import abstractmethod
from pyorm.models.columns import ColumnType, Column


class DbModel:
    """
    Providing a specific database and operations with it.
    """
    def __init__(self, query, table_name, columns, query_mddl, primary_key):
        self.table_name = table_name
        self.columns = columns
        self.query = query
        self.query_mddl = query_mddl
        self.primary_key = primary_key

    def add_columns(self):
        """
        Adding columns if they are not present in the database table.
        """
        db_columns_name = [i['Field'] for i in self.query_mddl.query(self.query.show_columns(self.table_name))]
        add_column_list = []
        fk_columns = []
        for model_column in self.columns:
            if not model_column.name in db_columns_name:
                match model_column.ftype:
                    case ColumnType.FOREIGN_KEY:
                        fk_columns.append(model_column)
                    case ColumnType.PRIMARY_KEY:
                        # only rename
                        pass
                    case _:
                        add_column_list.append('ADD ' + model_column.as_sql())
        # add simple columns
        if add_column_list:
            self.query_mddl.query(self.query.add_columns_by_sql(self.table_name, add_column_list), log=True, info='add')

        # add foreign key columns
        if fk_columns:
            self.add_fk(self.table_name, fk_columns)

    @abstractmethod
    def add_fk(self, table_name, fk_columns: list[Column]):
        """
        Logic for adding a foreign key.
        This method may look different for each database.
        """
        pass

    def delete_columns(self):
        """
        Removing columns.
        If a column is found in the database table but not found in the model, it will be deleted.
        """
        db_columns_name = [i['Field'] for i in self.query_mddl.query(self.query.show_columns(self.table_name),
                                                                     log=False, info='delete_columns()')]
        model_columns_name = [i.name for i in self.columns]

        delete_columns = []
        for db_col_name in db_columns_name:
            if not db_col_name in model_columns_name:
                delete_columns.append(db_col_name)

        if delete_columns:
            for del_col in delete_columns:
                # delete foreign key
                if self.query_mddl.query(self.query.show_fk_name(del_col)):
                    self.query_mddl.query(self.query.delete_foreign_key(self.table_name,
                                                                        self.query.show_fk_name(del_col)[0][0]),
                                          log=True)
                    self.query_mddl.query(self.query.delete_columns(self.table_name, [del_col]), log=True)
                # delete primary key
                elif del_col == self.query_mddl.query(self.query.show_pk_name(self.table_name))[0]:
                    self.query_mddl.query(self.query.rename_column(self.table_name, del_col, self.primary_key.name),
                                          log=True)
                # delete other columns
                else:
                    self.query_mddl.query(self.query.delete_columns(self.table_name, delete_columns), log=True)
                    break

    def _get_format_db_columns(self) -> list[dict]:
        """
        Formatting column data from the database according to the selected pattern.
        """
        db_show_columns = self.query_mddl.query(self.query.show_columns(self.table_name))
        db_columns = []

        for dcol in db_show_columns:
            length = 11
            ftype = dcol['Type']

            # get column type
            # find column length
            if dcol['Type'].rfind('(') != -1:
                length = dcol['Type'][dcol['Type'].rfind('(')+1:dcol['Type'].rfind(')')]
                ftype = dcol['Type'][:dcol['Type'].rfind('(')].upper()

            db_columns.append({
                'Field': dcol['Field'],
                'Type': ftype,
                'Lenght': int(length),
                'Null': dcol['Null'],
            })
        return db_columns

    def update_columns(self):
        """
        Search for columns to update.
        """
        db_columns = self._get_format_db_columns()
        update_columns = []
        update_fk = []

        for mcol in self.columns:
            # formatting null value to sql
            if mcol.null:
                null = 'YES'
            else:
                null = 'NO'

            col = {
                'Field': mcol.name,
                'Type': mcol.ftype.value,
                'Lenght': mcol.length,
                'Null': null,
            }

            # add to update foreign key
            if col['Type'] == ColumnType.FOREIGN_KEY.value:
                update_fk.append(mcol)

            for db_col in db_columns:
                # If the column names match and the column type is not equal to the primary key.
                if col['Field'] == db_col['Field'] and col['Type'] != ColumnType.PRIMARY_KEY.value:
                    if col != db_col:
                        # skip fk
                        if col['Type'] == ColumnType.FOREIGN_KEY.value and db_col['Type'] in ['INT', 'INTEGER']:
                            continue
                        update_columns.append(col)

        if update_fk:
            self._commit_update_fk(update_fk)
        if update_columns:
            self._commit_update(update_columns)

    def _commit_update_fk(self, update_fk: list[Column]):
        """
        Formatting database table and model column data to the same pattern.
        """
        fk_list = []
        for uf in update_fk:
            mfk = {
                'DELETE_RULE': uf.on_delete.value,
                'UPDATE_RULE': uf.on_update.value,
                'REFERENCED_TABLE_NAME': uf.related_table
            }
            db_fk = self.query_mddl.query(self.query.show_additional_fk_info(self.table_name, uf.name))[0]
            if mfk != db_fk:
                fk_list.append(uf)
        self.upd_fk(fk_list)

    @abstractmethod
    def upd_fk(self, fk_list: list[Column]):
        """
        Update foreign keys. Each database must have a separate algorithm.
        """
        pass

    def _commit_update(self, update_columns: list[dict]):
        """
        Update simple table columns.
        """
        model_update_columns = []
        for uc in update_columns:
            for mcol in self.columns:
                if mcol.name == uc['Field']:
                    model_update_columns.append({mcol.name: mcol.as_sql()})
        self.query_mddl.query(self.query.update_columns(self.table_name, model_update_columns), log=True, info='update')
