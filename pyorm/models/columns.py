from enum import Enum
from abc import abstractmethod


class ColumnType(Enum):
    INT = 'INT'
    VARCHAR = 'VARCHAR'
    FOREIGN_KEY = 'FOREIGN_KEY'
    PRIMARY_KEY = 'PRIMARY KEY'


class ForeignAction(Enum):
    RESTRICT = 'RESTRICT'
    SET_NULL = 'SET NULL'
    NO_ACTION = 'NO ACTION'
    CASCADE = 'CASCADE'


class Column:
    def __init__(self, ftype: ColumnType, name: str, null: bool):
        self.ftype = ftype
        self.name = name
        self.null = null

    @abstractmethod
    def as_sql(self) -> str:
        pass

    def null_to_sql(self) -> str:
        if self.null:
            return 'NULL'
        else:
            return 'NOT NULL'


class IntCol(Column):
    def __init__(self, name: str, length: int = 11, null=False):
        super().__init__(ftype=ColumnType.INT, name=name, null=null)
        self.length = length

    def as_sql(self) -> str:
        return f"`{self.name}` {self.ftype.value}({self.length}) {self.null_to_sql()}"


class VarcharCol(Column):
    def __init__(self, name: str, length: int, null=False):
        super().__init__(ftype=ColumnType.VARCHAR, name=name, null=null)
        self.length = length

    def as_sql(self) -> str:
        return f"`{self.name}` {self.ftype.value}({self.length}) {self.null_to_sql()}"


class ForeignKey(Column):
    def __init__(self, name: str, related_table: str, related_column: str, on_delete: ForeignAction,
                 on_update: ForeignAction = ForeignAction.RESTRICT, null=False):
        super().__init__(ftype=ColumnType.FOREIGN_KEY, name=name, null=null)
        self.length = 11
        self.related_table = related_table
        self.related_column = related_column
        self.on_delete = on_delete
        self.on_update = on_update

    def as_sql(self) -> str:
        return f"`{self.name}` INT({self.length}) {self.null_to_sql()}"

    def as_foreign_key(self) -> str:
        return f"FOREIGN KEY (`{self.name}`) REFERENCES `{self.related_table}`(`{self.related_column}`)" \
               f" ON DELETE {self.on_delete.value} ON UPDATE {self.on_update.value};"


class PrimaryKey(Column):
    def __init__(self, name: str):
        super().__init__(ftype=ColumnType.PRIMARY_KEY, name=name, null=True)
        self.length = 11

    def as_sql_for_mysql(self):
        return f"`{self.name}` INT({self.length}) {self.null_to_sql()} AUTO_INCREMENT, PRIMARY KEY(`{self.name}`)"

    def as_sql_for_sqlite(self):
        return f"{self.name} INTEGER PRIMARY KEY AUTOINCREMENT"

    def as_sql_add_mysql(self):
        return f"`{self.name}` INT({self.length}) {self.ftype.value} AUTO_INCREMENT"

    def as_sql_add_sqlite(self):
        return f"`{self.name}` INTEGER({self.length}) {self.ftype.value} AUTOINCREMENT"

    def as_sql(self) -> str:
        raise "The as_sql() method is not supported on a Primary Key column"


class ColumnMixin(IntCol, VarcharCol):
    int = IntCol
    varchar = VarcharCol
    foreignkey = ForeignKey
