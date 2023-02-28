from typing import Type
from pyorm.models.queries.sql_query import get_current_query
import pytest
from pyorm.connect.connection import Connect, DbType
from pyorm.models.columns import ColumnMixin, ForeignAction
from pyorm.models.model import ManageMigrate, CreateModel

TABLE_NAMES = ['modeltestfk', 'modeltest', 'simplemodeltest']


class SimpleModelTest(CreateModel):
    def __init__(self, connection):
        super().__init__(TABLE_NAMES[2], connection)

    def init_model(self, column: Type[ColumnMixin]):
        self.primary_key('id')
        self.init_columns()


class SimpleModelTestUpdPrimaryKey(CreateModel):
    def __init__(self, connection):
        super().__init__(TABLE_NAMES[2], connection)

    def init_model(self, column: Type[ColumnMixin]):
        self.primary_key('id_upd')
        self.init_columns()


class ModelTest(CreateModel):
    def __init__(self, connection):
        super().__init__(TABLE_NAMES[1], connection)

    def init_model(self, column: Type[ColumnMixin]):
        self.primary_key('id')
        self.init_columns(
            column.int('col1', null=True),
            column.varchar('col2', 100)
        )


class ModelTestUpdCols(CreateModel):
    def __init__(self, connection):
        super().__init__(TABLE_NAMES[1], connection)

    def init_model(self, column: Type[ColumnMixin]):
        self.primary_key('id')
        self.init_columns(
            column.int('col1', null=False),
            column.varchar('col2', 170, null=True)
        )


class ModelTestDeleteCols(CreateModel):
    def __init__(self, connection):
        super().__init__(TABLE_NAMES[1], connection)

    def init_model(self, column: Type[ColumnMixin]):
        self.primary_key('id')
        self.init_columns(

        )


class ModelTestFk(CreateModel):
    def __init__(self, connection):
        super().__init__(TABLE_NAMES[0], connection)

    def init_model(self, column: Type[ColumnMixin]):
        self.primary_key('id')
        self.init_columns(
            column.foreignkey('fk_col', TABLE_NAMES[2], 'id', ForeignAction.CASCADE, ForeignAction.RESTRICT),
            column.foreignkey('fk_col1', TABLE_NAMES[2], 'id', ForeignAction.CASCADE, ForeignAction.RESTRICT)
        )


class ModelTestFkDeleteCols(CreateModel):
    def __init__(self, connection):
        super().__init__(TABLE_NAMES[0], connection)

    def init_model(self, column: Type[ColumnMixin]):
        self.primary_key('id')
        self.init_columns(

        )


class ModelTestFkUpd(CreateModel):
    def __init__(self, connection):
        super().__init__(TABLE_NAMES[0], connection)

    def init_model(self, column: Type[ColumnMixin]):
        self.primary_key('id')
        self.init_columns(
            column.foreignkey('fk_col', TABLE_NAMES[1], 'id', ForeignAction.CASCADE, ForeignAction.RESTRICT),
            column.foreignkey('fk_col1', TABLE_NAMES[1], 'id', ForeignAction.CASCADE, ForeignAction.RESTRICT)
        )


@pytest.fixture(scope='session', autouse=True)
def test_connect_db():
    mysql_conn = Connect(DbType.MYSQL, user='uwine', password='1111', host='localhost', database='pyorm')
    sqlite_conn = Connect(DbType.SQLITE, database='q.db')
    return mysql_conn, sqlite_conn


@pytest.fixture(scope='session', autouse=True)
def test_clear_tables(test_connect_db):
    yield
    for table_name in TABLE_NAMES:
        if table_name in test_connect_db[0].query('SHOW TABLES')[0]:
            test_connect_db[0].query(f"DROP TABLE {table_name}")

        if table_name in test_connect_db[1].query("SELECT name FROM sqlite_master WHERE type='table'")[0]:
            test_connect_db[1].query(f"DROP TABLE {table_name}")


def test_migrate_simple_table(test_connect_db, test_clear_tables):
    ManageMigrate([SimpleModelTest], test_connect_db[0]).migrate()
    ManageMigrate([SimpleModelTest], test_connect_db[1]).migrate()

    mysql_create = TABLE_NAMES[2] in test_connect_db[0].query('SHOW TABLES')[0]
    sqlite_create = TABLE_NAMES[2] in test_connect_db[1].query("SELECT name FROM sqlite_master WHERE type='table'")[0]
    assert mysql_create and sqlite_create


def test_migrate_table(test_connect_db, test_clear_tables):
    ManageMigrate([ModelTest], test_connect_db[0]).migrate()
    ManageMigrate([ModelTest], test_connect_db[1]).migrate()
    mq = get_current_query(test_connect_db[0])
    sq = get_current_query(test_connect_db[1])

    m_ok = False
    s_ok = False

    mysql_columns = [{'Field': 'id', 'Type': 'int(11)', 'Null': 'NO', 'Default': None},
                     {'Field': 'col1', 'Type': 'int(11)', 'Null': 'YES', 'Default': None},
                     {'Field': 'col2', 'Type': 'varchar(100)', 'Null': 'NO', 'Default': None}]

    sqlite_columns = [{'Field': 'id', 'Type': 'INTEGER', 'Null': 'YES', 'Default': None},
                      {'Field': 'col1', 'Type': 'INT(11)', 'Null': 'YES', 'Default': None},
                      {'Field': 'col2', 'Type': 'VARCHAR(100)', 'Null': 'NO', 'Default': None}]

    if TABLE_NAMES[1] in test_connect_db[0].query('SHOW TABLES')[0]:
        if mq.show_columns(TABLE_NAMES[1])[0] == mysql_columns:
            m_ok = True

    if TABLE_NAMES[1] in test_connect_db[1].query("SELECT name FROM sqlite_master WHERE type='table'")[0]:
        print(sq.show_columns(TABLE_NAMES[1])[0])
        if sq.show_columns(TABLE_NAMES[1])[0] == sqlite_columns:
            s_ok = True

    assert m_ok and s_ok


def test_migrate_fk(test_connect_db, test_clear_tables):
    ManageMigrate([SimpleModelTest, ModelTestFk], test_connect_db[0]).migrate()
    ManageMigrate([SimpleModelTest, ModelTestFk], test_connect_db[1]).migrate()
    mq = get_current_query(test_connect_db[0])
    sq = get_current_query(test_connect_db[1])

    m_ok = False
    s_ok = False

    if mq.show_additional_fk_info(TABLE_NAMES[0], 'fk_col')[0][0]['REFERENCED_TABLE_NAME'] == TABLE_NAMES[2]:
        m_ok = True

    if sq.show_additional_fk_info(TABLE_NAMES[0], 'fk_col')[0][0]['REFERENCED_TABLE_NAME'] == TABLE_NAMES[2]:
        s_ok = True

    assert m_ok and s_ok


def test_migration_delete_columns(test_connect_db, test_clear_tables):
    ManageMigrate([ModelTest], test_connect_db[0]).migrate()
    ManageMigrate([ModelTest], test_connect_db[1]).migrate()
    mq = get_current_query(test_connect_db[0])
    sq = get_current_query(test_connect_db[1])
    m_ok = False
    s_ok = False
    ManageMigrate([ModelTestDeleteCols], test_connect_db[0]).migrate()
    ManageMigrate([ModelTestDeleteCols], test_connect_db[1]).migrate()

    if len(mq.show_columns(TABLE_NAMES[1])[0]) == 1 and mq.show_columns(TABLE_NAMES[1])[0][0]['Field'] == 'id':
        m_ok = True

    if len(sq.show_columns(TABLE_NAMES[1])[0]) == 1 and sq.show_columns(TABLE_NAMES[1])[0][0]['Field'] == 'id':
        s_ok = True

    assert m_ok and s_ok


def test_migration_delete_fk_columns(test_connect_db, test_clear_tables):
    ManageMigrate([SimpleModelTest, ModelTestFk], test_connect_db[0]).migrate()
    ManageMigrate([SimpleModelTest, ModelTestFk], test_connect_db[1]).migrate()
    mq = get_current_query(test_connect_db[0])
    sq = get_current_query(test_connect_db[1])
    m_ok = False
    s_ok = False
    ManageMigrate([ModelTestFkDeleteCols], test_connect_db[0]).migrate()
    ManageMigrate([ModelTestFkDeleteCols], test_connect_db[1]).migrate()

    if len(mq.show_columns(TABLE_NAMES[0])[0]) == 1 and mq.show_columns(TABLE_NAMES[0])[0][0]['Field'] == 'id':
        m_ok = True

    if len(sq.show_columns(TABLE_NAMES[0])[0]) == 1 and sq.show_columns(TABLE_NAMES[0])[0][0]['Field'] == 'id':
        s_ok = True

    assert m_ok and s_ok


def test_migration_upd_primary_key(test_connect_db, test_clear_tables):
    ManageMigrate([SimpleModelTest], test_connect_db[0]).migrate()
    ManageMigrate([SimpleModelTest], test_connect_db[1]).migrate()
    mq = get_current_query(test_connect_db[0])
    sq = get_current_query(test_connect_db[1])
    m_ok = False
    s_ok = False
    ManageMigrate([SimpleModelTestUpdPrimaryKey], test_connect_db[0]).migrate()
    ManageMigrate([SimpleModelTestUpdPrimaryKey], test_connect_db[1]).migrate()

    print(mq.show_columns(TABLE_NAMES[2])[0])

    if mq.show_columns(TABLE_NAMES[2])[0][0]['Field'] == 'id_upd':
        m_ok = True

    if sq.show_columns(TABLE_NAMES[2])[0][0]['Field'] == 'id_upd':
        s_ok = True

    assert m_ok and s_ok


def test_migration_upd_clumns(test_connect_db, test_clear_tables):
    ManageMigrate([ModelTest], test_connect_db[0]).migrate()
    ManageMigrate([ModelTest], test_connect_db[1]).migrate()
    mq = get_current_query(test_connect_db[0])
    sq = get_current_query(test_connect_db[1])
    m_ok = False
    s_ok = False
    ManageMigrate([ModelTestUpdCols], test_connect_db[0]).migrate()
    ManageMigrate([ModelTestUpdCols], test_connect_db[1]).migrate()

    current_mysql = [{'Field': 'id', 'Type': 'int(11)', 'Null': 'NO', 'Default': None},
                     {'Field': 'col1', 'Type': 'int(11)', 'Null': 'NO', 'Default': None},
                     {'Field': 'col2', 'Type': 'varchar(170)', 'Null': 'YES', 'Default': None}]

    current_sqlite = [{'Field': 'id', 'Type': 'INTEGER', 'Null': 'YES', 'Default': None},
                      {'Field': 'col1', 'Type': 'INT(11)', 'Null': 'NO', 'Default': None},
                      {'Field': 'col2', 'Type': 'VARCHAR(170)', 'Null': 'YES', 'Default': None}]

    if mq.show_columns(TABLE_NAMES[1])[0] == current_mysql:
        m_ok = True

    if sq.show_columns(TABLE_NAMES[1])[0] == current_sqlite:
        s_ok = True

    assert m_ok and s_ok


def test_migration_upd_fk(test_connect_db, test_clear_tables):
    ManageMigrate([SimpleModelTest, ModelTestFk], test_connect_db[0]).migrate()
    ManageMigrate([SimpleModelTest, ModelTestFk], test_connect_db[1]).migrate()
    mq = get_current_query(test_connect_db[0])
    sq = get_current_query(test_connect_db[1])
    m_ok = False
    s_ok = False
    ManageMigrate([ModelTest, ModelTestFkUpd], test_connect_db[0]).migrate()
    ManageMigrate([ModelTest, ModelTestFkUpd], test_connect_db[1]).migrate()

    if mq.show_additional_fk_info(TABLE_NAMES[0], 'fk_col')[0][0]['REFERENCED_TABLE_NAME'] == TABLE_NAMES[1] and \
            mq.show_additional_fk_info(TABLE_NAMES[0], 'fk_col')[0][0]['REFERENCED_TABLE_NAME'] == TABLE_NAMES[1]:
        m_ok = True

    if sq.show_additional_fk_info(TABLE_NAMES[0], 'fk_col')[0][0]['REFERENCED_TABLE_NAME'] == TABLE_NAMES[1] and \
            sq.show_additional_fk_info(TABLE_NAMES[0], 'fk_col')[0][0]['REFERENCED_TABLE_NAME'] == TABLE_NAMES[1]:
        s_ok = True

    assert m_ok and s_ok
