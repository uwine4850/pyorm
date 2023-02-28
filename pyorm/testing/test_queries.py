import pytest
from pyorm.connect.connection import Connect, DbType
from pyorm.models.columns import ForeignAction
from pyorm.models.queries.sql_query import get_current_query


TABLE_NAME = 'test_queries'
TABLE_COLUMNS = [
    'id INT NOT NULL',
    'test VARCHAR(100) NOT NULL',
    'test2 INT NOT NULL'
]


@pytest.fixture(scope='session', autouse=True)
def test_queries_db():
    mysql_conn = Connect(DbType.MYSQL, user='uwine', password='1111', host='localhost', database='pyorm')
    sqlite_conn = Connect(DbType.SQLITE, database='q.db')
    mysqlquery = get_current_query(mysql_conn)
    sqlitequery = get_current_query(sqlite_conn)
    return mysqlquery, sqlitequery


def test_show_tables(test_queries_db):
    mysql_ok = False
    sqlite_ok = False
    mysqlquery = test_queries_db[0]
    sqlitequery = test_queries_db[1]

    if [i['Name'] for i in mysqlquery.show_tables()[0]] == mysqlquery.custom_query('SHOW TABLES')[0]:
        mysql_ok = True

    if [i['Name'] for i in sqlitequery.show_tables()[0]] == sqlitequery.custom_query(
            "SELECT name FROM sqlite_master WHERE type='table'")[0]:
        sqlite_ok = True

    assert mysql_ok and sqlite_ok


def test_create_table(test_queries_db):
    mysql_ok = False
    sqlite_ok = False
    mysqlquery = test_queries_db[0]
    sqlitequery = test_queries_db[1]

    mysqlquery.create_table(TABLE_NAME, ['`id` INT'])
    sqlitequery.create_table(TABLE_NAME, ['`id` INT'])

    if TABLE_NAME in [i['Name'] for i in mysqlquery.show_tables()[0]]:
        mysqlquery.custom_query(f"DROP TABLE {TABLE_NAME}")
        mysql_ok = True

    if TABLE_NAME in [i['Name'] for i in sqlitequery.show_tables()[0]]:
        sqlitequery.custom_query(f"DROP TABLE {TABLE_NAME}")
        sqlite_ok = True

    assert mysql_ok and sqlite_ok


def test_show_columns(test_queries_db):
    mysql_ok = False
    sqlite_ok = False
    mysqlquery = test_queries_db[0]
    sqlitequery = test_queries_db[1]

    current_sqlite = [{'Field': 'id', 'Type': 'INT', 'Null': 'NO', 'Default': None},
                      {'Field': 'test', 'Type': 'VARCHAR(100)', 'Null': 'NO', 'Default': None},
                      {'Field': 'test2', 'Type': 'INT', 'Null': 'NO', 'Default': None},
                      ]

    current_mysql = [{'Field': 'id', 'Type': 'int(11)', 'Null': 'NO', 'Default': None},
                     {'Field': 'test', 'Type': 'varchar(100)', 'Null': 'NO', 'Default': None},
                     {'Field': 'test2', 'Type': 'int(11)', 'Null': 'NO', 'Default': None}
                     ]

    mysqlquery.create_table(TABLE_NAME, TABLE_COLUMNS)
    sqlitequery.create_table(TABLE_NAME, TABLE_COLUMNS)

    if TABLE_NAME in [i['Name'] for i in mysqlquery.show_tables()[0]]:
        if mysqlquery.show_columns(TABLE_NAME)[0] == current_mysql:
            mysql_ok = True
        mysqlquery.custom_query(f"DROP TABLE {TABLE_NAME}")

    if TABLE_NAME in [i['Name'] for i in sqlitequery.show_tables()[0]]:
        if sqlitequery.show_columns(TABLE_NAME)[0] == current_sqlite:
            sqlite_ok = True
        sqlitequery.custom_query(f"DROP TABLE {TABLE_NAME}")

    assert mysql_ok and sqlite_ok


def test_add_columns_by_sql(test_queries_db):
    mysql_ok = False
    sqlite_ok = False
    mysqlquery = test_queries_db[0]
    sqlitequery = test_queries_db[1]
    add_col_name = 'add_col'

    current_sqlite = [{'Field': 'id', 'Type': 'INT', 'Null': 'NO', 'Default': None},
                      {'Field': add_col_name, 'Type': 'VARCHAR(200)', 'Null': 'YES', 'Default': None}]

    current_mysql = [{'Field': 'id', 'Type': 'int(11)', 'Null': 'NO', 'Default': None},
                     {'Field': add_col_name, 'Type': 'varchar(200)', 'Null': 'YES', 'Default': None}]

    mysqlquery.create_table(TABLE_NAME, ['id INT NOT NULL'])
    sqlitequery.create_table(TABLE_NAME, ['id INT NOT NULL'])

    if TABLE_NAME in [i['Name'] for i in mysqlquery.show_tables()[0]]:
        mysqlquery.add_columns_by_sql(TABLE_NAME, [f'ADD `{add_col_name}` VARCHAR(200) NULL'])
        if mysqlquery.show_columns(TABLE_NAME)[0] == current_mysql:
            mysql_ok = True
        mysqlquery.custom_query(f"DROP TABLE {TABLE_NAME}")

    if TABLE_NAME in [i['Name'] for i in sqlitequery.show_tables()[0]]:
        sqlitequery.add_columns_by_sql(TABLE_NAME, [f'ADD `{add_col_name}` VARCHAR(200) NULL'])
        if sqlitequery.show_columns(TABLE_NAME)[0] == current_sqlite:
            sqlite_ok = True
        sqlitequery.custom_query(f"DROP TABLE {TABLE_NAME}")

    assert mysql_ok and sqlite_ok


def test_delete_columns(test_queries_db):
    mysql_ok = False
    sqlite_ok = False
    mysqlquery = test_queries_db[0]
    sqlitequery = test_queries_db[1]

    mysqlquery.create_table(TABLE_NAME, TABLE_COLUMNS)
    sqlitequery.create_table(TABLE_NAME, TABLE_COLUMNS)

    if TABLE_NAME in [i['Name'] for i in mysqlquery.show_tables()[0]]:
        mysqlquery.delete_columns(TABLE_NAME, ['test', 'test2'])
        if [i['Field'] for i in mysqlquery.show_columns(TABLE_NAME)[0]] == ['id']:
            mysql_ok = True
        mysqlquery.custom_query(f"DROP TABLE {TABLE_NAME}")

    if TABLE_NAME in [i['Name'] for i in sqlitequery.show_tables()[0]]:
        sqlitequery.delete_columns(TABLE_NAME, ['test', 'test2'])
        if [i['Field'] for i in sqlitequery.show_columns(TABLE_NAME)[0]] == ['id']:
            sqlite_ok = True
        sqlitequery.custom_query(f"DROP TABLE {TABLE_NAME}")

    assert mysql_ok and sqlite_ok


def test_add_and_delete_foreign_key(test_queries_db):
    mysql_ok = False
    sqlite_ok = False
    mysqlquery = test_queries_db[0]
    sqlitequery = test_queries_db[1]

    fk_table_name = 'fk_table'

    mysqlquery.create_table(TABLE_NAME, TABLE_COLUMNS)
    mysqlquery.create_table(fk_table_name, ['`id` INT NOT NULL AUTO_INCREMENT , PRIMARY KEY (`id`)'])
    sqlitequery.create_table(TABLE_NAME, ['id INT NOT NULL', 'test VARCHAR(100) NOT NULL'])
    sqlitequery.create_table(fk_table_name, ['`id` INTEGER NOT NULL PRIMARY KEY'])

    if TABLE_NAME in [i['Name'] for i in mysqlquery.show_tables()[0]]:
        mysqlquery.add_foreign_key(TABLE_NAME, 'test2', fk_table_name, 'id', ForeignAction.CASCADE.value,
                                   ForeignAction.RESTRICT.value)
        if mysqlquery.show_additional_fk_info(TABLE_NAME, 'test2')[0][0]['REFERENCED_TABLE_NAME'] == fk_table_name:
            if mysqlquery.show_fk_name('test2'):
                mysqlquery.delete_foreign_key(TABLE_NAME, mysqlquery.show_fk_name('test2')[0][0])
                if not mysqlquery.show_fk_name('test2')[0]:
                    mysql_ok = True
        mysqlquery.custom_query(f"DROP TABLE {TABLE_NAME}")
        mysqlquery.custom_query(f"DROP TABLE {fk_table_name}")

    if TABLE_NAME in [i['Name'] for i in sqlitequery.show_tables()[0]]:
        sqlitequery.add_foreign_key(TABLE_NAME, 'test2', fk_table_name, 'id', ForeignAction.CASCADE.value,
                                   ForeignAction.RESTRICT.value)
        if sqlitequery.show_additional_fk_info(TABLE_NAME, 'test2')[0][0]['REFERENCED_TABLE_NAME'] == fk_table_name:
            sqlitequery.delete_foreign_key(TABLE_NAME, sqlitequery.show_fk_name(TABLE_NAME)[0][0])
            if not sqlitequery.show_fk_name(TABLE_NAME)[0]:
                sqlite_ok = True
        sqlitequery.custom_query(f"DROP TABLE {TABLE_NAME}")
        sqlitequery.custom_query(f"DROP TABLE {fk_table_name}")

    assert mysql_ok and sqlite_ok


