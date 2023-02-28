import pytest
from pyorm.connect.connection import Connect, DbType


@pytest.fixture(scope='session', autouse=True)
def test_connect_db():
    mysql_conn = Connect(DbType.MYSQL, user='uwine', password='1111', host='localhost', database='pyorm')
    sqlite_conn = Connect(DbType.SQLITE, database='q.db')
    return mysql_conn, sqlite_conn


def test_connection_ok(test_connect_db):
    mysql = test_connect_db[0].query("select 'Echo' AS '';")
    sqlite = test_connect_db[1].query("select 'Echo' AS '';")
    assert isinstance(mysql, list) and isinstance(sqlite, list)
