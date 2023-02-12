import sqlite3


class SqliteConnect:
    def __init__(self, database):
        self.database = database

    def get_connect(self) -> sqlite3.connect:
        return sqlite3.connect(database=self.database)

    def query(self, query_string, dictionary=False):
        """
        Execute a database query. The connection is closed automatically.
        """
        try:
            with self.get_connect() as connect:
                try:
                    if dictionary:
                        connect.row_factory = self._dict_factory

                    cur = connect.cursor()
                    cur.execute(query_string)
                    result = []
                    for res in cur.fetchall():
                        if isinstance(res, dict):
                            result.append(res)
                        else:
                            if len(res) == 1:
                                result.append(res[0])
                            else:
                                result.append(res)
                    connect.commit()
                    return [result, query_string]
                except Exception as e:
                    raise e
                finally:
                    cur.close()
        except Exception as e:
            raise e

    def _dict_factory(self, cursor, row):
        """
        Convert query tresult to dict.
        """
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
