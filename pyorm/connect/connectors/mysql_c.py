import mysql.connector


class MysqlConnect:
    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database

    def get_connect(self):
        return mysql.connector.connect(user=self.user, password=self.password, host=self.host, database=self.database)

    def query(self, query_string, dictionary=False):
        """
        Execute a database query. The connection is closed automatically.
        """
        try:
            connect = self.get_connect()
            with connect.cursor(dictionary=dictionary) as cur:
                cur.execute(query_string)
                result = []
                for res in cur:
                    if isinstance(res, dict):
                        result.append(res)
                    else:
                        if len(res) == 1:
                            result.append(res[0])
                        else:
                            result.append(res)
                return [result, query_string]
        except Exception as e:
            raise e
        finally:
            if connect.is_connected():
                connect.commit()
                connect.close()
