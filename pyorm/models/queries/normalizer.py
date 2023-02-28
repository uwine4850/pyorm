from abc import ABCMeta, abstractmethod
from pyorm.connect.connection import DbType


class QueryResultNormalizerAbc(metaclass=ABCMeta):
    @abstractmethod
    def show_columns(self, query_res: list):
        pass

    @abstractmethod
    def show_tables(self, query_res: list):
        pass


class QueryResultNormalizer(QueryResultNormalizerAbc):
    def __init__(self, db_type: DbType):
        self.db_type = db_type

    def show_columns(self, query_res: list):
        # {'Field': 'id', 'Type': 'int(11)', 'Null': 'NO', 'Default': None,}
        qres = []
        match self.db_type:
            case DbType.SQLITE:
                for res in query_res[0]:
                    if res['notnull']:
                        null = 'NO'
                    else:
                        null = 'YES'
                    qres.append({'Field': res['name'], 'Type': res['type'], 'Null': null, 'Default': res['dflt_value']})
            case DbType.MYSQL:
                for res in query_res[0]:
                    qres.append({'Field': res['Field'], 'Type': res['Type'], 'Null': res['Null'],
                                 'Default': res['Default']})
        return [qres, query_res[1]]

    def show_tables(self, query_res: list):
        # [{'name': 'test'}]
        qres = []
        if query_res[0]:
            if isinstance(query_res[0][0], list):
                return query_res
            else:
                match self.db_type:
                    case DbType.SQLITE:
                        for res in query_res[0]:
                            qres.append({'Name': res['name']})
                    case DbType.MYSQL:
                        for res in query_res[0]:
                            for k in res.keys():
                                qres.append({'Name': res[k]})
                return [qres, query_res[1]]
        else:
            return [qres, query_res[1]]

    def show_additional_fk_info(self, res: list[dict]):
        # {'DELETE_RULE': '', 'UPDATE_RULE': '', 'TABLE_NAME': '', 'REFERENCED_TABLE_NAME': ''}
        qres = []
        match self.db_type:
            case DbType.MYSQL:
                return res
            case DbType.SQLITE:
                for i in res[0]:
                    qres.append({'DELETE_RULE': i['on_delete'], 'UPDATE_RULE': i['on_update'],
                                 'REFERENCED_TABLE_NAME': i['table']})
                return [qres, res[1]]
