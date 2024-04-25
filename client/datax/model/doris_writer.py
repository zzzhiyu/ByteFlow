from typing import List


class DorisWriterConnection(object):
    def __init__(self, jdbc_url: str, database: str, table: List):
        self.jdbc_url = jdbc_url
        self.database = database
        self.table = table

    def to_dict(self):
        return {'jdbcUrl': self.jdbc_url, 'selectedDatabase': self.database, 'table': self.table}


class DorisWriterLoadProps(object):
    def __init__(self):
        self.format = 'json'
        self.strip_outer_array = True

    def to_dict(self):
        return {'format': self.format, 'strip_outer_array': self.strip_outer_array}


class DorisWriterParameter(object):
    def __init__(self, load_url: List, column: List, username: str, password: str, post_sql: List, pre_sql: List,
                 flush_interval: int, conn: DorisWriterConnection, load_props: DorisWriterLoadProps):
        self.load_url = load_url
        self.column = column
        self.username = username
        self.password = password
        self.post_sql = post_sql
        self.pre_sql = pre_sql
        self.flush_interval = flush_interval
        self.conn = conn
        self.load_props = load_props

    def to_dict(self):
        return {'loadUrl': self.load_url, 'column': self.column, 'username': self.username, 'password': self.password,
                'postSql': self.post_sql, 'preSql': self.pre_sql, 'flushInterval': self.flush_interval,
                'connection': [self.conn.to_dict()], 'loadProps': self.load_props.to_dict()}


class DorisWriterModel(object):
    def __init__(self, name: str, param: DorisWriterParameter):
        self.name = name
        self.param = param

    def to_dict(self):
        return {'name': self.name, 'parameter': self.param.to_dict()}
