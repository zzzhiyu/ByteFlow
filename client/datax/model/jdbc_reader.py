class JDBCReaderConnection(object):
    def __init__(self, query_sqls: list, jdbc_urls: list):
        self.query_sqls = query_sqls
        self.jdbc_urls = jdbc_urls

    def to_dict(self):
        return {'querySql': self.query_sqls, 'jdbcUrl': self.jdbc_urls}


class JDBCReaderParameter(object):
    def __init__(self, username: str, password: str, split_pk: str, conn: JDBCReaderConnection):
        self.username = username
        self.password = password
        self.split_pk = split_pk
        self.conn = conn

    def to_dict(self):
        return {'username': self.username, 'password': self.password, 'splitPk': self.split_pk,
                'connection': [self.conn.to_dict()]}


class JDBCReader(object):
    def __init__(self, name: str, param: JDBCReaderParameter):
        self.name = name
        self.param = param

    def to_dict(self):
        return {'name': self.name, 'parameter': self.param.to_dict()}