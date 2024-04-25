from typing import List

from datax.reader_writer.doris_writer.doris_writer import DorisWriter
from datax.model.jdbc_reader import JDBCReaderConnection, JDBCReaderParameter, JDBCReader
from datax.reader_writer.reader_writer import ReaderWriter
from model.column_info import ColumnInfo
from model.table_info import TableInfo


class SqlserverDoris(ReaderWriter):
    """
    Sqlserver的数据迁移到Doris
    """

    def __init__(self, reader_table_info: TableInfo, writer_table_info: TableInfo):
        ReaderWriter.__init__(self, reader_table_info, writer_table_info)

    def _get_select_columns(self) -> str:
        """
        拼接select语句
        :return:
        """
        doris_col_names = self.writer_table_info.get_pk_column_names() + self.writer_table_info.get_not_pk_column_names()
        sqlserver_col_name_dict = self.reader_table_info.to_column_dict()
        query_sql = 'select '
        for doris_col_name in doris_col_names:
            col_info = sqlserver_col_name_dict.get(doris_col_name)
            if col_info is None:
                if doris_col_name == "dt":
                    time_update_col_info = sqlserver_col_name_dict.get(
                        self.writer_table_info.kwargs.get("time_update_col"))
                    if 'int' in time_update_col_info.c_type:
                        query_sql += f"cast(dateadd(s, `{time_update_col_info.name}`, '1970-01-01 08:00:00') as date) as `{doris_col_name}`,"
                    else:
                        query_sql += f"cast(`{time_update_col_info.name}` as date) as `{doris_col_name}`,"
                elif doris_col_name == "loadtime":
                    query_sql += 'getdate() as `loadtime`,'
                elif doris_col_name == "server_id":
                    query_sql += '${server_id} as `server_id`,'
            elif col_info.c_type in ('binary', 'varbinary', 'blob'):
                query_sql += f"convert(varchar(max), `{doris_col_name}`, 1) as `{doris_col_name}`,"
            elif col_info.c_type == 'bit':
                query_sql += f'cast(`{doris_col_name}` as varchar) as `{doris_col_name}`,'
            elif 'name' in doris_col_name or 'remark' in doris_col_name or 'text' in doris_col_name:
                query_sql += (
                    f"left(`{doris_col_name}`, "
                    f"len(`{doris_col_name}`) - patindex(\'%[^\\\\0]%\', replace(reverse(`{doris_col_name}`), \'0\', \'_\')) + 1"
                    f") as `{doris_col_name}`,")
            else:
                query_sql += f'`{doris_col_name}`,'
        return query_sql[:-1]

    def _get_full_where_sql(self, update_col_infos: List[ColumnInfo]) -> str:
        """
        全量拉取，拼接where语句
        :param update_col_info:
        :return:
        """
        where_sql = ""
        filter_sql = []
        if self.writer_table_info.kwargs.get("table_mode") != "unique key":
            where_sql += " where "
            for update_col_info in update_col_infos:
                if 'timestamp' in update_col_info.c_type:
                    filter_sql.append(
                        f" `{update_col_info.name}` < datediff(s, '1970-01-01 08:00:00', dateadd(dd, datediff(dd, 0, getdate()), 0)) ")
                elif 'int' in update_col_info.c_type and update_col_info.name == 'statdate':
                    filter_sql.append(f" `{update_col_info.name}`< convert(int, format(getdate(), 'yyyyMMdd')) ")
                elif 'int' in update_col_info.c_type:
                    filter_sql.append(
                        f" `{update_col_info.name}` < datediff(s, '1970-01-01 08:00:00', dateadd(dd, datediff(dd, 0, getdate()), 0)) ")
                else:
                    filter_sql.append(f" `{update_col_info.name}` < dateadd(dd, datediff(dd, 0, getdate()), 0) ")
        return where_sql + " or ".join(filter_sql)

    def _get_increment_where_sql(self, update_col_infos: List[ColumnInfo]) -> str:
        """
        每日增量，拼接增量语句
        :param update_col_info:
        :return:
        """
        where_sql = " where "
        filter_sql = []
        for update_col_info in update_col_infos:
            if self.writer_table_info.table_name.split("_")[-1] in ("ri", "rf"):
                if 'timestamp' in update_col_info.c_type:
                    filter_sql.append(
                        f" `{update_col_info.name}` >= datediff(s, '1970-01-01 08:00:00', dateadd(n, -30, getdate())) ")
                elif 'int' in update_col_info.c_type:
                    filter_sql.append(
                        f" `{update_col_info.name}` >= datediff(s, '1970-01-01 08:00:00', dateadd(n, -30, getdate())) ")
                else:
                    filter_sql.append(f" `{update_col_info.name}` >= dateadd(n, -30, getdate()) ")
            else:
                if "timestamp" in update_col_info.c_type:
                    filter_sql.append(
                        f" (`{update_col_info.name}` >= datediff(s, '1970-01-01 08:00:00', dateadd(dd, datediff(dd, 0, getdate()), -1)) and"
                        f" `{update_col_info.name}` < datediff(s, '1970-01-01 08:00:00', dateadd(dd, datediff(dd, 0, getdate()), 0))) ")
                elif 'int' in update_col_info.c_type and update_col_info.name == 'statdate':
                    filter_sql.append(
                        f" (`{update_col_info.name}` >= convert(int, format(dateadd(day, -1, getdate()), 'yyyyMMdd')) and"
                        f" `{update_col_info.name}`< convert(int, format(getdate(), 'yyyyMMdd'))) ")
                elif 'int' in update_col_info.c_type:
                    filter_sql.append(
                        f" (`{update_col_info.name}` >= datediff(s, '1970-01-01 08:00:00', dateadd(dd, datediff(dd, 0, getdate()), -1)) and"
                        f" `{update_col_info.name}` < datediff(s, '1970-01-01 08:00:00', dateadd(dd, datediff(dd, 0, getdate()), 0))) ")
                else:
                    filter_sql.append(
                        f" (`{update_col_info.name}` >= dateadd(dd, datediff(dd, 0, getdate()), -1) and"
                        f" `{update_col_info.name}` < dateadd(dd, datediff(dd, 0, getdate()), 0)) ")
        return where_sql + " or ".join(filter_sql)

    def _get_query_sql(self, is_full: bool) -> str:
        query_sql = self._get_select_columns()
        query_sql += f" from {self.reader_table_info.table_name} with(nolock) "
        time_update_col = self.writer_table_info.kwargs.get("time_update_col")
        if time_update_col:
            time_update_cols = time_update_col.split(",")
            update_col_infos = []
            for col in time_update_cols:
                update_col_info = self.writer_table_info.to_column_dict().get(col)
                update_col_infos.append(update_col_info)
            if is_full:
                query_sql += self._get_full_where_sql(update_col_infos)
            else:
                query_sql += self._get_increment_where_sql(update_col_infos)
        return (query_sql + " ;").replace('`', '"')

    # overwrite
    def get_reader(self, is_full: bool) -> dict:
        query_sqls = [self._get_query_sql(is_full)]  # 获取数据提取sql
        jdbc_urls = ['jdbc:sqlserver://{host}:{port};DatabaseName={db_name}']  # 获取jdbc_urls
        connection = JDBCReaderConnection(query_sqls, jdbc_urls)  # 获取connection
        username = self.reader_table_info.user
        password = self.reader_table_info.passwd
        parameter = JDBCReaderParameter(username, password, "", connection)
        return JDBCReader("sqlserverreader", parameter).to_dict()

    # overwrite
    def get_writer(self) -> dict:
        return DorisWriter.set_writer(self.writer_table_info)
