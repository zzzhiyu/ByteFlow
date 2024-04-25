from datetime import datetime, timedelta
from typing import List
import pymssql

from reader.reader import Reader
from ds.models.data_source import DataSource
from model.column_info import ColumnInfo
from model.column_type import ColumnType
from model.data_source_infos import DataSourceInfos
from model.table_info import TableInfo
from util import console


class SqlserverReader(Reader):
    """
    实现sqlserver_reader
    """
    # 获取表列信息sql
    table_column_info_sql = """               
            SELECT a.name as'column_name',
                   b.name as 'column_type',
                   a.length as 'column_length',
                   a.colorder as 'column_position',
                   (CASE WHEN (SELECT COUNT(*)
                               FROM sysobjects
                               WHERE (name in
                                         (SELECT name
                                          FROM sysindexes
                                          WHERE (id = a.id) AND (indid in
                                                  (SELECT indid
                                                   FROM sysindexkeys
                                                   WHERE (id = a.id) AND (colid in
                                                           (SELECT colid
                                                            FROM syscolumns
                                                            WHERE (id = a.id) AND (name = a.name))))))) AND (xtype = 'PK'))>0 
                          THEN 1 
                          ELSE 0 END) as 'is_pk',
                   ISNULL(convert(nvarchar(max), g.[value]), '') AS 'column_comment'
            FROM syscolumns a
                 LEFT JOIN systypes b on a.xtype=b.xusertype
                 INNER JOIN sysobjects d on a.id=d.id AND d.xtype='U' AND d.name<>'dtproperties'
                 LEFT JOIN syscomments e on a.cdefault=e.id
                 LEFT JOIN sys.extended_properties g on A.ID=G.major_id AND A.COLID=G.minor_id
                 LEFT JOIN INFORMATION_SCHEMA.TABLES h on d.name = h.TABLE_NAME
            WHERE h.TABLE_SCHEMA = '{0}' and d.name = '{1}'
            ORDER BY
                    h.TABLE_SCHEMA,d.name,a.colorder;"""

    # 获取表存储大小sql(单位为kb)
    table_storage_kb_sql = """exec sp_spaceused '{0}.{1}';"""

    # 获取数据库当前时间
    db_datetime_sql = """SELECT GETDATE();"""

    def __init__(self, data_source_infos: DataSourceInfos, data_source: DataSource, src_engine: str):
        Reader.__init__(self, data_source_infos, data_source, src_engine)
        self.conf_conn = None
        self.conf_cur = None

    # overwrite
    def open(self):
        try:
            if self.conf_conn or self.conf_cur:
                self.close()
            self.conf_conn = pymssql.connect(
                host=self.data_source.host,
                port=self.data_source.port,
                user=self.data_source.user,
                password=self.data_source.passwd,
                database=self.data_source.db_name
            )
            self.conf_cur = self.conf_conn.cursor()
        except Exception as err:
            raise Exception(f"连接sqlserver数据库失败:{err.__str__()}") from err

    # overwrite
    def _get_table_name(self) -> str:
        # 获取schema和源表名称
        schema = console.input_value("请输入当前表所属schema[可以输入db_name.schema_name, 这样会修改库名称]")
        r_table_name = console.input_value("请输入要读取的源表名")
        if len(db_schema := schema.split(".")) == 2:
            self.data_source_infos.set_db_name(db_schema[0])  # 修改库
        return f"{schema}.{r_table_name}"

    # overwrite
    def _get_table_storage_size_kb(self, table_name: str) -> float:
        all_size_kb = 0
        # 对于在同一个库上面的表可以一起计算
        same_data_source_infos = self.data_source_infos.merge_same_data_source_infos()
        for same_data_source in same_data_source_infos:
            head_data_source = same_data_source[0]
            sqlserver_reader = (
                self if len(same_data_source_infos) == 1 else
                SqlserverReader(self.data_source_infos, head_data_source, self.src_engine)
            )
            try:
                # 存在新服，需要进行连接
                if len(same_data_source_infos) > 1:
                    sqlserver_reader.open()
                db_names: List[str] = same_data_source[1]
                # 计算同一个服务每张表的大小并进行累加
                for db_name in db_names:
                    sql = sqlserver_reader.table_storage_kb_sql.format(db_name, table_name)
                    sqlserver_reader.conf_cur.execute(sql)
                    info = sqlserver_reader.conf_cur.fetchone()
                    size_kb = float(str(info[2]).lower().replace('kb', '').replace(' ', ''))
                    all_size_kb += size_kb
            except Exception as err:
                raise Exception(f"SqlserverReader计算{table_name}的大小失败 {head_data_source.host} {head_data_source.port}"
                                f":{err.__str__()}") from err
            finally:
                if len(same_data_source_infos) > 1:
                    sqlserver_reader.close()
        return all_size_kb

    # overwrite
    def _get_interval_unit(self) -> int:
        self.conf_cur.execute(self.db_datetime_sql)
        info = self.conf_cur.fetchone()
        db_datetime = info[0]
        now_datetime = datetime.now() + timedelta(minutes=10)
        sub_datetime = now_datetime - db_datetime
        return int(sub_datetime.total_seconds() / 3600)

    # overwrite
    def _set_common_type(self, column_infos: List[ColumnInfo]) -> List[ColumnInfo]:
        for column_info in column_infos:
            if column_info.c_type == 'tinyint':
                column_info.common_type = ColumnType.SHORT
            elif column_info.c_type in ['smallint', 'int']:
                column_info.common_type = ColumnType.INT
            elif column_info.c_type == 'float':
                column_info.common_type = ColumnType.DOUBLE
            elif column_info.c_type in ['char', 'varchar', 'nchar', 'nvarchar']:
                if column_info.c_len < 0:
                    column_info.c_len = 8000
                column_info.c_len *= 3
                column_info.common_type = ColumnType.VARCHAR
            elif column_info.c_type == 'uniqueidentifier':
                column_info.common_type = ColumnType.VARCHAR
                column_info.c_len *= 4
            elif column_info.c_type in ['int', 'bigint', 'decimal', 'timestamp', 'datetime', "date"]:
                column_info.common_type = column_info.c_type
            else:
                column_info.common_type = ColumnType.STRING
        return column_infos

    # overwrite
    def get_table_info(self, table_name: str = None) -> TableInfo:
        if not table_name:
            table_name = self._get_table_name()
        schema, r_table_name = table_name.split(".")[-2:]
        try:
            sql = self.table_column_info_sql.format(schema, r_table_name)
            self.conf_cur.execute(sql)
            rows = self.conf_cur.fetchall()
            if not rows:
                raise Exception("没有获取到表列信息")
        except Exception as err:
            raise Exception(f"{self.data_source.host} {self.data_source.port}: {err.__str__()}")
        storage_size_kb = self._get_table_storage_size_kb(f"{schema}.{r_table_name}")
        interval_unit = self._get_interval_unit()
        column_infos = self._rows_to_column_infos(rows)
        return TableInfo(source_name=self.data_source.source_name, db_type=self.data_source.db_type,
                         user=self.data_source.user, passwd=self.data_source.passwd,db_name=self.data_source.db_name,
                         table_name=table_name, columns=column_infos, storage_size_kb=storage_size_kb,
                         interval_unit=interval_unit)

    # overwrite
    def close(self):
        if self.conf_cur:
            self.conf_cur.close()
        if self.conf_conn:
            self.conf_conn.close()
        self.conf_cur = None
        self.conf_conn = None
