from datetime import datetime, timedelta
from typing import List
import pymysql

from reader.reader import Reader
from ds.models.data_source import DataSource
from model.column_info import ColumnInfo
from model.column_type import ColumnType
from model.data_source_infos import DataSourceInfos
from model.table_info import TableInfo
from util import console


class MysqlReader(Reader):
    """
    实现mysql_reader
    """
    # 获取表列信息sql
    table_column_info_sql = """
                SELECT  COLUMN_NAME AS 'column_name',
                        DATA_TYPE AS 'column_type',
                        CHARACTER_MAXIMUM_LENGTH as 'column_length',       
                        ORDINAL_POSITION AS 'column_position',
                        if(COLUMN_KEY='PRI', 1, 0) as 'is_pk',
                        COLUMN_COMMENT AS 'column_comment'
                FROM
                    information_schema.`COLUMNS`
                WHERE
                    TABLE_SCHEMA = "{0}" and TABLE_NAME  = "{1}"
                ORDER BY
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    ORDINAL_POSITION"""

    # 获取表存储大小sql(单位为kb)
    table_storage_kb_sql = """
                SELECT round(sum(data_length/1024) + sum(index_length/1024),2)
                FROM `information_schema`.`tables` 
                WHERE `table_name` = "{0}" and ( {1} )"""

    # 获取数据库当前时间
    db_datetime_sql = """SELECT NOW();"""

    def __init__(self, data_source_infos: DataSourceInfos, data_source: DataSource, src_engine: str):
        Reader.__init__(self, data_source_infos, data_source, src_engine)
        self.conf_conn = None
        self.conf_cur = None

    # overwrite
    def open(self):
        try:
            if self.conf_conn or self.conf_cur:
                self.close()
            self.conf_conn = pymysql.connect(
                host=self.data_source.host,
                port=self.data_source.port,
                user=self.data_source.user,
                password=self.data_source.passwd,
                db=self.data_source.db_name
            )
            self.conf_cur = self.conf_conn.cursor()
        except Exception as err:
            raise Exception(f"连接mysql数据库失败:{err.__str__()}") from err

    # overwrite
    def _get_table_name(self) -> str:
        table_name = console.input_value("请输入要读取的源表名[可以输入db_name.table_name, 这样会修改库名称]")
        if len(db_table := table_name.split(".")) == 2:
            self.data_source_infos.set_db_name(db_table[0])  # 修改库
        return table_name

    # overwrite
    def _set_common_type(self, column_infos: List[ColumnInfo]) -> List[ColumnInfo]:
        for column_info in column_infos:
            if column_info.c_type == 'tinyint':
                column_info.common_type = ColumnType.SHORT
            elif column_info.c_type in ['smallint', 'mediumint']:
                column_info.common_type = ColumnType.INT
            elif column_info.c_type in ['int', 'bigint', 'float', 'double', 'decimal', 'date', 'datetime',
                                             'timestamp', 'char', 'varchar']:
                column_info.common_type = column_info.c_type
            else:
                column_info.common_type = ColumnType.STRING
        return column_infos

    # overwrite
    def _get_table_storage_size_kb(self, table_name: str) -> float:
        all_size_kb = 0
        # 对于在同一个库上面的表可以一起计算
        same_data_source_infos = self.data_source_infos.merge_same_data_source_infos()
        for same_data_source in same_data_source_infos:
            head_data_source = same_data_source[0]
            # 多个服需要修改连接参数,创建新对象
            mysql_reader = (
                self if len(same_data_source_infos) == 1 else
                MysqlReader(self.data_source_infos, head_data_source, self.src_engine)
            )
            try:
                # 存在多个服务，需要进行连接
                if len(same_data_source_infos) > 1:
                    mysql_reader.open()
                db_names: List[str] = same_data_source[1]
                # 计算同一个服务所有表的大小
                db_name_filter = " or ".join([f'`table_schema` = "{db_name}"' for db_name in db_names])
                sql = mysql_reader.table_storage_kb_sql.format(table_name, db_name_filter)
                mysql_reader.conf_cur.execute(sql)
                size_kb = mysql_reader.conf_cur.fetchone()
                all_size_kb += int(size_kb[0])
            except Exception as err:
                raise Exception(f"MysqlReader计算{table_name}的大小失败 {head_data_source.host} {head_data_source.port}"
                                f":{err.__str__()}") from err
            finally:
                # 多个服断开连接
                if len(same_data_source_infos) > 1:
                    mysql_reader.close()
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
    def get_table_info(self, table_name: str = None) -> TableInfo:
        if not table_name:
            table_name = self._get_table_name()
        r_table_name = table_name.split(".")[-1]
        sql = self.table_column_info_sql.format(self.data_source.db_name, r_table_name)
        try:
            self.conf_cur.execute(sql)
            rows = self.conf_cur.fetchall()
            if not rows:
                raise Exception("没有获取到表列信息")
        except Exception as err:
            raise Exception(f"{self.data_source.host} {self.data_source.port}: {err.__str__()}")
        storage_size_kb = self._get_table_storage_size_kb(r_table_name)
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
