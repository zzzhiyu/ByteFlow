import datetime
from typing import List

import pymysql

from conf import config
from ds.crud.doris_table_conf import select_doris_table_conf
from model.column_info import ColumnInfo
from model.column_type import ColumnType
from util import console
from model.table_info import TableInfo
from writer.writer import Writer


class DorisWriter(Writer):
    """
    实现doris_writer
    """

    def __init__(self, dst_engine: str):
        Writer.__init__(self, dst_engine)
        self.conf_conn = None
        self.conf_cur = None

    # overwrite
    def open(self):
        try:
            if self.conf_conn or self.conf_cur:
                self.close()
            self.conf_conn = pymysql.connect(
                host=config.doris_host,
                port=config.doris_port,
                user=config.doris_user,
                password=config.doris_passwd,
                db=config.doris_db
            )
            self.conf_cur = self.conf_conn.cursor()
        except Exception as err:
            raise Exception(f"连接doris数据库失败:{err.__str__()}") from err

    # overwrite
    def _get_table_conf(self, reader_table_info: TableInfo) -> dict[str]:
        """
        获取doris表的配置信息
        :param reader_table_info:
        :return:
        """
        table_conf = select_doris_table_conf(reader_table_info.table_name.split(".")[-1])
        if table_conf:
            # 打印相应的表配置信息
            table_conf.print_info(reader_table_info.source_name, reader_table_info.db_type)
            return table_conf.__dict__
        return {}

    def _get_table_name(self, is_partition: bool) -> str:
        """
        生成doris的表名
        :param is_partition:
        :return:
        """
        src_table_name = self.reader_table_info.table_name.split(".")[-1]
        table_name = (f"{self.reader_table_info.source_name}_{self.reader_table_info.db_type}_{src_table_name}"
                      f"_{'di' if is_partition else 'df'}")
        table_name_modify = console.input_value_and_check_bool(
            f"doris的表名自动配置为: {table_name}, 是否需要更改(假如配置实时表需要把表的后缀配置为ri/rf)")
        if table_name_modify:
            table_name = console.input_value("输入表名")
        return table_name

    def _get_table_column_infos(self, server_id_pk: bool = None) -> List[ColumnInfo]:
        """
        转化doris的列属性相关信息
        :param server_id_pk:
        :return:
        """
        column_infos = []
        # 关系数据库列转换
        for col in self.reader_table_info.columns:
            if col.common_type == ColumnType.SHORT:
                c_type = "smallint"
            elif col.common_type in (
                    ColumnType.INT, ColumnType.BIGINT, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.DECIMAL):
                c_type = col.common_type
            elif col.common_type in (ColumnType.CHAR, ColumnType.VARCHAR):
                c_type = "varchar"
            elif col.common_type in (ColumnType.TIMESTAMP, ColumnType.DATETIME):
                c_type = "datetimev2(3)"
            elif col.common_type == ColumnType.DATE:
                c_type = "datev2"
            else:
                c_type = "string"
            column_info = ColumnInfo(col.name, c_type, col.c_len, col.index, col.is_pk, col.comment, col.common_type)
            column_infos.append(column_info)
        index = len(self.reader_table_info.columns)
        if not self.is_auto_input:
            server_id_pk = console.input_value_and_check_bool("是否增加数据源中的server_id为主键")
        server_id = ColumnInfo('server_id', 'int', 4, index + 1, server_id_pk, '服务器id', ColumnType.INT)
        column_infos.insert(0, server_id)
        load_time = ColumnInfo('loadtime', 'datetimev2(3)', 8, index + 2, False, '数据入库时间', ColumnType.DATETIME)
        column_infos.append(load_time)
        return column_infos

    def _get_table_mode(self, column_infos: List[ColumnInfo], is_partition: bool, table_name: str) -> str:
        """
        生产doris table_mode
        :param column_infos:
        :param is_partition:
        :return:
        """
        if is_partition:
            table_mode = "duplicate key"
            if table_name.split("_")[-1] in ("ri", "rf"):
                table_mode = "unique key"
        else:
            set_duplicate_key = console.input_value_and_check_bool("是否设置该表为duplicate key表(注意: duplicate key表不能进行主键去重)")
            if set_duplicate_key:
                table_mode = "duplicate key"
            else:
                table_mode = "unique key"
                if len(self.reader_table_info.get_pk_columns()) == 0:
                    key_column_names = console.input_values_and_check_contain(
                        f"源表没有没有primary key字段, 请设置doris的unique key, 请选择如下列{ColumnInfo.get_column_names(column_infos)}",
                        ColumnInfo.get_column_names(column_infos))
                    doris_col_dict = ColumnInfo.to_column_dict(column_infos)
                    for col_name in key_column_names:
                        column_info = doris_col_dict.get(col_name)
                        column_info.is_pk = True
        return table_mode

    @staticmethod
    def _set_hash_columns(column_infos: List[ColumnInfo], hash_bucket_col: List[str]):
        """
        修改hash字段的属性
        :param column_infos:
        :param hash_bucket_col:
        :param time_update_col:
        :return:
        """
        doris_col_dict = ColumnInfo.to_column_dict(column_infos)
        for index, bucket_col in enumerate(hash_bucket_col):
            column = doris_col_dict[bucket_col]
            column.is_pk = True
            # double和float类型需要转化为decimalv3(27, 6)
            if column.c_type in ('double', 'float'):
                column.c_type = 'decimalv3(27, 6)'
            # pk值重新排序
            column_infos.remove(column)
            column_infos.insert(index, column)

    @staticmethod
    def _set_dt_columns(column_infos: List[ColumnInfo], time_unit: str, time_update_col: str, table_mode: str,
                        is_partition: bool):
        """
        增加dt字段
        :param column_infos:
        :param time_unit:
        :param time_update_col:
        :param table_mode:
        :param is_partition:
        :return:
        """
        # 分区表都是按照时间进行分区
        if time_update_col and "," not in time_update_col:
            if is_partition or table_mode != "unique key":
                dt = ColumnInfo('dt', 'DATEV2', 8, len(column_infos), True, "分区字段", ColumnType.DATE)
                if time_unit == 'DAY':
                    column_infos.insert(len(ColumnInfo.get_pk_column_names(column_infos)), dt)
                else:
                    column_infos.insert(0, dt)
            else:
                dt = ColumnInfo('dt', 'DATEV2', 8, len(column_infos), False, "分区字段", ColumnType.DATE)
                column_infos.insert(len(ColumnInfo.get_pk_column_names(column_infos)), dt)

    def _get_partition_time_unit(self, is_partition: bool) -> str:
        """
        获取分区间隔
        :return:
        """
        if not is_partition:
            return ""
        time_unit_dict = {"1": "DAY", "2": "WEEK", "3": "MONTH", "DAY": "DAY", "WEEK": "WEEK", "MONTH": "MONTH"}
        time_unit = 'DAY'
        set_time_unit = console.input_value_and_check_bool(
            f'数据量为:{self.reader_table_info.get_storage_size_gb()}G, 默认按DAY进行分区(数据量小可以按WEEK,MONTH),是否重新设置')
        if set_time_unit:
            time_unit = console.input_value_and_check_contain('时间单元1.DAY, 2.WEEK, 3.MONTH, 请选择',
                                                              ['1', '2', '3', 'DAY', 'WEEK', 'MONTH'])
        return time_unit_dict.get(time_unit.upper())

    @staticmethod
    def _get_time_update_col(column_infos: List[ColumnInfo]) -> str:
        time_update_col = ""
        if date_column_names := ColumnInfo.get_date_column_names(column_infos):
            is_day_pull = console.input_value_and_check_bool("是否是增量拉取")
            if is_day_pull:
                time_update_col = console.input_values_and_check_contain(
                    f"请选择表的时间字段, 请选择如下列{date_column_names}", date_column_names)
        return ','.join(time_update_col)

    # overwrite
    def get_table_infos(self) -> TableInfo:
        if self.is_auto_input:
            self.is_auto_input = console.input_value_and_check_bool("该表的默认配置信息如上,是否进行自动配置")
        if self.is_auto_input:  # 不需要手动配置表
            is_partition: bool = self.table_conf.get("is_partition")  # 是否创建分区表
            table_name = (
                f"{self.reader_table_info.source_name}_{self.reader_table_info.db_type}_"
                f"{self.table_conf.get('table_name')}_{'di' if is_partition else 'df'}").lower()  # 设置表名
            column_infos: List[ColumnInfo] = self._get_table_column_infos(self.table_conf.get("server_id_pk"))  # 解析出列属性
            table_mode = self.table_conf.get("table_mode").lower()  # 设置table_mode [unique key or duplicate key]
            partition_col = self.table_conf.get("partition_col").lower()  # 设置分区列
            time_update_col = self.table_conf.get("time_update_col").lower()  # 设置时间更新列
            hash_bucket_col: List[str] = self.table_conf.get("hash_bucket_col").lower().split(",")  # 设置分桶列
            bloom_filter_col = self.table_conf.get("bloom_filter_col").lower()  # 设置布隆过滤列
            time_unit = self.table_conf.get("time_unit").upper()  # 设置分桶时间单元 DAY, WEEK, MONTH
            bucket_num = self.table_conf.get("bucket_num")  # 设置分桶数量
        else:  # 需要手动配置表
            is_partition: bool = console.input_value_and_check_bool(
                f"该表的全部数据量大约为{self.reader_table_info.get_storage_size_gb()}G, 请确认是否建立分区表")
            table_name = self._get_table_name(is_partition)
            column_infos: List[ColumnInfo] = self._get_table_column_infos()
            table_mode = self._get_table_mode(column_infos, is_partition, table_name)
            # 确定分区列和更新列
            if is_partition:
                partition_col = console.input_value_and_check_contain("请选择分区表的分区字段",
                                                                      ColumnInfo.get_date_column_names(column_infos))
                time_update_col = partition_col
            else:
                partition_col = ""
                time_update_col = DorisWriter._get_time_update_col(column_infos)
            # 确定hash列
            hash_bucket_col: List[str] = console.input_values_and_check_contain(
                f"原表的pk字段:{','.join(ColumnInfo.get_pk_column_names(column_infos))}, 请从该表已有的字段 "
                f"{ColumnInfo.get_column_names(column_infos)} 中选出相应的分桶字段", ColumnInfo.get_column_names(column_infos))
            # 是否进行布隆过滤
            bloom_filter_col = ""
            time_unit = self._get_partition_time_unit(is_partition)
            bucket_num = console.input_value_and_check_positive_integer(
                f"源表数据量为: {self.reader_table_info.get_storage_size_gb()}G, 请输入分桶数量")
        # 不是分区表, bucket_num和存储大小进行比较
        if not is_partition:
            bucket_num = max(int(self.reader_table_info.get_storage_size_gb() * 2), bucket_num)
        # 将hash_bucket_col设置为primary key
        DorisWriter._set_hash_columns(column_infos, hash_bucket_col)
        # 增加dt字段
        DorisWriter._set_dt_columns(column_infos, time_unit, time_update_col, table_mode, is_partition)
        # 假如是手动输入最后需要确认是否进行布隆过滤
        if not self.is_auto_input and is_partition and console.input_value_and_check_bool("是否设置布隆过滤列"):
            bloom_filter_col_list = console.input_values_and_check_contain(
                f"请选择相应的列进行布隆过滤, 请选择如下列{ColumnInfo.get_pk_column_names(column_infos)}",
                ColumnInfo.get_pk_column_names(column_infos)
            )
            bloom_filter_col = ",".join(bloom_filter_col_list)
        return TableInfo(source_name=self.reader_table_info.source_name, db_type=self.reader_table_info.db_type,
                         user=config.doris_user, passwd=config.doris_passwd, db_name=config.doris_db,
                         table_name=table_name, columns=column_infos, table_mode=table_mode,
                         is_partition=is_partition, partition_col=partition_col, time_update_col=time_update_col,
                         hash_bucket_col=hash_bucket_col, bloom_filter_col=bloom_filter_col, time_unit=time_unit,
                         bucket_num=bucket_num, replication_num=3)

    @staticmethod
    def _get_create_table_sql(writer_table: TableInfo) -> str:
        """
        配置doris的建表语句
        :param writer_table:
        :return:
        """
        ddl_sql = f'create table {writer_table.db_name}.{writer_table.table_name}\n(\n'
        all_columns = writer_table.get_pk_columns() + writer_table.get_not_pk_columns()
        for column in all_columns:
            ddl_sql += f'    `{column.name}` {column.get_column_type()} '
            if 'char' in column.c_type or column.c_type == 'string':
                ddl_sql += 'default "" '
            ddl_sql += f'comment "{column.comment}",\n'
        ddl_sql = ddl_sql[:len(ddl_sql) - 2] + '\n)\nengine=olap\n'
        pk_column_names = writer_table.get_pk_column_names()
        key_col = ','.join(f'`{pk_column_name}`' for pk_column_name in pk_column_names)
        ddl_sql += f'{writer_table.kwargs.get("table_mode")}({key_col})\n'
        hash_cols = ','.join({f'`{hash_col}`' for hash_col in writer_table.kwargs.get("hash_bucket_col")})
        if writer_table.kwargs.get("is_partition"):
            ddl_sql += f'partition by range(`dt`)()\n'
            ddl_sql += f'distributed by hash({hash_cols}) buckets {writer_table.kwargs.get("bucket_num")}\nproperties(\n'
            ddl_sql += f'    "replication_num" = "{writer_table.kwargs.get("replication_num")}",\n'
            if bloom_filter_col := writer_table.kwargs.get("bloom_filter_col"):
                ddl_sql += f'    "bloom_filter_columns"="{bloom_filter_col}",\n'
            ddl_sql += f'    "dynamic_partition.enable"="TRUE",\n'
            ddl_sql += f'    "dynamic_partition.time_unit"="{writer_table.kwargs.get("time_unit")}",\n'
            ddl_sql += f'    "dynamic_partition.time_zone"="Asia/Shanghai",\n'
            ddl_sql += f'    "dynamic_partition.end"="1",\n'
            ddl_sql += f'    "dynamic_partition.prefix"="p",\n'
            ddl_sql += f'    "dynamic_partition.buckets"="{writer_table.kwargs.get("bucket_num")}",\n'
            ddl_sql += f'    "dynamic_partition.replication_allocation" = "tag.location.default: {writer_table.kwargs.get("replication_num")}",\n'
            ddl_sql += f'    "light_schema_change"="true"\n);\n'
        else:
            ddl_sql += f'distributed by hash({hash_cols}) buckets {writer_table.kwargs.get("bucket_num")}\nproperties(\n'
            ddl_sql += f'    "replication_num" = "{writer_table.kwargs.get("replication_num")}",\n'
            if writer_table.kwargs.get("table_mode") == 'unique key':
                ddl_sql += f'    "enable_unique_key_merge_on_write" = "true",\n'
            ddl_sql += f'    "light_schema_change"="true"\n);\n'
        return ddl_sql

    def _create_ago_partitions(self, writer_table: TableInfo, hash_cols: str) -> List[str]:
        """
        创建历史分区
        :param writer_table:
        :param hash_cols:
        :return:
        """
        ago_partitions = []
        current_date = datetime.datetime.now()
        time_unit = writer_table.kwargs.get("time_unit")
        if time_unit == 'WEEK':
            current_date = current_date - datetime.timedelta(days=current_date.weekday())
        elif time_unit == "MONTH":
            current_date = datetime.datetime(current_date.year, current_date.month, 1)
        today_date = datetime.datetime(current_date.year, current_date.month, current_date.day).strftime("%Y-%m-%d")
        ago_bucket_num = max(self.reader_table_info.get_storage_size_gb() / 2, 1)  # 一个桶存2G数据
        ago_partitions.append(
            f"""ALTER TABLE {writer_table.db_name}.{writer_table.table_name} SET ("dynamic_partition.enable" = "false");\n""")
        bloom_filter = ""
        if bloom_filter_col := writer_table.kwargs.get("bloom_filter_col"):
            bloom_filter = f', "bloom_filter_columns"="{bloom_filter_col}"'
        ago_partitions.append(
            f"""ALTER TABLE {writer_table.db_name}.{writer_table.table_name}  ADD PARTITION ago VALUES LESS THAN ("{today_date}") ("storage_policy" = "doris_oss_60d" , "replication_num"="1" {bloom_filter}) DISTRIBUTED BY HASH({hash_cols}) BUCKETS {int(ago_bucket_num)};\n""")
        ago_partitions.append(
            f"""ALTER TABLE {writer_table.db_name}.{writer_table.table_name} SET ("dynamic_partition.enable" = "true");\n""")
        return ago_partitions

    # overwrite
    def create_table(self, writer_table: TableInfo):
        # 建表
        ddl = DorisWriter._get_create_table_sql(writer_table)
        self.conf_cur.execute(ddl)
        self.conf_conn.commit()
        print(f'\n建表成功!!!建表语句为:\n{ddl}')
        # 分区表需要建立历史分区
        if writer_table.kwargs.get("is_partition"):
            hash_cols = ','.join({f'`{hash_col}`' for hash_col in writer_table.kwargs.get("hash_bucket_col")})
            ago_partitions = self._create_ago_partitions(writer_table, hash_cols)
            for partition_sql in ago_partitions:
                self.conf_cur.execute(partition_sql)
            self.conf_conn.commit()
            print(f"增加了ago分区,语句为:\n{''.join(ago_partitions)}")

    # overwrite
    def close(self):
        if self.conf_cur:
            self.conf_cur.close()
        if self.conf_conn:
            self.conf_conn.close()
        self.conf_cur = None
        self.conf_conn = None
