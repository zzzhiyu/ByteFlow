import os.path
from typing import List

from ds.models.data_source import DataSource
from model.data_source_infos import DataSourceInfos
from model.column_info import ColumnInfo
from model.table_info import TableInfo
from util.dynamic_class import DynamicClass


class Reader(object):
    """
    Reader父类
    """

    parent_module = "reader"

    def __init__(self, data_source_infos: DataSourceInfos, data_source: DataSource, src_engine: str):
        self.data_source_infos = data_source_infos
        self.data_source = data_source
        self.src_engine = src_engine

    def open(self):
        """
        连接对应的reader
        :return:
        """
        pass

    def _get_table_name(self) -> str:
        """
        获取要读取数据的表名
        :return:
        """
        pass

    def _set_common_type(self, column_infos: List[ColumnInfo]) -> List[ColumnInfo]:
        """
        将行的列行转化为统一的类型：统一类型在 src/model/column_type 包中
        :param column_infos:
        :return:
        """
        pass

    def _rows_to_column_infos(self, rows: List[List]) -> List[ColumnInfo]:
        """
        将查询的数据转化为column_info对象
        :param rows:
        :return:
        """
        column_infos = []
        column_names = set()
        for row in rows:
            if row[0] in column_names:
                continue
            else:
                column_names.add(row[0])
            column_info = ColumnInfo(
                name=row[0].lower(),
                c_type=row[1].lower(),
                c_len=0 if row[2] is None else int(row[2]),
                index=row[3],
                is_pk=row[4] == 1,
                comment='' if row[5] is None else row[5]
            )
            column_infos.append(column_info)
        return self._set_common_type(column_infos)

    def _get_table_storage_size_kb(self, table_name: str) -> float:
        """
        获取该服务器上所有表的存储大小(kb): 存在分库的情况(没考虑分表)
        :param table_name:
        :param source_infos:
        :return:
        """
        pass

    def _get_interval_unit(self) -> int:
        """
        北京时间跟服务器时间进行比较, 获取时间差
        :param table_name:
        :return:
        """
        pass

    def get_table_info(self, table_name: str = None) -> TableInfo:
        """
        :param table_name:  表名称
        :return:
        """
        pass

    def close(self):
        """断开对应的reader"""
        pass

    @classmethod
    def create_reader(cls, data_source_infos: DataSourceInfos) -> 'Reader':
        """
        通过数据源信息获取到对应的Reader
        :return: 得到对应的读数据引擎
        """
        # 获取某一个服务
        data_source = data_source_infos.get_data_source()
        # 获取到父目录并与引擎类型拼接
        dir_path = os.path.dirname(os.path.realpath(__file__))
        full_path = os.path.join(dir_path, data_source.engine)
        # 拼接对应的引擎类的父module
        reader_engine_parent_module = f"{cls.parent_module}.{data_source.engine}"
        # 推断类名称
        infer_class_name = f"{data_source.engine}reader"
        if not os.path.exists(full_path):
            raise Exception(f"Reader: {data_source.engine} 不存在")
        dynamic_class = DynamicClass.load_class_info(full_path, reader_engine_parent_module, infer_class_name)
        reader_module = __import__(dynamic_class.module_path, fromlist=True)
        return getattr(reader_module, dynamic_class.class_name)(data_source_infos, data_source, data_source.engine)
