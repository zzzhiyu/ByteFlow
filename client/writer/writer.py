import os

from model.table_info import TableInfo
from util.dynamic_class import DynamicClass


class Writer(object):
    """
    Writer 父类
    """
    parent_module = "writer"

    reader_table_info: TableInfo
    is_auto_input: bool
    table_conf: dict[str]

    def __init__(self, dst_engine: str):
        self.dst_engine = dst_engine

    def open(self):
        """
        连接对应的writer
        :return:
        """
        pass

    def _get_table_conf(self, reader_table_info: TableInfo) -> dict[str]:
        """
        自动获取表的配置，如果没获取到则返回{}
        :return:
        """
        return {}

    def set_field(self, reader_table_info: TableInfo):
        self.reader_table_info = reader_table_info
        # 获取初始化配置
        if table_conf := self._get_table_conf(reader_table_info):
            self.is_auto_input = True
        else:
            self.is_auto_input = False
        self.table_conf = table_conf

    def get_table_infos(self) -> TableInfo:
        """
        获取doris表对应的信息
        :return:
        """
        pass

    def create_table(self, writer_table: TableInfo):
        """
        创建表
        :return:
        """
        pass

    def close(self):
        """断开对应的writer"""
        pass

    @classmethod
    def create_writer(cls, dst_engine: str) -> 'Writer':
        """
        通过数据源信息获取到对应的Writer
        :return: 得到对应的写数据引擎
        """
        # 获取到父目录并与引擎类型拼接
        dir_path = os.path.dirname(os.path.realpath(__file__))
        full_path = os.path.join(dir_path, dst_engine)
        if not os.path.exists(full_path):
            raise Exception(f"Writer: {dst_engine} 不存在")
        # 拼接对应的引擎类的父module
        writer_engine_parent_module = f"{cls.parent_module}.{dst_engine}"
        # 推断类名称
        infer_class_name = f"{dst_engine}writer"
        dynamic_class = DynamicClass.load_class_info(full_path, writer_engine_parent_module, infer_class_name)
        reader_module = __import__(dynamic_class.module_path, fromlist=True)
        return getattr(reader_module, dynamic_class.class_name)(dst_engine)
