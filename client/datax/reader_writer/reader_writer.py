import os

from datax.model import common
from model.table_info import TableInfo
from util.dynamic_class import DynamicClass


class ReaderWriter(object):
    """
    datax ReaderWriter的相关配置
    """
    parent_module = "datax.reader_writer"

    def __init__(self, reader_table_info: TableInfo, writer_table_info: TableInfo):
        self.reader_table_info = reader_table_info
        self.writer_table_info = writer_table_info

    def get_reader(self, is_full: bool) -> dict:
        """获取datax_reader增量拉取的相关配置"""
        pass

    def get_writer(self) -> dict:
        """获取datax_writer的相关配置"""
        pass

    def get_datax_json(self, is_full: bool) -> common.DataxJson:
        # 设置setting
        setting = common.Setting(common.Speed(), common.ErrorLimit())
        # 设置reader, writer -> content
        reader = self.get_reader(is_full)
        writer = self.get_writer()
        content = common.Content(reader, writer)
        # 设置setting, content -> job
        job = common.Job(setting, content)
        return common.DataxJson(job)

    @classmethod
    def create_reader_writer(cls, reader_table_info: TableInfo, writer_table_info: TableInfo, src_engine: str,
                             dst_engine: str) -> 'ReaderWriter':
        """
        通过数据源信息获取到对应Datax的迁移类
        :param reader_table_info:
        :param writer_table_info:
        :param src_engine:
        :param dst_engine:
        :return:
        """
        dir_path = os.path.dirname(os.path.realpath(__file__))
        full_path = os.path.join(dir_path, f"{src_engine}_{dst_engine}")
        if not os.path.exists(full_path):
            raise Exception(f"reader_writer: {full_path}目录不存在")
        # 拼接对应的引擎类的父module
        engine_parent_module = f"{cls.parent_module}.{src_engine}_{dst_engine}"
        # 推断类名称
        infer_class_name = f"{src_engine}{dst_engine}"
        dynamic_class = DynamicClass.load_class_info(full_path, engine_parent_module, infer_class_name)
        reader_module = __import__(dynamic_class.module_path, fromlist=True)
        return getattr(reader_module, dynamic_class.class_name)(reader_table_info, writer_table_info)
