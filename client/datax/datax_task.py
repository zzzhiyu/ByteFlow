from model.table_info import TableInfo
from datax.model.common import BuildJobJson
from datax.reader_writer.reader_writer import ReaderWriter


class DataxTask(object):
    """
    获取DataxJson
    """

    def __init__(self, reader_table_info: TableInfo, writer_table_info: TableInfo, src_engine: str, dst_engine: str,
                 conf_server_num: int):
        self.reader_table_info = reader_table_info
        self.writer_table_info = writer_table_info
        self.src_engine = src_engine
        self.dst_engine = dst_engine
        self.conf_server_num = conf_server_num
        self.reader_writer = self._init_reader_writer()

    def _init_reader_writer(self) -> ReaderWriter:
        """
        初始化Datax配置的reader和writer
        :return:
        """
        # 设置reader, writer -> content
        reader_writer = ReaderWriter.create_reader_writer(self.reader_table_info, self.writer_table_info,
                                                          self.src_engine, self.dst_engine)
        return reader_writer

    def get_datax_task(self, is_full: bool) -> str:
        datax_json = self.reader_writer.get_datax_json(is_full)
        parallel = 10
        if self.conf_server_num < parallel:
            parallel = self.conf_server_num
        job_content = BuildJobJson(self.reader_table_info.source_name, self.reader_table_info.db_type, parallel,
                                   datax_json).__str__()
        if is_full:
            print(f"全量任务的内容为:\n{job_content}")
        else:
            print(f"增量任务的内容为:\n{job_content}")
        return job_content
