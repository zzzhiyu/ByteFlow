from datax.model.doris_writer import DorisWriterConnection, DorisWriterLoadProps, DorisWriterParameter, DorisWriterModel
from conf import config
from model.table_info import TableInfo


class DorisWriter(object):

    @staticmethod
    def set_writer(writer_table_info: TableInfo) -> dict:
        connection = DorisWriterConnection(
            config.doris_jdbc_url, writer_table_info.db_name, [writer_table_info.table_name])
        load_props = DorisWriterLoadProps()
        column_names = writer_table_info.get_pk_column_names() + writer_table_info.get_not_pk_column_names()
        parameter = DorisWriterParameter(config.doris_load_url, column_names, writer_table_info.user,
                                         writer_table_info.passwd, [], [], config.doris_flush_interval, connection,
                                         load_props)
        return DorisWriterModel('doriswriter', parameter).to_dict()
