from typing import List

from model.column_info import ColumnInfo


class TableInfo(object):
    def __init__(self, source_name: str, db_type: str, user: str, passwd: str,  db_name: str, table_name: str,
                 columns: List[ColumnInfo], storage_size_kb: float = 0, interval_unit: int = 0, **kwargs):
        self.source_name = source_name
        self.db_type = db_type
        self.user = user
        self.passwd = passwd
        self.db_name = db_name
        self.table_name = table_name
        self.columns = columns
        self.storage_size_kb = storage_size_kb
        self.interval_unit = interval_unit
        self.kwargs = kwargs

    def get_storage_size_gb(self) -> float:
        """
        获取表的大小，单位为G
        :return:
        """
        return round(self.storage_size_kb / (1024 * 1024), 6)

    def get_pk_columns(self) -> List[ColumnInfo]:
        """
        得到主键列信息
        :return:
        """
        return [col for col in self.columns if col.is_pk]

    def get_pk_column_names(self) -> List[str]:
        """
        得到主键列名字
        :return:
        """
        return [col.name for col in self.columns if col.is_pk]

    def get_not_pk_columns(self) -> List[ColumnInfo]:
        """
        得到非主键列信息
        :return:
        """
        return [col for col in self.columns if not col.is_pk]

    def get_not_pk_column_names(self) -> List[str]:
        """
        得到非主键列名字
        :return:
        """
        return [col.name for col in self.columns if not col.is_pk]

    def get_column_names(self) -> List[str]:
        """
        得到列名字
        :return:
        """
        return [col.name for col in self.columns]

    def get_date_column_names(self) -> List[ColumnInfo]:
        """
        得到时间列信息
        :return: 
        """
        return [
            col.name
            for col in self.columns
            if col.name != 'loadtime'
               and (
                       'date' in col.c_type
                       or 'time' in col.c_type
                       or 'date' in col.name
                       or 'time' in col.name
                       or 'last_login' in col.name
                       or 'last_logout' in col.name
               )
        ]

    def to_column_dict(self) -> dict[str, ColumnInfo]:
        """
        转化为dict
        :return:
        """
        return {col.name: col for col in self.columns}
