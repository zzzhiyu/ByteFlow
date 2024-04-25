from typing import List


class ColumnInfo(object):
    def __init__(self, name: str, c_type: str, c_len: int, index: int, is_pk: bool = False, comment: str = "",
                 common_type: str = None, **kwargs):
        self.name = name.lower()  # 列名全部转化为小写
        self.c_type = c_type.lower()  # 类型全部转化为小写
        self.c_len = c_len
        self.index = index
        self.is_pk = is_pk
        self.comment = comment
        self.common_type = common_type  # 公共的类型，reader和writer之间需要进行转化
        self.kwargs = kwargs

    def get_column_type(self) -> str:
        """
        对char和varchar类型需要进行特殊处理, 返回varchar(c_len) or char(c_len)
        :return: 类型
        """
        if 'char' in self.c_type and self.c_len > 0:
            return f'{self.c_type}({str(self.c_len)})'
        return self.c_type

    @classmethod
    def get_pk_columns(cls, columns: List['ColumnInfo']) -> List['ColumnInfo']:
        """
        得到主键列信息
        :return:
        """
        return [col for col in columns if col.is_pk]

    @classmethod
    def get_pk_column_names(cls, columns: List['ColumnInfo']) -> List[str]:
        """
        得到主键列名字
        :return:
        """
        return [col.name for col in columns if col.is_pk]

    @classmethod
    def get_not_pk_columns(cls, columns: List['ColumnInfo']) -> List['ColumnInfo']:
        """
        得到非主键列信息
        :return:
        """
        return [col for col in columns if not col.is_pk]

    @classmethod
    def get_not_pk_column_names(cls, columns: List['ColumnInfo']) -> List[str]:
        """
        得到非主键列名字
        :return:
        """
        return [col.name for col in columns if not col.is_pk]

    @classmethod
    def get_column_names(cls, columns: List['ColumnInfo']) -> List[str]:
        """
        得到列名字
        :return:
        """
        return [col.name for col in columns]

    @classmethod
    def get_date_column_names(cls, columns: List['ColumnInfo']) -> List[str]:
        """
        得到时间列信息
        :return:
        """
        return [
            col.name
            for col in columns
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

    @classmethod
    def to_column_dict(cls, columns: List['ColumnInfo']) -> dict[str, "ColumnInfo"]:
        """
        转化为dict
        :return:
        """
        return {col.name: col for col in columns}
