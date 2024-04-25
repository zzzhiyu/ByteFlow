from enum import Enum


class ColumnType(str, Enum):
    """
    Reader的所有类型需要转化为统一的类型, Writer安装统一类型转化为特定的类型:
    """
    # 整数类型
    SHORT = "short"
    INT = "int"
    BIGINT = "bigint"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    # 字符串
    CHAR = "char"
    VARCHAR = "varchar"
    STRING = "string"
    # 时间
    TIMESTAMP = "timestamp"
    DATETIME = "datetime"
    DATE = "date"


