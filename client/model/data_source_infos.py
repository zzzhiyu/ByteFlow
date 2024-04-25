from typing import List, Tuple

from ds.models.data_source import DataSource


class DataSourceInfos(object):
    def __init__(self, data_sources: List[DataSource]):
        self.data_sources = data_sources
        self.num = len(data_sources)

    def get_data_source(self) -> DataSource:
        """
        获取特定data_source的信息: 假如有一库多服标志, 就返回该服信息。没有就放回第一个
        :return: data_source的信息
        """
        return self.data_sources[0]

    def set_db_name(self, db_name: str):
        for data_source in self.data_sources:
            data_source.db_name = db_name

    def merge_same_data_source_infos(self) -> List[Tuple[DataSource, List[str]]]:
        """对于多库多表，同一服务器(ip和port相同)的数据库进行合并"""
        data_source_dict = {}
        for data_source in self.data_sources:
            key = f"{data_source.host}{data_source.port}"
            if not (value := data_source_dict.get(key)):
                data_source_dict[key] = (data_source, [data_source.db_name])
            else:
                value[1].append(data_source.db_name)
        return list(data_source_dict.values())
