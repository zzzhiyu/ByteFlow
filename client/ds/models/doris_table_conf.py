from sqlalchemy import Column, Integer, String, Boolean

from ds.session import Base


class DorisTableConf(Base):
    """
    存储doris表配置相关信息
    """
    __tablename__ = "doris_table_conf"

    table_name = Column(String, nullable=False, primary_key=True)
    is_partition = Column(Boolean, nullable=False)
    server_id_pk = Column(Boolean, nullable=False)
    table_mode = Column(String, nullable=False)
    partition_col = Column(String, nullable=True)
    time_update_col = Column(String, nullable=True)
    hash_bucket_col = Column(String, nullable=False)
    bloom_filter_col = Column(String, nullable=True)
    time_unit = Column(String, nullable=True)
    bucket_num = Column(Integer, nullable=False)

    def print_info(self, game_type: str, db_type: str):
        print("doris表配置为:")
        print(f"表名称:{game_type}_{db_type}_{self.table_name}_{'di' if self.is_partition else 'df'}")
        print(f"是否进行分区: {self.is_partition}")
        print(f"server_id字段是否为主键: {self.server_id_pk}")
        print(f"表模式(table_mode): {self.table_mode}")
        print(f"分区字段: {self.partition_col}")
        if self.is_partition:
            print(f"分区的时间单位(DAY, WEEK, MONTH): {self.time_unit}")
        print(f"拉取的时间字段: {self.time_update_col}")
        print(f"分桶字段: {self.hash_bucket_col}")
        print(f"布隆过滤字段:{self.bloom_filter_col}")
        print(f"分桶数量:{self.bucket_num}")




