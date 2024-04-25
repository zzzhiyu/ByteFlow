from sqlalchemy import Column, Integer, String

from ds.session import Base

class DataSource(Base):
    """
    存储数据源相关信息
    """
    __tablename__ = "data_source"

    source_name = Column(String, nullable=False, primary_key=True) # 数据源名称
    db_type = Column(String, nullable=False, primary_key=True) # 库类型: 分库使用, 分库时库可能不相同
    server_id = Column(Integer, nullable=False, primary_key=True) # 服务器id
    desc = Column(String) # 信息的描述
    engine = Column(String, nullable=False) # sqlserver,mysql
    db_name = Column(String, nullable=False)
    host = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    user = Column(String, nullable=False)
    passwd = Column(String, nullable=False)
    flag = Column(Integer, nullable=False) # 是否有效


