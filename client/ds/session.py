from contextlib import contextmanager
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from conf.config import user, passwd, host, port, db

Base = declarative_base()

mysql_engine = create_engine(
    f"mysql+pymysql://{user}:{quote_plus(passwd)}@{host}:{port}/{db}?charset=utf8"
)

Mysql_SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=mysql_engine)

@contextmanager
def get_mysql_db() -> Session:
    db = Mysql_SessionLocal()
    try:
        yield db
    finally:
        db.close()
