from typing import List

from ds.models.data_source import DataSource
from ds.session import get_mysql_db


def select_data_sources(source_name: str, db_type: str) -> List[DataSource]:
    with get_mysql_db() as db:
        data_sources = (
            db.query(DataSource)
                .filter(DataSource.source_name == source_name,
                        DataSource.db_type == db_type,
                        DataSource.flag == 1)
                .all()
        )
    return data_sources


def select_distinct_source_names() -> List[str]:
    with get_mysql_db() as db:
        data_sources = (
            db.query(DataSource.source_name)
                .filter(DataSource.flag == 1)
                .distinct().all()
        )
    return [data_source[0] for data_source in data_sources]


def select_distinct_db_types() -> List[str]:
    with get_mysql_db() as db:
        db_types = (
            db.query(DataSource.db_type)
                .filter(DataSource.flag == 1)
                .distinct().all()
        )
    return [db_type[0] for db_type in db_types]
