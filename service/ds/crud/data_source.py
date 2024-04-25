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