from ds.models.doris_table_conf import DorisTableConf
from ds.session import get_mysql_db


def select_doris_table_conf(table_name: str) -> DorisTableConf:
    with get_mysql_db() as db:
        doris_table_conf = (
            db.query(DorisTableConf)
                .filter(DorisTableConf.table_name == table_name)
                .first()
        )
    return doris_table_conf


def insert_doris_table_conf(doris_table_conf: DorisTableConf):
    with get_mysql_db() as db:
        db.add(doris_table_conf)
        db.commit()
        db.refresh(doris_table_conf)
