import pandas as pd
import pytest
from sqlalchemy import inspect, text

from ashare.core.db import DatabaseConfig, MySQLWriter
from ashare.core.schema_manager import SchemaManager


@pytest.mark.requires_db
def test_schema_manager_ensure_all(mysql_writer):
    manager = SchemaManager(mysql_writer.engine, db_name=mysql_writer.config.db_name)
    manager.ensure_all()

    inspector = inspect(mysql_writer.engine)
    assert inspector.has_table("history_daily_kline")
    assert inspector.has_table("strategy_signal_events")
    assert inspector.has_table("strategy_open_monitor_eval")


@pytest.mark.requires_db
def test_mysql_writer_write_dataframe(mysql_writer):
    df = pd.DataFrame(
        [
            {"code": "000001", "value": 1.0},
            {"code": "000002", "value": 2.0},
        ]
    )
    table = "test_write_dataframe"
    mysql_writer.write_dataframe(df, table, if_exists="replace")
    with mysql_writer.engine.begin() as conn:
        row = conn.execute(text(f"SELECT COUNT(*) AS cnt FROM `{table}`")).mappings().first()
    assert int(row["cnt"]) == 2
