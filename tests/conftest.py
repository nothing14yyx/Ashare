import os
from typing import Dict

import pytest
from sqlalchemy import create_engine, text

from ashare.core.db import DatabaseConfig, MySQLWriter


def _mysql_available() -> bool:
    return bool(os.getenv("MYSQL_HOST") and os.getenv("MYSQL_USER"))


@pytest.fixture(scope="session")
def mysql_env() -> Dict[str, object]:
    if not _mysql_available():
        pytest.skip("MYSQL_HOST and MYSQL_USER must be set to run DB tests.")

    original_db = os.environ.get("MYSQL_DB_NAME")
    created = False
    db_name = original_db
    if not db_name:
        db_name = f"ashare_test_{os.getpid()}"
        created = True
        os.environ["MYSQL_DB_NAME"] = db_name

    try:
        yield {"db_name": db_name, "created": created, "original_db": original_db}
    finally:
        if original_db is None:
            os.environ.pop("MYSQL_DB_NAME", None)
        else:
            os.environ["MYSQL_DB_NAME"] = original_db


@pytest.fixture(scope="session")
def mysql_writer(mysql_env):
    cfg = DatabaseConfig.from_env()
    writer = MySQLWriter(cfg)
    try:
        yield writer
    finally:
        writer.dispose()
        if mysql_env["created"]:
            server_engine = create_engine(cfg.server_url(), future=True)
            with server_engine.begin() as conn:
                conn.execute(text(f"DROP DATABASE IF EXISTS `{cfg.db_name}`"))
            server_engine.dispose()
