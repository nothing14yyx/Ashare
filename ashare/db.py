"""MySQL 写入工具与配置."""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass
class DatabaseConfig:
    """MySQL 连接参数配置."""

    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    db_name: str = "ashare"

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """从环境变量读取数据库配置."""

        return cls(
            host=os.getenv("MYSQL_HOST", "127.0.0.1"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", ""),
            db_name=os.getenv("MYSQL_DB_NAME", "ashare"),
        )

    def _credential(self) -> str:
        password = quote_plus(self.password) if self.password else ""
        credential = self.user
        if password:
            credential = f"{credential}:{password}"
        return credential

    def server_url(self) -> str:
        """构造不带库名的连接 URL，便于创建数据库。"""

        return f"mysql+pymysql://{self._credential()}@{self.host}:{self.port}"

    def database_url(self) -> str:
        """构造带库名的连接 URL。"""

        return f"{self.server_url()}/{self.db_name}"


class MySQLWriter:
    """负责将 DataFrame 写入 MySQL 的工具类."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self.engine = self._create_engine_with_database()

    def _create_engine_with_database(self) -> Engine:
        """确保目标数据库存在，并返回绑定库的 Engine。"""

        server_engine = create_engine(self.config.server_url(), future=True)
        with server_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{self.config.db_name}`"))
        server_engine.dispose()

        return create_engine(self.config.database_url(), future=True)

    def write_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        if df.empty:
            raise RuntimeError("待写入的数据为空，已跳过数据库写入。")

        # 可选：提前检查列名大小写冲突
        cols_lower = [c.lower() for c in df.columns]
        if len(cols_lower) != len(set(cols_lower)):
            raise RuntimeError(
                f"DataFrame 列名存在仅大小写不同的重复：{list(df.columns)}"
            )

        with self.engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)

    def dispose(self) -> None:
        """释放数据库连接池资源。"""

        self.engine.dispose()
