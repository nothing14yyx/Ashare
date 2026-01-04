"""MySQL 写入工具与配置."""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ashare.core.config import get_section


@dataclass
class DatabaseConfig:
    """MySQL 连接参数配置."""

    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    db_name: str = "ashare"
    pool_size: int = 5
    max_overflow: int = 10
    pool_recycle: int = 1800
    pool_timeout: int = 30

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """从环境变量与 config.yaml 读取数据库配置。

        优先顺序：
        1. 环境变量 MYSQL_*；
        2. config.yaml 中 database.*；
        3. 类默认值。
        """
        section = get_section("database")

        host = os.getenv("MYSQL_HOST", section.get("host", cls.host))
        port_raw = os.getenv("MYSQL_PORT", section.get("port", cls.port))
        user = os.getenv("MYSQL_USER", section.get("user", cls.user))
        password = os.getenv(
            "MYSQL_PASSWORD",
            section.get("password", cls.password),
        )
        db_name = os.getenv("MYSQL_DB_NAME", section.get("db_name", cls.db_name))
        pool_size_raw = os.getenv(
            "MYSQL_POOL_SIZE", section.get("pool_size", cls.pool_size)
        )
        max_overflow_raw = os.getenv(
            "MYSQL_MAX_OVERFLOW", section.get("max_overflow", cls.max_overflow)
        )
        pool_recycle_raw = os.getenv(
            "MYSQL_POOL_RECYCLE", section.get("pool_recycle", cls.pool_recycle)
        )
        pool_timeout_raw = os.getenv(
            "MYSQL_POOL_TIMEOUT", section.get("pool_timeout", cls.pool_timeout)
        )

        try:
            port = int(port_raw)
        except (TypeError, ValueError):
            port = cls.port

        def _parse_positive_int(raw: str | int | float | None, default: int) -> int:
            try:
                value = int(raw)
                return value if value > 0 else default
            except (TypeError, ValueError):
                return default

        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            db_name=db_name,
            pool_size=_parse_positive_int(pool_size_raw, cls.pool_size),
            max_overflow=_parse_positive_int(max_overflow_raw, cls.max_overflow),
            pool_recycle=_parse_positive_int(pool_recycle_raw, cls.pool_recycle),
            pool_timeout=_parse_positive_int(pool_timeout_raw, cls.pool_timeout),
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

        return f"{self.server_url()}/{self.db_name}?charset=utf8mb4"


class MySQLWriter:
    """负责将 DataFrame 写入 MySQL 的工具类."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self.engine = self._create_engine_with_database()

    def _create_engine_with_database(self) -> Engine:
        """确保目标数据库存在，并返回绑定库的 Engine。"""

        server_engine = create_engine(self.config.server_url(), future=True)
        with server_engine.connect() as conn:
            conn.execute(
                text(
                    f"CREATE DATABASE IF NOT EXISTS `{self.config.db_name}` "
                    "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            )
        server_engine.dispose()

        return create_engine(
            self.config.database_url(),
            future=True,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
            pool_recycle=self.config.pool_recycle,
            pool_pre_ping=True,
        )

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        chunksize: int = 500,
        method: str | None = "multi",
    ) -> None:
        if df.empty:
            raise RuntimeError("待写入的数据为空，已跳过数据库写入。")

        # 可选：提前检查列名大小写冲突
        cols_lower = [c.lower() for c in df.columns]
        if len(cols_lower) != len(set(cols_lower)):
            raise RuntimeError(
                f"DataFrame 列名存在仅大小写不同的重复：{list(df.columns)}"
            )

        allowed_if_exists = {"fail", "replace", "append"}
        if if_exists not in allowed_if_exists:
            raise ValueError(
                "if_exists 仅支持 fail/replace/append，"
                f"当前值为: {if_exists}"
            )

        with self.engine.begin() as conn:
            df.to_sql(
                table_name,
                conn,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method=method,
            )

    def dispose(self) -> None:
        """释放数据库连接池资源。"""

        self.engine.dispose()
