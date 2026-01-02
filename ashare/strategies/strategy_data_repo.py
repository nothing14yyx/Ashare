"""Strategy data access layer for MA5/MA20."""

from __future__ import annotations

import datetime as dt
import logging
from typing import List

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.exc import OperationalError

from ashare.core.db import MySQLWriter


class StrategyDataRepository:
    def __init__(self, db_writer: MySQLWriter, logger: logging.Logger) -> None:
        self.db_writer = db_writer
        self.logger = logger

    def load_daily_kline(
        self,
        table: str,
        codes: List[str],
        start_date: str,
        end_date: str,
        *,
        lookback: int,
    ) -> pd.DataFrame:
        select_cols = "`date`,`code`,`high`,`low`,`close`,`preclose`,`volume`,`amount`"
        codes = [str(c) for c in (codes or []) if str(c).strip()]
        use_in = bool(codes) and len(codes) <= 2000

        with self.db_writer.engine.begin() as conn:
            if not codes:
                stmt = text(
                    f"""
                    SELECT {select_cols}
                    FROM `{table}`
                    WHERE `date` BETWEEN :start_date AND :end_date
                    ORDER BY `code`,`date`
                    """
                )
                df = pd.read_sql(
                    stmt,
                    conn,
                    params={"start_date": start_date, "end_date": end_date},
                )
            elif use_in:
                chunk_size = 800
                stmt = (
                    text(
                        f"""
                        SELECT {select_cols}
                        FROM `{table}`
                        WHERE `code` IN :codes AND `date` BETWEEN :start_date AND :end_date
                        ORDER BY `code`,`date`
                        """
                    )
                    .bindparams(bindparam("codes", expanding=True))
                )
                parts: List[pd.DataFrame] = []
                for i in range(0, len(codes), chunk_size):
                    part_codes = codes[i : i + chunk_size]
                    part = pd.read_sql(
                        stmt,
                        conn,
                        params={
                            "codes": part_codes,
                            "start_date": start_date,
                            "end_date": end_date,
                        },
                    )
                    if not part.empty:
                        parts.append(part)
                df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
            else:
                stmt = text(
                    f"""
                    SELECT {select_cols}
                    FROM `{table}`
                    WHERE `date` BETWEEN :start_date AND :end_date
                    ORDER BY `code`,`date`
                    """
                )
                df = pd.read_sql(
                    stmt,
                    conn,
                    params={"start_date": start_date, "end_date": end_date},
                )

        if df.empty:
            return df

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["code"] = df["code"].astype(str)
        for col in ["high", "low", "close", "preclose", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["date", "code", "close", "high", "low", "preclose"]).copy()
        df = df.sort_values(["code", "date"]).reset_index(drop=True)
        df = df.groupby("code", group_keys=False).tail(lookback).reset_index(drop=True)
        return df

    def load_fundamentals_latest(self, table: str = "fundamentals_latest_wide") -> pd.DataFrame:
        try:
            with self.db_writer.engine.begin() as conn:
                return pd.read_sql(text(f"SELECT * FROM `{table}`"), conn)
        except Exception as exc:  # noqa: BLE001
            self.logger.info("未能读取 %s（将跳过基本面标签）：%s", table, exc)
            return pd.DataFrame()

    def load_stock_basic(self, table: str = "a_share_stock_basic") -> pd.DataFrame:
        try:
            with self.db_writer.engine.begin() as conn:
                try:
                    return pd.read_sql(
                        text("SELECT `code`,`code_name`,`ipoDate` FROM `a_share_stock_basic`"),
                        conn,
                    )
                except OperationalError as exc:
                    if "1054" in str(exc) or "Unknown column" in str(exc):
                        self.logger.info(
                            "读取 %s 字段 ['code', 'code_name', 'ipoDate'] 失败，将回退基础字段：%s",
                            table,
                            exc,
                        )
                        return pd.read_sql(
                            text(f"SELECT `code`,`code_name` FROM `{table}`"), conn
                        )
                    raise
        except Exception as exc:  # noqa: BLE001
            self.logger.info(
                "读取 %s 失败，将跳过 ST 标签与板块限幅识别：%s",
                table,
                exc,
            )
            return pd.DataFrame()

    def load_indicator_daily(
        self,
        codes: List[str],
        latest_date: dt.date,
        lookback: int = 100,
        table: str = "strategy_indicator_daily",
    ) -> pd.DataFrame:
        """从数据库加载预计算好的技术指标。"""
        if not codes:
            return pd.DataFrame()

        start_date = latest_date - dt.timedelta(days=lookback * 2) # 留出足够的日历日
        
        stmt = (
            text(
                f"""
                SELECT *
                FROM `{table}`
                WHERE `code` IN :codes AND `trade_date` BETWEEN :start_date AND :latest_date
                ORDER BY `code`, `trade_date`
                """
            )
            .bindparams(bindparam("codes", expanding=True))
        )
        
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql(
                    stmt,
                    conn,
                    params={
                        "codes": codes,
                        "start_date": start_date.isoformat(),
                        "latest_date": latest_date.isoformat(),
                    },
                )
            
            if df.empty:
                return df
                
            # 统一字段名，确保兼容旧代码中的 'date' 引用
            df = df.rename(columns={"trade_date": "date"})
            df["date"] = pd.to_datetime(df["date"])
            return df
            
        except Exception as exc:
            self.logger.error("加载预计算指标表 %s 失败: %s", table, exc)
            return pd.DataFrame()
