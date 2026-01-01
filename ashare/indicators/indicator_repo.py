"""Indicator data access layer."""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd
from sqlalchemy import bindparam, text

from ashare.core.db import MySQLWriter


class IndicatorRepository:
    def __init__(self, db_writer: MySQLWriter, logger: logging.Logger) -> None:
        self.db_writer = db_writer
        self.logger = logger

    def fetch_board_history(
        self, trade_date: str, lookback_days: int = 90
    ) -> pd.DataFrame:
        lookback_days = max(int(lookback_days), 1)
        stmt = text(
            f"""
            SELECT `date`, `board_name`, `收盘` AS `close`
            FROM board_industry_hist_daily
            WHERE `date` >= DATE_SUB(:d, INTERVAL {lookback_days} DAY)
              AND `date` <= :d
            ORDER BY `date` ASC
            """
        )
        try:
            with self.db_writer.engine.begin() as conn:
                return pd.read_sql_query(stmt, conn, params={"d": trade_date})
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取板块历史失败：%s", exc)
            return pd.DataFrame()

    def fetch_board_spot(self) -> pd.DataFrame:
        try:
            with self.db_writer.engine.begin() as conn:
                return pd.read_sql_query(
                    "SELECT DISTINCT board_name, board_code FROM board_industry_spot",
                    conn,
                )
        except Exception:
            return pd.DataFrame()

    def persist_board_rotation(self, trade_date: str, df: pd.DataFrame) -> None:
        table = "strategy_board_rotation"
        if df.empty:
            return
        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(
                    text(f"DELETE FROM `{table}` WHERE `date` = :d"),
                    {"d": trade_date},
                )
                df.to_sql(
                    table,
                    conn,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("持久化板块轮动数据失败：%s", exc)

    def fetch_index_daily_kline(
        self,
        codes: list[str],
        start_date: dt.date,
        end_date: dt.date,
    ) -> pd.DataFrame:
        if not codes:
            return pd.DataFrame()
        stmt = (
            text(
                """
                SELECT `code`,`date`,`open`,`high`,`low`,`close`,`volume`,`amount`
                FROM history_index_daily_kline
                WHERE `code` IN :codes AND `date` BETWEEN :start_date AND :end_date
                ORDER BY `code`, `date`
                """
            ).bindparams(bindparam("codes", expanding=True))
        )
        try:
            with self.db_writer.engine.begin() as conn:
                return pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "codes": codes,
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取指数日线指标失败：%s", exc)
            return pd.DataFrame()

    def fetch_index_trade_dates(
        self, code: str, start_date: dt.date, end_date: dt.date
    ) -> pd.DataFrame:
        stmt = text(
            """
            SELECT CAST(`date` AS CHAR) AS trade_date
            FROM history_index_daily_kline
            WHERE `code` = :code AND `date` BETWEEN :start_date AND :end_date
            ORDER BY `date`
            """
        )
        try:
            with self.db_writer.engine.begin() as conn:
                return pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "code": code,
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取周线交易日失败：%s", exc)
            return pd.DataFrame()
