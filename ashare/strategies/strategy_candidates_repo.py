"""Strategy candidates data access layer."""

from __future__ import annotations

import datetime as dt
import logging
from typing import List

import pandas as pd
from sqlalchemy import inspect, text

from ashare.core.db import MySQLWriter
from ashare.core.schema_manager import TABLE_STRATEGY_CANDIDATES


class StrategyCandidatesRepository:
    def __init__(self, db_writer: MySQLWriter, logger: logging.Logger) -> None:
        self.db_writer = db_writer
        self.logger = logger

    def table_exists(self, table: str) -> bool:
        if not table:
            return False
        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1"))
            return True
        except Exception:
            return False

    def column_exists(self, table: str, column: str) -> bool:
        if not table or not column:
            return False
        try:
            inspector = inspect(self.db_writer.engine)
            cols = inspector.get_columns(table)
        except Exception:
            return False
        return any(col.get("name") == column for col in cols if isinstance(col, dict))

    def fetch_trade_dates(
        self, base_table: str, date_col: str, latest_date: dt.date, limit: int
    ) -> list[str]:
        lookback_days = max(int(limit), 1)
        stmt = text(
            f"""
            SELECT DISTINCT CAST(`{date_col}` AS CHAR) AS d
            FROM `{base_table}`
            WHERE `{date_col}` <= :base_date
            ORDER BY d DESC
            LIMIT {lookback_days}
            """
        )
        with self.db_writer.engine.begin() as conn:
            df = pd.read_sql_query(stmt, conn, params={"base_date": latest_date.isoformat()})
        if df is None or df.empty:
            return []
        return df["d"].dropna().astype(str).str[:10].tolist()

    def fetch_max_valid_days(self, table: str, min_date: str) -> int | None:
        stmt = text(
            f"""
            SELECT MAX(`valid_days`) AS max_valid_days
            FROM `{table}`
            WHERE `sig_date` >= :min_date
            """
        )
        with self.db_writer.engine.begin() as conn:
            row = conn.execute(stmt, {"min_date": min_date}).mappings().first()

        if not row:
            return None
        raw = row.get("max_valid_days")
        if raw is None or pd.isna(raw):
            return None
        try:
            max_days = int(raw)
        except Exception:
            return None
        return max_days if max_days > 0 else None

    def fetch_liquidity_codes(self, asof_date: dt.date) -> list[str]:
        stmt = text(
            """
            SELECT `code`
            FROM `a_share_top_liquidity`
            WHERE `date` = :d
            """
        )
        with self.db_writer.engine.begin() as conn:
            df = pd.read_sql_query(stmt, conn, params={"d": asof_date})
        if df.empty or "code" not in df.columns:
            return []
        return df["code"].dropna().astype(str).tolist()

    def fetch_signal_candidates(
        self, table: str, earliest: str, latest: str
    ) -> pd.DataFrame:
        select_cols = [
            "code",
            "sig_date",
            "strategy_code",
            "UPPER(COALESCE(`final_action`, `signal`)) AS action",
            "valid_days",
        ]
        stmt = text(
            f"""
            SELECT {", ".join(select_cols)}
            FROM `{table}`
            WHERE `sig_date` BETWEEN :earliest AND :latest
              AND UPPER(COALESCE(`final_action`, `signal`)) IN ('BUY','BUY_CONFIRM')
            """
        )
        with self.db_writer.engine.begin() as conn:
            return pd.read_sql_query(
                stmt,
                conn,
                params={"earliest": earliest, "latest": latest},
            )

    def fetch_near_signal_candidates(
        self,
        indicator_table: str,
        latest_date: str,
        prev_date: str,
        near_ma20_band: float,
        near_cross_gap_band: float,
        near_signal_macd_required: bool,
    ) -> pd.DataFrame:
        macd_clause = ""
        if near_signal_macd_required:
            macd_clause = " AND a.macd_hist IS NOT NULL AND a.macd_hist > 0"

        stmt = text(
            f"""
            SELECT
                a.code,
                a.trade_date as sig_date,
                'MA5_MA20_TREND' as strategy_code,
                'NEAR_SIGNAL' as action,
                1 as valid_days
            FROM `{indicator_table}` a
            JOIN `{indicator_table}` b
              ON a.code = b.code
             AND b.trade_date = :prev_date
            WHERE a.trade_date = :latest_date
              AND a.close IS NOT NULL
              AND a.ma20 IS NOT NULL
              AND a.ma5 IS NOT NULL
              AND b.ma20 IS NOT NULL
              AND b.ma5 IS NOT NULL
              AND ABS((a.close - a.ma20) / a.ma20) <= :near_ma20_band
              AND (
                    (
                        a.ma5 > a.ma20
                        AND (a.ma5 - a.ma20) >= (b.ma5 - b.ma20)
                        AND a.ma5 > b.ma5
                    )
                 OR (
                        a.ma5 <= a.ma20
                        AND ABS((a.ma5 - a.ma20) / a.ma20) <= :near_cross_gap_band
                        AND (a.ma5 - a.ma20) > (b.ma5 - b.ma20)
                        AND a.ma5 > b.ma5
                    )
                  )
              {macd_clause}
            """
        )
        with self.db_writer.engine.begin() as conn:
            return pd.read_sql_query(
                stmt,
                conn,
                params={
                    "latest_date": latest_date,
                    "prev_date": prev_date,
                    "near_ma20_band": near_ma20_band,
                    "near_cross_gap_band": near_cross_gap_band,
                },
            )

    def write_candidates(self, asof_date: dt.date, df: pd.DataFrame) -> None:
        delete_stmt = text(
            f"DELETE FROM `{TABLE_STRATEGY_CANDIDATES}` WHERE `asof_trade_date` = :d"
        )
        with self.db_writer.engine.begin() as conn:
            conn.execute(delete_stmt, {"d": asof_date})

        if df.empty:
            return
        df = df.copy()
        df["code"] = df["code"].astype(str)
        self.db_writer.write_dataframe(df, TABLE_STRATEGY_CANDIDATES, if_exists="append")

    def load_candidates(self, asof_date: dt.date) -> pd.DataFrame:
        stmt = text(
            """
            SELECT `code`, `is_liquidity`, `has_signal`
            FROM `strategy_candidates`
            WHERE `asof_trade_date` = :d
            """
        )
        with self.db_writer.engine.begin() as conn:
            return pd.read_sql_query(stmt, conn, params={"d": asof_date})
