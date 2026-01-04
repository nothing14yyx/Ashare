"""Chip filter data access layer."""

from __future__ import annotations

import logging
from typing import Iterable, Tuple

import pandas as pd
from sqlalchemy import bindparam, inspect, text

from ashare.core.db import MySQLWriter


class ChipFilterRepository:
    def __init__(self, db_writer: MySQLWriter, logger: logging.Logger) -> None:
        self.db_writer = db_writer
        self.logger = logger

    def table_exists(self, table: str) -> bool:
        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1"))
            return True
        except Exception:
            return False

    def resolve_gdhs_columns(
        self, table: str
    ) -> Tuple[str | None, str | None, str | None, str | None]:
        stmt = text(f"SELECT * FROM `{table}` LIMIT 0")
        with self.db_writer.engine.begin() as conn:
            meta_df = pd.read_sql_query(stmt, conn)
        columns = set(meta_df.columns)
        code_col = "code" if "code" in columns else None
        announce_candidates = [
            "公告日期",
            "股东户数公告日期",
            "股东户数统计截止日",
            "period",
        ]
        delta_pct_candidates = [
            "股东户数-增减比例",
            "增减比例",
            "holder_change_ratio",
        ]
        announce_col = next((c for c in announce_candidates if c in columns), None)
        delta_pct_col = next((c for c in delta_pct_candidates if c in columns), None)
        delta_abs_col = None
        if "股东户数-增减" in columns:
            delta_abs_col = "股东户数-增减"
        elif "股东户数-变动数量" in columns:
            delta_abs_col = "股东户数-变动数量"
        elif "holder_change" in columns:
            delta_abs_col = "holder_change"
        return code_col, announce_col, delta_pct_col, delta_abs_col

    def load_gdhs_detail(
        self,
        table: str,
        codes: Iterable[str],
        code_col: str,
        announce_col: str,
        delta_pct_col: str | None,
        delta_abs_col: str | None,
    ) -> pd.DataFrame:
        select_cols = [code_col, announce_col]
        for col in [delta_pct_col, delta_abs_col]:
            if col:
                select_cols.append(col)

        stmt = (
            text(
                f"""
                SELECT {",".join(f"`{c}`" for c in select_cols)}
                FROM `{table}`
                WHERE `{code_col}` IN :codes
                """
            ).bindparams(bindparam("codes", expanding=True))
        )
        with self.db_writer.engine.begin() as conn:
            try:
                return pd.read_sql_query(stmt, conn, params={"codes": list(codes)})
            except Exception:
                return pd.DataFrame()

    def get_table_columns(self, table: str) -> set[str]:
        try:
            return {
                col.get("name")
                for col in inspect(self.db_writer.engine).get_columns(table)
                if isinstance(col, dict)
            }
        except Exception:
            return set()

    def delete_existing(self, table: str, sig_dates: list, codes: list[str]) -> None:
        if not sig_dates or not codes:
            return
        delete_stmt = (
            text(
                f"""
                DELETE FROM `{table}`
                WHERE `sig_date` IN :dates AND `code` IN :codes
                """
            ).bindparams(bindparam("dates", expanding=True), bindparam("codes", expanding=True))
        )
        with self.db_writer.engine.begin() as conn:
            conn.execute(delete_stmt, {"dates": sig_dates, "codes": codes})

    def write_dataframe(self, df: pd.DataFrame, table: str) -> None:
        if df.empty:
            return
        self.db_writer.write_dataframe(df, table, if_exists="append")
