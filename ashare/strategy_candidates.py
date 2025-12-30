"""策略候选池生成与刷新。"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import List

import pandas as pd
from sqlalchemy import inspect, text

from .config import get_section
from .db import DatabaseConfig, MySQLWriter
from .schema_manager import SchemaManager, TABLE_STRATEGY_CANDIDATES


@dataclass(frozen=True)
class StrategyCandidatesConfig:
    signal_lookback_days: int = 3
    cross_valid_days: int = 3
    pullback_valid_days: int = 5

    @classmethod
    def from_config(cls) -> "StrategyCandidatesConfig":
        sec = get_section("open_monitor") or {}
        if not isinstance(sec, dict):
            return cls()

        def _get_int(key: str, default: int) -> int:
            raw = sec.get(key, default)
            try:
                return int(raw)
            except Exception:
                return default

        return cls(
            signal_lookback_days=_get_int(
                "signal_lookback_days",
                cls.signal_lookback_days,
            ),
            cross_valid_days=_get_int("cross_valid_days", cls.cross_valid_days),
            pullback_valid_days=_get_int("pullback_valid_days", cls.pullback_valid_days),
        )


class StrategyCandidatesService:
    def __init__(self, db_writer: MySQLWriter | None = None, logger=None) -> None:
        self.db_writer = db_writer or MySQLWriter(DatabaseConfig.from_env())
        self.logger = logger or logging.getLogger(__name__)
        self.params = StrategyCandidatesConfig.from_config()
        db_name = getattr(self.db_writer.config, "db_name", None)
        self.table_names = SchemaManager(
            self.db_writer.engine,
            db_name=db_name,
        ).get_table_names()

    def refresh(self, asof_trade_date: dt.date) -> None:
        asof_date = self._normalize_date(asof_trade_date)
        liquidity_codes = self._load_liquidity_codes(asof_date)
        if not liquidity_codes:
            self.logger.error(
                "a_share_top_liquidity 在 %s 无数据，终止刷新 candidates。",
                asof_date,
            )
            raise RuntimeError(f"a_share_top_liquidity 在 {asof_date} 无数据")
        signal_df = self._load_signal_candidates(asof_date)
        merged = self._merge_candidates(asof_date, liquidity_codes, signal_df)
        self._write_candidates(asof_date, merged)

        total = len(merged)
        liquidity_cnt = int(merged["is_liquidity"].sum()) if not merged.empty else 0
        signal_cnt = int(merged["has_signal"].sum()) if not merged.empty else 0
        both_cnt = (
            int(((merged["is_liquidity"] == 1) & (merged["has_signal"] == 1)).sum())
            if not merged.empty
            else 0
        )
        snapshot_only = total - liquidity_cnt
        self.logger.info(
            "strategy_candidates 刷新完成：asof=%s total=%s liquidity=%s signal=%s both=%s snapshot_only=%s",
            asof_date,
            total,
            liquidity_cnt,
            signal_cnt,
            both_cnt,
            snapshot_only,
        )

    def _normalize_date(self, raw: dt.date | str) -> dt.date:
        if isinstance(raw, dt.date):
            return raw
        parsed = pd.to_datetime(raw, errors="coerce")
        if pd.isna(parsed):
            raise ValueError(f"无法解析 asof_trade_date={raw!r}")
        return parsed.date()

    def _table_exists(self, table: str) -> bool:
        if not table:
            return False
        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1"))
            return True
        except Exception:
            return False

    def _column_exists(self, table: str, column: str) -> bool:
        if not table or not column:
            return False
        try:
            inspector = inspect(self.db_writer.engine)
            cols = inspector.get_columns(table)
        except Exception:
            return False
        return any(col.get("name") == column for col in cols if isinstance(col, dict))

    def _resolve_signal_window(self, latest_date: dt.date) -> tuple[dt.date, dt.date]:
        lookback_days = max(
            int(self.params.signal_lookback_days),
            int(max(self.params.cross_valid_days, self.params.pullback_valid_days)) + 1,
        )

        base_table = "history_daily_kline"
        date_col = "date"
        if not self._table_exists(base_table):
            indicator_table = self.table_names.indicator_table
            if self._table_exists(indicator_table):
                base_table = indicator_table
                date_col = "trade_date"
            else:
                base_table = self.table_names.signal_events_table
                date_col = "sig_date"

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

        dates = (
            df["d"].dropna().astype(str).str[:10].tolist()
            if df is not None and not df.empty
            else []
        )
        if not dates:
            return latest_date, latest_date
        latest = pd.to_datetime(dates[0], errors="coerce")
        earliest = pd.to_datetime(dates[-1], errors="coerce")
        if pd.isna(latest) or pd.isna(earliest):
            return latest_date, latest_date
        return earliest.date(), latest.date()

    def _load_liquidity_codes(self, asof_date: dt.date) -> List[str]:
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

    def _load_signal_candidates(self, asof_date: dt.date) -> pd.DataFrame:
        table = self.table_names.signal_events_table
        if not self._table_exists(table):
            return pd.DataFrame()

        earliest, latest = self._resolve_signal_window(asof_date)
        has_valid_days = self._column_exists(table, "valid_days")
        select_cols = [
            "code",
            "sig_date",
            "strategy_code",
            "UPPER(COALESCE(`final_action`, `signal`)) AS action",
        ]
        if has_valid_days:
            select_cols.append("valid_days")
        valid_clause = ""
        if has_valid_days:
            valid_clause = (
                "AND (`valid_days` IS NULL OR DATEDIFF(:latest, `sig_date`) <= `valid_days`)"
            )

        stmt = text(
            f"""
            SELECT {", ".join(select_cols)}
            FROM `{table}`
            WHERE `sig_date` BETWEEN :earliest AND :latest
              AND UPPER(COALESCE(`final_action`, `signal`)) IN ('BUY','BUY_CONFIRM')
              {valid_clause}
            """
        )
        with self.db_writer.engine.begin() as conn:
            df = pd.read_sql_query(
                stmt,
                conn,
                params={"earliest": earliest, "latest": latest},
            )

        if df.empty:
            return df

        df = df.copy()
        df["sig_date"] = pd.to_datetime(df["sig_date"], errors="coerce")
        df = df.dropna(subset=["sig_date", "code"])
        df["code"] = df["code"].astype(str)
        df["action"] = df["action"].astype(str).str.upper()

        if not has_valid_days:
            self.logger.info(
                "strategy_signal_events 缺少 valid_days，将不做有效期过滤。"
            )

        if df.empty:
            return df

        action_priority = {"BUY": 1, "BUY_CONFIRM": 2}
        df["action_priority"] = df["action"].map(action_priority).fillna(0).astype(int)
        df = df.sort_values(
            ["code", "sig_date", "action_priority"],
            ascending=[True, False, False],
        )
        latest_df = df.groupby("code", as_index=False).first()
        latest_df = latest_df.rename(
            columns={
                "sig_date": "latest_sig_date",
                "action": "latest_sig_action",
                "strategy_code": "latest_sig_strategy_code",
            }
        )
        return latest_df[
            ["code", "latest_sig_date", "latest_sig_action", "latest_sig_strategy_code"]
        ]

    def _merge_candidates(
        self,
        asof_date: dt.date,
        liquidity_codes: List[str],
        signal_df: pd.DataFrame,
    ) -> pd.DataFrame:
        liquidity_df = pd.DataFrame({"code": liquidity_codes}).drop_duplicates()
        if not liquidity_df.empty:
            liquidity_df["is_liquidity"] = 1
        else:
            liquidity_df = pd.DataFrame(columns=["code", "is_liquidity"])

        signal_base = signal_df.copy()
        if not signal_base.empty:
            signal_base["has_signal"] = 1
        else:
            signal_base = pd.DataFrame(
                columns=[
                    "code",
                    "latest_sig_date",
                    "latest_sig_action",
                    "latest_sig_strategy_code",
                    "has_signal",
                ]
            )

        merged = pd.merge(liquidity_df, signal_base, on="code", how="outer")
        if merged.empty:
            return merged

        merged["is_liquidity"] = (
            pd.to_numeric(merged.get("is_liquidity"), errors="coerce")
            .fillna(0)
            .astype(int)
        )
        merged["has_signal"] = (
            pd.to_numeric(merged.get("has_signal"), errors="coerce")
            .fillna(0)
            .astype(int)
        )
        merged["asof_trade_date"] = asof_date
        merged["latest_sig_date"] = pd.to_datetime(
            merged.get("latest_sig_date"),
            errors="coerce",
        ).dt.date
        merged["latest_sig_action"] = merged.get("latest_sig_action")
        merged["latest_sig_strategy_code"] = merged.get("latest_sig_strategy_code")
        merged["created_at"] = dt.datetime.now()
        return merged[
            [
                "asof_trade_date",
                "code",
                "is_liquidity",
                "has_signal",
                "latest_sig_date",
                "latest_sig_action",
                "latest_sig_strategy_code",
                "created_at",
            ]
        ]

    def _write_candidates(self, asof_date: dt.date, df: pd.DataFrame) -> None:
        delete_stmt = text(
            f"DELETE FROM `{TABLE_STRATEGY_CANDIDATES}` WHERE `asof_trade_date` = :d"
        )
        with self.db_writer.engine.begin() as conn:
            conn.execute(delete_stmt, {"d": asof_date})

        if df.empty:
            self.logger.warning(
                "strategy_candidates asof=%s 无可写入候选（已清空旧数据）。",
                asof_date,
            )
            return

        df = df.copy()
        df["code"] = df["code"].astype(str)
        self.db_writer.write_dataframe(df, TABLE_STRATEGY_CANDIDATES, if_exists="append")
