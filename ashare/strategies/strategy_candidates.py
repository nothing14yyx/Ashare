"""策略候选池生成与刷新。"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import List

import pandas as pd
from sqlalchemy import inspect, text

from ashare.core.config import get_section
from ashare.core.db import DatabaseConfig, MySQLWriter
from ashare.core.schema_manager import SchemaManager, TABLE_STRATEGY_CANDIDATES
from ashare.strategies.ma5_ma20_trend_strategy import MA5MA20Params


@dataclass(frozen=True)
class StrategyCandidatesConfig:
    signal_lookback_days: int = 3

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

    def _load_trade_dates(self, latest_date: dt.date, limit: int) -> List[str]:
        lookback_days = max(int(limit), 1)
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

        if df is None or df.empty:
            return []
        return df["d"].dropna().astype(str).str[:10].tolist()

    def _load_max_valid_days(self, latest_date: dt.date) -> int | None:
        table = self.table_names.signal_events_table
        if not self._table_exists(table):
            return None
        if not self._column_exists(table, "valid_days"):
            return None

        recent_dates = self._load_trade_dates(latest_date, 30)
        if not recent_dates:
            return None
        min_date = recent_dates[-1]

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

    def _resolve_scan_days(self, latest_date: dt.date) -> int:
        lookback = int(self.params.signal_lookback_days)
        max_valid_days = self._load_max_valid_days(latest_date)
        scan_days = lookback
        if max_valid_days is not None:
            scan_days = max(scan_days, max_valid_days + 1)
        return max(int(scan_days), 1)

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

        has_valid_days = self._column_exists(table, "valid_days")
        if not has_valid_days:
            self.logger.error("strategy_signal_events 缺少 valid_days，已跳过信号候选读取。")
            return pd.DataFrame()

        scan_days = self._resolve_scan_days(asof_date)
        trade_dates = self._load_trade_dates(asof_date, scan_days)
        if not trade_dates:
            return pd.DataFrame()
        latest = trade_dates[0]
        earliest = trade_dates[-1]
        select_cols = [
            "code",
            "sig_date",
            "strategy_code",
            "UPPER(COALESCE(`final_action`, `signal`)) AS action",
            "valid_days",
        ]

        # 包含BUY/BUY_CONFIRM信号的股票
        stmt = text(
            f"""
            SELECT {", ".join(select_cols)}
            FROM `{table}`
            WHERE `sig_date` BETWEEN :earliest AND :latest
              AND UPPER(COALESCE(`final_action`, `signal`)) IN ('BUY','BUY_CONFIRM')
            """
        )
        with self.db_writer.engine.begin() as conn:
            df = pd.read_sql_query(
                stmt,
                conn,
                params={"earliest": earliest, "latest": latest},
            )

        # 添加接近信号的股票
        near_signal_df = self._load_near_signal_candidates(asof_date)

        # 合并所有信号
        all_signals_df = pd.concat([df, near_signal_df], ignore_index=True)

        if all_signals_df.empty:
            return all_signals_df

        all_signals_df = all_signals_df.copy()
        all_signals_df["sig_date"] = pd.to_datetime(all_signals_df["sig_date"], errors="coerce")
        all_signals_df = all_signals_df.dropna(subset=["sig_date", "code"])
        all_signals_df["code"] = all_signals_df["code"].astype(str)
        all_signals_df["action"] = all_signals_df["action"].astype(str).str.upper()
        all_signals_df["valid_days"] = pd.to_numeric(all_signals_df.get("valid_days"), errors="coerce")
        all_signals_df["signal_age"] = all_signals_df["sig_date"].dt.strftime("%Y-%m-%d").map(
            {d: i for i, d in enumerate(trade_dates)}
        )
        all_signals_df = all_signals_df.dropna(subset=["valid_days", "signal_age"])
        if all_signals_df.empty:
            return all_signals_df
        all_signals_df["valid_days"] = all_signals_df["valid_days"].astype(int)
        all_signals_df["signal_age"] = all_signals_df["signal_age"].astype(int)
        all_signals_df = all_signals_df[all_signals_df["signal_age"] <= all_signals_df["valid_days"]]
        if all_signals_df.empty:
            return all_signals_df

        action_priority = {"BUY": 3, "BUY_CONFIRM": 2, "NEAR_SIGNAL": 1}
        all_signals_df["action_priority"] = all_signals_df["action"].map(action_priority).fillna(0).astype(int)
        all_signals_df = all_signals_df.sort_values(
            ["code", "sig_date", "action_priority"],
            ascending=[True, False, False],
        )
        latest_df = all_signals_df.groupby("code", as_index=False).first()
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

    def _load_near_signal_candidates(self, asof_date: dt.date) -> pd.DataFrame:
        """加载接近信号的股票"""
        # 从indicator表获取接近MA20的股票
        indicator_table = self.table_names.indicator_table
        if not self._table_exists(indicator_table):
            return pd.DataFrame()

        # 获取最近的交易日数据
        trade_dates = self._load_trade_dates(asof_date, 2)
        if not trade_dates:
            return pd.DataFrame()

        latest_date = trade_dates[0]
        if len(trade_dates) < 2:
            self.logger.warning(
                "near_signal skipped: prev_date missing (trade_dates=%s).",
                trade_dates,
            )
            return pd.DataFrame()
        prev_date = trade_dates[1]

        p = MA5MA20Params.from_config()
        near_ma20_band = float(getattr(p, "near_ma20_band", 0.02))
        near_cross_gap_band = float(getattr(p, "near_cross_gap_band", 0.005))
        near_signal_macd_required = bool(getattr(p, "near_signal_macd_required", False))

        # 查找接近MA20的股票
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
            df = pd.read_sql_query(
                stmt,
                conn,
                params={
                    "latest_date": latest_date,
                    "prev_date": prev_date,
                    "near_ma20_band": near_ma20_band,
                    "near_cross_gap_band": near_cross_gap_band,
                },
            )

        return df

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
