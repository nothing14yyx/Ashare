from __future__ import annotations

import datetime as dt
from typing import Dict, List, Tuple

import pandas as pd
from sqlalchemy import bindparam, text

from ashare.core.db import DatabaseConfig, MySQLWriter
from ashare.core.schema_manager import (
    STRATEGY_CODE_MA5_MA20_TREND,
    TABLE_STRATEGY_TRADE_METRICS,
    SchemaManager,
)
from ashare.utils import setup_logger


class TradeMetricsCalculator:
    def __init__(self) -> None:
        self.logger = setup_logger()
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())
        self.schema_manager = SchemaManager(self.db_writer.engine)
        tables = self.schema_manager.get_table_names()
        self.events_table = tables.signal_events_table
        self.indicator_table = tables.indicator_table

    def _load_events(self) -> pd.DataFrame:
        if not self.schema_manager._table_exists(self.events_table):
            self.logger.warning("事件表 %s 不存在，跳过交易结果计算。", self.events_table)
            return pd.DataFrame()

        stmt = text(
            f"""
            SELECT
              `sig_date`,
              `code`,
              COALESCE(`final_action`, `signal`) AS `action`,
              COALESCE(`final_reason`, `reason`) AS `reason`,
              `strategy_code`
            FROM `{self.events_table}`
            WHERE `strategy_code` = :strategy
            ORDER BY `code`,`sig_date`
            """
        )
        with self.db_writer.engine.begin() as conn:
            df = pd.read_sql_query(stmt, conn, params={"strategy": STRATEGY_CODE_MA5_MA20_TREND})
        df["sig_date"] = pd.to_datetime(df["sig_date"], errors="coerce").dt.date
        df["code"] = df["code"].astype(str)
        return df

    def _load_prices(self, dates: List[dt.date], codes: List[str]) -> pd.DataFrame:
        if not self.schema_manager._table_exists(self.indicator_table):
            return pd.DataFrame()

        stmt = (
            text(
                f"""
                SELECT `trade_date`,`code`,`close`,`atr14`
                FROM `{self.indicator_table}`
                WHERE `trade_date` IN :dates AND `code` IN :codes
                """
            ).bindparams(bindparam("dates", expanding=True), bindparam("codes", expanding=True))
        )
        with self.db_writer.engine.begin() as conn:
            df = pd.read_sql_query(
                stmt,
                conn,
                params={"dates": [d.isoformat() for d in dates], "codes": codes},
            )
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
        df["code"] = df["code"].astype(str)
        return df

    def _resolve_price_map(self, price_df: pd.DataFrame) -> Dict[Tuple[dt.date, str], Dict[str, float]]:
        if price_df.empty:
            return {}
        price_df = price_df.dropna(subset=["trade_date", "code"]).copy()
        price_df["close"] = pd.to_numeric(price_df["close"], errors="coerce")
        price_df["atr14"] = pd.to_numeric(price_df["atr14"], errors="coerce")
        price_df = price_df.sort_values(["trade_date", "code"]).drop_duplicates(subset=["trade_date", "code"], keep="last")
        return price_df.set_index(["trade_date", "code"])[["close", "atr14"]].to_dict(orient="index")

    def _build_metrics(self, events: pd.DataFrame, price_map: Dict[Tuple[dt.date, str], Dict[str, float]]) -> pd.DataFrame:
        rows: List[Dict[str, object]] = []
        grouped = events.groupby("code", sort=False)
        for code, sub in grouped:
            entry_row: Dict[str, object] | None = None
            for _, row in sub.iterrows():
                action = str(row.get("action") or "").upper()
                sig_date = row.get("sig_date")
                if not isinstance(sig_date, dt.date):
                    continue
                if entry_row is None and action in {"BUY", "BUY_CONFIRM"}:
                    entry_row = row.to_dict()
                    continue
                if entry_row and action in {"SELL"}:
                    entry_date = entry_row.get("sig_date")
                    if not isinstance(entry_date, dt.date):
                        entry_row = None
                        continue
                    entry_price_info = price_map.get((entry_date, code))
                    exit_price_info = price_map.get((sig_date, code))
                    if not entry_price_info or not exit_price_info:
                        entry_row = None
                        continue
                    entry_price = entry_price_info.get("close")
                    atr_at_entry = entry_price_info.get("atr14")
                    exit_price = exit_price_info.get("close")
                    if entry_price is None or exit_price is None:
                        entry_row = None
                        continue
                    pnl_pct = (exit_price - entry_price) / entry_price if entry_price else None
                    atr_denom = (atr_at_entry / entry_price) if entry_price and atr_at_entry else None
                    pnl_atr_ratio = (pnl_pct / atr_denom) if (pnl_pct is not None and atr_denom) else None
                    holding_days = (sig_date - entry_date).days if isinstance(sig_date, dt.date) else None
                    rows.append(
                        {
                            "strategy_code": STRATEGY_CODE_MA5_MA20_TREND,
                            "code": code,
                            "entry_date": entry_date,
                            "entry_price": entry_price,
                            "exit_date": sig_date,
                            "exit_price": exit_price,
                            "atr_at_entry": atr_at_entry,
                            "pnl_pct": pnl_pct,
                            "pnl_atr_ratio": pnl_atr_ratio,
                            "holding_days": holding_days,
                            "exit_reason": row.get("reason"),
                        }
                    )
                    entry_row = None
        return pd.DataFrame(rows)

    def run(self) -> None:
        self.schema_manager.ensure_all()
        events = self._load_events()
        if events.empty:
            self.logger.info("无交易事件，跳过交易指标计算。")
            return

        dates = [d for d in events["sig_date"].dropna().unique().tolist() if isinstance(d, dt.date)]
        codes = events["code"].dropna().astype(str).unique().tolist()
        price_df = self._load_prices(dates, codes)
        price_map = self._resolve_price_map(price_df)
        metrics_df = self._build_metrics(events, price_map)
        if metrics_df.empty:
            self.logger.info("未能生成任何交易指标行。")
            return

        delete_stmt = text(
            f"DELETE FROM `{TABLE_STRATEGY_TRADE_METRICS}` WHERE `strategy_code` = :strategy"
        )
        with self.db_writer.engine.begin() as conn:
            conn.execute(delete_stmt, {"strategy": STRATEGY_CODE_MA5_MA20_TREND})
        self.db_writer.write_dataframe(metrics_df, TABLE_STRATEGY_TRADE_METRICS, if_exists="append")
        self.logger.info("已写入 %s 条交易结果指标。", len(metrics_df))


def run() -> None:
    TradeMetricsCalculator().run()


if __name__ == "__main__":
    run()
