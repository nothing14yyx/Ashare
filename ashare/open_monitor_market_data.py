"""open_monitor 行情抓取与路由层。"""

from __future__ import annotations

import datetime as dt
from typing import Any, List

import pandas as pd

from .open_monitor_quotes import fetch_quotes_akshare, fetch_quotes_eastmoney


class OpenMonitorMarketData:
    """开盘监测行情抓取与数据源路由。"""

    def __init__(self, logger, params) -> None:
        self.logger = logger
        self.params = params

    def fetch_quotes(self, codes: List[str]) -> pd.DataFrame:
        """获取实时行情。

        A 修复点：
        - 行情源常不提供可用的交易日字段，导致宽表里 live_trade_date 全为 null；
        - 这里仅在“确实拿到了实时价”的行上，用本次 monitor_date（或 checked_at 的日期）回填 live_trade_date；
        - 若既无 monitor_date 也无 checked_at，则保持为空，让数据问题暴露出来（不回填 today）。
        """
        if not codes:
            return pd.DataFrame(columns=["code"])

        source = (self.params.quote_source or "eastmoney").strip().lower()
        if source == "akshare":
            df = self._fetch_quotes_akshare(codes)
        else:
            df = self._fetch_quotes_eastmoney(codes)

        # ---- A: 补齐 live_trade_date（仅对有实时价的记录回填）----
        if "live_trade_date" not in df.columns:
            df["live_trade_date"] = pd.NA

        monitor_date = getattr(self.params, "monitor_date", None)
        if not monitor_date:
            checked_at = getattr(self.params, "checked_at", None)
            if checked_at is not None:
                try:
                    monitor_date = checked_at.date().isoformat()
                except Exception:
                    monitor_date = None

        if monitor_date:
            has_live_price = pd.Series(False, index=df.index)
            for col in ("live_latest", "live_open", "live_high", "live_low"):
                if col in df.columns:
                    has_live_price = has_live_price | df[col].notna()

            need_fill = df["live_trade_date"].isna() & has_live_price
            if need_fill.any():
                df.loc[need_fill, "live_trade_date"] = monitor_date

        return df

    def fetch_index_live_quote(self) -> dict[str, Any]:
        code = str(self.params.index_code or "").strip()
        if not code:
            return {}
        df = self.fetch_quotes([code])
        if df.empty:
            return {"index_code": code}
        row = df.iloc[0].to_dict()
        row["index_code"] = code
        row["live_trade_date"] = row.get("live_trade_date") or row.get("trade_date") or row.get("date")
        return row

    def _fetch_quotes_akshare(self, codes: List[str]) -> pd.DataFrame:
        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        return fetch_quotes_akshare(codes, strict_quotes=strict_quotes, logger=self.logger)

    def _fetch_quotes_eastmoney(self, codes: List[str]) -> pd.DataFrame:
        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        return fetch_quotes_eastmoney(codes, strict_quotes=strict_quotes, logger=self.logger)
