"""open_monitor 行情抓取与路由层。"""

from __future__ import annotations

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
        - live_trade_date 优先使用行情源字段（trade_date/date/live_trade_date）。
        - 若行情源未提供交易日字段，使用本次运行的 monitor_date（或 checked_at.date）兜底，便于对账。
        """
        if not codes:
            return pd.DataFrame(columns=["code"])

        source = (self.params.quote_source or "eastmoney").strip().lower()
        if source == "akshare":
            df = self._fetch_quotes_akshare(codes)
        else:
            df = self._fetch_quotes_eastmoney(codes)

        # ---- A: 补齐 live_trade_date（优先使用行情源字段；缺失时兜底 monitor_date）----
        if "live_trade_date" not in df.columns:
            df["live_trade_date"] = pd.NA
        for cand in ("trade_date", "date"):
            if cand in df.columns:
                df["live_trade_date"] = df["live_trade_date"].fillna(df[cand])

        checked_at = getattr(self.params, "checked_at", None)
        monitor_date = getattr(self.params, "monitor_date", None)
        if monitor_date:
            df["live_trade_date"] = df["live_trade_date"].fillna(monitor_date)
        elif checked_at is not None:
            df["live_trade_date"] = df["live_trade_date"].fillna(
                checked_at.date().isoformat()
            )

        if checked_at is not None:
            df["quote_fetched_at"] = checked_at
            df["quote_fetched_date"] = checked_at.date().isoformat()

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

        live_trade_date = row.get("live_trade_date")
        if pd.isna(live_trade_date):
            live_trade_date = None
        if not live_trade_date:
            live_trade_date = row.get("trade_date") or row.get("date")
        if pd.isna(live_trade_date):
            live_trade_date = None
        if not live_trade_date:
            monitor_date = getattr(self.params, "monitor_date", None)
            checked_at = getattr(self.params, "checked_at", None)
            if monitor_date:
                live_trade_date = monitor_date
            elif checked_at is not None:
                live_trade_date = checked_at.date().isoformat()
        row["live_trade_date"] = live_trade_date
        return row

    def _fetch_quotes_akshare(self, codes: List[str]) -> pd.DataFrame:
        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        return fetch_quotes_akshare(codes, strict_quotes=strict_quotes, logger=self.logger)

    def _fetch_quotes_eastmoney(self, codes: List[str]) -> pd.DataFrame:
        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        return fetch_quotes_eastmoney(codes, strict_quotes=strict_quotes, logger=self.logger)
