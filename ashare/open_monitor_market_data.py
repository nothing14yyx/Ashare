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
        """获取实时行情。"""

        source = (self.params.quote_source or "eastmoney").strip().lower()
        if source == "akshare":
            return self._fetch_quotes_akshare(codes)
        return self._fetch_quotes_eastmoney(codes)

    def fetch_index_live_quote(self) -> dict[str, Any]:
        code = str(self.params.index_code or "").strip()
        if not code:
            return {}
        df = self.fetch_quotes([code])
        if df.empty:
            return {"index_code": code}
        row = df.iloc[0].to_dict()
        row["index_code"] = code
        row["live_trade_date"] = dt.date.today().isoformat()
        return row

    def _fetch_quotes_akshare(self, codes: List[str]) -> pd.DataFrame:
        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        return fetch_quotes_akshare(codes, strict_quotes=strict_quotes, logger=self.logger)

    def _fetch_quotes_eastmoney(self, codes: List[str]) -> pd.DataFrame:
        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        return fetch_quotes_eastmoney(codes, strict_quotes=strict_quotes, logger=self.logger)
