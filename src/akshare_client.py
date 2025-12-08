"""Simple AKShare client for fetching A-share data."""

from __future__ import annotations

from typing import List, Optional

import akshare as ak
import pandas as pd


class AKShareClient:
    """Wrapper around AKShare for commonly used A-share data queries."""

    def fetch_realtime_quotes(self, codes: List[str]) -> pd.DataFrame:
        """Retrieve real-time quotes for the given stock codes.

        Args:
            codes: A list of stock codes such as "600000" or "000001".

        Returns:
            A pandas DataFrame containing the real-time quotes for the requested
            stock codes, preserving the order of ``codes``.
        """
        if not codes:
            raise ValueError("请至少提供一个股票代码进行查询")

        quotes = ak.stock_zh_a_spot_em()
        quotes = quotes.rename(columns={"代码": "code"})
        selected = quotes[quotes["code"].isin(codes)]

        if selected.empty:
            raise LookupError("未能获取到对应股票的实时行情，请检查代码是否正确")

        ordered = pd.CategoricalIndex(codes, ordered=True)
        selected["code"] = selected["code"].astype(str)
        selected = selected.set_index("code").loc[ordered].reset_index()
        return selected

    def fetch_history(
        self,
        code: str,
        start_date: str,
        end_date: Optional[str] = None,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        """Retrieve historical data for a single stock.

        Args:
            code: Stock code such as "600000".
            start_date: Start date in ``YYYYMMDD`` format.
            end_date: End date in ``YYYYMMDD`` format. Defaults to today when omitted.
            adjust: ``qfq`` (前复权), ``hfq`` (后复权) or ```` for no adjustment.

        Returns:
            A pandas DataFrame with the historical quotes.
        """
        if not code:
            raise ValueError("股票代码不能为空")

        history = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )

        if history.empty:
            raise LookupError("未能获取到历史行情，请检查日期范围或股票代码")

        return history
