"""Simple AKShare client for fetching A-share data."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import List, Optional

import akshare as ak
import pandas as pd
import requests


class AKShareClient:
    """Wrapper around AKShare for commonly used A-share data queries."""

    def __init__(self, use_proxies: bool = True):
        """初始化客户端。

        Args:
            use_proxies: 是否允许从环境变量读取代理配置。默认 ``True``。
                当本地配置了失效或不可访问的代理时，可以传入 ``False``
                来临时屏蔽代理，避免 ``ProxyError`` 导致数据拉取失败。
        """

        self.use_proxies = use_proxies

    @contextmanager
    def _temporary_proxy_env(self):
        """根据 ``use_proxies`` 临时屏蔽或恢复代理环境变量。"""

        proxy_keys = [key for key in os.environ if key.lower().endswith("_proxy")]
        cached_values = {}

        if not self.use_proxies:
            for key in proxy_keys:
                cached_values[key] = os.environ.pop(key, None)

        try:
            yield
        finally:
            if not self.use_proxies:
                for key, value in cached_values.items():
                    if value is None:
                        continue
                    os.environ[key] = value

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

        try:
            with self._temporary_proxy_env():
                quotes = ak.stock_zh_a_spot_em()
        except requests.exceptions.ProxyError as exc:
            raise ConnectionError(
                "实时行情查询失败：检测到代理配置不可用，请关闭或修正代理后重试"
            ) from exc

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

        try:
            with self._temporary_proxy_env():
                history = ak.stock_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                )
        except requests.exceptions.ProxyError as exc:
            raise ConnectionError(
                "历史行情查询失败：检测到代理配置不可用，请关闭或修正代理后重试"
            ) from exc

        if history.empty:
            raise LookupError("未能获取到历史行情，请检查日期范围或股票代码")

        return history
