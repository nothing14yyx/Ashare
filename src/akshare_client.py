"""Simple AKShare client for fetching A-share data."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Callable, List, Optional, Sequence

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
    def _temporary_proxy_env(self, enable: bool | None = None):
        """根据 ``use_proxies`` 临时屏蔽或恢复代理环境变量。"""

        use_proxy = self.use_proxies if enable is None else enable
        proxy_keys = [key for key in os.environ if key.lower().endswith("_proxy")]
        cached_values = {}

        if not use_proxy:
            for key in proxy_keys:
                cached_values[key] = os.environ.pop(key, None)

        try:
            yield
        finally:
            if not use_proxy:
                for key, value in cached_values.items():
                    if value is None:
                        continue
                    os.environ[key] = value

    def _run_with_proxy_fallback(
        self, action: Callable[[], pd.DataFrame], error_message: str
    ) -> pd.DataFrame:
        """执行请求，若代理异常则自动回退到直连。"""

        try:
            with self._temporary_proxy_env():
                return action()
        except requests.exceptions.ProxyError as exc:
            if not self.use_proxies:
                raise ConnectionError(error_message) from exc

            try:
                with self._temporary_proxy_env(enable=False):
                    return action()
            except requests.exceptions.ProxyError as retry_exc:
                raise ConnectionError(error_message) from retry_exc

    @staticmethod
    def _normalize_code(code: str) -> str:
        """将股票代码规范化为 6 位数字字符串。"""

        digits = "".join(ch for ch in str(code) if ch.isdigit())
        return digits[-6:].zfill(6)

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

        normalized_codes = [self._normalize_code(code) for code in codes]

        quotes = self._run_with_proxy_fallback(
            action=ak.stock_zh_a_spot,
            error_message=(
                "实时行情查询失败：连接数据接口时被远端中断，可能是网络不稳定、网站风控或"
                "代理配置问题，请稍后重试"
            ),
        )

        quotes = quotes.copy()
        quotes["code_6"] = quotes["代码"].astype(str).str[-6:]
        selected = quotes[quotes["code_6"].isin(normalized_codes)].copy()
        selected.insert(0, "code", selected.pop("code_6"))

        if selected.empty:
            raise LookupError("未能获取到对应股票的实时行情，请检查代码是否正确")

        desired_columns = [
            "code",
            "名称",
            "最新价",
            "涨跌额",
            "涨跌幅",
            "今开",
            "昨收",
            "最高",
            "最低",
            "成交量",
            "成交额",
        ]
        selected = selected[desired_columns]

        ordered = pd.CategoricalIndex(normalized_codes, ordered=True)
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

        normalized_code = self._normalize_code(code)
        history = self._run_with_proxy_fallback(
            action=lambda: ak.stock_zh_a_hist(
                symbol=normalized_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            ),
            error_message=(
                "历史行情查询失败：连接数据接口时被远端中断，可能是网络不稳定、网站风控或"
                "代理配置问题，请稍后重试"
            ),
        )

        if history.empty:
            raise LookupError("未能获取到历史行情，请检查日期范围或股票代码")

        return history

    def fetch_recent_history(
        self,
        codes: Sequence[str],
        n_days: int = 30,
        adjust: str | None = "qfq",
    ) -> pd.DataFrame:
        """获取最近 ``n_days`` 天的历史行情数据。"""

        if not codes:
            raise ValueError("请至少提供一个股票代码进行查询")
        if n_days <= 0:
            raise ValueError("n_days 需要为正整数")

        today = date.today()
        start_date = (today - timedelta(days=n_days - 1)).strftime("%Y%m%d")
        end_date = today.strftime("%Y%m%d")

        records: list[pd.DataFrame] = []
        for code in codes:
            normalized_code = self._normalize_code(code)
            history = self._run_with_proxy_fallback(
                action=lambda c=normalized_code: ak.stock_zh_a_hist(
                    symbol=c,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                ),
                error_message=(
                    "历史行情查询失败：连接数据接口时被远端中断，可能是网络不稳定、网站风控或"
                    "代理配置问题，请稍后重试"
                ),
            )

            if history.empty:
                continue

            history = history.copy()
            history.insert(0, "代码", normalized_code)
            records.append(history)

        if not records:
            raise LookupError("未能获取到历史行情，请检查日期范围或股票代码")

        return pd.concat(records, ignore_index=True)
