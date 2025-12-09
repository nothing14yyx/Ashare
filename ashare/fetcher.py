"""基于 AKShare 的 A 股数据获取封装."""

from __future__ import annotations

from typing import Dict, List

import akshare as ak
import pandas as pd
from requests.exceptions import RequestException

from .config import ProxyConfig
from .dictionary import DataDictionaryFetcher


class AshareDataFetcher:
    """封装常用的 A 股数据请求逻辑.

    通过数据字典校验接口可用性, 再调用 AKShare 的真实接口。
    """

    def __init__(
        self,
        dictionary_fetcher: DataDictionaryFetcher | None = None,
        proxy_config: ProxyConfig | None = None,
    ):
        self.proxy_config = proxy_config or ProxyConfig.from_env()
        self.proxy_config.apply_to_environment()
        if dictionary_fetcher is None:
            dictionary_fetcher = DataDictionaryFetcher(
                proxies=self.proxy_config.as_requests_proxies()
            )
        self.dictionary_fetcher = dictionary_fetcher

    def _resolve_interface(self, interface_name: str):
        if not self.dictionary_fetcher.is_supported(interface_name):
            raise ValueError(f"接口 {interface_name} 不存在于 AKShare 数据字典中")

        try:
            return getattr(ak, interface_name)
        except AttributeError as exc:
            raise ValueError(
                f"接口 {interface_name} 在当前 AKShare 版本中不可用"
            ) from exc

    def fetch(self, interface_name: str, **kwargs) -> pd.DataFrame:
        """调用指定接口并返回 DataFrame."""

        interface = self._resolve_interface(interface_name)
        try:
            return interface(**kwargs)
        except RequestException as exc:
            raise RuntimeError(
                "网络请求失败, 请检查是否被代理或防火墙拦截。"
            ) from exc

    def available_interfaces(self) -> List[str]:
        """列出数据字典中支持的全部 A 股接口."""

        return self.dictionary_fetcher.list_a_share_endpoints()

    def realtime_quotes(self) -> pd.DataFrame:
        """获取沪深京 A 股的实时行情."""

        try:
            return self.fetch("stock_zh_a_spot")
        except (RuntimeError, ValueError):
            pass

        try:
            return self.fetch("stock_zh_a_spot_em")
        except (RuntimeError, ValueError) as exc:
            raise RuntimeError(
                "实时行情接口不可用, 已尝试 stock_zh_a_spot 与 stock_zh_a_spot_em"
            ) from exc

    def symbol_list(self) -> pd.DataFrame:
        """提取股票代码与名称便于后续查询."""

        data = self.realtime_quotes()
        if {"代码", "名称"}.issubset(data.columns):
            return data[["代码", "名称"]].copy()
        renamed = data.rename(columns={"symbol": "代码", "name": "名称"})
        return renamed[["代码", "名称"]]

    def history_quotes(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str = "daily",
        adjust: str | None = "qfq",
    ) -> pd.DataFrame:
        """获取指定股票的历史行情数据.

        Args:
            symbol: 股票代码, 例如 "000001"。
            start_date: 起始日期, 格式 "YYYYMMDD"。
            end_date: 结束日期, 格式 "YYYYMMDD"。
            period: 周期, 默认日线。
            adjust: 复权方式, 支持 "qfq", "hfq", None。
        """

        params: Dict[str, str | None] = {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "period": period,
            "adjust": adjust,
        }
        return self.fetch("stock_zh_a_hist", **params)

    def daily_quotes(self, symbol: str, adjust: str | None = "qfq") -> pd.DataFrame:
        """使用 ``stock_zh_a_daily`` 接口抓取日线行情数据.

        Args:
            symbol: 股票代码, 需包含交易所前缀, 例如 "sh600000" 或 "sz000001"。
            adjust: 复权方式, 支持 "qfq", "hfq" 或 None。

        Returns:
            对应股票的日线行情数据。
        """

        return self.fetch("stock_zh_a_daily", symbol=symbol, adjust=adjust)

    def minute_quotes(
        self, symbol: str, period: str = "5", adjust: str | None = "qfq"
    ) -> pd.DataFrame:
        """获取分时级别的行情数据."""

        return self.fetch(
            "stock_zh_a_minute", symbol=symbol, period=period, adjust=adjust
        )
