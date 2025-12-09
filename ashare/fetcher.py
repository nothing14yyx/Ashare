"""基于 AKShare 数据字典的接口清单获取封装."""

from __future__ import annotations

from typing import List

from .config import ProxyConfig
from .dictionary import DataDictionaryFetcher


class AshareDataFetcher:
    """提供与 A 股接口数据字典相关的功能."""

    def __init__(
        self,
        dictionary_fetcher: DataDictionaryFetcher | None = None,
        proxy_config: ProxyConfig | None = None,
    ):
        self.proxy_config = proxy_config or ProxyConfig.from_env()
        self.proxy_config.apply_to_environment()
        self.dictionary_fetcher = dictionary_fetcher or DataDictionaryFetcher(
            proxies=self.proxy_config.as_requests_proxies()
        )

    def available_interfaces(self) -> List[str]:
        """列出数据字典中支持的全部 A 股接口."""

        return self.dictionary_fetcher.list_a_share_endpoints()
