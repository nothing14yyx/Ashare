"""基于 AKShare 数据字典的接口清单获取封装."""

from __future__ import annotations

from typing import List

import akshare as ak

from .config import ProxyConfig
from .dictionary import DataDictionaryFetcher


class AshareDataFetcher:
    """提供与 A 股接口数据字典相关的功能."""

    def __init__(
        self,
        dictionary_fetcher: DataDictionaryFetcher | None = None,
        proxy_config: ProxyConfig | None = None,
    ):
        # 代理配置：优先使用传入的，其次从环境变量构造
        self.proxy_config = proxy_config or ProxyConfig.from_env()
        # 应用到环境变量，让后续请求（包括 akshare 内部）也复用这个代理
        self.proxy_config.apply_to_environment()

        # 数据字典抓取器
        self.dictionary_fetcher = dictionary_fetcher or DataDictionaryFetcher(
            proxies=self.proxy_config.as_requests_proxies()
        )

    def available_interfaces(self) -> List[str]:
        """
        列出可用的 A 股接口名。

        逻辑：
        1. 从 DataDictionaryFetcher 读出候选接口名（可能包含示例里的变量名等脏数据）
        2. 只保留 akshare 中真实存在的函数名
        3. 再过滤掉明显是示例变量名 / DataFrame 变量名的东西：
           例如 *_df / *_qfq_df / *_hfq_df 这类
        """

        # 1) 从数据字典获取候选接口名
        candidates = self.dictionary_fetcher.list_a_share_endpoints()

        # 2) 只保留 akshare 中真实存在的属性（函数）
        existing = [name for name in candidates if hasattr(ak, name)]

        # 3) 过滤掉常见的“变量名风格”后缀（不是接口，只是文档示例里的变量）
        filtered = [
            name
            for name in existing
            if not (
                name.endswith("_df")
                or name.endswith("_qfq_df")
                or name.endswith("_hfq_df")
            )
        ]

        return filtered
