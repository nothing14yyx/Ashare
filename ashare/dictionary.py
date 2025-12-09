"""数据字典解析与提取工具.

该模块负责从 AKShare 官方数据字典页面解析与 A 股相关的接口名称,
并提供缓存能力避免重复网络请求。
"""

from dataclasses import dataclass, field
from typing import List, Set
import re
import warnings

import requests
from urllib3.exceptions import InsecureRequestWarning

A_SHARE_PATTERN = re.compile(r"stock_zh_[a-zA-Z0-9_]+")


@dataclass
class DataDictionaryFetcher:
    """从 AKShare 数据字典页面提取接口列表的工具类.

    Attributes:
        stock_doc_url: 数据字典中股票板块的页面地址。
        verify_ssl: 是否验证 HTTPS 证书。由于部分环境证书链缺失, 默认关闭以提升可用性。
        timeout: 网络请求超时时间 (秒)。
    """

    stock_doc_url: str = "https://akshare.akfamily.xyz/data/stock/stock.html"
    verify_ssl: bool = False
    timeout: int = 10
    _cached_html: str | None = field(default=None, init=False, repr=False)
    _cached_endpoints: List[str] | None = field(default=None, init=False, repr=False)

    def fetch_raw_html(self) -> str:
        """获取股票数据字典页面的 HTML 文本并缓存结果.

        Returns:
            HTML 文本字符串。
        """

        if self._cached_html is not None:
            return self._cached_html

        if not self.verify_ssl:
            warnings.filterwarnings("ignore", category=InsecureRequestWarning)

        response = requests.get(
            self.stock_doc_url, timeout=self.timeout, verify=self.verify_ssl
        )
        response.raise_for_status()
        self._cached_html = response.text
        return response.text

    def extract_a_share_endpoints(self, html: str) -> Set[str]:
        """从 HTML 中提取所有 A 股相关接口名称.

        Args:
            html: 数据字典页面的 HTML 文本。

        Returns:
            去重后的接口名称集合。
        """

        return set(A_SHARE_PATTERN.findall(html))

    def list_a_share_endpoints(self) -> List[str]:
        """返回数据字典中出现的所有 A 股接口名称 (排序后).

        Returns:
            排序后的接口名称列表。
        """

        if self._cached_endpoints is not None:
            return self._cached_endpoints

        html = self.fetch_raw_html()
        endpoints = sorted(self.extract_a_share_endpoints(html))
        self._cached_endpoints = endpoints
        return endpoints

    def is_supported(self, interface_name: str) -> bool:
        """检查接口是否在数据字典中登记.

        Args:
            interface_name: 需要校验的接口名。

        Returns:
            True 表示接口存在于数据字典, False 表示不存在。
        """

        return interface_name in self.list_a_share_endpoints()
