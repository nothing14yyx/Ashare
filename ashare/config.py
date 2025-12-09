"""应用运行时的代理配置管理工具."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict


@dataclass
class ProxyConfig:
    """描述 HTTP/HTTPS 代理配置的简单数据类."""

    http: str | None = None
    https: str | None = None

    @classmethod
    def from_env(cls) -> "ProxyConfig":
        """从环境变量加载代理配置.

        优先读取 ASHARE_HTTP_PROXY 与 ASHARE_HTTPS_PROXY, 其次读取
        HTTP_PROXY 与 HTTPS_PROXY, 便于与现有代理工具兼容。
        """

        http_proxy = (
            os.environ.get("ASHARE_HTTP_PROXY")
            or os.environ.get("HTTP_PROXY")
            or os.environ.get("http_proxy")
        )
        https_proxy = (
            os.environ.get("ASHARE_HTTPS_PROXY")
            or os.environ.get("HTTPS_PROXY")
            or os.environ.get("https_proxy")
        )
        return cls(http=http_proxy, https=https_proxy)

    def as_requests_proxies(self) -> Dict[str, str]:
        """以 requests 可用的格式输出代理配置."""

        proxies: Dict[str, str] = {}
        if self.http:
            proxies["http"] = self.http
        if self.https:
            proxies["https"] = self.https
        return proxies

    def apply_to_environment(self) -> None:
        """将代理配置写入环境变量, 便于底层库读取."""

        if self.http:
            os.environ["HTTP_PROXY"] = self.http
            os.environ["http_proxy"] = self.http
        if self.https:
            os.environ["HTTPS_PROXY"] = self.https
            os.environ["https_proxy"] = self.https
