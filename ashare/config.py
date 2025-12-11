"""应用运行时的代理配置管理工具."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Any
from functools import lru_cache
from pathlib import Path

import yaml

# 环境变量中可以用 ASHARE_CONFIG_FILE 指定配置文件路径
CONFIG_FILE_ENV = "ASHARE_CONFIG_FILE"
# 默认的配置文件名，位于项目根目录
DEFAULT_CONFIG_FILENAME = "config.yaml"


@lru_cache()
def load_config() -> Dict[str, Any]:
    """
    读取并缓存 config.yaml 内容。

    优先顺序：
    1. 环境变量 ASHARE_CONFIG_FILE 指定的路径；
    2. 项目根目录下的 config.yaml。
    """
    path_str = os.getenv(CONFIG_FILE_ENV)
    if path_str:
        path = Path(path_str)
    else:
        # ashare/config.py -> ashare 目录 -> 项目根目录
        path = Path(__file__).resolve().parents[1] / DEFAULT_CONFIG_FILENAME

    if not path.is_file():
        # 没有配置文件时，返回空 dict，调用方自己处理默认值
        return {}

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError("config.yaml 顶层必须是一个对象（mapping）")

    return data


def get_section(name: str) -> Dict[str, Any]:
    """
    获取配置中的某个子节，例如 app/database/proxy/baostock 等。

    不存在时返回空 dict。
    """
    cfg = load_config()
    section = cfg.get(name) or {}
    if not isinstance(section, dict):
        raise ValueError(f"config.yaml 中 {name} 必须是一个对象（mapping）")
    return section


@dataclass
class ProxyConfig:
    """描述 HTTP/HTTPS 代理配置的简单数据类。"""

    http: str | None = None
    https: str | None = None

    @classmethod
    def from_env(cls) -> "ProxyConfig":
        """
        从环境变量与 config.yaml 加载代理配置。

        优先顺序：
        1. ASHARE_HTTP_PROXY / ASHARE_HTTPS_PROXY；
        2. HTTP_PROXY / HTTPS_PROXY（含小写）；
        3. config.yaml 中 proxy.http / proxy.https。
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
