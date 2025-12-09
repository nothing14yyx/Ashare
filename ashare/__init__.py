"""A 股数据获取工具包."""

from .dictionary import DataDictionaryFetcher
from .fetcher import AshareDataFetcher

__all__ = ["DataDictionaryFetcher", "AshareDataFetcher"]
