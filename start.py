"""项目入口脚本，演示如何使用 AKShare 获取 A 股数据。"""

from __future__ import annotations

import os
import pathlib
from typing import List

from src.config import (
    DEFAULT_ADJUST,
    DEFAULT_HISTORY_DAYS,
    DEFAULT_STOCK_CODES,
    HTTP_PROXY,
    HTTPS_PROXY,
    USE_PROXIES,
)

if USE_PROXIES:
    if HTTP_PROXY:
        os.environ["HTTP_PROXY"] = HTTP_PROXY
        os.environ["http_proxy"] = HTTP_PROXY
    if HTTPS_PROXY:
        os.environ["HTTPS_PROXY"] = HTTPS_PROXY
        os.environ["https_proxy"] = HTTPS_PROXY

import pandas as pd

from src.akshare_client import AKShareClient

OUTPUT_DIR = pathlib.Path("output")


def save_dataframe(df: pd.DataFrame, filename: str) -> pathlib.Path:
    """Save DataFrame to ``output`` folder as CSV and return the path."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = OUTPUT_DIR / filename
    df.to_csv(output_path, index=False)
    return output_path


def fetch_and_save_realtime(client: AKShareClient, codes: List[str]) -> pathlib.Path:
    """Fetch real-time quotes and save to CSV."""
    realtime_df = client.fetch_realtime_quotes(codes)
    return save_dataframe(realtime_df, "realtime_quotes.csv")


def fetch_and_save_history(
    client: AKShareClient,
    codes: List[str],
    n_days: int,
    adjust: str | None,
) -> pathlib.Path:
    """Fetch historical quotes for a single code and save to CSV."""
    history_df = client.fetch_recent_history(codes=codes, n_days=n_days, adjust=adjust)
    filename = f"history_recent_{n_days}_days.csv"
    return save_dataframe(history_df, filename)


def run() -> None:
    """项目的统一入口，不依赖命令行参数。"""
    client = AKShareClient(use_proxies=USE_PROXIES)
    realtime_path = fetch_and_save_realtime(client, DEFAULT_STOCK_CODES)

    history_path = fetch_and_save_history(
        client,
        DEFAULT_STOCK_CODES,
        DEFAULT_HISTORY_DAYS,
        DEFAULT_ADJUST,
    )

    print("实时行情已保存至:", realtime_path)
    print("历史行情已保存至:", history_path)


if __name__ == "__main__":
    run()
