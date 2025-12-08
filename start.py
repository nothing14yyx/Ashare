"""项目入口脚本，演示如何使用 AKShare 获取 A 股数据。"""

from __future__ import annotations

import pathlib
from typing import List

import pandas as pd

from src.akshare_client import AKShareClient
from src.config import (
    DEFAULT_ADJUST,
    DEFAULT_END_DATE,
    DEFAULT_START_DATE,
    DEFAULT_STOCK_CODES,
    USE_PROXIES,
)

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
    code: str,
    start_date: str,
    end_date: str | None,
    adjust: str,
) -> pathlib.Path:
    """Fetch historical quotes for a single code and save to CSV."""
    history_df = client.fetch_history(
        code=code,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
    )
    filename = f"history_{code}_{start_date}_{end_date or 'latest'}.csv"
    return save_dataframe(history_df, filename)


def run() -> None:
    """项目的统一入口，不依赖命令行参数。"""
    client = AKShareClient(use_proxies=USE_PROXIES)
    realtime_path = fetch_and_save_realtime(client, DEFAULT_STOCK_CODES)

    history_paths = [
        fetch_and_save_history(
            client,
            code,
            DEFAULT_START_DATE,
            DEFAULT_END_DATE,
            DEFAULT_ADJUST,
        )
        for code in DEFAULT_STOCK_CODES
    ]

    print("实时行情已保存至:", realtime_path)
    print("历史行情已保存至:")
    for path in history_paths:
        print(" -", path)


if __name__ == "__main__":
    run()
