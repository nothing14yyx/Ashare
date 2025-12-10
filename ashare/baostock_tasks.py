"""Baostock 相关的数据采集任务。"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd

from .baostock_core import BaostockDataFetcher
from .baostock_session import BaostockSession


def fetch_recent_30d_daily_k_all(output_dir: str) -> None:
    """采集全市场最近 30 天的日 K 线数据并保存为 CSV。"""

    session = BaostockSession()
    fetcher = BaostockDataFetcher(session)

    end_date = fetcher.get_latest_trading_date()
    end_day = datetime.strptime(end_date, "%Y-%m-%d").date()
    start_date = (end_day - timedelta(days=30)).isoformat()

    stock_df = fetcher.get_stock_list(end_date)
    if stock_df.empty:
        print("未获取到股票列表，终止采集。")
        return

    os.makedirs(output_dir, exist_ok=True)

    total = len(stock_df)
    print(
        "开始采集最近 30 天日 K 线：{count} 只股票，区间 {start} - {end}".format(
            count=total, start=start_date, end=end_date
        )
    )

    try:
        for idx, code in enumerate(stock_df["code"], start=1):
            kline_df: pd.DataFrame = fetcher.get_kline(
                code, start_date, end_date, freq="d", adjustflag="1"
            )
            normalized_code = code.replace(".", "").upper()
            file_path = os.path.join(output_dir, f"{normalized_code}_recent30d.csv")
            kline_df.to_csv(file_path, index=False)

            if idx % 100 == 0:
                print("已处理 {done}/{total} 只股票".format(done=idx, total=total))
    finally:
        session.logout()

    print("采集完成，共处理 {count} 只股票。".format(count=total))


if __name__ == "__main__":
    fetch_recent_30d_daily_k_all(output_dir="output/kline_daily")
