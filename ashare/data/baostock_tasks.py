"""Baostock 相关的数据采集任务。"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

import pandas as pd

from ashare.data.baostock_core import ADJUSTFLAG_NONE, BaostockDataFetcher
from ashare.data.baostock_session import BaostockSession


def fetch_recent_30d_daily_k_all(
    output_dir: str, session: BaostockSession | None = None
) -> None:
    """采集全市场最近 30 天的日 K 线数据并保存为 CSV。"""

    def _fetch_kline_with_retry(
        code: str,
        start_date: str,
        end_date: str,
        max_retries: int = 3,
    ) -> pd.DataFrame:
        last_exc: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                session.ensure_alive()
                return fetcher.get_kline(
                    code,
                    start_date,
                    end_date,
                    freq="d",
                    adjustflag=ADJUSTFLAG_NONE,
                )
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt >= max_retries:
                    break

                wait_time = min(10.0, 2 ** (attempt - 1))
                print(
                    "[%s] 拉取失败（第 %s/%s 次），%s 秒后重试：%s"
                    % (code, attempt, max_retries, wait_time, exc)
                )
                session.ensure_alive(force_refresh=True)
                time.sleep(wait_time)

        raise RuntimeError(f"{code} 拉取失败：{last_exc}")

    external_session = session is not None
    session = session or BaostockSession()
    fetcher = BaostockDataFetcher(session)
    session.ensure_alive()

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

    failed_codes: list[tuple[str, str]] = []

    try:
        for idx, code in enumerate(stock_df["code"], start=1):
            try:
                kline_df = _fetch_kline_with_retry(code, start_date, end_date)
            except Exception as exc:  # noqa: BLE001
                failed_codes.append((code, str(exc)))
                continue

            normalized_code = code.replace(".", "").upper()
            file_path = os.path.join(output_dir, f"{normalized_code}_recent30d.csv")
            kline_df.to_csv(file_path, index=False)

            if idx % 100 == 0:
                print("已处理 {done}/{total} 只股票".format(done=idx, total=total))
    finally:
        if not external_session:
            session.logout()

    print("采集完成，共处理 {count} 只股票。".format(count=total))
    if failed_codes:
        print("以下股票拉取失败（未写入 CSV）：")
        for code, error_msg in failed_codes:
            print(f"- {code}: {error_msg}")


if __name__ == "__main__":
    fetch_recent_30d_daily_k_all(output_dir="output/kline_daily")
