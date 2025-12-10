"""Baostock 数据访问层封装。"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Iterable

import baostock as bs
import pandas as pd

from .baostock_session import BaostockSession


class BaostockDataFetcher:
    """封装常用 Baostock 数据访问接口。"""

    def __init__(self, session: BaostockSession) -> None:
        """保存会话引用，供后续请求使用。"""

        self.session = session

    def _ensure_session(self) -> None:
        """确保会话已登录。"""

        self.session.connect()

    def _resultset_to_df(self, rs: Any) -> pd.DataFrame:
        """将 Baostock ResultSet 转换为 DataFrame，并在失败时抛出错误。"""

        if rs.error_code != "0":
            raise RuntimeError(f"Baostock 调用失败: {rs.error_code}, {rs.error_msg}")

        rows: list[Iterable[str]] = []
        while rs.next():
            rows.append(rs.get_row_data())
        return pd.DataFrame(rows, columns=rs.fields)

    def get_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """查询交易日历并过滤出交易日。"""

        self._ensure_session()
        rs = bs.query_trade_dates(start_date, end_date)
        df = self._resultset_to_df(rs)
        trading_df = df[df["is_trading_day"] == "1"].reset_index(drop=True)
        return trading_df[["calendar_date", "is_trading_day"]]

    def get_latest_trading_date(self, lookback_days: int = 365) -> str:
        """获取最近一个交易日。"""

        today = date.today()
        start = today - timedelta(days=lookback_days)
        trading_calendar = self.get_trade_calendar(start.isoformat(), today.isoformat())
        if trading_calendar.empty:
            raise ValueError(
                "在最近 {days} 天内未找到交易日，无法确定最近交易日。".format(
                    days=lookback_days
                )
            )

        latest_date = (
            trading_calendar.sort_values("calendar_date")["calendar_date"].iloc[-1]
        )
        return str(latest_date)

    def get_stock_list(self, trade_date: str, fallback_days: int = 15) -> pd.DataFrame:
        """按交易日获取 A 股列表。

        参数
        ----------
        trade_date : str
            期望的交易日，格式为 "YYYY-MM-DD"。
        fallback_days : int, 默认 15
            如果该日期没有返回股票列表（例如当天数据尚未生成、
            或遇到节假日），则自动向前回退最多 fallback_days 天，
            返回最近一个有数据的交易日的股票列表。

        返回
        ----------
        pd.DataFrame
            含有 code, code_name, tradeStatus 列的股票列表。
            若在 fallback_days 内仍未找到任何数据，则返回空 DataFrame。
        """

        self._ensure_session()

        def _query(day: str) -> pd.DataFrame:
            """内部封装一次 query_all_stock 调用。"""
            rs = bs.query_all_stock(day=day)
            df = self._resultset_to_df(rs)
            return df

        # 1) 先尝试用户指定的日期
        df = _query(trade_date)

        # 2) 如果没有数据，则向前回退，最多 fallback_days 天
        if df.empty:
            current = datetime.strptime(trade_date, "%Y-%m-%d").date()
            for i in range(1, fallback_days + 1):
                prev_day = (current - timedelta(days=i)).isoformat()
                df = _query(prev_day)
                if not df.empty:
                    # 找到了就停止回退，使用这个日期的数据
                    break

        # 3) 如果依然没有数据，直接返回空 DataFrame，由上层决定如何处理
        if df.empty:
            return df

        # 4) 过滤出 A 股主板并只保留常用字段
        prefixes = ("sh.60", "sz.00")
        filtered = df[
            df["code"].str.startswith(prefixes) & (df["tradeStatus"] == "1")
        ].reset_index(drop=True)

        columns = [
            col for col in ["code", "code_name", "tradeStatus"] if col in filtered
        ]
        return filtered[columns]

    def get_kline(
        self,
        code: str,
        start_date: str,
        end_date: str,
        freq: str = "d",
        adjustflag: str = "3",
    ) -> pd.DataFrame:
        """获取 K 线行情数据。"""

        self._ensure_session()
        fields = (
            "date,code,open,high,low,close,preclose,volume,amount,"
            "adjustflag,tradestatus,pctChg,isST"
        )
        rs = bs.query_history_k_data_plus(
            code,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency=freq,
            adjustflag=adjustflag,
        )
        df = self._resultset_to_df(rs)
        if df.empty:
            return df

        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
            "amount",
            "pctChg",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def get_profit_data(self, code: str, year: int, quarter: int) -> pd.DataFrame:
        """获取利润表数据。"""

        self._ensure_session()
        rs = bs.query_profit_data(code=code, year=year, quarter=quarter)
        return self._resultset_to_df(rs)

    def get_growth_data(self, code: str, year: int, quarter: int) -> pd.DataFrame:
        """获取成长能力数据。"""

        self._ensure_session()
        rs = bs.query_growth_data(code=code, year=year, quarter=quarter)
        return self._resultset_to_df(rs)

    def get_balance_data(self, code: str, year: int, quarter: int) -> pd.DataFrame:
        """获取资产负债表数据。"""

        self._ensure_session()
        rs = bs.query_balance_data(code=code, year=year, quarter=quarter)
        return self._resultset_to_df(rs)

    def get_cash_flow_data(self, code: str, year: int, quarter: int) -> pd.DataFrame:
        """获取现金流量表数据。"""

        self._ensure_session()
        rs = bs.query_cash_flow_data(code=code, year=year, quarter=quarter)
        return self._resultset_to_df(rs)


if __name__ == "__main__":
    session = BaostockSession()
    fetcher = BaostockDataFetcher(session)

    latest_date = fetcher.get_latest_trading_date()
    stock_df = fetcher.get_stock_list(latest_date)

    if not stock_df.empty:
        sample_code = stock_df.iloc[0]["code"]
        start_day = (
            datetime.strptime(latest_date, "%Y-%m-%d").date() - timedelta(days=30)
        ).isoformat()
        kline_df = fetcher.get_kline(sample_code, start_day, latest_date)
        print(f"最近 30 天 {sample_code} K 线行数：{len(kline_df)}")
    else:
        print("未获取到股票列表，无法演示 K 线数据查询。")
