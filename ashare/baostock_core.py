"""Baostock 数据访问层封装。"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable, Iterable

import baostock as bs
import pandas as pd

from .baostock_session import BaostockSession
from .config import get_section


class BaostockDataFetcher:
    """封装常用 Baostock 数据访问接口。"""

    def __init__(
        self,
        session: BaostockSession,
        max_retries: int | None = None,
        retry_backoff: float | None = None,
    ) -> None:
        """保存会话引用并加载接口重试配置。"""

        self.session = session
        cfg = get_section("baostock")
        raw_retry = max_retries if max_retries is not None else cfg.get("max_retries")
        raw_backoff = (
            retry_backoff if retry_backoff is not None else cfg.get("retry_sleep")
        )

        try:
            parsed_retry = int(raw_retry) if raw_retry is not None else 3
        except (TypeError, ValueError):
            parsed_retry = 3

        try:
            parsed_backoff = float(raw_backoff) if raw_backoff is not None else 2.0
        except (TypeError, ValueError):
            parsed_backoff = 2.0

        self.api_max_retries = max(1, parsed_retry)
        self.api_retry_backoff = max(0.5, parsed_backoff)
        self.logger = logging.getLogger(__name__)

    def _ensure_session(self) -> None:
        """确保会话已登录。"""

        self.session.ensure_alive()

    def _calc_backoff(self, attempt_index: int) -> float:
        base = self.api_retry_backoff
        max_sleep = 20.0
        return min(max_sleep, base * (2 ** (attempt_index - 1)))

    def _call_with_retry(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """对 Baostock 查询执行重试与会话自愈。"""

        last_error: Exception | None = None
        for attempt in range(1, self.api_max_retries + 1):
            self._ensure_session()
            try:
                result = func(*args, **kwargs)
                if getattr(result, "error_code", "0") == "0":
                    return result
                last_error = RuntimeError(
                    f"Baostock 调用失败: {result.error_code}, {result.error_msg}"
                )
            except Exception as exc:  # noqa: BLE001
                last_error = exc

            if attempt >= self.api_max_retries:
                break

            self.logger.warning(
                "Baostock 调用异常（第 %s 次），即将重试：%s",
                attempt,
                last_error,
            )
            try:
                self.session.ensure_alive(force_refresh=True)
            except Exception:  # noqa: BLE001
                self.session.logged_in = False
            time.sleep(self._calc_backoff(attempt))

        if last_error:
            raise last_error
        raise RuntimeError("未知的 Baostock 调用异常。")

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

        rs = self._call_with_retry(bs.query_trade_dates, start_date, end_date)
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

    def get_stock_basic(
        self, code: str | None = None, code_name: str | None = None
    ) -> pd.DataFrame:
        """查询证券基本资料。"""

        rs = self._call_with_retry(bs.query_stock_basic, code, code_name)
        df = self._resultset_to_df(rs)
        if df.empty:
            return df

        date_cols = ["ipoDate", "outDate", "endDate"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        for col in ["type", "status"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def get_stock_industry(self, code: str | None = None) -> pd.DataFrame:
        """查询行业分类信息。"""

        rs = self._call_with_retry(bs.query_stock_industry, code)
        df = self._resultset_to_df(rs)
        if df.empty:
            return df

        if "updateDate" in df.columns:
            df["updateDate"] = pd.to_datetime(df["updateDate"], errors="coerce")
        return df

    def get_index_members(self, index: str, date: str | None = None) -> pd.DataFrame:
        """查询指定指数的成分股列表。"""

        self._ensure_session()
        index_map = {
            "hs300": bs.query_hs300_stocks,
            "zz500": bs.query_zz500_stocks,
            "sz50": bs.query_sz50_stocks,
        }
        index_key = index.lower()
        if index_key not in index_map:
            raise ValueError("暂不支持的指数标识：{value}".format(value=index))

        query_fn = index_map[index_key]
        if date:
            rs = self._call_with_retry(query_fn, date=date)
        else:
            rs = self._call_with_retry(query_fn)
        df = self._resultset_to_df(rs)
        if df.empty:
            return df

        df.insert(0, "index", index_key)
        if "updateDate" in df.columns:
            df["updateDate"] = pd.to_datetime(df["updateDate"], errors="coerce")
        return df

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

        def _query(day: str) -> pd.DataFrame:
            """内部封装一次 query_all_stock 调用。"""

            rs = self._call_with_retry(bs.query_all_stock, day)
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

        fields = (
            "date,code,open,high,low,close,preclose,volume,amount,"
            "adjustflag,tradestatus,pctChg,isST"
        )
        rs = self._call_with_retry(
            bs.query_history_k_data_plus,
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

        rs = self._call_with_retry(bs.query_profit_data, code, year, quarter)
        return self._resultset_to_df(rs)

    def get_growth_data(self, code: str, year: int, quarter: int) -> pd.DataFrame:
        """获取成长能力数据。"""

        rs = self._call_with_retry(bs.query_growth_data, code, year, quarter)
        return self._resultset_to_df(rs)

    def get_balance_data(self, code: str, year: int, quarter: int) -> pd.DataFrame:
        """获取资产负债表数据。"""

        rs = self._call_with_retry(bs.query_balance_data, code, year, quarter)
        return self._resultset_to_df(rs)

    def get_cash_flow_data(self, code: str, year: int, quarter: int) -> pd.DataFrame:
        """获取现金流量表数据。"""

        rs = self._call_with_retry(bs.query_cash_flow_data, code, year, quarter)
        return self._resultset_to_df(rs)

    def get_operation_data(self, code: str, year: int, quarter: int) -> pd.DataFrame:
        """获取营运能力数据。"""

        rs = self._call_with_retry(bs.query_operation_data, code, year, quarter)
        return self._resultset_to_df(rs)

    def get_dupont_data(self, code: str, year: int, quarter: int) -> pd.DataFrame:
        """获取杜邦指标数据。"""

        rs = self._call_with_retry(bs.query_dupont_data, code, year, quarter)
        return self._resultset_to_df(rs)

    def get_performance_express_report(
        self, code: str, start_date: str | None = None, end_date: str | None = None
    ) -> pd.DataFrame:
        """获取业绩快报。"""

        rs = self._call_with_retry(
            bs.query_performance_express_report,
            code,
            start_date,
            end_date,
        )
        return self._resultset_to_df(rs)

    def get_forecast_report(
        self, code: str, start_date: str | None = None, end_date: str | None = None
    ) -> pd.DataFrame:
        """获取业绩预告。"""

        rs = self._call_with_retry(
            bs.query_forecast_report, code, start_date, end_date
        )
        return self._resultset_to_df(rs)

    def get_dividend_data(
        self, code: str, year: int, year_type: str = "report"
    ) -> pd.DataFrame:
        """获取分红送配信息。"""

        rs = self._call_with_retry(bs.query_dividend_data, code, year, year_type)
        return self._resultset_to_df(rs)

    def get_adjust_factor(
        self, code: str, start_date: str | None = None, end_date: str | None = None
    ) -> pd.DataFrame:
        """获取复权因子信息。"""

        rs = self._call_with_retry(bs.query_adjust_factor, code, start_date, end_date)
        return self._resultset_to_df(rs)

    def get_deposit_rate_data(
        self, start_date: str = "", end_date: str = ""
    ) -> pd.DataFrame:
        """获取存款利率数据。"""

        rs = self._call_with_retry(
            bs.query_deposit_rate_data, start_date=start_date, end_date=end_date
        )
        return self._resultset_to_df(rs)

    def get_loan_rate_data(
        self, start_date: str = "", end_date: str = ""
    ) -> pd.DataFrame:
        """获取贷款利率数据。"""

        rs = self._call_with_retry(
            bs.query_loan_rate_data, start_date=start_date, end_date=end_date
        )
        return self._resultset_to_df(rs)

    def get_required_reserve_ratio_data(
        self, start_date: str = "", end_date: str = "", year_type: str = "0"
    ) -> pd.DataFrame:
        """获取存款准备金率数据。"""

        rs = self._call_with_retry(
            bs.query_required_reserve_ratio_data,
            start_date,
            end_date,
            year_type,
        )
        return self._resultset_to_df(rs)

    def get_money_supply_data_month(
        self, start_date: str = "", end_date: str = ""
    ) -> pd.DataFrame:
        """获取月度货币供应量数据。"""

        rs = self._call_with_retry(
            bs.query_money_supply_data_month, start_date=start_date, end_date=end_date
        )
        return self._resultset_to_df(rs)

    def get_money_supply_data_year(
        self, start_date: str = "", end_date: str = ""
    ) -> pd.DataFrame:
        """获取年度货币供应量数据。"""

        rs = self._call_with_retry(
            bs.query_money_supply_data_year, start_date=start_date, end_date=end_date
        )
        return self._resultset_to_df(rs)

    def get_shibor_data(
        self, start_date: str = "", end_date: str = ""
    ) -> pd.DataFrame:
        """获取银行间同业拆借利率（Shibor）。"""

        if not hasattr(bs, "query_shibor_data"):
            raise RuntimeError("当前 Baostock 版本不支持 Shibor 接口。")

        rs = self._call_with_retry(
            bs.query_shibor_data, start_date=start_date, end_date=end_date
        )
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
