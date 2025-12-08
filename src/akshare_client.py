"""Simple AKShare client for fetching A-share data."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Callable, List, Optional, Sequence

import akshare as ak
from akshare.utils.demjson import JSONDecodeError
import pandas as pd
import requests


class AKShareClient:
    """Wrapper around AKShare for commonly used A-share data queries."""

    def __init__(self, use_proxies: bool = True):
        """初始化客户端。

        Args:
            use_proxies: 是否允许从环境变量读取代理配置。默认 ``True``。
                当本地配置了失效或不可访问的代理时，可以传入 ``False``
                来临时屏蔽代理，避免 ``ProxyError`` 导致数据拉取失败。
        """

        self.use_proxies = use_proxies

    @contextmanager
    def _temporary_proxy_env(self, enable: bool | None = None):
        """根据 ``use_proxies`` 临时屏蔽或恢复代理环境变量。"""

        use_proxy = self.use_proxies if enable is None else enable
        proxy_keys = [key for key in os.environ if key.lower().endswith("_proxy")]
        cached_values = {}

        if not use_proxy:
            for key in proxy_keys:
                cached_values[key] = os.environ.pop(key, None)

        try:
            yield
        finally:
            if not use_proxy:
                for key, value in cached_values.items():
                    if value is None:
                        continue
                    os.environ[key] = value

    def _run_with_proxy_fallback(
        self, action: Callable[[], pd.DataFrame], error_message: str
    ) -> pd.DataFrame:
        """执行请求，若代理异常或接口解析异常则自动回退到直连。"""

        attempts = [None]
        if self.use_proxies:
            attempts.append(False)

        last_error: Exception | None = None

        for enable_proxy in attempts:
            try:
                with self._temporary_proxy_env(enable=enable_proxy):
                    return action()
            except requests.exceptions.ProxyError as exc:
                last_error = exc
                if enable_proxy is False or not self.use_proxies:
                    break
            except JSONDecodeError as exc:
                last_error = exc
                if enable_proxy is False or not self.use_proxies:
                    break

        raise ConnectionError(
            error_message
            + "（数据接口返回异常或被风控，请稍后重试，必要时更换网络环境）"
        ) from last_error

    @staticmethod
    def _normalize_code(code: str) -> str:
        """将股票代码规范化为 6 位数字字符串。"""

        digits = "".join(ch for ch in str(code) if ch.isdigit())
        return digits[-6:].zfill(6)

    @staticmethod
    def _to_sina_symbol(code: str) -> str:
        """Convert normalized 6-digit code to Sina symbol."""

        return f"sh{code}" if code.startswith("6") else f"sz{code}"

    @staticmethod
    def _normalize_adjust(adjust: str | None) -> str:
        """Normalize adjust flag for Sina endpoints."""

        return "" if adjust is None else adjust

    def fetch_realtime_quotes(self, codes: List[str]) -> pd.DataFrame:
        """Retrieve real-time quotes for the given stock codes.

        Args:
            codes: A list of stock codes such as "600000" or "000001".

        Returns:
            A pandas DataFrame containing the real-time quotes for the requested
            stock codes, preserving the order of ``codes``.
        """
        if not codes:
            raise ValueError("请至少提供一个股票代码进行查询")

        normalized_codes = [self._normalize_code(code) for code in codes]

        quotes = self._run_with_proxy_fallback(
            action=ak.stock_zh_a_spot,
            error_message=(
                "实时行情查询失败：连接新浪数据接口时被远端中断，可能是网络不稳定、"
                "网站风控或代理配置问题，请稍后重试"
            ),
        )

        quotes = quotes.copy()
        quotes["code_6"] = quotes["代码"].astype(str).str[-6:]
        selected = quotes[quotes["code_6"].isin(normalized_codes)].copy()
        selected["代码"] = selected.pop("code_6")
        if selected.empty:
            raise LookupError("未能获取到对应股票的实时行情，请检查代码是否正确")

        desired_columns = [
            "代码",
            "名称",
            "最新价",
            "涨跌额",
            "涨跌幅",
            "今开",
            "昨收",
            "最高",
            "最低",
            "成交量",
            "成交额",
        ]
        selected = selected[desired_columns]

        ordered = pd.CategoricalIndex(normalized_codes, ordered=True)
        selected = selected.set_index("代码").loc[ordered].reset_index()
        return selected

    def _fetch_sina_daily(
        self, symbol: str, start_date: str, end_date: str, adjust: str
    ) -> pd.DataFrame:
        return self._run_with_proxy_fallback(
            action=lambda: ak.stock_zh_a_daily(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            ),
            error_message=(
                "历史行情查询失败：连接新浪数据接口时被远端中断，可能是网络不稳定、网站风控"
                "或代理配置问题，请稍后重试"
            ),
        )

    def fetch_history(
        self,
        code: str,
        start_date: str,
        end_date: Optional[str] = None,
        adjust: str | None = "qfq",
    ) -> pd.DataFrame:
        """Retrieve Sina historical data for a single stock.

        Args:
            code: Stock code such as "600000".
            start_date: Start date in ``YYYYMMDD`` format.
            end_date: End date in ``YYYYMMDD`` format. Defaults to today when omitted.
            adjust: ``qfq`` (前复权), ``hfq`` (后复权) or ```` for no adjustment.

        Returns:
            A pandas DataFrame with the historical quotes.
        """
        if not code:
            raise ValueError("股票代码不能为空")

        normalized_code = self._normalize_code(code)
        normalized_end = end_date or date.today().strftime("%Y%m%d")
        adjust_flag = self._normalize_adjust(adjust)
        symbol = self._to_sina_symbol(normalized_code)

        history = self._fetch_sina_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=normalized_end,
            adjust=adjust_flag,
        )

        if history.empty:
            raise LookupError("未能获取到历史行情，请检查日期范围或股票代码")

        return self._prepare_history(history, normalized_code)

    def fetch_board_industries(self) -> pd.DataFrame:
        """获取同花顺行业列表。"""

        return self._run_with_proxy_fallback(
            action=ak.stock_board_industry_name_ths,
            error_message="行业列表查询失败",
        )

    def fetch_board_industry_cons(self, symbol: str) -> pd.DataFrame:
        """获取同花顺行业成分股。"""

        return self._run_with_proxy_fallback(
            action=lambda: ak.stock_board_industry_cons_ths(symbol=symbol),
            error_message=f"行业成分股查询失败：{symbol}",
        )

    def fetch_board_concepts(self) -> pd.DataFrame:
        """获取同花顺概念列表。"""

        return self._run_with_proxy_fallback(
            action=ak.stock_board_concept_name_ths,
            error_message="概念列表查询失败",
        )

    def fetch_board_concept_cons(self, symbol: str) -> pd.DataFrame:
        """获取同花顺概念成分股。"""

        return self._run_with_proxy_fallback(
            action=lambda: ak.stock_board_concept_cons_ths(symbol=symbol),
            error_message=f"概念成分股查询失败：{symbol}",
        )

    def fetch_recent_history(
        self,
        codes: Sequence[str],
        n_days: int = 30,
        adjust: str | None = "qfq",
    ) -> pd.DataFrame:
        """获取最近 ``n_days`` 天的历史行情数据。"""

        if not codes:
            raise ValueError("请至少提供一个股票代码进行查询")
        if n_days <= 0:
            raise ValueError("n_days 需要为正整数")

        today = date.today()
        start_date = (today - timedelta(days=n_days - 1)).strftime("%Y%m%d")
        end_date = today.strftime("%Y%m%d")

        adjust_flag = self._normalize_adjust(adjust)

        records: list[pd.DataFrame] = []
        for code in codes:
            normalized_code = self._normalize_code(code)
            symbol = self._to_sina_symbol(normalized_code)
            history = self._fetch_sina_daily(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust_flag,
            )

            if history.empty:
                continue

            prepared = self._prepare_history(history, normalized_code)
            records.append(prepared)

        if not records:
            raise LookupError("未能获取到历史行情，请检查日期范围或股票代码")

        return pd.concat(records, ignore_index=True)

    @staticmethod
    def _ensure_float_columns(history: pd.DataFrame, columns: list[str]) -> None:
        for column in columns:
            if column in history:
                history[column] = pd.to_numeric(history[column], errors="coerce")

    def _prepare_history(self, history: pd.DataFrame, code: str) -> pd.DataFrame:
        """标准化历史行情列名并补充衍生指标。"""

        history = history.copy()
        column_mapping = {
            "date": "日期",
            "open": "开盘",
            "close": "收盘",
            "high": "最高",
            "low": "最低",
            "volume": "成交量",
            "amount": "成交额",
            "turnover": "换手率",
            "turnover_rate": "换手率",
            "pct_chg": "涨跌幅",
            "change": "涨跌额",
            "amplitude": "振幅",
        }
        history.rename(columns=column_mapping, inplace=True)

        if "代码" not in history:
            history.insert(0, "代码", code)
        else:
            history["代码"] = history["代码"].apply(self._normalize_code)

        if "日期" in history:
            history["日期"] = pd.to_datetime(history["日期"], errors="coerce").dt.date
            history.sort_values(["代码", "日期"], inplace=True)
            history["日期"] = history["日期"].astype(str)

        numeric_columns = [
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "涨跌额",
            "涨跌幅",
            "振幅",
            "换手率",
        ]
        self._ensure_float_columns(history, numeric_columns)

        if "收盘" in history:
            history["昨收"] = history.groupby("代码")["收盘"].shift(1)
        else:
            history["昨收"] = pd.NA

        if "涨跌额" not in history:
            if "收盘" in history:
                history["涨跌额"] = history["收盘"] - history["昨收"]
            else:
                history["涨跌额"] = pd.NA

        if "涨跌幅" not in history:
            pct_change = history["涨跌额"] / history["昨收"]
            history["涨跌幅"] = pct_change.replace(
                [pd.NA, pd.NaT, float("inf"), float("-inf")], pd.NA
            ) * 100

        if "振幅" not in history:
            if {"最高", "最低"}.issubset(history.columns):
                amplitude = (history["最高"] - history["最低"]) / history["昨收"]
                history["振幅"] = amplitude.replace(
                    [pd.NA, pd.NaT, float("inf"), float("-inf")], pd.NA
                ) * 100
            else:
                history["振幅"] = pd.NA

        standard_columns = [
            "代码",
            "日期",
            "开盘",
            "收盘",
            "昨收",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "涨跌额",
            "涨跌幅",
            "振幅",
            "换手率",
        ]

        for column in standard_columns:
            if column not in history:
                history[column] = pd.NA

        history = history[standard_columns]
        return history
