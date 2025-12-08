"""Simple AKShare client for fetching A-share data."""

from __future__ import annotations

import math
import os
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta
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

    def _run_with_proxy_and_ssl_fallback(
        self, action_builder: Callable[[bool], pd.DataFrame], error_message: str
    ) -> pd.DataFrame:
        """在代理与 SSL 验证之间尝试多次调用，提升可用性。"""

        proxy_attempts = [None]
        if self.use_proxies:
            proxy_attempts.append(False)

        last_error: Exception | None = None
        for enable_proxy in proxy_attempts:
            for verify_ssl in (True, False):
                try:
                    with self._temporary_proxy_env(enable=enable_proxy):
                        return action_builder(verify_ssl)
                except (
                    requests.exceptions.ProxyError,
                    requests.exceptions.SSLError,
                    requests.exceptions.HTTPError,
                    JSONDecodeError,
                ) as exc:
                    last_error = exc
                    if (enable_proxy is False or not self.use_proxies) and not verify_ssl:
                        break

        raise ConnectionError(
            error_message
            + "（网络波动或证书校验异常，已尝试直连与跳过 SSL 验证，请稍后重试）"
        ) from last_error

    @staticmethod
    def _normalize_code(code: str) -> str:
        """将股票代码规范化为 6 位数字字符串。"""

        digits = "".join(ch for ch in str(code) if ch.isdigit())
        return digits[-6:].zfill(6)

    @staticmethod
    def _detect_market(code: str) -> str:
        """根据代码推断市场标识。"""

        if code.startswith("6"):
            return "sh"
        if code.startswith("8") or code.startswith("4"):
            return "bj"
        return "sz"

    @staticmethod
    def _find_first_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
        """从候选列名中找到第一个存在的列。"""

        for column in candidates:
            if column in df.columns:
                return column
        return None

    @staticmethod
    def _to_sina_symbol(code: str) -> str:
        """Convert normalized 6-digit code to Sina symbol."""

        return f"sh{code}" if code.startswith("6") else f"sz{code}"

    @staticmethod
    def _normalize_adjust(adjust: str | None) -> str:
        """Normalize adjust flag for Sina endpoints."""

        return "" if adjust is None else adjust

    @staticmethod
    def _normalize_trade_date(trade_date: str | date) -> str:
        """将日期参数统一转换为 ``YYYYMMDD`` 格式的字符串。"""

        normalized = pd.to_datetime(trade_date, errors="coerce")
        if pd.isna(normalized):
            raise ValueError("无法解析提供的日期，请使用 YYYYMMDD 或 YYYY-MM-DD 格式")

        return normalized.date().strftime("%Y%m%d")

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

    def fetch_all_a_stocks(self) -> pd.DataFrame:
        """获取全 A 股基础信息列表。"""

        stocks = self._run_with_proxy_fallback(
            action=ak.stock_info_a_code_name,
            error_message="全 A 股列表查询失败",
        )

        stocks = stocks.copy()
        code_column = self._find_first_column(
            stocks, ["code", "代码", "股票代码", "证券代码"]
        )
        name_column = self._find_first_column(stocks, ["name", "名称", "股票简称"])
        exchange_column = self._find_first_column(
            stocks, ["exchange", "交易所", "市场类型", "市场"]
        )
        list_date_column = self._find_first_column(
            stocks, ["list_date", "上市日期", "ipo_date", "上市时间"]
        )

        stocks["code"] = (
            stocks[code_column].apply(
                lambda value: self._normalize_code(value) if pd.notna(value) else pd.NA
            )
            if code_column
            else pd.NA
        )
        stocks["name"] = stocks[name_column] if name_column else pd.NA
        stocks["exchange"] = stocks[exchange_column] if exchange_column else pd.NA
        if list_date_column:
            stocks["list_date"] = (
                pd.to_datetime(stocks[list_date_column], errors="coerce")
                .dt.date.astype(str)
            )
        else:
            stocks["list_date"] = pd.NA

        return stocks[["code", "name", "exchange", "list_date"]]

    def fetch_index_constituents(self, index_codes: list[str]) -> pd.DataFrame:
        """获取多个指数的成分股列表。"""

        if not index_codes:
            raise ValueError("请至少提供一个指数代码进行查询")

        records: list[dict[str, object]] = []
        for index_code in index_codes:
            normalized_index_code = self._normalize_code(index_code)
            constituents = self._run_with_proxy_fallback(
                action=lambda code=normalized_index_code: ak.index_stock_cons(symbol=code),
                error_message=f"指数成分股查询失败：{normalized_index_code}",
            )

            if constituents.empty:
                continue

            stock_code_column = self._find_first_column(
                constituents, ["品种代码", "成分券代码", "代码", "证券代码", "股票代码"]
            )
            stock_name_column = self._find_first_column(
                constituents, ["品种简称", "名称", "证券简称", "股票简称", "stock_name"]
            )
            index_name_column = self._find_first_column(
                constituents, ["指数名称", "名称", "品种名称", "指数简称", "index_name"]
            )
            index_name = (
                str(constituents.iloc[0][index_name_column]).strip()
                if index_name_column
                else normalized_index_code
            )

            for _, row in constituents.iterrows():
                stock_code = row[stock_code_column] if stock_code_column else None
                stock_name = row[stock_name_column] if stock_name_column else None
                if pd.isna(stock_code) and pd.isna(stock_name):
                    continue

                records.append(
                    {
                        "index_code": normalized_index_code,
                        "index_name": index_name,
                        "stock_code": self._normalize_code(stock_code)
                        if stock_code is not None and pd.notna(stock_code)
                        else pd.NA,
                        "stock_name": stock_name if stock_name is not None else pd.NA,
                    }
                )

        if not records:
            raise LookupError("未能获取到任何指数成分股，请检查指数代码或网络连接")

        return pd.DataFrame(records)

    def fetch_northbound_summary(self) -> pd.DataFrame:
        """获取北向资金整体及沪深分项的净流入汇总。"""

        def action_builder(verify_ssl: bool) -> pd.DataFrame:
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            params = {
                "reportName": "RPT_MUTUAL_QUOTA",
                "columns": (
                    "TRADE_DATE,MUTUAL_TYPE,BOARD_TYPE,MUTUAL_TYPE_NAME,FUNDS_DIRECTION,"
                    "INDEX_CODE,INDEX_NAME,BOARD_CODE"
                ),
                "quoteColumns": (
                    "status~07~BOARD_CODE,dayNetAmtIn~07~BOARD_CODE,dayAmtRemain~07~BOARD_CODE,"
                    "dayAmtThreshold~07~BOARD_CODE,f104~07~BOARD_CODE,f105~07~BOARD_CODE,"
                    "f106~07~BOARD_CODE,f3~03~INDEX_CODE~INDEX_f3,netBuyAmt~07~BOARD_CODE"
                ),
                "quoteType": "0",
                "pageNumber": "1",
                "pageSize": "2000",
                "sortTypes": "1",
                "sortColumns": "MUTUAL_TYPE",
                "source": "WEB",
                "client": "WEB",
            }
            response = requests.get(
                url, params=params, timeout=15, verify=verify_ssl
            )
            response.raise_for_status()
            data_json = response.json()
            records = data_json.get("result", {}).get("data", [])
            if not records:
                raise LookupError("未获取到北向资金净流入数据")

            return pd.DataFrame(records)

        raw_summary = self._run_with_proxy_and_ssl_fallback(
            action_builder=action_builder, error_message="北向资金净流入查询失败"
        )

        normalized = raw_summary.copy()
        normalized["date"] = pd.to_datetime(
            normalized.get("TRADE_DATE"), errors="coerce"
        ).dt.date
        normalized["net_inflow"] = (
            pd.to_numeric(normalized.get("dayNetAmtIn"), errors="coerce") / 10000
        )
        normalized["balance"] = (
            pd.to_numeric(normalized.get("dayAmtRemain"), errors="coerce") / 10000
        )
        normalized["index_change_pct"] = pd.to_numeric(
            normalized.get("INDEX_f3"), errors="coerce"
        )

        north_data = normalized[normalized.get("FUNDS_DIRECTION") == "北向"]
        if north_data.empty:
            raise LookupError("未获取到北向资金净流入数据")

        summary: dict[str, object] = {
            "date": north_data.iloc[0]["date"],
            "north_net_inflow": north_data["net_inflow"].sum(min_count=1),
            "north_balance": north_data["balance"].sum(min_count=1),
        }

        type_map = {"001": "sh", "003": "sz"}
        for type_code, prefix in type_map.items():
            row = north_data[north_data.get("MUTUAL_TYPE") == type_code]
            summary[f"{prefix}_net_inflow"] = (
                row.iloc[0]["net_inflow"] if not row.empty else pd.NA
            )
            summary[f"{prefix}_balance"] = (
                row.iloc[0]["balance"] if not row.empty else pd.NA
            )
            summary[f"{prefix}_index_change_pct"] = (
                row.iloc[0]["index_change_pct"] if not row.empty else pd.NA
            )

        return pd.DataFrame([summary])

    def fetch_northbound_stock_stats(self, trade_date: str | date) -> pd.DataFrame:
        """获取指定交易日的北向持股与增减统计。"""

        normalized_date = self._normalize_trade_date(trade_date)
        formatted_date = f"{normalized_date[:4]}-{normalized_date[4:6]}-{normalized_date[6:]}"

        def action_builder(verify_ssl: bool) -> pd.DataFrame:
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            base_filter = (
                f"(INTERVAL_TYPE=\"1\")(MUTUAL_TYPE in (\"001\",\"003\"))"
                f"(TRADE_DATE>='{formatted_date}')(TRADE_DATE<='{formatted_date}')"
            )
            params = {
                "sortColumns": "TRADE_DATE",
                "sortTypes": "-1",
                "pageSize": "1000",
                "pageNumber": "1",
                "columns": "ALL",
                "source": "WEB",
                "client": "WEB",
                "filter": base_filter,
                "rt": "53160469",
                "reportName": "RPT_MUTUAL_STOCK_NORTHSTA",
            }

            response = requests.get(
                url, params=params, timeout=15, verify=verify_ssl
            )
            response.raise_for_status()
            data_json = response.json()
            result = data_json.get("result")
            if not result or not result.get("data"):
                raise LookupError("未获取到北向持股统计数据")

            pages = int(result.get("pages") or 1)
            frames = [pd.DataFrame(result["data"])]

            for page in range(2, pages + 1):
                params.update({"pageNumber": page})
                resp = requests.get(
                    url, params=params, timeout=15, verify=verify_ssl
                )
                resp.raise_for_status()
                page_data = resp.json().get("result", {}).get("data")
                if not page_data:
                    continue
                frames.append(pd.DataFrame(page_data))

            return pd.concat(frames, ignore_index=True)

        stats = self._run_with_proxy_and_ssl_fallback(
            action_builder=action_builder, error_message="北向持股统计查询失败"
        )

        if stats.empty:
            raise LookupError("未获取到北向持股统计数据")

        stats = stats.copy()
        stats["date"] = pd.to_datetime(stats.get("TRADE_DATE"), errors="coerce").dt.date
        stats["code"] = (
            stats.get("SECURITY_CODE", pd.Series(dtype=str)).astype(str).str[-6:].str.zfill(6)
        )
        stats["name"] = stats.get("SECURITY_NAME")
        stats["close_price"] = pd.to_numeric(stats.get("CLOSE_PRICE"), errors="coerce")
        stats["price_change_pct"] = pd.to_numeric(
            stats.get("CHANGE_RATE"), errors="coerce"
        )
        stats["hold_shares"] = pd.to_numeric(stats.get("HOLD_SHARES"), errors="coerce")
        stats["hold_value"] = pd.to_numeric(
            stats.get("HOLD_MARKET_CAP"), errors="coerce"
        )
        stats["hold_ratio"] = pd.to_numeric(
            stats.get("HOLD_SHARES_RATIO"), errors="coerce"
        )
        stats["daily_change_value"] = pd.to_numeric(
            stats.get("HOLD_MCAP_CHANGE"), errors="coerce"
        )

        previous_value = stats["hold_value"] - stats["daily_change_value"]
        with pd.option_context("mode.use_inf_as_na", True):
            stats["daily_change_ratio"] = (
                stats["daily_change_value"] / previous_value.replace(0, pd.NA)
            ) * 100

        desired_columns = [
            "date",
            "code",
            "name",
            "close_price",
            "price_change_pct",
            "hold_shares",
            "hold_value",
            "hold_ratio",
            "daily_change_value",
            "daily_change_ratio",
        ]

        for column in desired_columns:
            if column not in stats:
                stats[column] = pd.NA

        return stats[desired_columns]

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

    def fetch_industry_list(self) -> pd.DataFrame:
        """获取行业板块列表，返回标准化列名。"""

        industries = self.fetch_board_industries()
        standardized = self._standardize_board_list(
            industries, code_label="industry_code", name_label="industry_name"
        )
        if standardized.empty:
            raise LookupError("未能获取到行业列表，请检查网络或数据源是否可用")

        return standardized

    def fetch_board_industry_cons(self, symbol: str) -> pd.DataFrame:
        """获取同花顺行业成分股。"""

        return self._run_with_proxy_fallback(
            action=lambda: ak.stock_board_industry_cons_ths(symbol=symbol),
            error_message=f"行业成分股查询失败：{symbol}",
        )

    def fetch_industry_members(self, industry_code: str) -> pd.DataFrame:
        """获取指定行业的成分股列表。"""

        if not industry_code:
            raise ValueError("行业代码不能为空")

        members = self.fetch_board_industry_cons(industry_code)
        standardized = self._standardize_board_members(
            members,
            board_code=industry_code,
            code_label="industry_code",
            name_label="industry_name",
        )
        if standardized.empty:
            raise LookupError("未能获取到行业成分股，请检查行业代码或网络")

        return standardized

    def fetch_board_fund_flow(self, board_type: str, indicator: str = "今日") -> pd.DataFrame:
        """获取行业、概念等板块层面的资金流排行。"""

        board_type_map = {
            "industry": "行业资金流",
            "concept": "概念资金流",
            "area": "地域资金流",
        }
        normalized_type = board_type.lower()
        if normalized_type not in board_type_map:
            raise ValueError("board_type 仅支持 industry、concept 或 area")

        indicator_map = {
            "今日": [
                "f62",
                "1",
                "f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124",
            ],
            "5日": [
                "f164",
                "5",
                "f12,f14,f2,f109,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f257,f258,f124",
            ],
            "10日": [
                "f174",
                "10",
                "f12,f14,f2,f160,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f260,f261,f124",
            ],
        }

        if indicator not in indicator_map:
            raise ValueError("indicator 仅支持 今日、5日 或 10日")

        def action_builder(verify_ssl: bool) -> pd.DataFrame:
            url = "https://push2.eastmoney.com/api/qt/clist/get"
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"
                )
            }
            params = {
                "pn": "1",
                "pz": "100",
                "po": "1",
                "np": "1",
                "ut": "b2884a393a59ad64002292a3e90d46a5",
                "fltt": "2",
                "invt": "2",
                "fid0": indicator_map[indicator][0],
                "fs": f"m:90 t:{'2' if normalized_type == 'industry' else '3' if normalized_type == 'concept' else '1'}",
                "stat": indicator_map[indicator][1],
                "fields": indicator_map[indicator][2],
                "rt": "52975239",
                "_": int(time.time() * 1000),
            }
            response = requests.get(
                url, params=params, headers=headers, timeout=15, verify=verify_ssl
            )
            response.raise_for_status()
            data_json = response.json()
            total_page = math.ceil(data_json["data"]["total"] / 100)
            temp_list: list[pd.DataFrame] = []
            for page in range(1, total_page + 1):
                params.update({"pn": page})
                response = requests.get(
                    url, params=params, headers=headers, timeout=15, verify=verify_ssl
                )
                response.raise_for_status()
                data_json = response.json()
                inner_df = pd.DataFrame(data_json.get("data", {}).get("diff", []))
                temp_list.append(inner_df)
            if not temp_list:
                raise LookupError("未获取到板块资金流数据")

            temp_df = pd.concat(temp_list, ignore_index=True)

            if indicator == "今日":
                temp_df.columns = [
                    "-",
                    "今日涨跌幅",
                    "_",
                    "名称",
                    "今日主力净流入-净额",
                    "今日超大单净流入-净额",
                    "今日超大单净流入-净占比",
                    "今日大单净流入-净额",
                    "今日大单净流入-净占比",
                    "今日中单净流入-净额",
                    "今日中单净流入-净占比",
                    "今日小单净流入-净额",
                    "今日小单净流入-净占比",
                    "-",
                    "今日主力净流入-净占比",
                    "今日主力净流入最大股",
                    "今日主力净流入最大股代码",
                    "是否净流入",
                ]

                temp_df = temp_df[
                    [
                        "名称",
                        "今日涨跌幅",
                        "今日主力净流入-净额",
                        "今日主力净流入-净占比",
                        "今日超大单净流入-净额",
                        "今日超大单净流入-净占比",
                        "今日大单净流入-净额",
                        "今日大单净流入-净占比",
                        "今日中单净流入-净额",
                        "今日中单净流入-净占比",
                        "今日小单净流入-净额",
                        "今日小单净流入-净占比",
                        "今日主力净流入最大股",
                    ]
                ]
                temp_df.sort_values(
                    ["今日主力净流入-净额"], ascending=False, inplace=True
                )
            elif indicator == "5日":
                temp_df.columns = [
                    "-",
                    "_",
                    "名称",
                    "5日涨跌幅",
                    "_",
                    "5日主力净流入-净额",
                    "5日主力净流入-净占比",
                    "5日超大单净流入-净额",
                    "5日超大单净流入-净占比",
                    "5日大单净流入-净额",
                    "5日大单净流入-净占比",
                    "5日中单净流入-净额",
                    "5日中单净流入-净占比",
                    "5日小单净流入-净额",
                    "5日小单净流入-净占比",
                    "5日主力净流入最大股",
                    "_",
                    "_",
                ]

                temp_df = temp_df[
                    [
                        "名称",
                        "5日涨跌幅",
                        "5日主力净流入-净额",
                        "5日主力净流入-净占比",
                        "5日超大单净流入-净额",
                        "5日超大单净流入-净占比",
                        "5日大单净流入-净额",
                        "5日大单净流入-净占比",
                        "5日中单净流入-净额",
                        "5日中单净流入-净占比",
                        "5日小单净流入-净额",
                        "5日小单净流入-净占比",
                        "5日主力净流入最大股",
                    ]
                ]
                temp_df.sort_values(["5日主力净流入-净额"], ascending=False, inplace=True)
            else:
                temp_df.columns = [
                    "-",
                    "_",
                    "名称",
                    "_",
                    "10日涨跌幅",
                    "10日主力净流入-净额",
                    "10日主力净流入-净占比",
                    "10日超大单净流入-净额",
                    "10日超大单净流入-净占比",
                    "10日大单净流入-净额",
                    "10日大单净流入-净占比",
                    "10日中单净流入-净额",
                    "10日中单净流入-净占比",
                    "10日小单净流入-净额",
                    "10日小单净流入-净占比",
                    "10日主力净流入最大股",
                    "_",
                    "_",
                ]

                temp_df = temp_df[
                    [
                        "名称",
                        "10日涨跌幅",
                        "10日主力净流入-净额",
                        "10日主力净流入-净占比",
                        "10日超大单净流入-净额",
                        "10日超大单净流入-净占比",
                        "10日大单净流入-净额",
                        "10日大单净流入-净占比",
                        "10日中单净流入-净额",
                        "10日中单净流入-净占比",
                        "10日小单净流入-净额",
                        "10日小单净流入-净占比",
                        "10日主力净流入最大股",
                    ]
                ]
                temp_df.sort_values(
                    ["10日主力净流入-净额"], ascending=False, inplace=True
                )

            temp_df.reset_index(drop=True, inplace=True)
            temp_df.index = range(1, len(temp_df) + 1)
            temp_df.insert(0, "序号", temp_df.index)
            return temp_df

        raw_flows = self._run_with_proxy_and_ssl_fallback(
            action_builder=action_builder,
            error_message=f"板块资金流查询失败：{normalized_type}",
        )

        rename_prefix = indicator if indicator in {"今日", "5日", "10日"} else ""
        rename_mapping = {
            f"{rename_prefix}主力净流入-净额": "net_main_inflow",
            f"{rename_prefix}主力净流入-净占比": "net_main_ratio",
            f"{rename_prefix}超大单净流入-净额": "net_super_large_order",
            f"{rename_prefix}超大单净流入-净占比": "net_super_large_ratio",
            f"{rename_prefix}大单净流入-净额": "net_large_order",
            f"{rename_prefix}大单净流入-净占比": "net_large_ratio",
            f"{rename_prefix}中单净流入-净额": "net_medium_order",
            f"{rename_prefix}中单净流入-净占比": "net_medium_ratio",
            f"{rename_prefix}小单净流入-净额": "net_small_order",
            f"{rename_prefix}小单净流入-净占比": "net_small_ratio",
            f"{rename_prefix}涨跌幅": "change_ratio",
            f"{rename_prefix}主力净流入最大股": "top_stock",
            "名称": "board_name",
        }

        flows = raw_flows.rename(columns=rename_mapping)
        numeric_columns = [
            "net_main_inflow",
            "net_main_ratio",
            "net_super_large_order",
            "net_super_large_ratio",
            "net_large_order",
            "net_large_ratio",
            "net_medium_order",
            "net_medium_ratio",
            "net_small_order",
            "net_small_ratio",
            "change_ratio",
        ]
        self._ensure_float_columns(flows, numeric_columns)

        flows.insert(0, "board_type", normalized_type)
        flows["period"] = indicator
        flows["date"] = date.today().isoformat()

        ordered_columns = [
            "board_type",
            "board_name",
            "date",
            "period",
            "net_main_inflow",
            "net_main_ratio",
            "net_super_large_order",
            "net_super_large_ratio",
            "net_large_order",
            "net_large_ratio",
            "net_medium_order",
            "net_medium_ratio",
            "net_small_order",
            "net_small_ratio",
            "change_ratio",
            "top_stock",
        ]

        for column in ordered_columns:
            if column not in flows:
                flows[column] = pd.NA

        flows = flows[ordered_columns]
        flows.sort_values("net_main_inflow", ascending=False, inplace=True)
        flows.reset_index(drop=True, inplace=True)
        return flows

    def fetch_board_concepts(self) -> pd.DataFrame:
        """获取同花顺概念列表。"""

        return self._run_with_proxy_fallback(
            action=ak.stock_board_concept_name_ths,
            error_message="概念列表查询失败",
        )

    def fetch_concept_list(self) -> pd.DataFrame:
        """获取概念板块列表，返回标准化列名。"""

        concepts = self.fetch_board_concepts()
        standardized = self._standardize_board_list(
            concepts, code_label="concept_code", name_label="concept_name"
        )
        if standardized.empty:
            raise LookupError("未能获取到概念列表，请检查网络或数据源是否可用")

        return standardized

    def fetch_board_concept_cons(self, symbol: str) -> pd.DataFrame:
        """获取同花顺概念成分股。"""

        return self._run_with_proxy_fallback(
            action=lambda: ak.stock_board_concept_cons_ths(symbol=symbol),
            error_message=f"概念成分股查询失败：{symbol}",
        )

    def fetch_concept_members(self, concept_code: str) -> pd.DataFrame:
        """获取指定概念的成分股列表。"""

        if not concept_code:
            raise ValueError("概念代码不能为空")

        members = self.fetch_board_concept_cons(concept_code)
        standardized = self._standardize_board_members(
            members,
            board_code=concept_code,
            code_label="concept_code",
            name_label="concept_name",
        )
        if standardized.empty:
            raise LookupError("未能获取到概念成分股，请检查概念代码或网络")

        return standardized

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

    def fetch_stock_fund_flow(
        self, code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """获取个股近区间的主力净流入与单笔资金流情况。"""

        if not code:
            raise ValueError("股票代码不能为空")

        start_dt = pd.to_datetime(start_date, errors="coerce").date()
        end_dt = pd.to_datetime(end_date, errors="coerce").date()
        if start_dt > end_dt:
            raise ValueError("开始日期不能晚于结束日期")

        normalized_code = self._normalize_code(code)
        market = self._detect_market(normalized_code)

        def action_builder(verify_ssl: bool) -> pd.DataFrame:
            url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"
                )
            }
            params = {
                "lmt": "0",
                "klt": "101",
                "secid": f"{1 if market == 'sh' else 0}.{normalized_code}",
                "fields1": "f1,f2,f3,f7",
                "fields2": (
                    "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65"
                ),
                "ut": "b2884a393a59ad64002292a3e90d46a5",
                "_": int(time.time() * 1000),
            }
            response = requests.get(
                url, params=params, headers=headers, timeout=15, verify=verify_ssl
            )
            response.raise_for_status()
            data_json = response.json()
            klines = data_json.get("data", {}).get("klines")
            if not klines:
                raise LookupError("未获取到个股资金流数据")

            columns = [
                "日期",
                "主力净流入-净额",
                "小单净流入-净额",
                "中单净流入-净额",
                "大单净流入-净额",
                "超大单净流入-净额",
                "主力净流入-净占比",
                "小单净流入-净占比",
                "中单净流入-净占比",
                "大单净流入-净占比",
                "超大单净流入-净占比",
                "收盘价",
                "涨跌幅",
                "-",
                "-",
            ]
            df = pd.DataFrame([item.split(",") for item in klines], columns=columns)
            df = df[
                [
                    "日期",
                    "收盘价",
                    "涨跌幅",
                    "主力净流入-净额",
                    "主力净流入-净占比",
                    "超大单净流入-净额",
                    "超大单净流入-净占比",
                    "大单净流入-净额",
                    "大单净流入-净占比",
                    "中单净流入-净额",
                    "中单净流入-净占比",
                    "小单净流入-净额",
                    "小单净流入-净占比",
                ]
            ]
            return df

        flows = self._run_with_proxy_and_ssl_fallback(
            action_builder=action_builder,
            error_message=f"资金流向查询失败：{normalized_code}",
        )

        flows = flows.copy()
        flows["日期"] = pd.to_datetime(flows["日期"], errors="coerce").dt.date
        flows = flows[(flows["日期"] >= start_dt) & (flows["日期"] <= end_dt)]

        if flows.empty:
            raise LookupError("指定时间区间内无资金流向数据")

        rename_mapping = {
            "日期": "date",
            "主力净流入-净额": "net_main_inflow",
            "主力净流入-净占比": "net_main_ratio",
            "超大单净流入-净额": "net_super_large_order",
            "超大单净流入-净占比": "net_super_large_ratio",
            "大单净流入-净额": "net_large_order",
            "大单净流入-净占比": "net_large_ratio",
            "中单净流入-净额": "net_medium_order",
            "中单净流入-净占比": "net_medium_ratio",
            "小单净流入-净额": "net_small_order",
            "小单净流入-净占比": "net_small_ratio",
            "收盘价": "close",  # noqa: RUF100
            "涨跌幅": "pct_change",
        }
        flows.rename(columns=rename_mapping, inplace=True)

        numeric_columns = list(rename_mapping.values())
        numeric_columns.remove("date")
        self._ensure_float_columns(flows, numeric_columns)

        flows.insert(0, "code", normalized_code)
        flows["date"] = flows["date"].astype(str)

        ordered_columns = [
            "code",
            "date",
            "close",
            "pct_change",
            "net_main_inflow",
            "net_main_ratio",
            "net_super_large_order",
            "net_super_large_ratio",
            "net_large_order",
            "net_large_ratio",
            "net_medium_order",
            "net_medium_ratio",
            "net_small_order",
            "net_small_ratio",
        ]

        for column in ordered_columns:
            if column not in flows:
                flows[column] = pd.NA

        return flows[ordered_columns]

    def _standardize_board_list(
        self, boards: pd.DataFrame, code_label: str, name_label: str
    ) -> pd.DataFrame:
        boards = boards.copy()
        code_column = self._find_first_column(
            boards, ["代码", "code", "板块代码", "板块编号", "行业代码", "概念代码", "编号"]
        )
        name_column = self._find_first_column(
            boards, ["名称", "name", "板块名称", "行业名称", "概念名称", "指数名称"]
        )

        boards[code_label] = boards[code_column] if code_column else pd.NA
        boards[name_label] = boards[name_column] if name_column else pd.NA
        boards[code_label] = boards[code_label].astype(str).str.strip()
        boards[name_label] = boards[name_label].astype(str).str.strip()

        boards = boards[[code_label, name_label]]
        boards.dropna(how="all", inplace=True)
        boards = boards[boards[name_label] != ""]
        boards.reset_index(drop=True, inplace=True)
        return boards

    def _extract_board_name(self, members: pd.DataFrame) -> str:
        name_column = self._find_first_column(
            members, ["板块名称", "行业名称", "概念名称", "名称", "name"]
        )
        if name_column is None or members.empty:
            return ""

        valid_names = members[name_column].dropna()
        if valid_names.empty:
            return ""

        return str(valid_names.iloc[0]).strip()

    def _standardize_board_members(
        self,
        members: pd.DataFrame,
        board_code: str,
        code_label: str,
        name_label: str,
    ) -> pd.DataFrame:
        members = members.copy()
        stock_code_column = self._find_first_column(
            members, ["代码", "code", "股票代码", "证券代码", "成分券代码"]
        )
        stock_name_column = self._find_first_column(
            members, ["名称", "name", "股票简称", "证券简称", "股票名称"]
        )

        board_name = self._extract_board_name(members) or str(board_code).strip()
        members[code_label] = str(board_code).strip()
        members[name_label] = board_name

        stock_codes = (
            members[stock_code_column].apply(
                lambda value: self._normalize_code(value) if pd.notna(value) else pd.NA
            )
            if stock_code_column
            else pd.Series(pd.NA, index=members.index)
        )
        stock_names = (
            members[stock_name_column]
            if stock_name_column
            else pd.Series(pd.NA, index=members.index)
        )

        standardized = pd.DataFrame(
            {
                code_label: members[code_label],
                name_label: members[name_label],
                "stock_code": stock_codes,
                "stock_name": stock_names,
            }
        )

        standardized = standardized[
            (standardized["stock_code"].notna()) | (standardized["stock_name"].notna())
        ]
        standardized.reset_index(drop=True, inplace=True)
        return standardized

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
