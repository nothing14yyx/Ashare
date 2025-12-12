"""Akshare 数据访问封装，用于行为证据信号采集."""

from __future__ import annotations

from typing import Any, Iterable

try:
    import akshare as ak
except ImportError:  # pragma: no cover - 环境未安装 akshare 时延迟失败
    ak = None

import pandas as pd


class AkshareDataFetcher:
    """封装常用的 Akshare 行为证据接口。"""

    def __init__(self) -> None:
        if ak is None:  # pragma: no cover - 运行时缺少依赖
            raise ImportError("akshare 未安装，无法初始化 AkshareDataFetcher")

    def _ensure_df(self, value: Any) -> pd.DataFrame:
        """把 AkShare 返回值尽量规整成 DataFrame；None / 不可转换 -> 空 DF。"""
        if value is None:
            return pd.DataFrame()
        if isinstance(value, pd.DataFrame):
            return value
        try:
            return pd.DataFrame(value)
        except Exception:  # noqa: BLE001
            return pd.DataFrame()

    def get_lhb_detail(self, trade_date: str) -> pd.DataFrame:
        """获取指定交易日的龙虎榜详情。"""
        normalized_date = str(trade_date).replace("-", "")
        raw = ak.stock_lhb_detail_em(start_date=normalized_date, end_date=normalized_date)
        return self._ensure_df(raw)

    def get_margin_detail(self, trade_date: str, exchange: str) -> pd.DataFrame:
        """获取沪深交易所的融资融券明细。"""
        exchange_map = {
            "sse": ak.stock_margin_detail_sse,
            "sh": ak.stock_margin_detail_sse,
            "shanghai": ak.stock_margin_detail_sse,
            "szse": ak.stock_margin_detail_szse,
            "sz": ak.stock_margin_detail_szse,
            "shenzhen": ak.stock_margin_detail_szse,
        }
        key = str(exchange).lower()
        if key not in exchange_map:
            raise ValueError(f"不支持的交易所标识: {exchange}")

        fetch_fn = exchange_map[key]
        raw = fetch_fn(date=str(trade_date).replace("-", ""))
        return self._ensure_df(raw)

    def get_hsgt_hold_rank(
        self, market: str = "沪股通", indicator: str = "5日排行"
    ) -> pd.DataFrame:
        """获取北向持股排行数据。"""
        raw = ak.stock_hsgt_hold_stock_em(market=market, indicator=indicator)
        return self._ensure_df(raw)

    def get_shareholder_count(self, period: str) -> pd.DataFrame:
        """获取全市场股东户数汇总。"""
        raw = ak.stock_zh_a_gdhs(symbol=period)
        return self._ensure_df(raw)

    def get_shareholder_count_detail(self, symbol: str) -> pd.DataFrame:
        """获取单只股票的股东户数明细。"""
        raw = ak.stock_zh_a_gdhs_detail_em(symbol=symbol)
        return self._ensure_df(raw)

    def batch_get_shareholder_count_detail(
        self, symbols: Iterable[str]
    ) -> list[pd.DataFrame]:
        """批量获取股东户数明细，过滤掉空结果；单个失败不影响整体。"""
        frames: list[pd.DataFrame] = []
        for symbol in symbols:
            try:
                df = self.get_shareholder_count_detail(symbol)
            except Exception:  # noqa: BLE001
                # 外部逻辑只关心“能拿到多少”，这里不抛异常，直接跳过
                continue

            if df is not None and not df.empty:
                frames.append(df)

        return frames
