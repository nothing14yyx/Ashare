"""基于 Baostock 日线数据的交易标的筛选工具."""

from __future__ import annotations

from typing import Set

import pandas as pd


class AshareUniverseBuilder:
    """使用 Baostock 数据构建当日交易候选池。"""

    def __init__(
        self,
        top_liquidity_count: int = 100,
    ) -> None:
        self.top_liquidity_count = top_liquidity_count

    def _infer_st_codes(
        self, stock_df: pd.DataFrame, history_df: pd.DataFrame
    ) -> Set[str]:
        if "code" not in stock_df.columns or "code_name" not in stock_df.columns:
            return set()

        names = stock_df["code_name"].astype(str)
        mask_name = (
            names.str.contains(r"^(ST|\*ST)", regex=True, case=False)
            | names.str.contains(r"(退|delist)", regex=True, case=False)
        )

        st_candidates = set(stock_df.loc[mask_name, "code"])

        if history_df.empty:
            return st_candidates

        required_cols = {"code", "date", "isST"}
        if not required_cols.issubset(history_df.columns):
            return st_candidates

        latest_kline = (
            history_df.sort_values("date")
            .groupby("code", as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )
        mask_official = latest_kline["isST"].astype(str) == "1"
        st_candidates.update(set(latest_kline.loc[mask_official, "code"]))

        return st_candidates

    def _infer_stop_codes(self, latest_kline: pd.DataFrame) -> Set[str]:
        cols = latest_kline.columns
        if "code" not in cols:
            return set()

        if "tradestatus" in cols:
            stopped = latest_kline[latest_kline["tradestatus"] != "1"]
            return set(stopped["code"])

        return set()

    def build_universe(
        self, stock_df: pd.DataFrame, history_df: pd.DataFrame
    ) -> pd.DataFrame:
        if stock_df.empty:
            raise RuntimeError("候选池构建失败：股票列表为空。")
        if history_df.empty:
            raise RuntimeError("候选池构建失败：历史日线数据为空。")

        # 提取每个标的最新一个交易日的日线数据
        latest_rows = (
            history_df.sort_values("date")
            .groupby("code", as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )

        st_codes = self._infer_st_codes(stock_df, history_df)
        stop_codes = self._infer_stop_codes(latest_rows)
        bad_codes = st_codes | stop_codes

        merged = stock_df.merge(latest_rows, on="code", how="left")
        filtered = merged[~merged["code"].isin(bad_codes)].copy()

        if "amount" in filtered.columns:
            filtered["amount"] = pd.to_numeric(filtered["amount"], errors="coerce")

        return filtered

    def pick_top_liquidity(self, universe_df: pd.DataFrame) -> pd.DataFrame:
        if universe_df.empty:
            raise RuntimeError("候选池为空，无法筛选流动性。")
        if "amount" not in universe_df.columns:
            raise RuntimeError("候选池缺少成交额字段，无法进行排序。")

        sorted_df = universe_df.sort_values("amount", ascending=False)
        return sorted_df.head(self.top_liquidity_count)
