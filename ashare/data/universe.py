"""基于 Baostock 日线数据的交易标的筛选工具."""

from __future__ import annotations

from typing import Set

import pandas as pd


class AshareUniverseBuilder:
    """使用 Baostock 数据构建当日交易候选池。"""

    def __init__(
        self,
        top_liquidity_count: int = 100,
        min_listing_days: int = 60,
    ) -> None:
        self.top_liquidity_count = top_liquidity_count
        self.min_listing_days = min_listing_days

    def _infer_st_codes(
        self, stock_df: pd.DataFrame, latest_kline: pd.DataFrame
    ) -> Set[str]:
        if "code" not in stock_df.columns or "code_name" not in stock_df.columns:
            return set()

        names = stock_df["code_name"].astype(str)

        # 使用非捕获分组 (?:...)，避免 pandas 对捕获分组的 warning
        mask_name = (
            names.str.contains(r"^(?:ST|\*ST)", case=False)
            | names.str.contains(r"(?:退|delist)", case=False)
        )

        st_candidates = set(stock_df.loc[mask_name, "code"])

        if latest_kline.empty:
            return st_candidates

        required_cols = {"code", "date", "isST"}
        if not required_cols.issubset(latest_kline.columns):
            return st_candidates
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
        self,
        stock_df: pd.DataFrame,
        history_df: pd.DataFrame,
        stock_basic_df: pd.DataFrame | None = None,
        industry_df: pd.DataFrame | None = None,
        index_membership: dict[str, set[str]] | None = None,
    ) -> pd.DataFrame:
        if stock_df.empty:
            raise RuntimeError("候选池构建失败：股票列表为空。")
        if history_df.empty:
            raise RuntimeError("候选池构建失败：历史日线数据为空。")

        latest_trade_date = None
        if "date" in history_df.columns:
            latest_trade_date = (
                pd.to_datetime(history_df["date"], errors="coerce")
                .dropna()
                .max()
            )

        filtered_stock_df = stock_df.copy()
        if stock_basic_df is not None and not stock_basic_df.empty:
            filtered_stock_df = filtered_stock_df.merge(
                stock_basic_df,
                on="code",
                how="left",
                suffixes=(None, "_basic"),
            )

            type_col = filtered_stock_df.get("type")
            status_col = filtered_stock_df.get("status")
            if type_col is not None and status_col is not None:
                filtered_stock_df = filtered_stock_df[
                    (pd.to_numeric(type_col, errors="coerce") == 1)
                    & (pd.to_numeric(status_col, errors="coerce") == 1)
                ]

            if latest_trade_date is not None and "ipoDate" in filtered_stock_df.columns:
                ipo_dates = pd.to_datetime(
                    filtered_stock_df["ipoDate"], errors="coerce"
                )
                age_days = (latest_trade_date - ipo_dates).dt.days
                filtered_stock_df = filtered_stock_df[
                    (age_days >= self.min_listing_days) | age_days.isna()
                ]

        # 提取每个标的最新一个交易日的日线数据
        latest_rows = (
            history_df.sort_values("date")
            .groupby("code", as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )

        st_codes = self._infer_st_codes(filtered_stock_df, latest_rows)
        stop_codes = self._infer_stop_codes(latest_rows)
        bad_codes = st_codes | stop_codes

        merged = filtered_stock_df.merge(latest_rows, on="code", how="left")
        filtered = merged[~merged["code"].isin(bad_codes)].copy()

        # --- 新增：解决 MySQL 大小写不敏感导致的重复列名问题 ---
        # stock_df 里有 `tradeStatus`，latest_rows 里有 `tradestatus`
        # 在 MySQL 里会被视为同一个列名，导致 1060 Duplicate column name 错误。
        if "tradeStatus" in filtered.columns and "tradestatus" in filtered.columns:
            # 这里保留日线里的 `tradestatus`，删除股票列表里的 `tradeStatus`
            filtered = filtered.drop(columns=["tradeStatus"])
        # --- 新增结束 ---

        if industry_df is not None and not industry_df.empty:
            filtered = filtered.merge(
                industry_df,
                on="code",
                how="left",
                suffixes=(None, "_industry"),
            )

        if index_membership:
            for index_name, members in index_membership.items():
                flag_col = f"in_{index_name}"
                filtered[flag_col] = filtered["code"].isin(members)

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
