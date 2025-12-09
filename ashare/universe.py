"""实时行情与交易标的筛选工具."""

from __future__ import annotations

from typing import Set

import pandas as pd
import akshare as ak


class AshareUniverseBuilder:
    """基于 AKShare 实时行情构建当日交易标的候选池."""

    def __init__(self, top_liquidity_count: int = 100):
        self.top_liquidity_count = top_liquidity_count

    def _fetch_spot(self) -> pd.DataFrame:
        try:
            return ak.stock_zh_a_spot_em()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("获取全市场实时行情失败, 请检查网络或数据源可用性。") from exc

    def _fetch_st_codes(self) -> Set[str]:
        try:
            st_df = ak.stock_zh_a_st_em()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("获取 ST 股票列表失败, 无法完成过滤。") from exc
        return set(st_df.get("代码", []))

    def _fetch_stop_codes(self) -> Set[str]:
        try:
            stop_df = ak.stock_zh_a_stop_em()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("获取停牌股票列表失败, 无法完成过滤。") from exc
        return set(stop_df.get("代码", []))

    def _fetch_new_stock_codes(self) -> Set[str]:
        fetchers = (ak.stock_zh_a_new_em, ak.stock_zh_a_new_df, ak.stock_zh_a_new)
        for fetcher in fetchers:
            try:
                new_df = fetcher()
                return set(new_df.get("代码", []))
            except Exception:  # noqa: BLE001
                continue
        return set()

    def build_universe(self) -> pd.DataFrame:
        """生成剔除 ST 与停牌标的后的实时行情清单."""

        spot_df = self._fetch_spot()
        st_codes = self._fetch_st_codes()
        stop_codes = self._fetch_stop_codes()
        new_codes = self._fetch_new_stock_codes()

        bad_codes = st_codes | stop_codes
        filtered = spot_df[~spot_df["代码"].isin(bad_codes)].copy()
        filtered["是否次新股"] = filtered["代码"].isin(new_codes)
        return filtered

    def pick_top_liquidity(self, universe_df: pd.DataFrame) -> pd.DataFrame:
        """从候选池中筛选成交额最高的标的."""

        if "成交额" not in universe_df.columns:
            raise RuntimeError("候选池缺少成交额字段, 无法进行成交额排序。")
        sorted_df = universe_df.sort_values("成交额", ascending=False)
        return sorted_df.head(self.top_liquidity_count)
