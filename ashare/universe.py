"""实时行情与交易标的筛选工具."""

from __future__ import annotations

from typing import Set

import pandas as pd
import akshare as ak


class AshareUniverseBuilder:
    """基于 AKShare 实时行情构建当日交易标的候选池（仅使用新浪数据源）."""

    def __init__(self, top_liquidity_count: int = 100):
        # 挑选成交额前多少名
        self.top_liquidity_count = top_liquidity_count
        # 缓存实时行情，避免多次请求
        self._spot_cache: pd.DataFrame | None = None

    # ======================== 核心底层方法 ========================

    def _fetch_spot(self) -> pd.DataFrame:
        """从新浪获取全市场实时行情（stock_zh_a_spot）."""
        if self._spot_cache is not None:
            return self._spot_cache

        try:
            df = ak.stock_zh_a_spot()
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                "获取全市场实时行情失败：无法访问新浪 stock_zh_a_spot，请检查网络或代理设置。"
            ) from e

        # df 默认已经有“代码、名称、最新价、涨跌幅、成交量、成交额”等字段
        self._spot_cache = df
        return df

    def _infer_st_codes(self, spot_df: pd.DataFrame) -> Set[str]:
        """根据名称识别 ST / *ST / 含“退”的股票，近似替代 ST 接口."""
        if "名称" not in spot_df.columns or "代码" not in spot_df.columns:
            return set()

        names = spot_df["名称"].astype(str)
        mask_st = names.str.startswith("ST") | names.str.startswith("*ST")
        mask_tui = names.str.contains("退")
        return set(spot_df.loc[mask_st | mask_tui, "代码"])

    def _infer_stop_codes(self, spot_df: pd.DataFrame) -> Set[str]:
        """根据成交量和价格近似识别停牌股票，替代停牌接口."""
        cols = spot_df.columns
        if "代码" not in cols or "成交量" not in cols:
            return set()

        vol_zero = spot_df["成交量"] == 0
        if "最新价" in cols and "昨收" in cols:
            same_price = spot_df["最新价"] == spot_df["昨收"]
            mask = vol_zero & same_price
        else:
            mask = vol_zero

        return set(spot_df.loc[mask, "代码"])

    def _fetch_new_stock_codes(self) -> Set[str]:
        """用非 em 接口获取次新股代码，失败则返回空集合."""
        # 只用不带 _em 的接口：stock_zh_a_new_df / stock_zh_a_new
        for func_name in ("stock_zh_a_new_df", "stock_zh_a_new"):
            fetcher = getattr(ak, func_name, None)
            if fetcher is None:
                continue
            try:
                new_df = fetcher()
            except Exception:  # noqa: BLE001
                continue
            if new_df is not None and "代码" in new_df.columns:
                return set(new_df["代码"])

        # 实在拿不到就全部当成非次新
        return set()

    # ======================== 对外方法 ========================

    def build_universe(self) -> pd.DataFrame:
        """生成剔除 ST 与停牌标的后的实时行情清单."""
        spot_df = self._fetch_spot()

        st_codes = self._infer_st_codes(spot_df)
        stop_codes = self._infer_stop_codes(spot_df)
        new_codes = self._fetch_new_stock_codes()

        bad_codes = st_codes | stop_codes

        filtered = spot_df[~spot_df["代码"].isin(bad_codes)].copy()
        filtered["是否次新股"] = filtered["代码"].isin(new_codes)
        return filtered

    def pick_top_liquidity(self, universe_df: pd.DataFrame) -> pd.DataFrame:
        """从候选池中筛选成交额最高的标的."""
        if "成交额" not in universe_df.columns:
            raise RuntimeError("候选池缺少成交额字段，无法进行成交额排序。")

        sorted_df = universe_df.sort_values("成交额", ascending=False)
        return sorted_df.head(self.top_liquidity_count)
