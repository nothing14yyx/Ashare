from __future__ import annotations

import akshare as ak
import pandas as pd


class AshareCoreFetcher:
    """
    包装当前环境下稳定可用的 A 股核心接口。

    所有 public 方法：
    - 只调用你在 test_akshare_network.py 中测试为 success 的那 25 个接口；
    - 出错时不抛异常，而是打印警告并返回空 DataFrame。
    """

    def __init__(self) -> None:
        # 全市场实时行情缓存（同一进程内复用）
        self._spot_cache: pd.DataFrame | None = None

    # ========== 实时行情 ==========

    def get_realtime_all_a(self, use_cache: bool = True, **kwargs) -> pd.DataFrame:
        """
        全市场 A 股实时行情（新浪）
        对应 ak.stock_zh_a_spot(**kwargs)

        use_cache:
            True  - 优先使用本进程内缓存（推荐，大幅降低被风控几率）
            False - 强制重新请求一次新浪接口
        """
        # 1) 优先使用缓存
        if use_cache and self._spot_cache is not None:
            return self._spot_cache

        # 2) 真正发请求
        df = self._safe_call_df(ak.stock_zh_a_spot, "stock_zh_a_spot", **kwargs)

        # 3) 只有拿到非空数据才写缓存
        if use_cache and not df.empty:
            self._spot_cache = df

        return df

    def get_realtime_kcb(self, **kwargs) -> pd.DataFrame:
        """科创板实时行情，对应 ak.stock_zh_kcb_spot(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_kcb_spot, "stock_zh_kcb_spot", **kwargs)

    def get_realtime_ah(self, **kwargs) -> pd.DataFrame:
        """A+H 股实时行情，对应 ak.stock_zh_ah_spot(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_ah_spot, "stock_zh_ah_spot", **kwargs)

    # ========== 历史日线 / 分时 / 分笔 ==========

    def get_daily_a_sina(self, **kwargs) -> pd.DataFrame:
        """A 股历史日线-新浪，对应 ak.stock_zh_a_daily(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_a_daily, "stock_zh_a_daily", **kwargs)

    def get_daily_a_tx(self, **kwargs) -> pd.DataFrame:
        """A 股历史日线-腾讯，对应 ak.stock_zh_a_hist_tx(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_a_hist_tx, "stock_zh_a_hist_tx", **kwargs)

    def get_minute_a_today(self, **kwargs) -> pd.DataFrame:
        """A 股当日分时-新浪，对应 ak.stock_zh_a_minute(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_a_minute, "stock_zh_a_minute", **kwargs)

    def get_tick_a_today(self, **kwargs) -> pd.DataFrame:
        """A 股当日分笔-Tencent JS，对应 ak.stock_zh_a_tick_tx_js(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_a_tick_tx_js, "stock_zh_a_tick_tx_js", **kwargs)

    # B 股 / 科创板 / CDR

    def get_daily_b(self, **kwargs) -> pd.DataFrame:
        """B 股历史日线，对应 ak.stock_zh_b_daily(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_b_daily, "stock_zh_b_daily", **kwargs)

    def get_minute_b_today(self, **kwargs) -> pd.DataFrame:
        """B 股分钟线-新浪，对应 ak.stock_zh_b_minute(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_b_minute, "stock_zh_b_minute", **kwargs)

    def get_daily_kcb(self, **kwargs) -> pd.DataFrame:
        """科创板历史日线，对应 ak.stock_zh_kcb_daily(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_kcb_daily, "stock_zh_kcb_daily", **kwargs)

    def get_daily_cdr(self, **kwargs) -> pd.DataFrame:
        """CDR 历史日线，对应 ak.stock_zh_a_cdr_daily(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_a_cdr_daily, "stock_zh_a_cdr_daily", **kwargs)

    # A+H

    def get_ah_name(self, **kwargs) -> pd.DataFrame:
        """A+H 股列表，对应 ak.stock_zh_ah_name(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_ah_name, "stock_zh_ah_name", **kwargs)

    def get_daily_ah(self, **kwargs) -> pd.DataFrame:
        """A+H 股历史日线，对应 ak.stock_zh_ah_daily(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_ah_daily, "stock_zh_ah_daily", **kwargs)

    # ========== 股东 / 机构持股 ==========

    def get_gdhs(self, **kwargs) -> pd.DataFrame:
        """A 股股东户数，对应 ak.stock_zh_a_gdhs(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_a_gdhs, "stock_zh_a_gdhs", **kwargs)

    def get_gdhs_detail(self, **kwargs) -> pd.DataFrame:
        """股东户数明细-东财，对应 ak.stock_zh_a_gdhs_detail_em(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_a_gdhs_detail_em, "stock_zh_a_gdhs_detail_em", **kwargs)

    def get_gbjg(self, **kwargs) -> pd.DataFrame:
        """机构持股-东财，对应 ak.stock_zh_a_gbjg_em(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_a_gbjg_em, "stock_zh_a_gbjg_em", **kwargs)

    # ========== 公告 / 披露 ==========

    def get_disclosure_relation(self, **kwargs) -> pd.DataFrame:
        """巨潮资讯-关系，对应 ak.stock_zh_a_disclosure_relation_cninfo(**kwargs)。"""
        return self._safe_call_df(
            ak.stock_zh_a_disclosure_relation_cninfo,
            "stock_zh_a_disclosure_relation_cninfo",
            **kwargs,
        )

    def get_disclosure_report(self, **kwargs) -> pd.DataFrame:
        """巨潮资讯-公告，对应 ak.stock_zh_a_disclosure_report_cninfo(**kwargs)。"""
        return self._safe_call_df(
            ak.stock_zh_a_disclosure_report_cninfo,
            "stock_zh_a_disclosure_report_cninfo",
            **kwargs,
        )

    def get_kcb_reports(self, **kwargs) -> pd.DataFrame:
        """科创板公告-东财，对应 ak.stock_zh_kcb_report_em(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_kcb_report_em, "stock_zh_kcb_report_em", **kwargs)

    # ========== 估值 / 比较因子 ==========

    def get_growth_comparison(self, **kwargs) -> pd.DataFrame:
        """成长性比较-东财，对应 ak.stock_zh_growth_comparison_em(**kwargs)。"""
        return self._safe_call_df(
            ak.stock_zh_growth_comparison_em,
            "stock_zh_growth_comparison_em",
            **kwargs,
        )

    def get_valuation_comparison(self, **kwargs) -> pd.DataFrame:
        """估值比较-东财，对应 ak.stock_zh_valuation_comparison_em(**kwargs)。"""
        return self._safe_call_df(
            ak.stock_zh_valuation_comparison_em,
            "stock_zh_valuation_comparison_em",
            **kwargs,
        )

    def get_dupont_comparison(self, **kwargs) -> pd.DataFrame:
        """杜邦分析比较-东财，对应 ak.stock_zh_dupont_comparison_em(**kwargs)。"""
        return self._safe_call_df(
            ak.stock_zh_dupont_comparison_em,
            "stock_zh_dupont_comparison_em",
            **kwargs,
        )

    def get_scale_comparison(self, **kwargs) -> pd.DataFrame:
        """公司规模比较-东财，对应 ak.stock_zh_scale_comparison_em(**kwargs)。"""
        return self._safe_call_df(
            ak.stock_zh_scale_comparison_em,
            "stock_zh_scale_comparison_em",
            **kwargs,
        )

    def get_valuation_baidu(self, **kwargs) -> pd.DataFrame:
        """A 股整体估值-百度，对应 ak.stock_zh_valuation_baidu(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_valuation_baidu, "stock_zh_valuation_baidu", **kwargs)

    def get_vote_baidu(self, **kwargs) -> pd.DataFrame:
        """涨跌投票-百度情绪，对应 ak.stock_zh_vote_baidu(**kwargs)。"""
        return self._safe_call_df(ak.stock_zh_vote_baidu, "stock_zh_vote_baidu", **kwargs)

    # ========== 内部通用封装 ==========

    def _safe_call_df(self, fn, fn_name: str, **kwargs) -> pd.DataFrame:
        """
        安全调用一个返回 DataFrame 的 akshare 函数：
        - 捕获所有异常，打印警告；
        - 失败时返回空 DataFrame。
        """
        try:
            result = fn(**kwargs)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] 调用 {fn_name} 失败: {type(e).__name__}: {e}")
            return pd.DataFrame()

        if isinstance(result, pd.DataFrame):
            return result
        if result is None:
            return pd.DataFrame()
        return pd.DataFrame(result)
