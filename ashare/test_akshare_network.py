"""
用于日常检查核心交易接口是否可用，和项目代码无直接依赖。

本脚本是独立的调试工具，会测试一批“目前在你机器上稳定可用”的 A 股核心接口，
并将结果输出到控制台和 output/akshare_core_ashare_test_*.csv。
接口白名单基于最近一次 25/25 成功的测试结果，已剔除不稳定的东财历史分钟线接口。
"""

import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd


# ===== 核心 A 股交易相关接口白名单（已去掉失败的接口） =====
CORE_INTERFACES = [
    # --- 实时行情 / 股票基础 ---
    ("A 股全市场实时行情-新浪", "stock_zh_a_spot", lambda: ak.stock_zh_a_spot()),
    ("科创板实时行情", "stock_zh_kcb_spot", lambda: ak.stock_zh_kcb_spot()),
    ("A+H 股实时行情", "stock_zh_ah_spot", lambda: ak.stock_zh_ah_spot()),

    # --- 历史行情（日线 / 当日分时 / 分笔） ---
    ("A 股历史日线-新浪", "stock_zh_a_daily", lambda: ak.stock_zh_a_daily()),
    ("A 股历史日线-腾讯", "stock_zh_a_hist_tx", lambda: ak.stock_zh_a_hist_tx()),
    # 已移除：stock_zh_a_hist_min_em / stock_zh_a_hist_pre_min_em
    ("A 股当日分时-新浪", "stock_zh_a_minute", lambda: ak.stock_zh_a_minute()),
    ("A 股当日分笔-Tencent JS", "stock_zh_a_tick_tx_js", lambda: ak.stock_zh_a_tick_tx_js()),

    # --- 股东结构 / 资金抱团相关 ---
    ("A 股股东户数-整体", "stock_zh_a_gdhs", lambda: ak.stock_zh_a_gdhs()),
    ("A 股股东户数-明细(东财)", "stock_zh_a_gdhs_detail_em", lambda: ak.stock_zh_a_gdhs_detail_em()),
    ("A 股机构持股(股本结构)-东财", "stock_zh_a_gbjg_em", lambda: ak.stock_zh_a_gbjg_em()),

    # --- 公告 / 信息披露 ---
    ("巨潮资讯-公司信息披露-关系", "stock_zh_a_disclosure_relation_cninfo", lambda: ak.stock_zh_a_disclosure_relation_cninfo()),
    ("巨潮资讯-公司信息披露-公告", "stock_zh_a_disclosure_report_cninfo", lambda: ak.stock_zh_a_disclosure_report_cninfo()),
    ("科创板公告-东财", "stock_zh_kcb_report_em", lambda: ak.stock_zh_kcb_report_em()),

    # --- 估值 / 财务横向比较 ---
    ("成长性比较-东财", "stock_zh_growth_comparison_em", lambda: ak.stock_zh_growth_comparison_em()),
    ("估值比较-东财", "stock_zh_valuation_comparison_em", lambda: ak.stock_zh_valuation_comparison_em()),
    ("杜邦分析比较-东财", "stock_zh_dupont_comparison_em", lambda: ak.stock_zh_dupont_comparison_em()),
    ("公司规模比较-东财", "stock_zh_scale_comparison_em", lambda: ak.stock_zh_scale_comparison_em()),
    ("A 股整体估值-百度", "stock_zh_valuation_baidu", lambda: ak.stock_zh_valuation_baidu()),
    ("涨跌投票-百度情绪", "stock_zh_vote_baidu", lambda: ak.stock_zh_vote_baidu()),

    # --- A+H / CDR / 科创板 / B 股（扩展） ---
    ("A+H 股列表字典", "stock_zh_ah_name", lambda: ak.stock_zh_ah_name()),
    ("A+H 股历史日线", "stock_zh_ah_daily", lambda: ak.stock_zh_ah_daily()),
    ("科创板历史日线", "stock_zh_kcb_daily", lambda: ak.stock_zh_kcb_daily()),
    ("CDR 历史日线", "stock_zh_a_cdr_daily", lambda: ak.stock_zh_a_cdr_daily()),
    ("B 股历史日线", "stock_zh_b_daily", lambda: ak.stock_zh_b_daily()),
    ("B 股分钟线-新浪", "stock_zh_b_minute", lambda: ak.stock_zh_b_minute()),
]


def print_env_info():
    print("===== 环境信息 =====")
    print("Python 版本:", sys.version.replace("\n", " "))
    print("AkShare 版本:", getattr(ak, "__version__", "unknown"))
    print("HTTP_PROXY :", os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy"))
    print("HTTPS_PROXY:", os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"))
    print("====================\n")


def format_result(result):
    """把返回结果压缩成一行描述字符串。"""
    if isinstance(result, pd.DataFrame):
        return f"DataFrame, shape={result.shape}"
    if isinstance(result, pd.Series):
        return f"Series, shape={result.shape}"
    if isinstance(result, (list, tuple, set)):
        return f"{type(result).__name__}, len={len(result)}"
    if isinstance(result, dict):
        return f"dict, keys={list(result.keys())[:10]}"
    return f"{type(result).__name__}"


def main():
    print_env_info()

    records = []
    total = len(CORE_INTERFACES)
    print(f"准备测试核心接口数量: {total}")
    print("-" * 60)

    for idx, (cn_desc, name, func) in enumerate(CORE_INTERFACES, start=1):
        tag = f"[{idx:02d}/{total}] {name}"
        print(f"{tag} ({cn_desc}) ... ", end="", flush=True)

        try:
            result = func()
            status = "success"
            detail = format_result(result)
            print("success")
        except TypeError as e:
            status = "need_params"
            detail = f"TypeError: {e}"
            print("need_params")
        except Exception as e:
            status = "error"
            detail = f"{type(e).__name__}: {e}"
            print("error")
            traceback.print_exc(limit=1)

        records.append(
            {
                "interface": name,
                "cn_desc": cn_desc,
                "status": status,
                "detail": detail,
            }
        )

    df = pd.DataFrame(records)

    print("\n===== 汇总统计 =====")
    print(df["status"].value_counts())

    out_dir = Path("output")
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"akshare_core_ashare_test_{ts}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n详细结果已保存到: {out_path}")


if __name__ == "__main__":
    main()
