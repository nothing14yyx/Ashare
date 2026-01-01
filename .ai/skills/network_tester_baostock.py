from __future__ import annotations

"""
Baostock 核心接口网络与稳定性测试脚本

用途：
- 登录 Baostock，一次性跑一圈常用 A 股接口；
- 记录每个接口是否成功、耗时、返回行数；
- 把结果保存到当前目录下 output/baostock_core_ashare_test_*.csv，方便长期对比。

使用方法（在项目根目录）：
    python -m ashare.test_baostock_network --loops 3
或在 ashare 目录下：
    python test_baostock_network.py --loops 3
"""

import argparse
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List

import baostock as bs
import pandas as pd


def _resultset_to_df(rs: Any) -> pd.DataFrame:
    """通用 ResultSet -> DataFrame 转换。"""
    data_list: List[List[str]] = []
    # Baostock 的错误码是字符串，'0' 表示成功
    while (getattr(rs, "error_code", None) == "0") and rs.next():
        data_list.append(rs.get_row_data())
    fields = getattr(rs, "fields", None)
    if fields is not None:
        return pd.DataFrame(data_list, columns=fields)
    return pd.DataFrame(data_list)


def _run_case(
    name: str,
    func: Callable[..., Any],
    kwargs: Dict[str, Any],
    round_idx: int,
) -> Dict[str, Any]:
    """执行单个接口测试，捕获异常并统计耗时、行数等信息。"""
    t0 = time.perf_counter()
    status = "unknown"
    error_code = None
    error_msg = ""
    row_count = None

    try:
        rs = func(**kwargs)
    except Exception as e:  # 网络异常、参数错误等
        elapsed_ms = (time.perf_counter() - t0) * 1000
        return {
            "round": round_idx,
            "case": name,
            "status": "exception",
            "error_code": None,
            "error_msg": repr(e),
            "row_count": None,
            "elapsed_ms": round(elapsed_ms, 2),
        }

    # 尝试读取 error_code / error_msg
    error_code = getattr(rs, "error_code", None)
    error_msg = getattr(rs, "error_msg", "")

    try:
        df = _resultset_to_df(rs)
        row_count = len(df)
        status = "success" if error_code == "0" else "api_error"
    except Exception as e:
        status = "read_failed"
        error_msg = f"{error_msg}; read_failed={repr(e)}"

    elapsed_ms = (time.perf_counter() - t0) * 1000
    return {
        "round": round_idx,
        "case": name,
        "status": status,
        "error_code": error_code,
        "error_msg": error_msg,
        "row_count": row_count,
        "elapsed_ms": round(elapsed_ms, 2),
    }


def build_test_cases() -> List[Dict[str, Any]]:
    """构造一批典型 A 股接口的测试用例。"""
    today = date.today()
    end = today.strftime("%Y-%m-%d")
    start_1y = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    start_30d = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    start_5y = (today - timedelta(days=365 * 5)).strftime("%Y-%m-%d")
    # 基本面数据用稍微滞后的完整财报年份，这样成功概率更高
    fin_year = today.year - 2
    if fin_year < 2007:
        fin_year = 2007

    cases: List[Dict[str, Any]] = [
        # --- 核心：证券列表 / 基本信息 ---
        {
            "name": "query_all_stock_today",
            "func": bs.query_all_stock,
            "kwargs": {},
        },
        {
            "name": "query_stock_basic_sh600000",
            "func": bs.query_stock_basic,
            "kwargs": {"code": "sh.600000"},
        },
        # --- 核心：指数 & 股票 K 线 ---
        {
            "name": "history_k_sh000001_daily_1y",
            "func": bs.query_history_k_data_plus,
            "kwargs": {
                "code": "sh.000001",
                "fields": "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                "start_date": start_1y,
                "end_date": end,
                "frequency": "d",
            },
        },
        {
            "name": "history_k_sz000001_5min_30d",
            "func": bs.query_history_k_data_plus,
            "kwargs": {
                "code": "sz.000001",
                # 分钟线字段：注意多了 time，去掉 preclose、pctChg
                "fields": "date,time,code,open,high,low,close,volume,amount,adjustflag",
                "start_date": start_30d,
                "end_date": end,
                "frequency": "5",
            },
        },
        # --- 交易日历 ---
        {
            "name": "trade_dates_last_5y",
            "func": bs.query_trade_dates,
            "kwargs": {"start_date": start_5y, "end_date": end},
        },
        # --- 财务数据：盈利 / 成长 / 偿债 / 现金流 ---
        {
            "name": "profit_data_sh600000",
            "func": bs.query_profit_data,
            "kwargs": {"code": "sh.600000", "year": str(fin_year), "quarter": 4},
        },
        {
            "name": "growth_data_sh600000",
            "func": bs.query_growth_data,
            "kwargs": {"code": "sh.600000", "year": str(fin_year), "quarter": 4},
        },
        {
            "name": "balance_data_sh600000",
            "func": bs.query_balance_data,
            "kwargs": {"code": "sh.600000", "year": str(fin_year), "quarter": 4},
        },
        {
            "name": "cash_flow_data_sh600000",
            "func": bs.query_cash_flow_data,
            "kwargs": {"code": "sh.600000", "year": str(fin_year), "quarter": 4},
        },
    ]

    # 某些环境可能没有升级到最新 baostock，个别接口不存在时在运行阶段兜底
    return cases


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Baostock 接口网络 / 稳定性测试脚本")
    parser.add_argument(
        "--loops",
        type=int,
        default=1,
        help="重复测试轮数（默认 1 轮，建议 3~5 轮观察稳定性）",
    )
    args = parser.parse_args(argv)

    print("==== Baostock 接口稳定性自检 ====")
    print(f"执行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试轮数：{args.loops}")
    print("================================\n")

    # 登录
    print("登录 Baostock ...")
    lg = bs.login()
    print(f"login error_code={lg.error_code}, error_msg={lg.error_msg}")
    if lg.error_code != "0":
        print("登录失败，无法继续测试，请检查网络或 baostock 安装。")
        return

    all_results: List[Dict[str, Any]] = []
    cases = build_test_cases()

    try:
        for round_idx in range(1, args.loops + 1):
            print(f"\n===== Round {round_idx}/{args.loops} =====")
            for case in cases:
                name = case["name"]
                func = case["func"]
                kwargs = case["kwargs"]

                # 防止老版本 baostock 缺少某些接口
                if not callable(func):
                    print(f"[SKIP] {name}: 接口不可用（非可调用对象）")
                    continue

                print(f"[RUN ] {name} ...", end="", flush=True)
                try:
                    result = _run_case(name, func, kwargs, round_idx)
                except Exception:
                    # 理论上 _run_case 已经兜底，这里只是双保险
                    traceback.print_exc()
                    continue

                all_results.append(result)
                print(
                    f" {result['status']} "
                    f"(code={result['error_code']}, rows={result['row_count']}, "
                    f"{result['elapsed_ms']} ms)"
                )
    finally:
        print("\n登出 Baostock ...")
        bs.logout()

    if not all_results:
        print("没有任何结果，可能所有接口都执行失败。")
        return

    df = pd.DataFrame(all_results)
    print("\n===== 汇总统计 =====")
    print(df["status"].value_counts())
    print("\n按接口维度统计：")
    print(df.groupby(["case", "status"]).size())

    base_dir = Path(__file__).resolve().parent
    out_dir = base_dir.parents[1] / "tool" / "output"
    out_dir.mkdir(exist_ok=True, parents=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"baostock_core_ashare_test_{ts}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n详细结果已保存到: {out_path}")


if __name__ == "__main__":
    main()
