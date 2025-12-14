from __future__ import annotations

import csv
import json
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd

try:
    import akshare as ak
except ImportError:
    ak = None


@dataclass
class CaseResult:
    round: int
    case: str
    status: str
    row_count: int | None
    elapsed_ms: float
    error_msg: str | None
    used_date: str | None = None
    tries: int | None = None


def _ensure_df(value: Any) -> pd.DataFrame:
    if value is None:
        return pd.DataFrame()
    if isinstance(value, pd.DataFrame):
        return value
    try:
        return pd.DataFrame(value)
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


def _count_rows(result: Any) -> int:
    if result is None:
        return 0
    if isinstance(result, pd.DataFrame):
        return int(len(result))
    if isinstance(result, list):
        total = 0
        for item in result:
            if isinstance(item, pd.DataFrame):
                total += len(item)
            else:
                total += len(_ensure_df(item))
        return int(total)
    return int(len(_ensure_df(result)))


def _run_case(func: Callable[[], Any]) -> tuple[str, int | None, float, str | None, Any]:
    t0 = time.perf_counter()
    try:
        result = func()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        rows = _count_rows(result)
        if rows == 0:
            return "success_empty", 0, elapsed_ms, None, result

        return "success", rows, elapsed_ms, None, result
    except Exception as exc:  # noqa: BLE001
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        return "exception", None, elapsed_ms, repr(exc), None


def _date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _date_compact(d: date) -> str:
    return d.strftime("%Y%m%d")


def _try_with_date_fallback(
    name: str,
    date_list: list[date],
    call_factory: Callable[[date], Callable[[], Any]],
) -> tuple[CaseResult, Any]:
    last_result: Any = None
    for i, d in enumerate(date_list, start=1):
        status, rows, elapsed_ms, err, result = _run_case(call_factory(d))
        last_result = result

        if status == "success":
            return (
                CaseResult(
                    round=0,
                    case=f"{name}_{_date_compact(date_list[0])}",
                    status=status,
                    row_count=rows,
                    elapsed_ms=elapsed_ms,
                    error_msg=None,
                    used_date=_date_str(d),
                    tries=i,
                ),
                result,
            )

        # 如果是空数据：有些接口当天确实可能为空，这里不当作异常；但如果是第一个日期为空，也继续尝试回退
        if status == "success_empty" and i < len(date_list):
            continue

        # 异常：继续尝试回退日期
        if status == "exception" and i < len(date_list):
            continue

        # 最后一次（无论 success_empty 还是 exception）
        return (
            CaseResult(
                round=0,
                case=f"{name}_{_date_compact(date_list[0])}",
                status=status,
                row_count=rows,
                elapsed_ms=elapsed_ms,
                error_msg=err,
                used_date=_date_str(d),
                tries=i,
            ),
            last_result,
        )

    # 理论上不会到这里
    return (
        CaseResult(
            round=0,
            case=f"{name}_{_date_compact(date_list[0])}",
            status="exception",
            row_count=None,
            elapsed_ms=0.0,
            error_msg="unknown",
            used_date=None,
            tries=None,
        ),
        None,
    )


def build_cases(test_day: date, test_symbol: str) -> list[tuple[str, Callable[[], Any]]]:
    if ak is None:
        raise ImportError("akshare 未安装，无法运行 AkShare 自检脚本")

    cases: list[tuple[str, Callable[[], Any]]] = []

    # 这几个接口依赖“交易日”。遇到周末/节假日时，仅回退 1 天仍可能不是交易日；
    # 这里统一回退 7 天，尽量命中最近一个交易日。
    trade_day_candidates = [test_day - timedelta(days=i) for i in range(0, 7)]

    def lhb_case() -> CaseResult:
        res, _ = _try_with_date_fallback(
            name="lhb_detail_em",
            date_list=trade_day_candidates,
            call_factory=lambda d: (
                lambda: ak.stock_lhb_detail_em(
                    start_date=_date_compact(d), end_date=_date_compact(d)
                )
            ),
        )
        return res

    def margin_sse_case() -> CaseResult:
        res, _ = _try_with_date_fallback(
            name="margin_detail_sse",
            date_list=trade_day_candidates,
            call_factory=lambda d: (lambda: ak.stock_margin_detail_sse(date=_date_compact(d))),
        )
        return res

    # SZSE：允许当天为空，不强制回退（但也提供一次回退尝试）
    def margin_szse_case() -> CaseResult:
        res, _ = _try_with_date_fallback(
            name="margin_detail_szse",
            date_list=trade_day_candidates,
            call_factory=lambda d: (lambda: ak.stock_margin_detail_szse(date=_date_compact(d))),
        )
        return res

    cases.append((f"lhb_detail_em_{_date_compact(test_day)}", lhb_case))
    cases.append((f"margin_detail_sse_{_date_compact(test_day)}", margin_sse_case))
    cases.append((f"margin_detail_szse_{_date_compact(test_day)}", margin_szse_case))

    cases.append(
        ("hsgt_hold_stock_em_hgt_5d", lambda: ak.stock_hsgt_hold_stock_em(market="沪股通", indicator="5日排行"))
    )
    cases.append(
        ("hsgt_hold_stock_em_sgt_5d", lambda: ak.stock_hsgt_hold_stock_em(market="深股通", indicator="5日排行"))
    )
    cases.append(("gdhs_summary_latest", lambda: ak.stock_zh_a_gdhs(symbol="最新")))
    cases.append((f"gdhs_detail_{test_symbol}", lambda: ak.stock_zh_a_gdhs_detail_em(symbol=test_symbol)))

    # 新增：用项目里的 AkshareDataFetcher 跑一遍 batch_get_shareholder_count_detail
    def fetcher_batch_case() -> Any:
        # 为了支持“直接运行这个脚本文件”，这里把项目根目录塞进 sys.path
        import sys

        root = Path(__file__).resolve().parents[1]
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))

        from ashare.akshare_fetcher import AkshareDataFetcher  # noqa: WPS433

        fetcher = AkshareDataFetcher()
        return fetcher.batch_get_shareholder_count_detail([test_symbol])

    cases.append((f"fetcher_batch_gdhs_detail_{test_symbol}", fetcher_batch_case))

    return cases


def main() -> None:
    rounds = 1
    test_day = date.today()
    test_symbol = "600000"

    print("==== AkShare 接口稳定性自检 ====")
    print(f"执行时间：{datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"测试轮数：{rounds}")
    print("================================\n")
    print(f"测试日期：{_date_str(test_day)}（用于龙虎榜/两融接口）")
    print(f"测试股票：{test_symbol}（用于股东户数明细接口）\n")

    out_dir = Path(__file__).resolve().parents[1] / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"akshare_network_report_{timestamp}.csv"
    json_path = out_dir / f"akshare_network_report_{timestamp}.json"

    all_results: list[CaseResult] = []

    for r in range(1, rounds + 1):
        print(f"\n===== Round {r}/{rounds} =====")

        cases = build_cases(test_day=test_day, test_symbol=test_symbol)
        for name, fn in cases:
            # 兼容两种：普通接口（返回 DF）/ 特殊接口（返回 CaseResult）
            print(f"[RUN ] {name} ... ", end="", flush=True)

            t0 = time.perf_counter()
            try:
                maybe = fn()
            except Exception as exc:  # noqa: BLE001
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                result = CaseResult(
                    round=r,
                    case=name,
                    status="exception",
                    row_count=None,
                    elapsed_ms=elapsed_ms,
                    error_msg=repr(exc),
                    used_date=None,
                    tries=None,
                )
                all_results.append(result)
                print(f"exception (error={result.error_msg})")
                continue

            if isinstance(maybe, CaseResult):
                # 日期回退那种 case
                maybe.round = r
                all_results.append(maybe)
                if maybe.status == "success":
                    print(f"success (rows={maybe.row_count}, used_date={maybe.used_date}, tries={maybe.tries})")
                elif maybe.status == "success_empty":
                    print(f"success_empty (rows=0, used_date={maybe.used_date}, tries={maybe.tries})")
                else:
                    print(f"exception (used_date={maybe.used_date}, tries={maybe.tries}, error={maybe.error_msg})")
                continue

            # 普通 case：统计行数 + 状态
            status, rows, elapsed_ms, err, _ = _run_case(lambda: maybe)
            result = CaseResult(
                round=r,
                case=name,
                status=status,
                row_count=rows,
                elapsed_ms=elapsed_ms,
                error_msg=err,
                used_date=None,
                tries=1,
            )
            all_results.append(result)

            if status == "success":
                print(f"success (rows={rows})")
            elif status == "success_empty":
                print("success_empty (rows=0)")
            else:
                print(f"exception (error={err})")

    # 写 CSV
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "round",
                "case",
                "status",
                "row_count",
                "elapsed_ms",
                "error_msg",
                "used_date",
                "tries",
            ],
        )
        writer.writeheader()
        for item in all_results:
            row = asdict(item)
            writer.writerow(row)

    # 写 JSON（更方便你后续程序读）
    with json_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(x) for x in all_results], f, ensure_ascii=False, indent=2)

    # 总结
    ok = sum(1 for x in all_results if x.status in ("success", "success_empty"))
    fail = sum(1 for x in all_results if x.status == "exception")
    print("\n================================")
    print(f"完成：ok={ok}, fail={fail}")
    print(f"CSV: {csv_path}")
    print(f"JSON: {json_path}")
    print("================================")


if __name__ == "__main__":
    main()
