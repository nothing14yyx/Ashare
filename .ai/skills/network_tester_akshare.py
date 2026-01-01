from __future__ import annotations

import argparse
import csv
import json
import os
import time
from contextlib import contextmanager
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


@contextmanager
def _temp_env(overrides: dict[str, str | None]):
    """临时覆盖环境变量（用于对比：走代理 vs 不走代理）。"""

    old: dict[str, str | None] = {}
    for k, v in overrides.items():
        old[k] = os.environ.get(k)
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

    try:
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _print_proxy_env() -> None:
    keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
        "NO_PROXY",
        "no_proxy",
    ]
    print("代理环境变量：")
    for k in keys:
        v = os.environ.get(k)
        if v:
            print(f"  {k}={v}")
        else:
            print(f"  {k}=<empty>")


def _eastmoney_push2_ping() -> pd.DataFrame:
    """直连测试：东方财富 push2 行情接口（模拟 AkShare 的底层请求）。"""

    try:
        import requests
    except ImportError as exc:  # noqa: BLE001
        raise ImportError("缺少 requests 依赖，无法进行 push2 连通性测试") from exc

    url = "https://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 1,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f12",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
        "fields": "f12,f13,f14,f2,f3,f4",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json() if resp.content else {}

    diff = (((data or {}).get("data") or {}).get("diff")) or []
    if not diff:
        return pd.DataFrame()

    first = diff[0] or {}
    return pd.DataFrame(
        [
            {
                "f12": first.get("f12"),
                "f13": first.get("f13"),
                "name": first.get("f14"),
                "latest": first.get("f2"),
                "pct": first.get("f3"),
                "chg": first.get("f4"),
            }
        ]
    )


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


def build_cases(
    test_day: date,
    test_symbol: str,
    *,
    with_spot: bool = False,
) -> list[tuple[str, Callable[[], Any]]]:
    """构造测试用例列表。

    - 默认只测试“日频/数据中心类接口”。
    - 当 with_spot=True 时，额外测试“实时行情”接口（会更慢、更容易触发限流）。
    """

    if ak is None:
        raise ImportError("akshare 未安装，无法运行 AkShare 自检脚本")

    cases: list[tuple[str, Callable[[], Any]]] = []

    # -------------------------
    # 网络连通性诊断：走代理 vs 不走代理
    # -------------------------
    no_proxy_hosts = "82.push2.eastmoney.com,push2.eastmoney.com,eastmoney.com"

    def push2_env_case() -> Any:
        return _eastmoney_push2_ping()

    def push2_no_proxy_case() -> Any:
        # 清掉常见代理环境变量，同时设置 no_proxy/NO_PROXY 让 requests/urllib 走直连
        with _temp_env(
            {
                "HTTP_PROXY": None,
                "HTTPS_PROXY": None,
                "http_proxy": None,
                "https_proxy": None,
                "NO_PROXY": no_proxy_hosts,
                "no_proxy": no_proxy_hosts,
            }
        ):
            return _eastmoney_push2_ping()

    cases.append(("eastmoney_push2_ping_env", push2_env_case))
    cases.append(("eastmoney_push2_ping_no_proxy", push2_no_proxy_case))

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

    cases.append(("gdhs_summary_latest", lambda: ak.stock_zh_a_gdhs(symbol="最新")))
    cases.append((f"gdhs_detail_{test_symbol}", lambda: ak.stock_zh_a_gdhs_detail_em(symbol=test_symbol)))

    # 新增：用项目里的 AkshareDataFetcher 跑一遍 batch_get_shareholder_count_detail
    def fetcher_batch_case() -> Any:
        # 为了支持“直接运行这个脚本文件”，这里把项目根目录塞进 sys.path
        import sys

        root = Path(__file__).resolve().parents[2]
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))

        from ashare.data.akshare_fetcher import AkshareDataFetcher  # noqa: WPS433

        fetcher = AkshareDataFetcher()
        return fetcher.batch_get_shareholder_count_detail([test_symbol])

    cases.append((f"fetcher_batch_gdhs_detail_{test_symbol}", fetcher_batch_case))

    # -------------------------
    # 可选：实时行情（会更慢、更容易被限流）
    # -------------------------
    if with_spot:
        cases.append(("spot_zh_a_spot_em_env", lambda: ak.stock_zh_a_spot_em()))

        def spot_no_proxy_case() -> Any:
            with _temp_env(
                {
                    "HTTP_PROXY": None,
                    "HTTPS_PROXY": None,
                    "http_proxy": None,
                    "https_proxy": None,
                    "NO_PROXY": no_proxy_hosts,
                    "no_proxy": no_proxy_hosts,
                }
            ):
                return ak.stock_zh_a_spot_em()

        cases.append(("spot_zh_a_spot_em_no_proxy", spot_no_proxy_case))

    return cases


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="test_akshare_network",
        description=(
            "AkShare 接口稳定性自检脚本：包含数据中心类接口 +（可选）实时行情接口，"
            "并提供东方财富 push2 连通性（走代理/不走代理）对比测试。"
        ),
    )
    parser.add_argument("--rounds", type=int, default=1, help="测试轮数（默认 1）")
    parser.add_argument("--symbol", type=str, default="600000", help="测试股票代码（默认 600000）")
    parser.add_argument(
        "--with-spot",
        action="store_true",
        help="额外测试 AkShare 实时行情接口（stock_zh_a_spot_em，较慢且可能触发限流）",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="每轮之间休眠秒数（默认 0.5，避免频繁请求触发限流）",
    )

    args = parser.parse_args()

    rounds = max(1, int(args.rounds))
    test_day = date.today()
    test_symbol = str(args.symbol).strip() or "600000"
    with_spot = bool(args.with_spot)
    sleep_s = max(0.0, float(args.sleep))

    print("==== AkShare 接口稳定性自检 ====")
    print(f"执行时间：{datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"测试轮数：{rounds}")
    print(f"实时行情：{'ON' if with_spot else 'OFF'}")
    print("================================\n")

    _print_proxy_env()
    print(
        "\n提示：如果你在实时行情/东财接口看到 ProxyError，通常是系统/环境代理不可用或被断开；"
        "脚本里会对比 env vs no_proxy 两种方式，帮助你快速定位问题。\n"
    )
    print(f"测试日期：{_date_str(test_day)}（用于龙虎榜/两融接口）")
    print(f"测试股票：{test_symbol}（用于股东户数明细接口）\n")

    out_dir = Path(__file__).resolve().parents[2] / "tool" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"akshare_network_report_{timestamp}.csv"
    json_path = out_dir / f"akshare_network_report_{timestamp}.json"

    all_results: list[CaseResult] = []

    for r in range(1, rounds + 1):
        print(f"\n===== Round {r}/{rounds} =====")

        cases = build_cases(test_day=test_day, test_symbol=test_symbol, with_spot=with_spot)
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

        if sleep_s > 0 and r < rounds:
            time.sleep(sleep_s)

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
