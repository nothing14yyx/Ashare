"""定时调度执行开盘监测（每整 N 分钟触发一次，自动跳过非交易日）。

用法：
  # 每整 5 分钟跑一次
  python run_open_monitor_scheduler.py --interval 5

  # 每整 10 分钟跑一次
  python run_open_monitor_scheduler.py --interval 10

说明：
  - 这是一个前台常驻脚本（Ctrl+C 退出）。
  - 默认严格对齐“整点分钟边界”：00/05/10/... 或 00/10/20/...
  - 每次触发都会执行 ashare.open_monitor.MA5MA20OpenMonitorRunner().run(force=True)
"""

from __future__ import annotations

import argparse
import datetime as dt
from datetime import timedelta
import time

from ashare.config import get_section
from ashare.env_snapshot_utils import resolve_weekly_asof_date
from ashare.open_monitor import MA5MA20OpenMonitorRunner


def _next_run_at(now: dt.datetime, interval_min: int) -> dt.datetime:
    """计算下一个“整 interval_min 分钟”的触发时间（秒=0）。"""

    if interval_min <= 0:
        raise ValueError("interval_min must be positive")

    # 如果正好落在边界（例如 09:30:00），就返回 now（便于立即执行）
    if now.second == 0 and now.microsecond == 0 and (now.minute % interval_min == 0):
        return now

    minute_block = (now.minute // interval_min) * interval_min
    next_minute = minute_block + interval_min

    base = now.replace(second=0, microsecond=0)
    if next_minute < 60:
        return base.replace(minute=next_minute)
    # 进位到下一小时
    return (base.replace(minute=0) + dt.timedelta(hours=1))


def _default_interval_from_config() -> int:
    cfg = get_section("open_monitor") or {}
    if not isinstance(cfg, dict):
        return 5
    try:
        interval = int(cfg.get("interval_minutes", 5))
        return interval if interval > 0 else 5
    except Exception:  # noqa: BLE001
        return 5


TRADING_WINDOWS = [
    (dt.time(hour=9, minute=20), dt.time(hour=11, minute=35)),
    (dt.time(hour=12, minute=50), dt.time(hour=15, minute=10)),
]


def _in_trading_window(ts: dt.datetime) -> bool:
    t = ts.time()
    for start, end in TRADING_WINDOWS:
        if start <= t <= end:
            return True
    return False


def _is_trading_day(runner: MA5MA20OpenMonitorRunner, d: dt.date) -> bool:
    """判断是否交易日：优先用 baostock 交易日历，失败则回退工作日。"""
    try:
        # 覆盖一小段范围，便于缓存复用
        start = d - timedelta(days=30)
        end = d + timedelta(days=30)
        ok = runner._load_trading_calendar(start, end)  # 复用 runner 内置缓存（交易日已过滤）
        if ok:
            return d.isoformat() in runner._calendar_cache
    except Exception:
        pass
    return d.weekday() < 5


def _next_trading_day(d: dt.date, runner: MA5MA20OpenMonitorRunner) -> dt.date:
    """返回 >=d 的下一个交易日。"""
    cur = d
    for _ in range(370):  # 最多兜底一年，避免死循环
        if _is_trading_day(runner, cur):
            return cur
        cur = cur + timedelta(days=1)
    return d


def _next_trading_start(ts: dt.datetime, runner: MA5MA20OpenMonitorRunner) -> dt.datetime:
    today = ts.date()
    t = ts.time()

    today = _next_trading_day(today, runner)
    for start, end in TRADING_WINDOWS:
        if t < start:
            return dt.datetime.combine(today, start)
        if start <= t <= end:
            return ts
    return dt.datetime.combine(today + dt.timedelta(days=1), TRADING_WINDOWS[0][0])


def main() -> None:
    parser = argparse.ArgumentParser(description="每整 N 分钟执行一次 run_open_monitor")
    parser.add_argument(
        "--interval",
        type=int,
        default=_default_interval_from_config(),
        help="调度间隔（分钟），默认读取 config.yaml 的 open_monitor.interval_minutes",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="只执行一次就退出（仍会按整点边界对齐）",
    )
    parser.add_argument(
        "--include-current-week",
        action="store_true",
        dest="include_current_week",
        help="周线 asof_date 是否包含当前形成周（默认使用最近已收盘周）",
    )
    args = parser.parse_args()

    runner = MA5MA20OpenMonitorRunner()
    logger = runner.logger
    interval_min = int(args.interval)
    if interval_min <= 0:
        raise ValueError("interval must be positive")

    snapshot_bucket = "PREOPEN"
    ensured_monitor_date: str | None = None

    def _ensure_env_snapshot(now: dt.datetime) -> str:
        nonlocal ensured_monitor_date

        trade_day = _next_trading_day(now.date(), runner)
        monitor_date = trade_day.isoformat()
        if ensured_monitor_date == monitor_date:
            return monitor_date

        env_context = runner.load_env_snapshot_context(monitor_date, None)
        if env_context:
            ensured_monitor_date = monitor_date
            return monitor_date

        try:
            asof_date = resolve_weekly_asof_date(args.include_current_week)
        except ValueError as exc:
            logger.warning("解析周线 asof_date 失败，跳过环境快照：%s", exc)
            return monitor_date

        checked_at = dt.datetime.now()
        runner.build_and_persist_env_snapshot(
            asof_date,
            monitor_date=monitor_date,
            dedupe_bucket=snapshot_bucket,
            checked_at=checked_at,
        )
        ensured_monitor_date = monitor_date
        return monitor_date

    _ensure_env_snapshot(dt.datetime.now())

    logger.info("开盘监测调度器启动：interval=%s 分钟（整点对齐）", interval_min)

    try:
        while True:
            now = dt.datetime.now()
            monitor_date = _ensure_env_snapshot(now)
            run_at = _next_run_at(now, interval_min)
            if not _in_trading_window(run_at):
                next_start = _next_trading_start(run_at, runner)
                if next_start > now:
                    logger.info(
                        "当前不在交易时段，下一交易窗口：%s", next_start.strftime("%Y-%m-%d %H:%M:%S")
                    )
                run_at = _next_run_at(next_start, interval_min)
            while not _in_trading_window(run_at):
                next_start = _next_trading_start(run_at + dt.timedelta(minutes=interval_min))
                run_at = _next_run_at(next_start, interval_min)
            sleep_s = (run_at - now).total_seconds()
            if sleep_s > 0:
                logger.info("下一次触发：%s（%.1fs 后）", run_at.strftime("%Y-%m-%d %H:%M:%S"), sleep_s)
                time.sleep(sleep_s)

            # 到点执行
            logger.info("触发开盘监测：%s", dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            try:
                runner.run(force=True)
            except Exception as exc:  # noqa: BLE001
                logger.exception("开盘监测执行异常（将等待下一轮）：%s", exc)

            if args.once:
                logger.info("--once 已完成，退出调度器。")
                return

            # 防止“刚好运行很快又落在同一秒边界”导致重复触发
            time.sleep(0.2)

    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，调度器退出。")


if __name__ == "__main__":
    main()
