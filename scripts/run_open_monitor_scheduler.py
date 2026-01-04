"""定时调度执行开盘监测（每整 N 分钟触发一次，自动跳过非交易日）。"""

from __future__ import annotations

import datetime as dt
from datetime import timedelta
import time

from ashare.core.config import get_section
from ashare.monitor.open_monitor import MA5MA20OpenMonitorRunner
from ashare.core.env_snapshot_utils import load_trading_calendar
from ashare.core.schema_manager import ensure_schema


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
    return base.replace(minute=0) + dt.timedelta(hours=1)


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
        # 覆盖一小段范围，便于缓存复用（模块级缓存）。
        start = d - timedelta(days=30)
        end = d + timedelta(days=30)
        calendar = load_trading_calendar(start, end)
        if calendar:
            return d.isoformat() in calendar
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

    nxt = _next_trading_day(today + dt.timedelta(days=1), runner)
    return dt.datetime.combine(nxt, TRADING_WINDOWS[0][0])


def main(*, interval_minutes: int | None = None, once: bool = False) -> None:
    ensure_schema()
    runner = MA5MA20OpenMonitorRunner()
    logger = runner.logger
    interval_min = int(interval_minutes or _default_interval_from_config())
    if interval_min <= 0:
        raise ValueError("interval must be positive")

    logger.info("开盘监测调度器启动：interval=%s 分钟（整点对齐）", interval_min)

    try:
        while True:
            now = dt.datetime.now()
            run_at = _next_run_at(now, interval_min)

            if not _in_trading_window(run_at):
                next_start = _next_trading_start(run_at, runner)
                if next_start > now:
                    logger.info(
                        "当前不在交易时段，下一交易窗口：%s",
                        next_start.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                run_at = _next_run_at(next_start, interval_min)

            while not _in_trading_window(run_at):
                next_start = _next_trading_start(run_at + dt.timedelta(minutes=interval_min), runner)
                run_at = _next_run_at(next_start, interval_min)

            sleep_s = (run_at - dt.datetime.now()).total_seconds()
            if sleep_s > 0:
                logger.info("下一次触发：%s（%.1fs 后）", run_at.strftime("%Y-%m-%d %H:%M:%S"), sleep_s)
                time.sleep(sleep_s)

            trigger_at = run_at
            logger.info(
                "触发开盘监测：%s",
                trigger_at.strftime("%Y-%m-%d %H:%M:%S"),
            )
            try:
                runner.run(force=True, checked_at=trigger_at)
            except Exception as exc:  # noqa: BLE001
                logger.exception("开盘监测执行异常（将等待下一轮）：%s", exc)

            if once:
                logger.info("调度器已按 once 执行完成，退出。")
                return

            # 防止“刚好运行很快又落在同一秒边界”导致重复触发
            time.sleep(0.2)

    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，调度器退出。")


if __name__ == "__main__":
    main()
