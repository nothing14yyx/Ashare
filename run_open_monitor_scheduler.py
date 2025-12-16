"""定时调度执行开盘监测（每整 N 分钟触发一次）。

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
import time

from ashare.open_monitor import MA5MA20OpenMonitorRunner
from ashare.config import get_section


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
    args = parser.parse_args()

    runner = MA5MA20OpenMonitorRunner()
    logger = runner.logger
    interval_min = int(args.interval)
    if interval_min <= 0:
        raise ValueError("interval must be positive")

    logger.info("开盘监测调度器启动：interval=%s 分钟（整点对齐）", interval_min)

    try:
        while True:
            now = dt.datetime.now()
            run_at = _next_run_at(now, interval_min)
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
