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
  - 如果缺少当次 (monitor_date, run_id) 环境快照，会在触发前自动 build_and_persist_env_snapshot 补齐
"""

from __future__ import annotations

import argparse
import datetime as dt
from datetime import timedelta
import time

import pandas as pd
from sqlalchemy import text

from ashare.config import get_section
from ashare.open_monitor import MA5MA20OpenMonitorRunner
from ashare.env_snapshot_utils import load_trading_calendar
from ashare.schema_manager import ensure_schema


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


def _env_snapshot_exists(
    runner: MA5MA20OpenMonitorRunner, *, monitor_date: str, run_id: str
) -> bool:
    table = str(getattr(runner.params, "env_snapshot_table", "") or "").strip()
    if not table:
        return False
    if not runner._table_exists(table):  # noqa: SLF001
        return False
    stmt = text(
        f"""
        SELECT 1 FROM `{table}`
        WHERE `run_id` = :b AND `monitor_date` = :d
        LIMIT 1
        """
    )
    try:
        with runner.db_writer.engine.begin() as conn:
            row = conn.execute(stmt, {"b": run_id, "d": monitor_date}).fetchone()
            return row is not None
    except Exception:  # noqa: BLE001
        return False


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

    ensure_schema()
    runner = MA5MA20OpenMonitorRunner()
    logger = runner.logger
    interval_min = int(args.interval)
    if interval_min <= 0:
        raise ValueError("interval must be positive")

    ensured_key: tuple[str, str] | None = None
    ensured_ready: bool = False

    def _load_latest_trade_date() -> str | None:
        """尽量解析最新交易日，用于自动补齐环境快照。

        约束：
        - 不再回退读取 open_monitor.indicator_table（该字段仅为旧配置兼容，不应再作为调度入口依赖）；
        - 优先 daily_table.date；若无日线表，则回退 ready_signals_view.sig_date。
        """
        base_table = runner._daily_table()  # noqa: SLF001
        date_col = "date"
        if not runner._table_exists(base_table):  # noqa: SLF001
            view = str(getattr(runner.params, "ready_signals_view", "") or "").strip()
            if not view or (not runner._table_exists(view)):  # noqa: SLF001
                return None
            base_table = view
            date_col = "sig_date"
        try:
            with runner.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    f"SELECT MAX(`{date_col}`) AS max_date FROM `{base_table}`",
                    conn,
                )
        except Exception:  # noqa: BLE001
            return None
        if df.empty:
            return None
        val = df.iloc[0].get("max_date")
        if pd.isna(val) or not str(val).strip():
            return None
        return str(val)[:10]

    def _ensure_env_snapshot(trigger_at: dt.datetime) -> tuple[str, str]:
        nonlocal ensured_key, ensured_ready

        trade_day = _next_trading_day(trigger_at.date(), runner)
        monitor_date = trade_day.isoformat()

        # open_monitor 严格按 run_id 匹配，因此这里必须用当前触发点计算 run_id
        run_id = runner._calc_run_id(trigger_at)  # noqa: SLF001
        key = (monitor_date, run_id)

        if ensured_key == key and ensured_ready:
            return monitor_date, run_id

        if _env_snapshot_exists(runner, monitor_date=monitor_date, run_id=run_id):
            ensured_key = key
            ensured_ready = True
            return monitor_date, run_id

        latest_trade_date = _load_latest_trade_date()
        if not latest_trade_date:
            logger.warning(
                "无法解析 latest_trade_date，跳过自动补齐环境快照（monitor_date=%s, run_id=%s）。",
                monitor_date,
                run_id,
            )
            ensured_key = key
            ensured_ready = False
            return monitor_date, run_id

        # 兜底：避免触发点跨 run_id 桶（±60s）
        candidate_times = [
            trigger_at - dt.timedelta(seconds=60),
            trigger_at,
            trigger_at + dt.timedelta(seconds=60),
        ]
        for ts in candidate_times:
            rid = runner._calc_run_id(ts)  # noqa: SLF001
            if not rid:
                continue
            if _env_snapshot_exists(runner, monitor_date=monitor_date, run_id=rid):
                continue
            try:
                runner.build_and_persist_env_snapshot(
                    latest_trade_date,
                    monitor_date=monitor_date,
                    run_id=rid,
                    checked_at=ts,
                )
                logger.info(
                    "已自动补齐环境快照（monitor_date=%s, run_id=%s, latest_trade_date=%s）。",
                    monitor_date,
                    rid,
                    latest_trade_date,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "自动补齐环境快照失败（monitor_date=%s, run_id=%s）：%s",
                    monitor_date,
                    rid,
                    exc,
                )

        ensured_key = key
        ensured_ready = _env_snapshot_exists(runner, monitor_date=monitor_date, run_id=run_id)
        if not ensured_ready:
            logger.warning(
                "环境快照仍不存在（monitor_date=%s, run_id=%s），open_monitor 可能会终止；建议检查 env_snapshot 写入或相关表/配置。",
                monitor_date,
                run_id,
            )
        return monitor_date, run_id

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

            trigger_at = dt.datetime.now()
            monitor_date, run_id = _ensure_env_snapshot(trigger_at)

            logger.info(
                "触发开盘监测：%s（monitor_date=%s, run_id=%s）",
                trigger_at.strftime("%Y-%m-%d %H:%M:%S"),
                monitor_date,
                run_id,
            )
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
