"""输出指数周线通道情景（线性回归通道 + 30/60 周均线）。

用法：
  python run_index_weekly_channel.py [--include-current-week]

说明：
  - 从数据库 history_index_daily_kline 读取配置里的指数日线数据
  - 聚合成周线后，默认只输出最近一个“已收盘周”（避免周内未来日期）；
    若指定 --include-current-week，则会包含当前形成中的周线
  - 会调用开盘监测的环境构建逻辑，计算周线计划并写入环境快照表，供 open_monitor 复用
"""

from __future__ import annotations

import argparse
import datetime as dt
import json

from ashare.env_snapshot_utils import resolve_weekly_asof_date
from ashare.open_monitor import MA5MA20OpenMonitorRunner


def main() -> None:
    parser = argparse.ArgumentParser(description="输出指数周线通道情景")
    parser.add_argument(
        "--include-current-week",
        action="store_true",
        dest="include_current_week",
        help="包含当前形成中的周线（默认只输出最近已收盘周）",
    )
    args = parser.parse_args()

    try:
        asof_date = resolve_weekly_asof_date(args.include_current_week)
    except ValueError as exc:
        print(str(exc))
        return

    runner = MA5MA20OpenMonitorRunner()
    checked_at = dt.datetime.now()
    monitor_date = checked_at.date().isoformat()
    dedupe_bucket = "DAILY"
    env_context = runner.build_and_persist_env_snapshot(
        asof_date,
        monitor_date=monitor_date,
        dedupe_bucket=dedupe_bucket,
        checked_at=checked_at,
    )

    output = {"env_snapshot": env_context or {}, "asof_trade_date": asof_date}
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
