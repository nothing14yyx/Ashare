"""构建并落地开盘监测环境快照。"""

from __future__ import annotations

import datetime as dt

from ashare.open_monitor import MA5MA20OpenMonitorRunner
from ashare.schema_manager import ensure_schema


def main() -> None:
    ensure_schema()
    runner = MA5MA20OpenMonitorRunner()

    checked_at = dt.datetime.now()
    monitor_date = checked_at.date().isoformat()
    run_id = runner._calc_run_id(checked_at)  # noqa: SLF001
    runner.repo.ensure_run_context(
        monitor_date,
        run_id,
        checked_at=checked_at,
        triggered_at=checked_at,
        params_json=runner._build_run_params_json(),  # noqa: SLF001
    )

    runner.build_and_persist_env_snapshot(
        latest_trade_date=monitor_date,
        monitor_date=monitor_date,
        run_id=run_id,
        checked_at=checked_at,
    )


if __name__ == "__main__":
    main()
