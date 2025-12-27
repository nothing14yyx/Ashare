"""构建并落地开盘监测环境快照。"""

from __future__ import annotations

import datetime as dt

from ashare.open_monitor import MA5MA20OpenMonitorRunner
from ashare.schema_manager import ensure_schema


def main() -> None:
    ensure_schema()
    runner = MA5MA20OpenMonitorRunner()

    checked_at = dt.datetime.now()
    monitor_date = runner.repo.resolve_monitor_trade_date(checked_at)
    biz_ts = dt.datetime.combine(dt.date.fromisoformat(monitor_date), checked_at.time())
    run_id = runner._calc_run_id(biz_ts)  # noqa: SLF001
    view = str(getattr(runner.params, "ready_signals_view", "") or "").strip() or None
    latest_trade_date = runner.repo._resolve_latest_trade_date(  # noqa: SLF001
        ready_view=view
    ) or monitor_date
    runner.repo.ensure_run_context(
        monitor_date,
        run_id,
        checked_at=checked_at,
        triggered_at=checked_at,
        params_json=runner._build_run_params_json(),  # noqa: SLF001
    )

    runner.build_and_persist_env_snapshot(
        latest_trade_date=latest_trade_date,
        monitor_date=monitor_date,
        run_id=run_id,
        checked_at=checked_at,
    )


if __name__ == "__main__":
    main()
