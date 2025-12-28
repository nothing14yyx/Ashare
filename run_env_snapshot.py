"""构建并落地开盘监测环境快照。"""

from __future__ import annotations

import datetime as dt

from ashare.open_monitor import MA5MA20OpenMonitorRunner
from ashare.open_monitor_consts import ENV_RUN_PREOPEN
from ashare.schema_manager import ensure_schema


def main() -> int | None:
    ensure_schema()
    runner = MA5MA20OpenMonitorRunner()

    checked_at = dt.datetime.now()
    monitor_date = runner.repo.resolve_monitor_trade_date(checked_at)
    run_id = ENV_RUN_PREOPEN
    view = str(getattr(runner.params, "ready_signals_view", "") or "").strip() or None
    latest_trade_date = runner.repo._resolve_latest_trade_date(  # noqa: SLF001
        ready_view=view
    ) or monitor_date
    run_pk = runner.repo.ensure_run_context(
        monitor_date,
        run_id,
        checked_at=checked_at,
        triggered_at=checked_at,
        params_json=runner._build_run_params_json(),  # noqa: SLF001
    )
    if run_pk is None:
        print("run_pk 获取失败，已跳过环境快照写入。")
        return None

    runner.build_and_persist_env_snapshot(
        latest_trade_date=latest_trade_date,
        monitor_date=monitor_date,
        run_id=run_id,
        run_pk=run_pk,
        checked_at=checked_at,
        allow_auto_compute=False,
        fetch_index_live_quote=None,
    )
    return run_pk


if __name__ == "__main__":
    main()
