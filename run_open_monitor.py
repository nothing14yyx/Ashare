from __future__ import annotations

import datetime as dt

from sqlalchemy import text

from ashare.open_monitor import MA5MA20OpenMonitorRunner
from ashare.schema_manager import ensure_schema


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


def _ensure_env_snapshot(
    runner: MA5MA20OpenMonitorRunner, *, checked_at: dt.datetime
) -> None:
    monitor_date = checked_at.date().isoformat()

    try:
        latest_trade_date, _, signals = runner._load_recent_buy_signals()  # noqa: SLF001
    except Exception as exc:  # noqa: BLE001
        runner.logger.exception("读取 ready_signals_view 失败，无法补齐环境快照：%s", exc)
        return

    if signals is None or signals.empty:
        runner.logger.info("本次无待监测标的（signals empty），跳过环境快照补齐。")
        return

    if not latest_trade_date:
        runner.logger.warning("latest_trade_date 为空，跳过环境快照补齐。")
        return

    # 兜底：避免触发点刚好跨 run_id 桶（±60s）
    candidate_times = [
        checked_at - dt.timedelta(seconds=60),
        checked_at,
        checked_at + dt.timedelta(seconds=60),
    ]
    for ts in candidate_times:
        run_id = runner._calc_run_id(ts)  # noqa: SLF001
        if not run_id:
            continue
        if _env_snapshot_exists(runner, monitor_date=monitor_date, run_id=run_id):
            continue

        try:
            runner.build_and_persist_env_snapshot(
                latest_trade_date,
                monitor_date=monitor_date,
                run_id=run_id,
                checked_at=ts,
            )
            runner.logger.info(
                "已自动补齐环境快照（monitor_date=%s, run_id=%s, latest_trade_date=%s）。",
                monitor_date,
                run_id,
                latest_trade_date,
            )
        except Exception as exc:  # noqa: BLE001
            runner.logger.exception(
                "自动补齐环境快照失败（monitor_date=%s, run_id=%s）：%s",
                monitor_date,
                run_id,
                exc,
            )


def main() -> None:
    ensure_schema()

    runner = MA5MA20OpenMonitorRunner()

    checked_at = dt.datetime.now()
    _ensure_env_snapshot(runner, checked_at=checked_at)

    runner.run(force=True)


if __name__ == "__main__":
    main()
