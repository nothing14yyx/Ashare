"""项目启动脚本入口, 直接执行即可运行示例."""

import datetime as dt
import json
import logging
from pathlib import Path

from sqlalchemy import text

from ashare.app import AshareApp
from ashare.ma5_ma20_trend_strategy import MA5MA20StrategyRunner
from ashare.open_monitor import MA5MA20OpenMonitorRunner
from ashare.schema_manager import ensure_schema
from ashare.utils.logger import setup_logger
from run_index_weekly_channel import run_weekly_market_indicator
from run_chip_filter import main as run_chip_filter
from run_daily_market_indicator import run_daily_market_indicator


def _parse_asof_date(raw: str | None) -> str | None:
    if not raw:
        return None
    try:
        return dt.date.fromisoformat(str(raw)).isoformat()
    except Exception:  # noqa: BLE001
        logger = logging.getLogger("ashare")
        logger.warning("start --asof-date 无法解析：%s", raw)
        return None


def _resolve_latest_sig_date(
    repo, table: str, strategy_code: str
) -> str | None:
    if not table or not repo._table_exists(table):  # noqa: SLF001
        return None
    if "sig_date" not in repo._get_table_columns(table):  # noqa: SLF001
        return None
    clause = "WHERE `strategy_code` = :strategy_code" if strategy_code else ""
    stmt = text(
        f"SELECT MAX(`sig_date`) AS latest_sig_date FROM `{table}` {clause}"
    )
    with repo.engine.begin() as conn:
        row = conn.execute(stmt, {"strategy_code": strategy_code}).mappings().first()
    if not row:
        return None
    v = row.get("latest_sig_date")
    if isinstance(v, dt.datetime):
        return v.date().isoformat()
    if isinstance(v, dt.date):
        return v.isoformat()
    if v:
        try:
            return dt.datetime.fromisoformat(str(v)).date().isoformat()
        except Exception:  # noqa: BLE001
            return None
    return None


def _load_sig_date_breakdown(
    repo, table: str, strategy_code: str, limit: int = 7
) -> list[dict]:
    if not table or not repo._table_exists(table):  # noqa: SLF001
        return []
    cols = set(repo._get_table_columns(table))  # noqa: SLF001
    if "sig_date" not in cols:
        return []

    clauses = []
    params: dict = {"limit": limit}
    if strategy_code and "strategy_code" in cols:
        clauses.append("`strategy_code` = :strategy_code")
        params["strategy_code"] = strategy_code
    if "signal" in cols:
        clauses.append("`signal` = 'BUY'")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    stmt = text(
        f"""
        SELECT `sig_date`, COUNT(*) AS cnt
        FROM `{table}`
        {where}
        GROUP BY `sig_date`
        ORDER BY `sig_date` DESC
        LIMIT :limit
        """
    )

    with repo.engine.begin() as conn:
        rows = conn.execute(stmt, params).fetchall()

    breakdown = []
    for row in rows:
        sig_date = row[0]
        cnt = row[1]
        if sig_date:
            breakdown.append({"sig_date": str(sig_date), "count": int(cnt or 0)})
    return breakdown


def _self_check(
    repo,
    *,
    ready_view: str | None,
    total_buy_cnt: int,
    latest_sig_date_cnt: int,
    weekly_status: dict,
    skipped_weekly: bool,
    daily_indicator_written: int,
    chip_processed: int,
) -> None:
    logger = logging.getLogger("ashare")
    if ready_view and repo._table_exists(ready_view):  # noqa: SLF001
        ready_view_msg = f"ready_signals_view={ready_view}"
    else:
        ready_view_msg = "ready_signals_view=missing"

    weekly_msg = "weekly=skipped" if skipped_weekly else f"weekly_written={weekly_status.get('written', 0)}"
    logger.info(
        "start summary: %s buy_total_recent=%s latest_buy=%s daily_written=%s chip_processed=%s %s",
        ready_view_msg,
        total_buy_cnt,
        latest_sig_date_cnt,
        daily_indicator_written,
        chip_processed,
        weekly_msg,
    )
    if skipped_weekly:
        return
    written = weekly_status.get("written", 0)
    if written:
        logger.debug("start weekly indicator written=%s", written)
    else:
        logger.warning("start weekly indicator missing; check weekly pipeline")


def main(
    *,
    skip_fetch: bool = False,
    skip_strategy: bool = False,
    skip_weekly: bool = False,
    skip_chip: bool = False,
    skip_daily_indicator: bool = False,
    asof_date: str | None = None,
) -> None:
    setup_logger()
    logger = logging.getLogger("ashare")
    ensure_schema()
    if not skip_fetch:
        AshareApp().run()
    strategy_runner = MA5MA20StrategyRunner()
    if not skip_strategy:
        # 策略是否执行由 config.yaml: strategy_ma5_ma20_trend.enabled 控制
        strategy_runner.run()

    monitor_runner = MA5MA20OpenMonitorRunner()
    repo = monitor_runner.repo
    params = monitor_runner.params
    ready_view = str(params.ready_signals_view or "").strip() or None
    signal_table = str(getattr(strategy_runner.params, "signal_events_table", "") or "").strip()

    # 执行日线市场指标计算
    daily_indicator_status: dict = {"written": 0}
    if not skip_daily_indicator:
        try:
            daily_indicator_status = run_daily_market_indicator(
                start_date=asof_date,
                end_date=asof_date,
                mode="incremental",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("start daily indicator failed: %s", exc)

    # 执行筹码筛选计算
    chip_filter_status: dict = {"processed": 0}
    if not skip_chip:
        try:
            chip_filter_status["processed"] = run_chip_filter()
        except Exception as exc:  # noqa: BLE001
            logger.warning("start chip filter failed: %s", exc)

    weekly_status: dict = {"written": 0}
    skipped_weekly = bool(skip_weekly)
    if not skipped_weekly:
        asof_date = _parse_asof_date(asof_date)
        try:
            weekly_status = run_weekly_market_indicator(
                start_date=asof_date,
                end_date=asof_date,
                mode="incremental",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("start weekly indicator failed: %s", exc)

    breakdown = []
    if ready_view and repo._table_exists(ready_view):  # noqa: SLF001
        breakdown = _load_sig_date_breakdown(repo, ready_view, params.strategy_code)
    if not breakdown and signal_table:
        breakdown = _load_sig_date_breakdown(repo, signal_table, params.strategy_code)

    latest_trade_date = repo._resolve_latest_trade_date(ready_view=ready_view)  # noqa: SLF001
    if not latest_trade_date and signal_table:
        latest_trade_date = _resolve_latest_sig_date(repo, signal_table, params.strategy_code)

    latest_sig_date_buy_cnt = breakdown[0]["count"] if breakdown else 0
    buy_cnt_total_recent_n_days = sum(item.get("count", 0) for item in breakdown)

    _self_check(
        repo,
        ready_view=ready_view,
        total_buy_cnt=buy_cnt_total_recent_n_days,
        latest_sig_date_cnt=latest_sig_date_buy_cnt,
        weekly_status=weekly_status,
        skipped_weekly=skipped_weekly,
        daily_indicator_written=int(daily_indicator_status.get("written", 0) or 0),
        chip_processed=int(chip_filter_status.get("processed", 0) or 0),
    )


if __name__ == "__main__":
    main()

# 验收自检（最小）
# 1) python -m py_compile ashare/open_monitor_repo.py run_open_monitor_scheduler.py run_index_weekly_channel.py start.py
# 2) 周末/非交易日：python run_open_monitor_scheduler.py --once
#    - monitor_date 回落到最近交易日，open_monitor 可正常构建环境
# 3) python start.py
#    - 报告中的 BUY 总数与 SQL 查询一致，周线 run_id= WEEKLY_{asof_date} 存在
