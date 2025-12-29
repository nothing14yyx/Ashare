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
from run_index_weekly_channel import run_weekly_market_indicator


def _parse_asof_date(raw: str | None) -> str | None:
    if not raw:
        return None
    try:
        return dt.date.fromisoformat(str(raw)).isoformat()
    except Exception:  # noqa: BLE001
        logging.warning(f"[WARN] --asof-date 无法解析：{raw}")
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
) -> None:
    if ready_view and repo._table_exists(ready_view):  # noqa: SLF001
        logging.info(f"[CHECK] ready_signals_view OK: {ready_view}")
    else:
        logging.info("[CHECK] ready_signals_view 缺失或未配置。")

    logging.info(f"[CHECK] 近 N 天 BUY 总数：{total_buy_cnt}")
    logging.info(f"[CHECK] 最新信号日 BUY 数量：{latest_sig_date_cnt}")

    if skipped_weekly:
        logging.info("[CHECK] 周线环境：已跳过。")
        return
    written = weekly_status.get("written", 0)
    if written:
        logging.info(f"[CHECK] 周线指标 OK（写入 {written} 条）。")
    else:
        logging.info("[CHECK] 周线指标缺失，请检查周线指标生成流程。")


def main(
    *,
    skip_fetch: bool = False,
    skip_strategy: bool = False,
    skip_weekly: bool = False,
    asof_date: str | None = None,
) -> None:
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
            logging.warning(f"[WARN] 周线环境生成失败：{exc}")

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

    report = {
        "latest_trade_date": latest_trade_date,
        "latest_sig_date_buy_cnt": latest_sig_date_buy_cnt,
        "buy_cnt_total_recent_n_days": buy_cnt_total_recent_n_days,
        "buy_sig_date_breakdown": breakdown[:7],
        "weekly_env_status": weekly_status if not skipped_weekly else {"skipped": True},
    }

    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = output_dir / f"prep_report_{ts}.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logging.info(f"[REPORT] 准备报告：已写入 {report_path.as_posix()}")

    _self_check(
        repo,
        ready_view=ready_view,
        total_buy_cnt=buy_cnt_total_recent_n_days,
        latest_sig_date_cnt=latest_sig_date_buy_cnt,
        weekly_status=weekly_status,
        skipped_weekly=skipped_weekly,
    )


if __name__ == "__main__":
    main()

# 验收自检（最小）
# 1) python -m py_compile ashare/open_monitor_repo.py run_open_monitor_scheduler.py run_index_weekly_channel.py start.py
# 2) 周末/非交易日：python run_open_monitor_scheduler.py --once
#    - monitor_date 回落到最近交易日，open_monitor 可正常构建环境
# 3) python start.py
#    - 报告中的 BUY 总数与 SQL 查询一致，周线 run_id= WEEKLY_{asof_date} 存在
