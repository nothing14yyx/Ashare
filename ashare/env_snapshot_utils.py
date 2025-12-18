"""环境快照通用工具。"""

from __future__ import annotations

import datetime as dt
from typing import Iterable

import pandas as pd
from sqlalchemy import bindparam, text

from .baostock_core import BaostockDataFetcher
from .baostock_session import BaostockSession
from .config import get_section
from .db import DatabaseConfig, MySQLWriter


def _parse_date(val: str) -> dt.date | None:
    try:
        return dt.datetime.strptime(val, "%Y-%m-%d").date()
    except Exception:  # noqa: BLE001
        return None


def _load_trading_calendar(start: dt.date, end: dt.date) -> set[str]:
    try:
        client = BaostockDataFetcher(BaostockSession())
        calendar_df = client.get_trade_calendar(start.isoformat(), end.isoformat())
    except Exception:  # noqa: BLE001
        return set()

    if "is_trading_day" in calendar_df.columns:
        calendar_df = calendar_df[calendar_df["is_trading_day"].astype(str) == "1"]

    dates = (
        pd.to_datetime(calendar_df["calendar_date"], errors="coerce").dt.date.dropna().tolist()
    )
    return {d.isoformat() for d in dates}


def _resolve_latest_closed_week_end(latest_trade_date: str) -> tuple[str, bool]:
    trade_date = _parse_date(latest_trade_date)
    if trade_date is None:
        return latest_trade_date, True

    week_start = trade_date - dt.timedelta(days=trade_date.weekday())
    week_end = week_start + dt.timedelta(days=6)
    calendar = _load_trading_calendar(week_start - dt.timedelta(days=60), week_end)

    if calendar:
        last_trade_day = None
        for i in range(7):
            candidate = week_end - dt.timedelta(days=i)
            if candidate.isoformat() in calendar:
                last_trade_day = candidate
                break

        if last_trade_day:
            if trade_date == last_trade_day:
                return trade_date.isoformat(), True

            prev_candidate = week_start - dt.timedelta(days=1)
            for _ in range(30):
                if prev_candidate.isoformat() in calendar:
                    return prev_candidate.isoformat(), False
                prev_candidate -= dt.timedelta(days=1)

    fallback_friday = week_start + dt.timedelta(days=4)
    if trade_date >= fallback_friday:
        return fallback_friday.isoformat(), trade_date == fallback_friday

    prev_friday = fallback_friday - dt.timedelta(days=7)
    return prev_friday.isoformat(), False


def _load_index_codes_from_config() -> list[str]:
    app_cfg = get_section("app") or {}
    codes: Iterable[str] = []
    if isinstance(app_cfg, dict):
        raw = app_cfg.get("index_codes", [])
        if isinstance(raw, (list, tuple)):
            codes = [str(c).strip() for c in raw if str(c).strip()]
    return list(codes)


def resolve_weekly_asof_date(include_current_week: bool) -> str:
    """基于指数日线数据推导周线 asof_date。"""

    codes = _load_index_codes_from_config()
    if not codes:
        raise ValueError("config.yaml 未配置 app.index_codes，无法解析 asof_date。")

    db = MySQLWriter(DatabaseConfig.from_env())
    stmt_latest = (
        text(
            """
            SELECT `code`, MAX(`date`) AS latest_date
            FROM history_index_daily_kline
            WHERE `code` IN :codes
            GROUP BY `code`
            """
        ).bindparams(bindparam("codes", expanding=True))
    )

    with db.engine.begin() as conn:
        latest_date_df = pd.read_sql_query(stmt_latest, conn, params={"codes": codes})

    if latest_date_df.empty:
        raise ValueError("history_index_daily_kline 为空或未找到指定指数。")

    latest_per_code = pd.to_datetime(latest_date_df["latest_date"], errors="coerce").dt.date.dropna()
    if latest_per_code.empty:
        raise ValueError("history_index_daily_kline 为空或未找到指定指数。")

    latest_date_val = min(latest_per_code)
    latest_date_str = pd.to_datetime(latest_date_val).date().isoformat()

    week_end_asof, _ = _resolve_latest_closed_week_end(latest_date_str)
    return latest_date_str if include_current_week else week_end_asof

