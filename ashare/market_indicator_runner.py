from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Optional

import pandas as pd
from sqlalchemy import bindparam, text

from .market_indicator_builder import MarketIndicatorBuilder
from .open_monitor_repo import OpenMonitorRepository


class MarketIndicatorRunner:
    """日线/周线指标回填入口。"""

    def __init__(
        self,
        *,
        repo: OpenMonitorRepository,
        builder: MarketIndicatorBuilder,
        logger: logging.Logger,
    ) -> None:
        self.repo = repo
        self.builder = builder
        self.logger = logger

    def run_weekly_indicator(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        mode: str = "incremental",
    ) -> dict[str, Any]:
        index_code = str(self.repo.params.index_code or "").strip()
        end_dt = self._resolve_end_date(end_date, index_code)
        if end_dt is None:
            raise ValueError("无法解析 weekly 指标的 end_date。")

        start_dt = self._resolve_start_date(
            start_date,
            end_dt,
            mode=mode,
            latest_date=self.repo.get_latest_weekly_indicator_date(index_code),
        )
        weekly_dates = self.builder.resolve_weekly_asof_dates(start_dt, end_dt)
        if not weekly_dates:
            return {
                "written": 0,
                "start_date": start_dt.isoformat(),
                "end_date": end_dt.isoformat(),
            }

        written = 0
        for asof_date in weekly_dates:
            rows = self.builder.compute_weekly_indicator(
                asof_date.isoformat(), checked_at=dt.datetime.now()
            )
            written += self.repo.upsert_weekly_indicator(rows)
        return {
            "written": written,
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
        }

    def run_daily_indicator(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        mode: str = "incremental",
    ) -> dict[str, Any]:
        index_code = str(self.repo.params.index_code or "").strip()
        end_dt = self._resolve_end_date(end_date, index_code)
        if end_dt is None:
            raise ValueError("无法解析 daily 指标的 end_date。")

        start_dt = self._resolve_start_date(
            start_date,
            end_dt,
            mode=mode,
            latest_date=self.repo.get_latest_daily_indicator_date(index_code),
        )
        rows = self.builder.compute_daily_indicators(start_dt, end_dt)
        rows = self._attach_cycle_phase_from_weekly(rows)
        written = self.repo.upsert_daily_indicator(rows)
        return {
            "written": written,
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
        }

    def _resolve_end_date(
        self, end_date: Optional[str], index_code: str
    ) -> dt.date | None:
        if end_date:
            try:
                return dt.date.fromisoformat(str(end_date))
            except Exception:
                return None
        stmt = text(
            """
            SELECT MAX(`date`) AS latest_date
            FROM history_index_daily_kline
            WHERE `code` = :code
            """
        )
        try:
            with self.repo.engine.begin() as conn:
                row = conn.execute(stmt, {"code": index_code}).mappings().first()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取最新交易日失败：%s", exc)
            return None
        if not row:
            return None
        latest = row.get("latest_date")
        if isinstance(latest, dt.datetime):
            return latest.date()
        if isinstance(latest, dt.date):
            return latest
        if latest:
            try:
                return pd.to_datetime(latest).date()
            except Exception:
                return None
        return None

    def _resolve_start_date(
        self,
        start_date: Optional[str],
        end_dt: dt.date,
        *,
        mode: str,
        latest_date: Optional[dt.date],
    ) -> dt.date:
        if start_date:
            return dt.date.fromisoformat(str(start_date))
        mode_norm = str(mode or "").strip().lower() or "incremental"
        if mode_norm == "incremental" and latest_date:
            return latest_date + dt.timedelta(days=1)
        default_days = 365
        return end_dt - dt.timedelta(days=default_days)

    def _attach_cycle_phase_from_weekly(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not rows:
            return rows
        table = self.repo.params.weekly_indicator_table
        if not table:
            return rows

        df_daily = pd.DataFrame(rows)
        if df_daily.empty:
            return rows
        if (
            "asof_trade_date" not in df_daily.columns
            or "index_code" not in df_daily.columns
        ):
            return rows

        df_daily = df_daily.copy()
        df_daily["__order"] = range(len(df_daily))
        df_daily["__asof_trade_date"] = pd.to_datetime(
            df_daily["asof_trade_date"], errors="coerce"
        )
        parsed_dates = df_daily["__asof_trade_date"].dropna()
        if parsed_dates.empty:
            df_daily = df_daily.sort_values("__order")
            df_daily = df_daily.drop(columns=["__order", "__asof_trade_date"])
            return df_daily.to_dict(orient="records")

        min_date = parsed_dates.min()
        max_date = parsed_dates.max()
        min_date_floor = (min_date - pd.Timedelta(days=14)).date()
        max_date_val = max_date.date()

        codes = (
            df_daily["index_code"]
            .dropna()
            .astype(str)
            .map(str.strip)
            .loc[lambda s: s != ""]
            .unique()
            .tolist()
        )
        if not codes:
            df_daily = df_daily.sort_values("__order")
            df_daily = df_daily.drop(columns=["__order", "__asof_trade_date"])
            return df_daily.to_dict(orient="records")

        stmt = (
            text(
                f"""
                SELECT weekly_asof_trade_date, index_code, weekly_scene_code
                FROM `{table}`
                WHERE index_code IN :codes
                  AND weekly_asof_trade_date <= :max_date
                  AND weekly_asof_trade_date >= :min_date
                """
            )
            .bindparams(bindparam("codes", expanding=True))
        )
        try:
            with self.repo.engine.begin() as conn:
                df_weekly = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "codes": codes,
                        "max_date": max_date_val,
                        "min_date": min_date_floor,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取周线场景失败：%s", exc)
            df_daily = df_daily.sort_values("__order")
            df_daily = df_daily.drop(columns=["__order", "__asof_trade_date"])
            return df_daily.to_dict(orient="records")

        if df_weekly.empty:
            df_daily = df_daily.sort_values("__order")
            df_daily = df_daily.drop(columns=["__order", "__asof_trade_date"])
            return df_daily.to_dict(orient="records")

        df_weekly = df_weekly.copy()
        df_weekly["weekly_asof_trade_date"] = pd.to_datetime(
            df_weekly["weekly_asof_trade_date"], errors="coerce"
        )
        df_weekly = df_weekly.dropna(subset=["weekly_asof_trade_date", "index_code"])
        if df_weekly.empty:
            df_daily = df_daily.sort_values("__order")
            df_daily = df_daily.drop(columns=["__order", "__asof_trade_date"])
            return df_daily.to_dict(orient="records")

        df_daily_valid = df_daily.dropna(subset=["__asof_trade_date", "index_code"]).copy()
        if df_daily_valid.empty:
            df_daily = df_daily.sort_values("__order")
            df_daily = df_daily.drop(columns=["__order", "__asof_trade_date"])
            return df_daily.to_dict(orient="records")

        df_daily_valid["index_code"] = df_daily_valid["index_code"].astype(str)
        df_weekly["index_code"] = df_weekly["index_code"].astype(str)

        df_daily_valid = df_daily_valid.sort_values(["index_code", "__asof_trade_date"])
        df_weekly = df_weekly.sort_values(["index_code", "weekly_asof_trade_date"])

        merged = pd.merge_asof(
            df_daily_valid,
            df_weekly,
            left_on="__asof_trade_date",
            right_on="weekly_asof_trade_date",
            by="index_code",
            direction="backward",
        )
        df_daily.loc[df_daily_valid.index, "cycle_phase"] = merged["weekly_scene_code"].values
        df_daily["cycle_phase"] = df_daily["cycle_phase"].where(
            pd.notna(df_daily["cycle_phase"]), None
        )

        df_daily = df_daily.sort_values("__order")
        df_daily = df_daily.drop(columns=["__order", "__asof_trade_date"])
        return df_daily.to_dict(orient="records")
