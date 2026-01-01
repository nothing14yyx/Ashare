"""策略相关表的读写收口。"""

from __future__ import annotations

import datetime as dt
import logging
from typing import List

import pandas as pd
from sqlalchemy import bindparam, text

from ashare.core.env_snapshot_utils import load_trading_calendar
from ashare.core.schema_manager import (
    STRATEGY_CODE_MA5_MA20_TREND,
    SchemaManager,
    TABLE_STRATEGY_CHIP_FILTER,
)
from ashare.core.db import MySQLWriter


class StrategyStore:
    """策略指标/信号/ready_signals 的持久化入口。"""

    def __init__(self, db_writer: MySQLWriter, params, logger: logging.Logger) -> None:
        self.db_writer = db_writer
        self.params = params
        self.logger = logger

    def table_exists(self, table: str) -> bool:
        if not table:
            return False
        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1"))
            return True
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("检查表 %s 是否存在失败：%s", table, exc)
            return False

    def write_indicator_daily(
        self,
        latest_date: dt.date,
        signals: pd.DataFrame,
        codes: List[str],
        *,
        scope_override: str | None = None,
    ) -> None:
        table = self.params.indicator_table
        scope = (
            (scope_override or getattr(self.params, "signals_write_scope", "latest"))
            or "latest"
        ).strip().lower()
        if scope not in {"latest", "window"}:
            self.logger.warning("signals_write_scope=%s 无效，已回退为 latest。", scope)
            scope = "latest"

        base = signals.copy()
        codes_clean = [str(c) for c in (codes or []) if str(c).strip()]
        if codes_clean:
            base = base[base["code"].astype(str).isin(codes_clean)].copy()
        if base.empty:
            self.logger.warning("指标写入为空（scope=%s），已跳过。", scope)
            return

        if scope == "latest":
            base = base[base["date"].dt.date == latest_date].copy()
        if base.empty:
            self.logger.warning("signals_write_scope=%s 下无任何指标行，已跳过写入。", scope)
            return

        keep_cols = [
            "date",
            "code",
            "close",
            "volume",
            "amount",
            "avg_volume_20",
            "ma5",
            "ma10",
            "ma20",
            "ma60",
            "ma250",
            "vol_ratio",
            "macd_dif",
            "macd_dea",
            "macd_hist",
            "prev_macd_hist",
            "kdj_k",
            "kdj_d",
            "kdj_j",
            "atr14",
            "rsi14",
            "ret_10",
            "ret_20",
            "limit_up_cnt_20",
            "ma20_bias",
            "yearline_state",
        ]
        indicator_df = base[keep_cols].copy()
        indicator_df = indicator_df.rename(columns={"date": "trade_date"})
        indicator_df["trade_date"] = pd.to_datetime(indicator_df["trade_date"]).dt.date
        indicator_df["code"] = indicator_df["code"].astype(str)
        indicator_df = indicator_df.drop_duplicates(
            subset=["trade_date", "code"], keep="last"
        ).copy()

        if scope == "latest":
            delete_stmt = (
                text(f"DELETE FROM `{table}` WHERE `trade_date` = :d AND `code` IN :codes")
                .bindparams(bindparam("codes", expanding=True))
            )
            del_codes = indicator_df["code"].tolist()
            with self.db_writer.engine.begin() as conn:
                conn.execute(delete_stmt, {"d": latest_date, "codes": del_codes})
        else:
            start_d = min(indicator_df["trade_date"])
            end_d = max(indicator_df["trade_date"])
            delete_by_date_only = (not codes_clean) or (len(codes_clean) > 2000)
            with self.db_writer.engine.begin() as conn:
                if delete_by_date_only:
                    delete_stmt = text(
                        f"DELETE FROM `{table}` WHERE `trade_date` BETWEEN :start_date AND :end_date"
                    )
                    conn.execute(delete_stmt, {"start_date": start_d, "end_date": end_d})
                else:
                    delete_stmt = (
                        text(
                            f"""
                            DELETE FROM `{table}`
                            WHERE `trade_date` BETWEEN :start_date AND :end_date
                              AND `code` IN :codes
                            """
                        )
                        .bindparams(bindparam("codes", expanding=True))
                    )
                    chunk_size = 800
                    for i in range(0, len(codes_clean), chunk_size):
                        part_codes = codes_clean[i : i + chunk_size]
                        conn.execute(
                            delete_stmt,
                            {
                                "start_date": start_d,
                                "end_date": end_d,
                                "codes": part_codes,
                            },
                        )

            self.logger.info(
                "signals_write_scope=window：指标已覆盖写入 %s~%s（%s）。",
                start_d,
                end_d,
                "all-codes" if delete_by_date_only else f"{len(codes_clean)} codes",
            )

        self.db_writer.write_dataframe(indicator_df, table, if_exists="append")

    def write_signal_events(
        self,
        latest_date: dt.date,
        signals: pd.DataFrame,
        codes: List[str],
        *,
        scope_override: str | None = None,
    ) -> None:
        table = self.params.signal_events_table
        scope = (
            (scope_override or getattr(self.params, "signals_write_scope", "latest"))
            or "latest"
        ).strip().lower()
        if scope not in {"latest", "window"}:
            self.logger.warning("signals_write_scope=%s 无效，已回退为 latest。", scope)
            scope = "latest"

        base = signals.copy()
        codes_clean = [str(c) for c in (codes or []) if str(c).strip()]
        if codes_clean:
            base = base[base["code"].astype(str).isin(codes_clean)].copy()
        if base.empty:
            self.logger.warning("信号事件写入为空（scope=%s），已跳过。", scope)
            return

        if scope == "latest":
            base = base[base["date"].dt.date == latest_date].copy()
        if base.empty:
            self.logger.warning("signals_write_scope=%s 下无任何信号事件，已跳过写入。", scope)
            return

        keep_cols = [
            "date",
            "code",
            "signal",
            "final_action",
            "final_reason",
            "final_cap",
            "reason",
            "risk_tag",
            "risk_note",
            "stop_ref",
            "macd_event",
            "chip_score",
            "gdhs_delta_pct",
            "gdhs_announce_date",
            "chip_reason",
            "chip_penalty",
            "chip_note",
            "age_days",
            "deadzone_hit",
            "stale_hit",
            "fear_score",
            "wave_type",
            "extra_json",
        ]
        events_df = base[keep_cols].copy()
        events_df = events_df.rename(columns={"date": "sig_date"})
        events_df["sig_date"] = pd.to_datetime(events_df["sig_date"]).dt.date
        events_df["code"] = events_df["code"].astype(str)
        events_df["strategy_code"] = STRATEGY_CODE_MA5_MA20_TREND
        valid_days = None
        try:
            valid_days = int(getattr(self.params, "valid_days", 3))
            events_df["valid_days"] = valid_days
        except Exception:
            self.logger.warning(
                "valid_days=%s 解析失败，将跳过有效期字段写入。",
                getattr(self.params, "valid_days", None),
            )
            events_df["valid_days"] = pd.NA
            valid_days = None

        expires_cache: dict[dt.date, dt.date | None] = {}

        def _resolve_expires_on(sig_date: dt.date) -> dt.date | None:
            if sig_date in expires_cache:
                return expires_cache[sig_date]
            if valid_days is None:
                expires_cache[sig_date] = None
                return None
            if valid_days <= 0:
                expires_cache[sig_date] = sig_date
                return sig_date
            end_date = sig_date + dt.timedelta(days=max(valid_days * 3, 10))
            calendar = load_trading_calendar(sig_date, end_date)
            if calendar:
                trading_dates = []
                for raw in calendar:
                    if not isinstance(raw, str):
                        continue
                    try:
                        trade_date = dt.datetime.strptime(raw, "%Y-%m-%d").date()
                    except Exception:
                        continue
                    if sig_date <= trade_date <= end_date:
                        trading_dates.append(trade_date)
                trading_dates.sort()
                if sig_date not in trading_dates:
                    trading_dates.insert(0, sig_date)
                if len(trading_dates) > valid_days:
                    expires_cache[sig_date] = trading_dates[valid_days]
                    return trading_dates[valid_days]
            expires_cache[sig_date] = sig_date + dt.timedelta(days=valid_days)
            return expires_cache[sig_date]

        events_df["expires_on"] = events_df["sig_date"].apply(
            lambda d: _resolve_expires_on(d) if pd.notna(d) else pd.NA
        )

        events_df = events_df.drop_duplicates(
            subset=["strategy_code", "sig_date", "code"], keep="last"
        ).copy()
        for col in ["risk_tag", "risk_note", "reason", "chip_reason"]:
            if col in events_df.columns:
                events_df[col] = (
                    events_df[col].fillna("").astype(str).str.slice(0, 250)
                )
        for col in ["final_reason", "macd_event", "wave_type"]:
            if col in events_df.columns:
                events_df[col] = (
                    events_df[col].fillna("").astype(str).str.slice(0, 255)
                )
        if "chip_note" in events_df.columns:
            events_df["chip_note"] = (
                events_df["chip_note"]
                .fillna("")
                .astype(str)
                .str.slice(0, 255)
                .replace("", pd.NA)
            )
        events_df["gdhs_announce_date"] = pd.to_datetime(
            events_df.get("gdhs_announce_date"), errors="coerce"
        ).dt.date
        events_df["extra_json"] = None
        if "extra_json" in base.columns:
            events_df["extra_json"] = base["extra_json"].fillna("").astype(str)
        if "chip_score" in events_df.columns:
            events_df["chip_score"] = pd.to_numeric(
                events_df["chip_score"], errors="coerce"
            )
        if "chip_penalty" in events_df.columns:
            events_df["chip_penalty"] = pd.to_numeric(
                events_df["chip_penalty"], errors="coerce"
            )
        if "age_days" in events_df.columns:
            events_df["age_days"] = (
                pd.to_numeric(events_df["age_days"], errors="coerce").astype("Int64")
            )
        for flag_col in ["deadzone_hit", "stale_hit"]:
            if flag_col in events_df.columns:
                events_df[flag_col] = events_df[flag_col].astype(bool)

        if scope == "latest":
            delete_stmt = (
                text(
                    f"""
                    DELETE FROM `{table}`
                    WHERE `sig_date` = :d AND `code` IN :codes AND `strategy_code` = :strategy
                    """
                ).bindparams(bindparam("codes", expanding=True))
            )
            del_codes = events_df["code"].tolist()
            with self.db_writer.engine.begin() as conn:
                conn.execute(
                    delete_stmt,
                    {
                        "d": latest_date,
                        "codes": del_codes,
                        "strategy": STRATEGY_CODE_MA5_MA20_TREND,
                    },
                )
        else:
            start_d = min(events_df["sig_date"])
            end_d = max(events_df["sig_date"])
            delete_by_date_only = (not codes_clean) or (len(codes_clean) > 2000)
            with self.db_writer.engine.begin() as conn:
                if delete_by_date_only:
                    delete_stmt = text(
                        f"""
                        DELETE FROM `{table}`
                        WHERE `sig_date` BETWEEN :start_date AND :end_date
                          AND `strategy_code` = :strategy
                        """
                    )
                    conn.execute(
                        delete_stmt,
                        {
                            "start_date": start_d,
                            "end_date": end_d,
                            "strategy": STRATEGY_CODE_MA5_MA20_TREND,
                        },
                    )
                else:
                    delete_stmt = (
                        text(
                            f"""
                            DELETE FROM `{table}`
                            WHERE `sig_date` BETWEEN :start_date AND :end_date
                              AND `strategy_code` = :strategy
                              AND `code` IN :codes
                            """
                        ).bindparams(bindparam("codes", expanding=True))
                    )
                    chunk_size = 800
                    for i in range(0, len(codes_clean), chunk_size):
                        part_codes = codes_clean[i : i + chunk_size]
                        conn.execute(
                            delete_stmt,
                            {
                                "start_date": start_d,
                                "end_date": end_d,
                                "codes": part_codes,
                                "strategy": STRATEGY_CODE_MA5_MA20_TREND,
                            },
                        )
            self.logger.info(
                "signals_write_scope=window：事件已覆盖写入 %s~%s（%s）。",
                start_d,
                end_d,
                "all-codes" if delete_by_date_only else f"{len(codes_clean)} codes",
            )

        self.db_writer.write_dataframe(events_df, table, if_exists="append")

    def refresh_ready_signals_table(self, sig_dates: List[dt.date]) -> None:
        if not sig_dates:
            return

        schema_manager = SchemaManager(self.db_writer.engine)
        table_names = schema_manager.get_table_names()
        ready_table = table_names.ready_signals_view
        if not ready_table or not self.table_exists(ready_table):
            self.logger.warning("ready_signals 表 %s 不存在，跳过刷新。", ready_table)
            return

        select_sql, column_order = schema_manager.build_ready_signals_select(
            self.params.signal_events_table,
            self.params.indicator_table,
            TABLE_STRATEGY_CHIP_FILTER,
        )
        if not select_sql or not column_order:
            self.logger.warning("ready_signals 物化 SQL 未生成，跳过刷新。")
            return

        sig_dates_clean = sorted({d for d in sig_dates if isinstance(d, dt.date)})
        if not sig_dates_clean:
            return

        filtered_select = f"""{select_sql}
              AND e.`strategy_code` = :strategy_code
              AND e.`sig_date` IN :sig_dates
            """
        columns_clause = ", ".join(f"`{col}`" for col in column_order)
        delete_stmt = (
            text(
                f"""
                DELETE FROM `{ready_table}`
                WHERE `strategy_code` = :strategy_code AND `sig_date` IN :sig_dates
                """
            ).bindparams(bindparam("sig_dates", expanding=True))
        )
        insert_stmt = text(
            f"""
            INSERT INTO `{ready_table}` ({columns_clause})
            {filtered_select}
            """
        ).bindparams(bindparam("sig_dates", expanding=True))

        payload = {
            "strategy_code": STRATEGY_CODE_MA5_MA20_TREND,
            "sig_dates": sig_dates_clean,
        }
        with self.db_writer.engine.begin() as conn:
            conn.execute(delete_stmt, payload)
            conn.execute(insert_stmt, payload)
        self.logger.info(
            "ready_signals 表 %s 已刷新（共 %d 个日期）。",
            ready_table,
            len(sig_dates_clean),
        )
