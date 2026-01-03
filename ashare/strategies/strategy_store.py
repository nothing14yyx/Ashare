"""策略相关表的读写收口。"""

from __future__ import annotations

import datetime as dt
import logging
import os
import time
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
            "bull_engulf",
            "bear_engulf",
            "engulf_body_atr",
            "engulf_score",
            "engulf_stop_ref",
            "one_word_limit_up",
        ]
        # 容错：只保留实际存在的列
        indicator_df = base[[c for c in keep_cols if c in base.columns]].copy()
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
            chunk_size = 1000
            with self.db_writer.engine.begin() as conn:
                for i in range(0, len(del_codes), chunk_size):
                    part_codes = del_codes[i : i + chunk_size]
                    conn.execute(delete_stmt, {"d": latest_date, "codes": part_codes})
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
        t0 = time.perf_counter()
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
        # 容错处理：只保留实际存在的列
        events_df = base[[c for c in keep_cols if c in base.columns]].copy()
        events_df = events_df.rename(columns={"date": "sig_date"})
        events_df["sig_date"] = pd.to_datetime(events_df["sig_date"]).dt.date
        events_df["code"] = events_df["code"].astype(str)
        events_df["strategy_code"] = STRATEGY_CODE_MA5_MA20_TREND
        self.logger.info(
            "write_signal_events: scope=%s rows=%s codes=%s",
            scope,
            len(events_df),
            len(events_df["code"].unique()) if "code" in events_df.columns else 0,
        )
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

        skip_calendar = os.getenv("ASHARE_SKIP_TRADING_CALENDAR", "").strip().lower() in {"1", "true", "yes", "y", "on"}
        if skip_calendar:
            self.logger.info("write_signal_events: skip trading calendar (ASHARE_SKIP_TRADING_CALENDAR=1)")
            events_df["expires_on"] = events_df["sig_date"].apply(
                lambda d: (d if valid_days is None else d + dt.timedelta(days=valid_days)) if pd.notna(d) else pd.NA
            )
        else:
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
        
        if "gdhs_announce_date" in events_df.columns:
            events_df["gdhs_announce_date"] = pd.to_datetime(
                events_df["gdhs_announce_date"], errors="coerce"
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

        # 兜底补齐：age_days / deadzone_hit / stale_hit
        if "age_days" not in events_df.columns:
            events_df["age_days"] = pd.NA
        if events_df["age_days"].isna().any():
            try:
                sig_ts = pd.to_datetime(events_df["sig_date"], errors="coerce")
                events_df["age_days"] = (pd.Timestamp(latest_date) - sig_ts).dt.days
            except Exception:
                pass

        if "deadzone_hit" not in events_df.columns:
            events_df["deadzone_hit"] = False
        if events_df["deadzone_hit"].isna().any() or "chip_note" in events_df.columns:
            note = events_df.get("chip_note", pd.Series("", index=events_df.index)).fillna("").astype(str)
            reason = events_df.get("chip_reason", pd.Series("", index=events_df.index)).fillna("").astype(str)
            deadzone_mask = note.str.contains("DEADZONE", case=False, regex=False) | reason.str.contains(
                "DEADZONE", case=False, regex=False
            )
            events_df.loc[deadzone_mask, "deadzone_hit"] = True

        if "stale_hit" not in events_df.columns:
            events_df["stale_hit"] = False
        if events_df["stale_hit"].isna().any() and "expires_on" in events_df.columns:
            try:
                stale_mask = pd.to_datetime(events_df["expires_on"], errors="coerce").dt.date < latest_date
                events_df.loc[stale_mask, "stale_hit"] = True
            except Exception:
                pass

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
            chunk_size = 1000
            with self.db_writer.engine.begin() as conn:
                for i in range(0, len(del_codes), chunk_size):
                    part_codes = del_codes[i : i + chunk_size]
                    conn.execute(
                        delete_stmt,
                        {
                            "d": latest_date,
                            "codes": part_codes,
                            "strategy": STRATEGY_CODE_MA5_MA20_TREND,
                        },
                    )
        else:
            start_d = min(events_df["sig_date"])
            end_d = max(events_df["sig_date"])
            delete_by_date_only = (not codes_clean) or (len(codes_clean) > 2000)
            batch_days = max(int(getattr(self.params, "signals_write_batch_days", 0) or 0), 0)
            if batch_days > 0:
                unique_dates = sorted(events_df["sig_date"].dropna().unique().tolist())
                if unique_dates:
                    total_batches = 0
                    for i in range(0, len(unique_dates), batch_days):
                        chunk_dates = unique_dates[i : i + batch_days]
                        if not chunk_dates:
                            continue
                        total_batches += 1
                        chunk_df = events_df[events_df["sig_date"].isin(chunk_dates)].copy()
                        chunk_start = min(chunk_dates)
                        chunk_end = max(chunk_dates)
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
                                        "start_date": chunk_start,
                                        "end_date": chunk_end,
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
                                for j in range(0, len(codes_clean), chunk_size):
                                    part_codes = codes_clean[j : j + chunk_size]
                                    conn.execute(
                                        delete_stmt,
                                        {
                                            "start_date": chunk_start,
                                            "end_date": chunk_end,
                                            "codes": part_codes,
                                            "strategy": STRATEGY_CODE_MA5_MA20_TREND,
                                        },
                                    )
                        self.db_writer.write_dataframe(chunk_df, table, if_exists="append")
                        self.logger.info(
                            "write_signal_events: batch %s~%s rows=%s",
                            chunk_start,
                            chunk_end,
                            len(chunk_df),
                        )
                    self.logger.info(
                        "signals_write_scope=window：事件已分批写入 %s~%s（%s batches, %s）。",
                        start_d,
                        end_d,
                        total_batches,
                        "all-codes" if delete_by_date_only else f"{len(codes_clean)} codes",
                    )
                    self.logger.info("write_signal_events: done in %.2fs", time.perf_counter() - t0)
                    return
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
        self.logger.info("write_signal_events: done in %.2fs", time.perf_counter() - t0)

    def refresh_ready_signals_table(self, sig_dates: List[dt.date]) -> None:
        if not sig_dates:
            return

        schema_manager = SchemaManager(self.db_writer.engine)
        table_names = schema_manager.get_table_names()
        ready_table = table_names.ready_signals_view
        if not ready_table:
            return

        # 重构：检查是否为视图。如果是视图，则无需手动刷新（数据通过 SQL Join 自动生成）
        if schema_manager._relation_type(ready_table) == "VIEW":
            self.logger.debug("ready_signals %s 是动态视图，跳过手动刷新逻辑。", ready_table)
            return

        if not self.table_exists(ready_table):
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
