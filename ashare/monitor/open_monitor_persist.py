from __future__ import annotations

from typing import Any, Callable, List

import pandas as pd
from sqlalchemy import bindparam, text


class OpenMonitorPersister:
    """Persist open monitor outputs with dedup and cleanup logic."""

    def __init__(
        self,
        *,
        engine,
        logger,
        params,
        db_writer,
        table_exists: Callable[[str], bool],
        get_table_columns: Callable[[str], List[str]],
        make_snapshot_hash: Callable[[dict[str, Any]], str],
    ) -> None:
        self.engine = engine
        self.logger = logger
        self.params = params
        self.db_writer = db_writer
        self._table_exists = table_exists
        self._get_table_columns = get_table_columns
        self._make_snapshot_hash = make_snapshot_hash

    def _delete_existing_run_rows(
        self, table: str, monitor_date: str, run_pk: int, codes: List[str]
    ) -> int:
        if not (
            table
            and monitor_date
            and run_pk
            and codes
            and self._table_exists(table)
        ):
            return 0

        stmt = text(
            "DELETE FROM `{table}` WHERE `monitor_date` = :d AND `run_pk` = :b AND `code` IN :codes".format(
                table=table
            )
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    stmt, {"d": monitor_date, "b": run_pk, "codes": codes}
                )
                return int(getattr(result, "rowcount", 0) or 0)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Failed to clean old snapshots during overwrite: %s", exc)
            return 0

    def persist_quote_snapshots(self, df: pd.DataFrame) -> None:
        if df.empty or not self.params.write_to_db:
            return
        table = self.params.quote_table
        if not table:
            return

        keep_cols = [
            "monitor_date",
            "run_pk",
            "code",
            "live_trade_date",
            "live_open",
            "live_high",
            "live_low",
            "live_latest",
            "live_volume",
            "live_amount",
        ]
        for col in keep_cols:
            if col not in df.columns:
                df[col] = None
        quotes = df[keep_cols].copy()
        quotes["monitor_date"] = pd.to_datetime(quotes["monitor_date"]).dt.date
        quotes["live_trade_date"] = pd.to_datetime(quotes["live_trade_date"]).dt.date
        quotes["run_pk"] = pd.to_numeric(quotes["run_pk"], errors="coerce")
        quotes["code"] = quotes["code"].astype(str)
        quotes = quotes.dropna(subset=["run_pk"]).copy()
        if quotes.empty:
            return

        if self._table_exists(table):
            quotes["monitor_date_str"] = quotes["monitor_date"].astype(str)
            quotes["code_str"] = quotes["code"].astype(str)
            delete_calls = 0
            deleted_rows = 0
            for (monitor_date_str, run_pk), group in quotes.groupby(
                ["monitor_date_str", "run_pk"]
            ):
                run_pk_codes = group["code_str"].dropna().unique().tolist()
                if not run_pk_codes:
                    continue
                deleted_rows += self._delete_existing_run_rows(
                    table,
                    monitor_date_str,
                    int(run_pk),
                    run_pk_codes,
                )
                delete_calls += 1
            if delete_calls:
                self.logger.debug(
                    "open_monitor quote snapshot delete calls=%s rows=%s",
                    delete_calls,
                    deleted_rows,
                )
            quotes = quotes.drop(columns=["monitor_date_str", "code_str"])

        try:
            self.db_writer.write_dataframe(quotes, table, if_exists="append")
            self.logger.info(
                "open_monitor quote snapshots written to %s rows=%s",
                table,
                len(quotes),
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Failed to write open_monitor quote snapshots: %s", exc)

    def persist_minute_snapshots(self, df: pd.DataFrame) -> None:
        if df.empty or not self.params.write_to_db:
            return
        table = getattr(self.params, "minute_table", None)
        if not table:
            return

        keep_cols = [
            "monitor_date",
            "run_pk",
            "strategy_code",
            "sig_date",
            "code",
            "minute_time",
            "minute_date",
            "price",
            "volume",
            "avg_price",
            "source",
        ]
        for col in keep_cols:
            if col not in df.columns:
                df[col] = None
        minute_df = df[keep_cols].copy()
        minute_df["monitor_date"] = pd.to_datetime(minute_df["monitor_date"]).dt.date
        minute_df["sig_date"] = pd.to_datetime(minute_df["sig_date"], errors="coerce").dt.date
        minute_df["minute_time"] = pd.to_datetime(minute_df["minute_time"], errors="coerce")
        minute_df["minute_date"] = pd.to_datetime(minute_df["minute_date"], errors="coerce").dt.date
        minute_df["run_pk"] = pd.to_numeric(minute_df["run_pk"], errors="coerce")
        minute_df["code"] = minute_df["code"].astype(str)
        minute_df = minute_df.dropna(subset=["run_pk", "code", "minute_time"]).copy()
        if minute_df.empty:
            return

        if self._table_exists(table):
            minute_df["monitor_date_str"] = minute_df["monitor_date"].astype(str)
            minute_df["code_str"] = minute_df["code"].astype(str)
            delete_calls = 0
            deleted_rows = 0
            for (monitor_date_str, run_pk), group in minute_df.groupby(
                ["monitor_date_str", "run_pk"]
            ):
                run_pk_codes = group["code_str"].dropna().unique().tolist()
                if not run_pk_codes:
                    continue
                deleted_rows += self._delete_existing_run_rows(
                    table,
                    monitor_date_str,
                    int(run_pk),
                    run_pk_codes,
                )
                delete_calls += 1
            if delete_calls:
                self.logger.debug(
                    "open_monitor minute snapshot delete calls=%s rows=%s",
                    delete_calls,
                    deleted_rows,
                )
            minute_df = minute_df.drop(columns=["monitor_date_str", "code_str"])

        try:
            self.db_writer.write_dataframe(minute_df, table, if_exists="append")
            self.logger.info(
                "open_monitor minute snapshots written to %s rows=%s",
                table,
                len(minute_df),
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Failed to write open_monitor minute snapshots: %s", exc)

    def persist_results(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        df = df.copy()
        table = self.params.output_table
        if not self.params.write_to_db:
            return

        codes = df["code"].dropna().astype(str).unique().tolist()
        table_exists = self._table_exists(table)
        table_columns = set(self._get_table_columns(table))

        for col in ["signal_strength", "strength_delta"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "run_pk" not in df.columns:
            df["run_pk"] = None
        df["run_pk"] = pd.to_numeric(df["run_pk"], errors="coerce")
        if "strategy_code" not in df.columns:
            df["strategy_code"] = (
                str(getattr(self.params, "strategy_code", "") or "").strip() or None
            )
        else:
            fallback_strategy = (
                str(getattr(self.params, "strategy_code", "") or "").strip() or None
            )
            df["strategy_code"] = df["strategy_code"].fillna(fallback_strategy)
        if "run_pk" in df.columns:
            missing_run_pk = df["run_pk"].isna()
            if missing_run_pk.any():
                self.logger.warning(
                    "Missing run_pk in open_monitor results; skipped rows=%s",
                    int(missing_run_pk.sum()),
                )
                df = df.loc[~missing_run_pk].copy()
                if df.empty:
                    return

        for col in ["monitor_date", "sig_date", "asof_trade_date", "live_trade_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        monitor_date_val = (
            df["monitor_date"].iloc[0] if "monitor_date" in df.columns and not df.empty else None
        )
        monitor_date = monitor_date_val.isoformat() if monitor_date_val is not None else ""

        for col in [
            "risk_tag",
            "risk_note",
            "sig_reason",
            "action_reason",
            "status_reason",
        ]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.slice(0, 250)

        if "summary_line" in df.columns:
            df["summary_line"] = df["summary_line"].fillna("").astype(str).str.slice(0, 512)

        if "rule_hits_json" in df.columns:
            df["rule_hits_json"] = (
                df["rule_hits_json"].fillna("").astype(str).str.slice(0, 4000)
            )

        df["snapshot_hash"] = df.apply(
            lambda row: self._make_snapshot_hash(row.to_dict()), axis=1
        )
        df = df.drop_duplicates(subset=["run_pk", "strategy_code", "sig_date", "code"])

        self.persist_quote_snapshots(df)

        if table_columns:
            keep_cols = [c for c in df.columns if c in table_columns]
            df = df[keep_cols]

        if (not self.params.incremental_write) and monitor_date and codes and table_exists:
            delete_stmt = text(
                "DELETE FROM `{table}` WHERE `monitor_date` = :d AND `code` IN :codes".format(
                    table=table
                )
            ).bindparams(bindparam("codes", expanding=True))
            try:
                with self.engine.begin() as conn:
                    conn.execute(delete_stmt, {"d": monitor_date, "codes": codes})
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Failed to delete prior open_monitor rows: %s", exc)

        try:
            self.db_writer.write_dataframe(df, table, if_exists="append")
            self.logger.info("open_monitor results written to %s rows=%s", table, len(df))
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Failed to write open_monitor results: %s", exc)
