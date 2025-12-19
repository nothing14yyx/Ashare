from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Iterable

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from .config import get_section
from .db import DatabaseConfig, MySQLWriter

TABLE_STRATEGY_SIGNAL_INDICATORS = "strategy_signal_indicators"
TABLE_STRATEGY_MA5_MA20_SIGNALS = "strategy_ma5_ma20_signals"
TABLE_STRATEGY_MA5_MA20_CANDIDATES = "strategy_ma5_ma20_candidates"
VIEW_STRATEGY_MA5_MA20_CANDIDATES = "v_strategy_ma5_ma20_candidates"
TABLE_STRATEGY_OPEN_MONITOR = "strategy_ma5_ma20_open_monitor"
TABLE_STRATEGY_OPEN_MONITOR_ENV = "strategy_ma5_ma20_open_monitor_env"
TABLE_ENV_INDEX_SNAPSHOT = "strategy_env_index_snapshot"


@dataclass(frozen=True)
class TableNames:
    signals_table: str
    candidates_table: str
    candidates_view: str
    candidates_as_view: bool
    open_monitor_table: str
    env_snapshot_table: str
    env_index_snapshot_table: str


class SchemaManager:
    def __init__(self, engine: Engine, *, db_name: str | None = None) -> None:
        self.engine = engine
        self.db_name = db_name or engine.url.database
        self.logger = logging.getLogger(__name__)

    def ensure_all(self) -> None:
        tables = self._resolve_table_names()

        self._ensure_signals_table(tables.signals_table)
        if tables.candidates_as_view:
            self._ensure_candidates_view(tables.candidates_view, tables.signals_table)
        else:
            self._ensure_candidates_table(tables.candidates_table)

        self._ensure_open_monitor_table(tables.open_monitor_table)
        self._ensure_env_snapshot_table(tables.env_snapshot_table)
        self._ensure_env_index_snapshot_table(tables.env_index_snapshot_table)

    def _resolve_table_names(self) -> TableNames:
        strat_cfg = get_section("strategy_ma5_ma20_trend") or {}
        open_monitor_cfg = get_section("open_monitor") or {}

        default_signals = strat_cfg.get("signals_table", TABLE_STRATEGY_MA5_MA20_SIGNALS)
        signals_table = (
            str(open_monitor_cfg.get("signals_table", default_signals)).strip()
            or TABLE_STRATEGY_MA5_MA20_SIGNALS
        )
        candidates_table = (
            str(strat_cfg.get("candidates_table", TABLE_STRATEGY_MA5_MA20_CANDIDATES)).strip()
            or TABLE_STRATEGY_MA5_MA20_CANDIDATES
        )
        candidates_view = (
            str(strat_cfg.get("candidates_view", VIEW_STRATEGY_MA5_MA20_CANDIDATES)).strip()
            or VIEW_STRATEGY_MA5_MA20_CANDIDATES
        )
        candidates_as_view = bool(strat_cfg.get("candidates_as_view", True))

        open_monitor_table = (
            str(open_monitor_cfg.get("output_table", TABLE_STRATEGY_OPEN_MONITOR)).strip()
            or TABLE_STRATEGY_OPEN_MONITOR
        )
        env_snapshot_table = (
            str(open_monitor_cfg.get("env_snapshot_table", TABLE_STRATEGY_OPEN_MONITOR_ENV)).strip()
            or TABLE_STRATEGY_OPEN_MONITOR_ENV
        )
        env_index_snapshot_table = (
            str(
                open_monitor_cfg.get(
                    "env_index_snapshot_table",
                    TABLE_ENV_INDEX_SNAPSHOT,
                )
            ).strip()
            or TABLE_ENV_INDEX_SNAPSHOT
        )

        return TableNames(
            signals_table=signals_table,
            candidates_table=candidates_table,
            candidates_view=candidates_view,
            candidates_as_view=candidates_as_view,
            open_monitor_table=open_monitor_table,
            env_snapshot_table=env_snapshot_table,
            env_index_snapshot_table=env_index_snapshot_table,
        )

    # ---------- generic helpers ----------
    def _table_exists(self, table: str) -> bool:
        with self.engine.connect() as conn:
            inspector = inspect(conn)
            return inspector.has_table(table)

    def _index_exists(self, table: str, index: str) -> bool:
        condition = "table_schema = :schema" if self.db_name else "table_schema = DATABASE()"
        stmt = text(
            f"""
            SELECT COUNT(*) AS cnt
            FROM information_schema.statistics
            WHERE {condition} AND table_name = :table AND index_name = :index
            """
        )
        params: Dict[str, str] = {"table": table, "index": index}
        if self.db_name:
            params["schema"] = str(self.db_name)
        with self.engine.connect() as conn:
            row = conn.execute(stmt, params).mappings().first()
        return bool(row and row.get("cnt"))

    def _column_meta(self, table: str) -> Dict[str, Dict[str, str]]:
        condition = "TABLE_SCHEMA = :schema" if self.db_name else "TABLE_SCHEMA = DATABASE()"
        stmt = text(
            f"""
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM information_schema.COLUMNS
            WHERE {condition} AND TABLE_NAME = :table
            """
        )
        params: Dict[str, str] = {"table": table}
        if self.db_name:
            params["schema"] = str(self.db_name)
        with self.engine.connect() as conn:
            rows = conn.execute(stmt, params).mappings().all()
        meta: Dict[str, Dict[str, str]] = {}
        for row in rows:
            meta[str(row["COLUMN_NAME"])] = {
                "data_type": str(row.get("DATA_TYPE") or "").lower(),
                "column_type": str(row.get("COLUMN_TYPE") or "").lower(),
                "char_len": str(row.get("CHARACTER_MAXIMUM_LENGTH") or ""),
            }
        return meta

    def _add_missing_columns(self, table: str, columns: Dict[str, str]) -> None:
        existing = set(self._column_meta(table).keys())
        to_add = [(col, ddl) for col, ddl in columns.items() if col not in existing]
        if not to_add:
            return
        with self.engine.begin() as conn:
            for col, ddl in to_add:
                conn.execute(text(f"ALTER TABLE `{table}` ADD COLUMN `{col}` {ddl}"))
                self.logger.info("表 %s 已新增列 %s。", table, col)

    def _ensure_varchar_length(self, table: str, column: str, length: int) -> None:
        meta = self._column_meta(table)
        if column not in meta:
            return
        info = meta[column]
        data_type = info["data_type"]
        column_type = info["column_type"]
        char_len_raw = info["char_len"]

        mysql_safe_max = 16383
        safe_len = min(length, mysql_safe_max)
        is_text_like = ("text" in data_type) or ("blob" in data_type) or ("text" in column_type)
        is_varchar_like = data_type in {"varchar", "char"}
        if not (is_text_like or is_varchar_like):
            return

        try:
            current_len = int(char_len_raw or 0)
        except ValueError:
            current_len = 0

        if is_varchar_like and current_len >= safe_len:
            return

        target_len = safe_len if is_text_like else max(current_len, safe_len)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` VARCHAR({target_len})"
                )
            )
        self.logger.info("表 %s.%s 已调整为 VARCHAR(%s)。", table, column, target_len)

    def _ensure_numeric_column(self, table: str, column: str, definition: str) -> None:
        meta = self._column_meta(table)
        info = meta.get(column)
        if not info:
            self._add_missing_columns(table, {column: definition})
            return

        numeric_types = {
            "double",
            "float",
            "decimal",
            "int",
            "bigint",
            "smallint",
            "tinyint",
        }
        if info["data_type"] in numeric_types:
            return

        with self.engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` {definition}"))
        self.logger.info("表 %s.%s 已调整为数值类型 %s。", table, column, definition)

    def _ensure_datetime_column(self, table: str, column: str) -> None:
        meta = self._column_meta(table)
        info = meta.get(column)
        if not info:
            self._add_missing_columns(table, {column: "DATETIME(6) NULL"})
            return
        if info["data_type"] != "datetime" or "datetime(6)" not in info["column_type"]:
            with self.engine.begin() as conn:
                conn.execute(
                    text(f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` DATETIME(6) NULL")
                )
            self.logger.info("表 %s.%s 已调整为 DATETIME(6)。", table, column)

    def _create_table(
        self,
        table: str,
        columns: Dict[str, str],
        *,
        primary_key: Iterable[str] | None = None,
    ) -> None:
        cols_clause = ",\n".join(f"  `{name}` {ddl}" for name, ddl in columns.items())
        pk_clause = ""
        if primary_key:
            pk = ", ".join(f"`{c}`" for c in primary_key)
            pk_clause = f",\n  PRIMARY KEY ({pk})"
        ddl = f"CREATE TABLE IF NOT EXISTS `{table}` (\n{cols_clause}{pk_clause}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        with self.engine.begin() as conn:
            conn.execute(text(ddl))
        self.logger.info("已创建表 %s。", table)

    # ---------- MA5-MA20 strategy ----------
    def _ensure_signals_table(self, table: str) -> None:
        columns = {
            "date": "DATE NOT NULL",
            "code": "VARCHAR(20) NOT NULL",
            "close": "DOUBLE NULL",
            "volume": "DOUBLE NULL",
            "amount": "DOUBLE NULL",
            "ma5": "DOUBLE NULL",
            "ma10": "DOUBLE NULL",
            "ma20": "DOUBLE NULL",
            "ma60": "DOUBLE NULL",
            "ma250": "DOUBLE NULL",
            "vol_ratio": "DOUBLE NULL",
            "macd_dif": "DOUBLE NULL",
            "macd_dea": "DOUBLE NULL",
            "macd_hist": "DOUBLE NULL",
            "kdj_k": "DOUBLE NULL",
            "kdj_d": "DOUBLE NULL",
            "kdj_j": "DOUBLE NULL",
            "atr14": "DOUBLE NULL",
            "stop_ref": "DOUBLE NULL",
            "ret_10": "DOUBLE NULL",
            "ret_20": "DOUBLE NULL",
            "limit_up_cnt_20": "DOUBLE NULL",
            "ma20_bias": "DOUBLE NULL",
            "yearline_state": "VARCHAR(50) NULL",
            "risk_tag": "VARCHAR(255) NULL",
            "risk_note": "VARCHAR(255) NULL",
            "signal": "VARCHAR(10) NULL",
            "reason": "VARCHAR(255) NULL",
        }
        if not self._table_exists(table):
            self._create_table(table, columns, primary_key=("date", "code"))
            return
        self._add_missing_columns(table, columns)

    def _ensure_candidates_table(self, table: str) -> None:
        columns = {
            "date": "DATE NOT NULL",
            "code": "VARCHAR(20) NOT NULL",
            "close": "DOUBLE NULL",
            "ma5": "DOUBLE NULL",
            "ma20": "DOUBLE NULL",
            "ma60": "DOUBLE NULL",
            "ma250": "DOUBLE NULL",
            "vol_ratio": "DOUBLE NULL",
            "macd_hist": "DOUBLE NULL",
            "kdj_k": "DOUBLE NULL",
            "kdj_d": "DOUBLE NULL",
            "atr14": "DOUBLE NULL",
            "stop_ref": "DOUBLE NULL",
            "yearline_state": "VARCHAR(50) NULL",
            "risk_tag": "VARCHAR(255) NULL",
            "risk_note": "VARCHAR(255) NULL",
            "reason": "VARCHAR(255) NULL",
        }
        if not self._table_exists(table):
            self._create_table(table, columns, primary_key=("date", "code"))
            return
        self._add_missing_columns(table, columns)

    def _ensure_candidates_view(self, view: str, signals_table: str) -> None:
        stmt = text(
            f"""
            CREATE OR REPLACE VIEW `{view}` AS
            SELECT
              `date`,`code`,`close`,
              `ma5`,`ma20`,`ma60`,`ma250`,
              `vol_ratio`,`macd_hist`,`kdj_k`,`kdj_d`,`atr14`,`stop_ref`,
              `yearline_state`,`risk_tag`,`risk_note`,
              `reason`
            FROM `{signals_table}`
            WHERE `signal` = 'BUY'
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新候选视图 %s。", view)

    # ---------- Open monitor ----------
    def _ensure_open_monitor_table(self, table: str) -> None:
        columns = {
            "monitor_date": "VARCHAR(10) NULL",
            "sig_date": "VARCHAR(10) NULL",
            "asof_trade_date": "VARCHAR(10) NULL",
            "live_trade_date": "VARCHAR(10) NULL",
            "signal_age": "INT NULL",
            "valid_days": "INT NULL",
            "code": "VARCHAR(20) NULL",
            "name": "VARCHAR(64) NULL",
            "industry": "VARCHAR(64) NULL",
            "board_name": "VARCHAR(64) NULL",
            "board_code": "VARCHAR(32) NULL",
            "board_status": "VARCHAR(32) NULL",
            "board_rank": "INT NULL",
            "board_chg_pct": "DOUBLE NULL",
            "avg_amount_20": "DOUBLE NULL",
            "avg_volume_20": "DOUBLE NULL",
            "live_open": "DOUBLE NULL",
            "live_high": "DOUBLE NULL",
            "live_low": "DOUBLE NULL",
            "live_latest": "DOUBLE NULL",
            "live_volume": "DOUBLE NULL",
            "live_amount": "DOUBLE NULL",
            "live_gap_pct": "DOUBLE NULL",
            "live_pct_change": "DOUBLE NULL",
            "live_intraday_vol_ratio": "DOUBLE NULL",
            "sig_close": "DOUBLE NULL",
            "sig_ma5": "DOUBLE NULL",
            "sig_ma20": "DOUBLE NULL",
            "sig_ma60": "DOUBLE NULL",
            "sig_ma250": "DOUBLE NULL",
            "sig_vol_ratio": "DOUBLE NULL",
            "sig_macd_hist": "DOUBLE NULL",
            "sig_atr14": "DOUBLE NULL",
            "sig_stop_ref": "DOUBLE NULL",
            "effective_stop_ref": "DOUBLE NULL",
            "asof_close": "DOUBLE NULL",
            "asof_ma5": "DOUBLE NULL",
            "asof_ma20": "DOUBLE NULL",
            "asof_ma60": "DOUBLE NULL",
            "asof_ma250": "DOUBLE NULL",
            "asof_vol_ratio": "DOUBLE NULL",
            "asof_macd_hist": "DOUBLE NULL",
            "asof_atr14": "DOUBLE NULL",
            "asof_stop_ref": "DOUBLE NULL",
            "sig_kdj_k": "DOUBLE NULL",
            "sig_kdj_d": "DOUBLE NULL",
            "trade_stop_ref": "DOUBLE NULL",
            "dev_ma5": "DOUBLE NULL",
            "dev_ma20": "DOUBLE NULL",
            "dev_ma5_atr": "DOUBLE NULL",
            "dev_ma20_atr": "DOUBLE NULL",
            "runup_from_sigclose": "DOUBLE NULL",
            "runup_from_sigclose_atr": "DOUBLE NULL",
            "runup_ref_price": "DOUBLE NULL",
            "runup_ref_source": "VARCHAR(32) NULL",
            "entry_exposure_cap": "DOUBLE NULL",
            "env_index_score": "DOUBLE NULL",
            "env_regime": "VARCHAR(32) NULL",
            "env_position_hint": "DOUBLE NULL",
            "env_index_snapshot_hash": "VARCHAR(32) NULL",
            "env_final_gate_action": "VARCHAR(16) NULL",
            "env_weekly_asof_trade_date": "VARCHAR(10) NULL",
            "env_weekly_risk_level": "VARCHAR(16) NULL",
            "env_weekly_scene": "VARCHAR(32) NULL",
            "env_weekly_gate_action": "VARCHAR(16) NULL",
            "signal_strength": "DOUBLE NULL",
            "strength_delta": "DOUBLE NULL",
            "strength_trend": "VARCHAR(16) NULL",
            "strength_note": "VARCHAR(512) NULL",
            "risk_tag": "VARCHAR(255) NULL",
            "risk_note": "VARCHAR(255) NULL",
            "status_tags": "VARCHAR(255) NULL",
            "status_tags_json": "TEXT NULL",
            "summary_line": "VARCHAR(512) NULL",
            "signal_kind": "VARCHAR(16) NULL",
            "sig_signal": "VARCHAR(16) NULL",
            "sig_reason": "VARCHAR(255) NULL",
            "candidate_stage": "VARCHAR(16) NULL",
            "candidate_state": "VARCHAR(32) NULL",
            "candidate_status": "VARCHAR(32) NULL",
            "status_reason": "VARCHAR(255) NULL",
            "action": "VARCHAR(16) NULL",
            "action_reason": "VARCHAR(255) NULL",
            "checked_at": "DATETIME(6) NULL",
            "dedupe_bucket": "VARCHAR(32) NULL",
            "snapshot_hash": "VARCHAR(64) NULL",
        }
        if not self._table_exists(table):
            self._create_table(table, columns)
        else:
            self._add_missing_columns(table, columns)

        for col in ["monitor_date", "sig_date", "code", "snapshot_hash", "dedupe_bucket"]:
            self._ensure_varchar_length(table, col, 64 if col == "snapshot_hash" else 32)

        self._ensure_datetime_column(table, "checked_at")
        self._ensure_numeric_column(table, "live_intraday_vol_ratio", "DOUBLE NULL")
        self._ensure_numeric_column(table, "signal_strength", "DOUBLE NULL")
        self._ensure_numeric_column(table, "strength_delta", "DOUBLE NULL")

        self._ensure_open_monitor_indexes(table)

    def _cleanup_duplicate_snapshots(self, table: str) -> None:
        dedupe_cols = {"monitor_date", "sig_date", "code", "dedupe_bucket", "checked_at"}
        if not dedupe_cols.issubset(set(self._column_meta(table).keys())):
            return
        stmt = text(
            f"""
            DELETE t1
            FROM `{table}` t1
            JOIN `{table}` t2
              ON t1.`monitor_date` = t2.`monitor_date`
             AND t1.`sig_date` = t2.`sig_date`
             AND t1.`code` = t2.`code`
             AND t1.`dedupe_bucket` = t2.`dedupe_bucket`
             AND (
                  (t1.`checked_at` < t2.`checked_at`)
                  OR (t1.`checked_at` IS NULL AND t2.`checked_at` IS NOT NULL)
             )
            """
        )
        with self.engine.begin() as conn:
            res = conn.execute(stmt)
        if getattr(res, "rowcount", 0) and res.rowcount > 0:
            self.logger.info("表 %s 已清理 %s 条重复快照。", table, res.rowcount)

    def _ensure_open_monitor_indexes(self, table: str) -> None:
        unique_index = "ux_open_monitor_dedupe"
        if not self._index_exists(table, unique_index):
            self._cleanup_duplicate_snapshots(table)
            with self.engine.begin() as conn:
                try:
                    conn.execute(
                        text(
                            f"""
                            CREATE UNIQUE INDEX `{unique_index}`
                            ON `{table}` (`monitor_date`, `sig_date`, `code`, `dedupe_bucket`)
                            """
                        )
                    )
                    self.logger.info("表 %s 已创建唯一索引 %s。", table, unique_index)
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("创建唯一索引 %s 失败：%s", unique_index, exc)

        index_name = "idx_open_monitor_strength_time"
        if not self._index_exists(table, index_name):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"CREATE INDEX `{index_name}` ON `{table}` (`monitor_date`, `code`, `checked_at`)"
                    )
                )
            self.logger.info("表 %s 已新增索引 %s。", table, index_name)

        code_time_index = "idx_open_monitor_code_time"
        if not self._index_exists(table, code_time_index):
            with self.engine.begin() as conn:
                conn.execute(
                    text(f"CREATE INDEX `{code_time_index}` ON `{table}` (`code`, `checked_at`)")
                )
            self.logger.info("表 %s 已新增索引 %s。", table, code_time_index)

    # ---------- Environment snapshots ----------
    def _ensure_env_snapshot_table(self, table: str) -> None:
        columns = {
            "monitor_date": "VARCHAR(10) NOT NULL",
            "dedupe_bucket": "VARCHAR(32) NOT NULL",
            "checked_at": "DATETIME(6) NULL",
            "env_weekly_asof_trade_date": "VARCHAR(10) NULL",
            "env_weekly_risk_level": "VARCHAR(16) NULL",
            "env_weekly_scene": "VARCHAR(32) NULL",
            "env_weekly_gate_policy": "VARCHAR(16) NULL",
            "env_weekly_plan_json": "TEXT NULL",
            "env_weekly_plan_a": "VARCHAR(255) NULL",
            "env_weekly_plan_b": "VARCHAR(255) NULL",
            "env_weekly_plan_a_exposure_cap": "DOUBLE NULL",
            "env_weekly_bias": "VARCHAR(16) NULL",
            "env_weekly_status": "VARCHAR(32) NULL",
            "env_weekly_gating_enabled": "TINYINT(1) NULL",
            "env_weekly_tags": "VARCHAR(255) NULL",
            "env_weekly_money_proxy": "VARCHAR(255) NULL",
            "env_weekly_note": "VARCHAR(255) NULL",
            "env_regime": "VARCHAR(32) NULL",
            "env_position_hint": "DOUBLE NULL",
            "env_position_hint_raw": "DOUBLE NULL",
            "env_index_code": "VARCHAR(16) NULL",
            "env_index_asof_trade_date": "VARCHAR(10) NULL",
            "env_index_asof_close": "DOUBLE NULL",
            "env_index_asof_ma20": "DOUBLE NULL",
            "env_index_asof_ma60": "DOUBLE NULL",
            "env_index_asof_macd_hist": "DOUBLE NULL",
            "env_index_asof_atr14": "DOUBLE NULL",
            "env_index_live_trade_date": "VARCHAR(10) NULL",
            "env_index_live_open": "DOUBLE NULL",
            "env_index_live_high": "DOUBLE NULL",
            "env_index_live_low": "DOUBLE NULL",
            "env_index_live_latest": "DOUBLE NULL",
            "env_index_live_pct_change": "DOUBLE NULL",
            "env_index_live_volume": "DOUBLE NULL",
            "env_index_live_amount": "DOUBLE NULL",
            "env_index_dev_ma20_atr": "DOUBLE NULL",
            "env_index_gate_action": "VARCHAR(16) NULL",
            "env_index_gate_reason": "VARCHAR(255) NULL",
            "env_index_position_cap": "DOUBLE NULL",
            "env_final_gate_action": "VARCHAR(16) NULL",
        }
        if not self._table_exists(table):
            self._create_table(table, columns, primary_key=("monitor_date", "dedupe_bucket"))
        else:
            self._add_missing_columns(table, columns)
        self._ensure_datetime_column(table, "checked_at")

    def _ensure_env_index_snapshot_table(self, table: str) -> None:
        columns = {
            "snapshot_hash": "VARCHAR(32) NOT NULL",
            "monitor_date": "VARCHAR(10) NULL",
            "dedupe_bucket": "VARCHAR(16) NULL",
            "checked_at": "DATETIME(6) NULL",
            "index_code": "VARCHAR(16) NULL",
            "asof_trade_date": "DATE NULL",
            "live_trade_date": "DATE NULL",
            "asof_close": "DOUBLE NULL",
            "asof_ma20": "DOUBLE NULL",
            "asof_ma60": "DOUBLE NULL",
            "asof_macd_hist": "DOUBLE NULL",
            "asof_atr14": "DOUBLE NULL",
            "live_open": "DOUBLE NULL",
            "live_high": "DOUBLE NULL",
            "live_low": "DOUBLE NULL",
            "live_latest": "DOUBLE NULL",
            "live_pct_change": "DOUBLE NULL",
            "live_volume": "DOUBLE NULL",
            "live_amount": "DOUBLE NULL",
            "dev_ma20_atr": "DOUBLE NULL",
            "gate_action": "VARCHAR(16) NULL",
            "gate_reason": "VARCHAR(255) NULL",
            "position_cap": "DOUBLE NULL",
        }
        if not self._table_exists(table):
            self._create_table(table, columns, primary_key=("snapshot_hash",))
        else:
            self._add_missing_columns(table, columns)
        self._ensure_datetime_column(table, "checked_at")

        unique_name = "uk_env_index_snapshot"
        if not self._index_exists(table, unique_name):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE UNIQUE INDEX `{unique_name}`
                        ON `{table}` (`monitor_date`, `dedupe_bucket`, `index_code`, `checked_at`)
                        """
                    )
                )
            self.logger.info("指数环境快照表 %s 已添加唯一索引 %s。", table, unique_name)


def ensure_schema() -> None:
    db_config = DatabaseConfig.from_env()
    bootstrap_writer = MySQLWriter(db_config)
    try:
        SchemaManager(bootstrap_writer.engine, db_name=db_config.db_name).ensure_all()
    finally:
        bootstrap_writer.dispose()
