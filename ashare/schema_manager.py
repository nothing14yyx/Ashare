from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Iterable

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from .config import get_section
from .db import DatabaseConfig, MySQLWriter

STRATEGY_CODE_MA5_MA20_TREND = "MA5_MA20_TREND"

# 维度/事实视图：拆分 a_share_universe
VIEW_DIM_STOCK_BASIC = "dim_stock_basic"
VIEW_FACT_STOCK_DAILY = "fact_stock_daily"
VIEW_DIM_INDEX_MEMBERSHIP_SNAPSHOT = "dim_index_membership_snapshot"
TABLE_A_SHARE_UNIVERSE = "a_share_universe"

# 统一策略信号体系表命名：按单一职责拆分
TABLE_STRATEGY_INDICATOR_DAILY = "strategy_indicator_daily"
TABLE_STRATEGY_SIGNAL_EVENTS = "strategy_signal_events"
# 策略准备就绪信号（含筹码）
VIEW_STRATEGY_READY_SIGNALS = "v_strategy_ready_signals"
TABLE_STRATEGY_CHIP_FILTER = "strategy_chip_filter"
TABLE_STRATEGY_TRADE_METRICS = "strategy_trade_metrics"
VIEW_STRATEGY_BACKTEST = "v_backtest"
VIEW_STRATEGY_PNL = "v_pnl"

# 开盘监测输出
TABLE_STRATEGY_OPEN_MONITOR_EVAL = "strategy_open_monitor_eval"
TABLE_STRATEGY_OPEN_MONITOR_ENV = "strategy_open_monitor_env"
TABLE_STRATEGY_WEEKLY_MARKET_ENV = "strategy_weekly_market_env"
WEEKLY_MARKET_BENCHMARK_CODE = "sh.000001"
TABLE_STRATEGY_DAILY_MARKET_ENV = "strategy_daily_market_env"
TABLE_STRATEGY_OPEN_MONITOR_QUOTE = "strategy_open_monitor_quote"
TABLE_STRATEGY_OPEN_MONITOR_RUN = "strategy_open_monitor_run"
VIEW_STRATEGY_OPEN_MONITOR_WIDE = "v_strategy_open_monitor_wide"
# 开盘监测环境视图（env 快照）
VIEW_STRATEGY_OPEN_MONITOR_ENV = "v_strategy_open_monitor_env"
# 开盘监测默认查询视图（精简字段；完整字段请查 v_strategy_open_monitor_wide）
VIEW_STRATEGY_OPEN_MONITOR = "v_strategy_open_monitor"

@dataclass(frozen=True)
class TableNames:
    indicator_table: str
    signal_events_table: str
    ready_signals_view: str
    open_monitor_eval_table: str
    env_snapshot_table: str
    open_monitor_run_table: str
    open_monitor_env_view: str
    open_monitor_view: str
    open_monitor_wide_view: str
    open_monitor_quote_table: str
    weekly_indicator_table: str
    daily_indicator_table: str


def _to_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return default


class SchemaManager:
    def __init__(self, engine: Engine, *, db_name: str | None = None) -> None:
        self.engine = engine
        self.db_name = db_name or engine.url.database
        self.logger = logging.getLogger(__name__)

    def ensure_all(self) -> None:
        tables = self._resolve_table_names()

        self._ensure_dim_stock_basic_view()
        self._ensure_fact_stock_daily_view()
        self._ensure_index_membership_view()
        self._ensure_universe_table()

        self._ensure_indicator_table(tables.indicator_table)
        self._ensure_signal_events_table(tables.signal_events_table)
        self._ensure_trade_metrics_table()
        self._ensure_backtest_view()
        self._ensure_chip_filter_table()
        self._ensure_ready_signals_view(
            tables.ready_signals_view,
            tables.signal_events_table,
            tables.indicator_table,
            TABLE_STRATEGY_CHIP_FILTER,
        )
        self._ensure_v_pnl_view()

        self._ensure_open_monitor_eval_table(tables.open_monitor_eval_table)
        self._ensure_open_monitor_run_table(tables.open_monitor_run_table)
        self._ensure_open_monitor_quote_table(tables.open_monitor_quote_table)
        self._ensure_weekly_indicator_table(tables.weekly_indicator_table)
        self._ensure_daily_market_env_table(tables.daily_indicator_table)
        self._ensure_env_snapshot_table(tables.env_snapshot_table)
        self._ensure_open_monitor_env_view(
            tables.open_monitor_env_view,
            tables.env_snapshot_table,
            tables.weekly_indicator_table,
            tables.daily_indicator_table,
            tables.open_monitor_run_table,
            tables.open_monitor_quote_table,
        )

        self._ensure_open_monitor_view(
            tables.open_monitor_view,
            tables.open_monitor_wide_view,
            tables.open_monitor_eval_table,
            tables.open_monitor_env_view,
            tables.open_monitor_quote_table,
            tables.open_monitor_run_table,
        )

    def get_table_names(self) -> TableNames:
        return self._resolve_table_names()

    def _resolve_table_names(self) -> TableNames:
        strat_cfg = get_section("strategy_ma5_ma20_trend") or {}
        open_monitor_cfg = get_section("open_monitor") or {}

        default_indicator = strat_cfg.get("indicator_table", TABLE_STRATEGY_INDICATOR_DAILY)
        indicator_table = (
                str(open_monitor_cfg.get("indicator_table", default_indicator)).strip()
                or TABLE_STRATEGY_INDICATOR_DAILY
        )
        default_events = (
                strat_cfg.get("signal_events_table")
                or strat_cfg.get("signals_table")
                or TABLE_STRATEGY_SIGNAL_EVENTS
        )
        signal_events_table = (
                str(open_monitor_cfg.get("signal_events_table", default_events)).strip()
                or TABLE_STRATEGY_SIGNAL_EVENTS
        )
        ready_signals_view = (
                str(open_monitor_cfg.get("ready_signals_view", VIEW_STRATEGY_READY_SIGNALS)).strip()
                or VIEW_STRATEGY_READY_SIGNALS
        )
        open_monitor_eval_table = (
                str(open_monitor_cfg.get("output_table", TABLE_STRATEGY_OPEN_MONITOR_EVAL)).strip()
                or TABLE_STRATEGY_OPEN_MONITOR_EVAL
        )
        open_monitor_run_table = (
                str(open_monitor_cfg.get("run_table", TABLE_STRATEGY_OPEN_MONITOR_RUN)).strip()
                or TABLE_STRATEGY_OPEN_MONITOR_RUN
        )
        env_snapshot_table = (
                str(
                    open_monitor_cfg.get(
                        "env_snapshot_table",
                        TABLE_STRATEGY_OPEN_MONITOR_ENV,
                    )
                ).strip()
                or TABLE_STRATEGY_OPEN_MONITOR_ENV
        )
        open_monitor_env_view = (
                str(
                    open_monitor_cfg.get(
                        "open_monitor_env_view",
                        VIEW_STRATEGY_OPEN_MONITOR_ENV,
                    )
                ).strip()
                or VIEW_STRATEGY_OPEN_MONITOR_ENV
        )
        open_monitor_view = (
                str(
                    open_monitor_cfg.get(
                        "open_monitor_view",
                        VIEW_STRATEGY_OPEN_MONITOR,
                    )
                ).strip()
                or VIEW_STRATEGY_OPEN_MONITOR
        )
        open_monitor_wide_view = (
                str(
                    open_monitor_cfg.get(
                        "open_monitor_wide_view",
                        VIEW_STRATEGY_OPEN_MONITOR_WIDE,
                    )
                ).strip()
                or VIEW_STRATEGY_OPEN_MONITOR_WIDE
        )
        open_monitor_quote_table = (
                str(
                    open_monitor_cfg.get(
                        "quote_table",
                        TABLE_STRATEGY_OPEN_MONITOR_QUOTE,
                    )
                ).strip()
                or TABLE_STRATEGY_OPEN_MONITOR_QUOTE
        )
        weekly_indicator_table = (
            str(
                open_monitor_cfg.get(
                    "weekly_indicator_table",
                    TABLE_STRATEGY_WEEKLY_MARKET_ENV,
                )
            ).strip()
            or TABLE_STRATEGY_WEEKLY_MARKET_ENV
        )
        if weekly_indicator_table == "strategy_weekly_market_indicator":
            weekly_indicator_table = TABLE_STRATEGY_WEEKLY_MARKET_ENV
        daily_indicator_table = (
                str(
                    open_monitor_cfg.get(
                        "daily_indicator_table",
                        TABLE_STRATEGY_DAILY_MARKET_ENV,
                    )
                ).strip()
                or TABLE_STRATEGY_DAILY_MARKET_ENV
        )

        return TableNames(
            indicator_table=indicator_table,
            signal_events_table=signal_events_table,
            ready_signals_view=ready_signals_view,
            open_monitor_eval_table=open_monitor_eval_table,
            env_snapshot_table=env_snapshot_table,
            open_monitor_run_table=open_monitor_run_table,
            open_monitor_env_view=open_monitor_env_view,
            open_monitor_view=open_monitor_view,
            open_monitor_wide_view=open_monitor_wide_view,
            open_monitor_quote_table=open_monitor_quote_table,
            weekly_indicator_table=weekly_indicator_table,
            daily_indicator_table=daily_indicator_table,
        )

    def _rename_table_if_needed(self, old_name: str, new_name: str) -> None:
        if not old_name or not new_name or old_name == new_name:
            return
        if not self._table_exists(old_name):
            return
        if self._table_exists(new_name):
            self.logger.warning(
                "检测到旧表 %s 但新表 %s 已存在，跳过重命名。",
                old_name,
                new_name,
            )
            return
        with self.engine.begin() as conn:
            conn.execute(text(f"RENAME TABLE `{old_name}` TO `{new_name}`"))
        self.logger.info("已将旧表 %s 重命名为 %s。", old_name, new_name)

    # ---------- generic helpers ----------
    def _table_exists(self, table: str) -> bool:
        with self.engine.connect() as conn:
            inspector = inspect(conn)
            return inspector.has_table(table)

    def _view_exists(self, view: str) -> bool:
        condition = "TABLE_SCHEMA = :schema" if self.db_name else "TABLE_SCHEMA = DATABASE()"
        stmt = text(
            f"""
            SELECT COUNT(*) AS cnt
            FROM information_schema.TABLES
            WHERE {condition} AND TABLE_NAME = :view AND TABLE_TYPE = 'VIEW'
            """
        )
        params: Dict[str, str] = {"view": view}
        if self.db_name:
            params["schema"] = str(self.db_name)
        with self.engine.connect() as conn:
            row = conn.execute(stmt, params).mappings().first()
        return bool(row and row.get("cnt"))

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

    def _drop_columns(self, table: str, columns: Iterable[str]) -> None:
        existing = set(self._column_meta(table).keys())
        to_drop = [col for col in columns if col in existing]
        if not to_drop:
            return
        with self.engine.begin() as conn:
            for col in to_drop:
                conn.execute(text(f"ALTER TABLE `{table}` DROP COLUMN `{col}`"))
                self.logger.info("表 %s 已删除旧列 %s。", table, col)

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

    def _ensure_date_column(self, table: str, column: str, *, not_null: bool) -> None:
        meta = self._column_meta(table)
        info = meta.get(column)
        target_def = "DATE NOT NULL" if not_null else "DATE NULL"
        if not info:
            self._add_missing_columns(table, {column: target_def})
            return
        if info["data_type"] != "date":
            with self.engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` {target_def}"))
            self.logger.info("表 %s.%s 已调整为 DATE。", table, column)

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

    def _drop_relation(self, name: str) -> None:
        if not name:
            return
        with self.engine.begin() as conn:
            conn.execute(text(f"DROP VIEW IF EXISTS `{name}`"))

    def _drop_relation_any(self, name: str) -> None:
        """删除同名关系对象（先视图后表），用于视图/表互转兼容。"""

        if not name:
            return
        with self.engine.begin() as conn:
            conn.execute(text(f"DROP VIEW IF EXISTS `{name}`"))
            conn.execute(text(f"DROP TABLE IF EXISTS `{name}`"))

    # ---------- Base dims/facts ----------
    def _ensure_dim_stock_basic_view(self) -> None:
        source = "a_share_stock_list"
        if not self._table_exists(source):
            self.logger.debug(
                "源表 %s 不存在，将创建空视图 %s。",
                source,
                VIEW_DIM_STOCK_BASIC,
            )
            self._drop_relation(VIEW_DIM_STOCK_BASIC)
            empty_stmt = text(
                f"""
                CREATE OR REPLACE VIEW `{VIEW_DIM_STOCK_BASIC}` AS
                SELECT
                  CAST(NULL AS CHAR(20)) AS `code`,
                  CAST(NULL AS CHAR(64)) AS `code_name`,
                  CAST(NULL AS CHAR(8)) AS `tradeStatus`,
                  CAST(NULL AS DATE) AS `ipoDate`,
                  CAST(NULL AS DATE) AS `outDate`,
                  CAST(NULL AS CHAR(8)) AS `type`,
                  CAST(NULL AS CHAR(8)) AS `status`
                WHERE 1 = 0
                """
            )
            with self.engine.begin() as conn:
                conn.execute(empty_stmt)
            return

        meta = self._column_meta(source)
        mapping = {
            "code": ["code"],
            "code_name": ["code_name"],
            "tradeStatus": ["tradeStatus", "tradestatus"],
            "ipoDate": ["ipoDate"],
            "outDate": ["outDate"],
            "type": ["type"],
            "status": ["status"],
        }
        select_cols: list[str] = []
        for target, candidates in mapping.items():
            for col in candidates:
                if col in meta:
                    expr = f"`{col}` AS `{target}`" if col != target else f"`{col}`"
                    select_cols.append(expr)
                    break
        if "code" not in {c.split(" AS ")[-1].strip("`") for c in select_cols}:
            self.logger.warning(
                "源表 %s 缺少 code 列，将创建空视图 %s。",
                source,
                VIEW_DIM_STOCK_BASIC,
            )
            self._drop_relation(VIEW_DIM_STOCK_BASIC)
            empty_stmt = text(
                f"""
                CREATE OR REPLACE VIEW `{VIEW_DIM_STOCK_BASIC}` AS
                SELECT
                  CAST(NULL AS CHAR(20)) AS `code`,
                  CAST(NULL AS CHAR(64)) AS `code_name`,
                  CAST(NULL AS CHAR(8)) AS `tradeStatus`,
                  CAST(NULL AS DATE) AS `ipoDate`,
                  CAST(NULL AS DATE) AS `outDate`,
                  CAST(NULL AS CHAR(8)) AS `type`,
                  CAST(NULL AS CHAR(8)) AS `status`
                WHERE 1 = 0
                """
            )
            with self.engine.begin() as conn:
                conn.execute(empty_stmt)
            return

        columns_sql = ", ".join(select_cols)
        self._drop_relation(VIEW_DIM_STOCK_BASIC)
        stmt = text(
            f"""
            CREATE OR REPLACE VIEW `{VIEW_DIM_STOCK_BASIC}` AS
            SELECT {columns_sql}
            FROM `{source}`
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新维度视图 %s。", VIEW_DIM_STOCK_BASIC)

    def _ensure_fact_stock_daily_view(self) -> None:
        source = "history_daily_kline"
        if not self._table_exists(source):
            self.logger.debug(
                "源表 %s 不存在，将创建空视图 %s。",
                source,
                VIEW_FACT_STOCK_DAILY,
            )
            self._drop_relation(VIEW_FACT_STOCK_DAILY)
            empty_stmt = text(
                f"""
                CREATE OR REPLACE VIEW `{VIEW_FACT_STOCK_DAILY}` AS
                SELECT
                  CAST(NULL AS CHAR(20)) AS `code`,
                  CAST(NULL AS DATE) AS `date`,
                  CAST(NULL AS DATE) AS `trade_date`,
                  CAST(NULL AS DECIMAL(18,6)) AS `open`,
                  CAST(NULL AS DECIMAL(18,6)) AS `high`,
                  CAST(NULL AS DECIMAL(18,6)) AS `low`,
                  CAST(NULL AS DECIMAL(18,6)) AS `close`,
                  CAST(NULL AS DECIMAL(18,6)) AS `volume`,
                  CAST(NULL AS DECIMAL(18,6)) AS `amount`,
                  CAST(NULL AS DECIMAL(18,6)) AS `preclose`,
                  CAST(NULL AS CHAR(8)) AS `tradestatus`
                WHERE 1 = 0
                """
            )
            with self.engine.begin() as conn:
                conn.execute(empty_stmt)
            return

        meta = self._column_meta(source)
        mapping = {
            "code": ["code"],
            "date": ["date"],
            "open": ["open"],
            "high": ["high"],
            "low": ["low"],
            "close": ["close"],
            "volume": ["volume"],
            "amount": ["amount"],
            "tradestatus": ["tradestatus", "tradeStatus"],
            "preclose": ["preclose"],
        }
        select_cols: list[str] = []
        for target, candidates in mapping.items():
            for col in candidates:
                if col in meta:
                    expr = f"`{col}` AS `{target}`" if col != target else f"`{col}`"
                    select_cols.append(expr)
                    break
        if "date" in meta and "trade_date" not in {c.split(" AS ")[-1].strip("`") for c in select_cols}:
            select_cols.append("CAST(`date` AS DATE) AS `trade_date`")

        if not select_cols or "code" not in {c.split(" AS ")[-1].strip('`') for c in select_cols}:
            self.logger.warning(
                "源表 %s 缺少必需列，将创建空视图 %s。",
                source,
                VIEW_FACT_STOCK_DAILY,
            )
            self._drop_relation(VIEW_FACT_STOCK_DAILY)
            empty_stmt = text(
                f"""
                CREATE OR REPLACE VIEW `{VIEW_FACT_STOCK_DAILY}` AS
                SELECT
                  CAST(NULL AS CHAR(20)) AS `code`,
                  CAST(NULL AS DATE) AS `date`,
                  CAST(NULL AS DATE) AS `trade_date`,
                  CAST(NULL AS DECIMAL(18,6)) AS `open`,
                  CAST(NULL AS DECIMAL(18,6)) AS `high`,
                  CAST(NULL AS DECIMAL(18,6)) AS `low`,
                  CAST(NULL AS DECIMAL(18,6)) AS `close`,
                  CAST(NULL AS DECIMAL(18,6)) AS `volume`,
                  CAST(NULL AS DECIMAL(18,6)) AS `amount`,
                  CAST(NULL AS DECIMAL(18,6)) AS `preclose`,
                  CAST(NULL AS CHAR(8)) AS `tradestatus`
                WHERE 1 = 0
                """
            )
            with self.engine.begin() as conn:
                conn.execute(empty_stmt)
            return

        columns_sql = ", ".join(select_cols)
        self._drop_relation(VIEW_FACT_STOCK_DAILY)
        stmt = text(
            f"""
            CREATE OR REPLACE VIEW `{VIEW_FACT_STOCK_DAILY}` AS
            SELECT {columns_sql}
            FROM `{source}`
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新事实视图 %s。", VIEW_FACT_STOCK_DAILY)

    def _ensure_index_membership_view(self) -> None:
        parts: list[str] = []
        index_tables = [
            ("hs300", "index_hs300_members"),
            ("zz500", "index_zz500_members"),
            ("sz50", "index_sz50_members"),
        ]
        for name, table in index_tables:
            if not self._table_exists(table):
                continue
            meta = self._column_meta(table)
            meta_norm = {str(col).lower(): str(col) for col in meta.keys()}

            code_col = meta_norm.get("code")
            date_col: str | None = None
            for cand in ("snapshot_date", "date", "updatedate", "update_date", "trade_date"):
                if cand in meta_norm:
                    date_col = meta_norm[cand]
                    break

            if not code_col or not date_col:
                self.logger.warning(
                    "指数成分表 %s 缺少必需列（code/date），将跳过并继续。",
                    table,
                )
                continue

            parts.append(
                "SELECT "
                f"'{name}' AS `index_name`, "
                f"CAST(`{date_col}` AS DATE) AS `snapshot_date`, "
                f"`{code_col}` AS `code` "
                f"FROM `{table}`"
            )

        self._drop_relation(VIEW_DIM_INDEX_MEMBERSHIP_SNAPSHOT)
        if not parts:
            stmt = text(
                f"""
                CREATE OR REPLACE VIEW `{VIEW_DIM_INDEX_MEMBERSHIP_SNAPSHOT}` AS
                SELECT CAST(NULL AS CHAR(32)) AS `index_name`,
                       CAST(NULL AS DATE) AS `snapshot_date`,
                       CAST(NULL AS CHAR(20)) AS `code`
                WHERE 1 = 0
                """
            )
        else:
            union_sql = " UNION ALL ".join(parts)
            stmt = text(
                f"""
                CREATE OR REPLACE VIEW `{VIEW_DIM_INDEX_MEMBERSHIP_SNAPSHOT}` AS
                {union_sql}
                """
            )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新指数成分维度视图 %s。", VIEW_DIM_INDEX_MEMBERSHIP_SNAPSHOT)

    def _ensure_universe_table(self) -> None:
        """确保 a_share_universe 作为表存在（不再使用视图）。

        说明：
        - 该表用于承载你运行时生成的 universe 快照数据；
        - 如历史版本曾创建同名 VIEW，这里会先安全地 drop 掉 view。
        """

        table = TABLE_A_SHARE_UNIVERSE

        # 兼容旧版本：如果同名对象是 VIEW，则先删除，避免 CREATE TABLE 冲突
        if self._view_exists(table):
            self._drop_relation_any(table)

        columns = {
            "date": "DATE NOT NULL",
            "code": "VARCHAR(20) NOT NULL",
            "code_name": "VARCHAR(64) NULL",
            "tradeStatus": "VARCHAR(8) NULL",
            "amount": "DOUBLE NULL",
            "volume": "DOUBLE NULL",
            "open": "DOUBLE NULL",
            "high": "DOUBLE NULL",
            "low": "DOUBLE NULL",
            "close": "DOUBLE NULL",
            "ipoDate": "DATE NULL",
            "type": "VARCHAR(8) NULL",
            "status": "VARCHAR(8) NULL",
            "in_hs300": "TINYINT NULL",
            "in_zz500": "TINYINT NULL",
            "in_sz50": "TINYINT NULL",
        }

        if not self._table_exists(table):
            self._create_table(table, columns, primary_key=("date", "code"))
        else:
            self._add_missing_columns(table, columns)

        idx_name = "idx_a_share_universe_code_date"
        if not self._index_exists(table, idx_name):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE INDEX `{idx_name}`
                        ON `{table}` (`code`, `date`)
                        """
                    )
                )

        self.logger.info("已创建/更新 Universe 表 %s。", table)

    # ---------- MA5-MA20 strategy ----------
    def _ensure_indicator_table(self, table: str) -> None:
        columns = {
            "trade_date": "DATE NOT NULL",
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
            "prev_macd_hist": "DOUBLE NULL",
            "kdj_k": "DOUBLE NULL",
            "kdj_d": "DOUBLE NULL",
            "kdj_j": "DOUBLE NULL",
            "atr14": "DOUBLE NULL",
            "rsi14": "DOUBLE NULL",
            "ret_10": "DOUBLE NULL",
            "ret_20": "DOUBLE NULL",
            "limit_up_cnt_20": "DOUBLE NULL",
            "ma20_bias": "DOUBLE NULL",
            "yearline_state": "VARCHAR(50) NULL",
        }
        if not self._table_exists(table):
            self._create_table(table, columns, primary_key=("trade_date", "code"))
            return
        self._add_missing_columns(table, columns)

    def _ensure_signal_events_table(self, table: str) -> None:
        columns = {
            "sig_date": "DATE NOT NULL",
            "code": "VARCHAR(20) NOT NULL",
            "strategy_code": "VARCHAR(32) NOT NULL",
            "signal": "VARCHAR(64) NULL",
            "final_action": "VARCHAR(16) NULL",
            "final_reason": "VARCHAR(255) NULL",
            "final_cap": "DOUBLE NULL",
            "reason": "VARCHAR(255) NULL",
            "risk_tag": "VARCHAR(255) NULL",
            "risk_note": "VARCHAR(255) NULL",
            "stop_ref": "DOUBLE NULL",
            "macd_event": "VARCHAR(32) NULL",
            "chip_score": "DOUBLE NULL",
            "gdhs_delta_pct": "DOUBLE NULL",
            "gdhs_announce_date": "DATE NULL",
            "chip_reason": "VARCHAR(255) NULL",
            "chip_penalty": "DOUBLE NULL",
            "chip_note": "VARCHAR(255) NULL",
            "age_days": "INT NULL",
            "deadzone_hit": "TINYINT(1) NULL",
            "stale_hit": "TINYINT(1) NULL",
            "fear_score": "DOUBLE NULL",
            "wave_type": "VARCHAR(64) NULL",
            "extra_json": "TEXT NULL",
        }
        if not self._table_exists(table):
            self._create_table(
                table,
                columns,
                primary_key=("sig_date", "code", "strategy_code"),
            )
            return
        self._add_missing_columns(table, columns)
        self._ensure_varchar_length(table, "risk_tag", 255)
        self._ensure_varchar_length(table, "risk_note", 255)
        self._ensure_varchar_length(table, "reason", 255)
        self._ensure_varchar_length(table, "final_reason", 255)
        self._ensure_varchar_length(table, "macd_event", 32)
        self._ensure_varchar_length(table, "wave_type", 64)
        self._ensure_numeric_column(table, "final_cap", "DOUBLE NULL")
        self._ensure_numeric_column(table, "chip_score", "DOUBLE NULL")
        self._ensure_numeric_column(table, "gdhs_delta_pct", "DOUBLE NULL")
        self._ensure_numeric_column(table, "chip_penalty", "DOUBLE NULL")
        self._ensure_numeric_column(table, "fear_score", "DOUBLE NULL")
        self._ensure_numeric_column(table, "age_days", "INT NULL")
        self._ensure_numeric_column(table, "deadzone_hit", "TINYINT(1) NULL")
        self._ensure_numeric_column(table, "stale_hit", "TINYINT(1) NULL")
        self._ensure_date_column(table, "gdhs_announce_date", not_null=False)
        self._ensure_varchar_length(table, "chip_reason", 255)
        self._ensure_varchar_length(table, "chip_note", 255)
        unique_name = "ux_signal_events_strategy_date_code"
        if not self._index_exists(table, unique_name):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE UNIQUE INDEX `{unique_name}`
                        ON `{table}` (`strategy_code`, `sig_date`, `code`)
                        """
                    )
                )
            self.logger.info("信号事件表 %s 已新增唯一索引 %s。", table, unique_name)

    def _ensure_ready_signals_view(
            self,
            view: str,
            events_table: str,
            indicator_table: str,
            chip_table: str,
    ) -> None:
        if not view or not events_table:
            return

        ind_join = ""
        ind_fields = """
              NULL AS `close`,
              NULL AS `ma5`,
              NULL AS `ma20`,
              NULL AS `ma60`,
              NULL AS `ma250`,
              NULL AS `vol_ratio`,
              NULL AS `macd_hist`,
              NULL AS `kdj_k`,
              NULL AS `kdj_d`,
              NULL AS `atr14`,
              NULL AS `yearline_state`
            """
        avg_volume_expr = "NULL AS `avg_volume_20`"
        if indicator_table and self._table_exists(indicator_table):
            ind_join = (
                f"""
                LEFT JOIN `{indicator_table}` ind
                  ON e.`sig_date` = ind.`trade_date`
                 AND e.`code` = ind.`code`
                """
            )
            ind_fields = """
              ind.`close`,
              ind.`ma5`,
              ind.`ma20`,
              ind.`ma60`,
              ind.`ma250`,
              ind.`vol_ratio`,
              ind.`macd_hist`,
              ind.`kdj_k`,
              ind.`kdj_d`,
              ind.`atr14`,
              ind.`yearline_state`
            """
            avg_volume_expr = f"""
              (
                SELECT AVG(t.`volume`)
                FROM (
                  SELECT `volume`
                  FROM `{indicator_table}` hist
                  WHERE hist.`code` = e.`code`
                    AND hist.`trade_date` <= e.`sig_date`
                  ORDER BY hist.`trade_date` DESC
                  LIMIT 20
                ) t
              ) AS `avg_volume_20`
            """
        elif indicator_table:
            self.logger.warning("指标表 %s 不存在，ready_signals_view 将以 NULL 补齐指标列。", indicator_table)

        chip_join = ""
        chip_enabled = False
        if chip_table and self._table_exists(chip_table):
            chip_enabled = True
            chip_join = (
                f"""
                LEFT JOIN `{chip_table}` cf
                  ON e.`sig_date` = cf.`sig_date`
                 AND e.`code` = cf.`code`
                """
            )

        meta = self._column_meta(events_table)

        def _col(name: str) -> str | None:
            return name if name in meta else None

        # feat: reason 字段优先使用 final_reason，减少下游口径回填
        field_exprs = [
            "e.`sig_date`",
            "e.`code`",
            "e.`strategy_code`",
            "COALESCE(e.`final_action`, e.`signal`) AS `signal`",
            "COALESCE(e.`final_action`, e.`signal`) AS `final_action`",
            "COALESCE(e.`final_reason`, e.`reason`) AS `final_reason`",
            "e.`final_cap`",
            "COALESCE(e.`final_reason`, e.`reason`) AS `reason`",
            "e.`risk_tag`",
            "e.`risk_note`",
            "e.`extra_json`",
        ]
        optional_cols = [
            "stop_ref",
            "macd_event",
            "fear_score",
            "wave_type",
            "yearline_state",
        ]
        for col in optional_cols:
            expr = _col(col)
            if expr:
                field_exprs.append(f"e.`{expr}`")

        def _coalesce_expr(cols: list[str], alias: str) -> str:
            if not cols:
                return f"NULL AS `{alias}`"
            return f"COALESCE({', '.join(cols)}) AS `{alias}`"

        gdhs_delta_sources = []
        announce_sources = []
        chip_score_sources = []
        chip_reason_sources = []
        chip_penalty_sources = []
        chip_note_sources = []
        chip_age_sources = []
        chip_deadzone_sources = []
        chip_stale_sources = []

        if _col("gdhs_delta_pct"):
            gdhs_delta_sources.append("e.`gdhs_delta_pct`")
        if _col("gdhs_announce_date"):
            announce_sources.append("e.`gdhs_announce_date`")
        if _col("chip_score"):
            chip_score_sources.append("e.`chip_score`")
        if _col("chip_reason"):
            chip_reason_sources.append("e.`chip_reason`")
        if _col("chip_penalty"):
            chip_penalty_sources.append("e.`chip_penalty`")
        if _col("chip_note"):
            chip_note_sources.append("e.`chip_note`")
        if _col("age_days"):
            chip_age_sources.append("e.`age_days`")
        if _col("deadzone_hit"):
            chip_deadzone_sources.append("e.`deadzone_hit`")
        if _col("stale_hit"):
            chip_stale_sources.append("e.`stale_hit`")

        if ind_fields:
            field_exprs.append(ind_fields.strip())
        field_exprs.append(avg_volume_expr.strip())

        if chip_enabled:
            gdhs_delta_sources.append("cf.`gdhs_delta_pct`")
            announce_sources.append("cf.`announce_date`")
            chip_score_sources.append("cf.`chip_score`")
            chip_reason_sources.append("cf.`chip_reason`")
            chip_penalty_sources.append("cf.`chip_penalty`")
            chip_note_sources.append("cf.`chip_note`")
            chip_age_sources.append("cf.`age_days`")
            chip_deadzone_sources.append("cf.`deadzone_hit`")
            chip_stale_sources.append("cf.`stale_hit`")

        field_exprs.extend(
            [
                _coalesce_expr(gdhs_delta_sources, "gdhs_delta_pct"),
                _coalesce_expr(announce_sources, "gdhs_announce_date"),
                _coalesce_expr(chip_score_sources, "chip_score"),
                _coalesce_expr(chip_reason_sources, "chip_reason"),
                _coalesce_expr(chip_penalty_sources, "chip_penalty"),
                _coalesce_expr(chip_note_sources, "chip_note"),
                _coalesce_expr(chip_age_sources, "age_days"),
                _coalesce_expr(chip_deadzone_sources, "deadzone_hit"),
                _coalesce_expr(chip_stale_sources, "stale_hit"),
            ]
        )

        industry_join = ""
        industry_fields: list[str] = ["NULL AS `industry`", "NULL AS `industry_classification`"]
        for candidate in ("dim_stock_industry", "a_share_stock_industry"):
            if self._table_exists(candidate):
                industry_join = (
                    f"""
                    LEFT JOIN `{candidate}` ind_dim
                      ON e.`code` = ind_dim.`code`
                    """
                )
                industry_meta = self._column_meta(candidate)
                industry_name_col = None
                for key in ["industry", "industryClassification"]:
                    if key in industry_meta:
                        industry_name_col = key
                        break
                industry_class_col = "industry_classification" if "industry_classification" in industry_meta else None
                industry_fields = [
                    f"ind_dim.`{industry_name_col}` AS `industry`" if industry_name_col else "NULL AS `industry`",
                    f"ind_dim.`{industry_class_col}` AS `industry_classification`"
                    if industry_class_col
                    else "NULL AS `industry_classification`",
                ]
                break

        board_join = ""
        board_fields: list[str] = ["NULL AS `board_name`", "NULL AS `board_code`"]
        board_table = "dim_stock_board_industry"
        if self._table_exists(board_table):
            board_join = (
                f"""
                LEFT JOIN `{board_table}` bd
                  ON e.`code` = bd.`code`
                """
            )
            board_meta = self._column_meta(board_table)
            board_name_col = "board_name" if "board_name" in board_meta else None
            board_code_col = "board_code" if "board_code" in board_meta else None
            board_fields = [
                f"bd.`{board_name_col}` AS `board_name`" if board_name_col else "NULL AS `board_name`",
                f"bd.`{board_code_col}` AS `board_code`" if board_code_col else "NULL AS `board_code`",
            ]

        field_exprs.extend(industry_fields)
        field_exprs.extend(board_fields)

        select_clause = ",\n              ".join(field_exprs)
        stmt = text(
            f"""
            CREATE OR REPLACE VIEW `{view}` AS
            SELECT
              {select_clause}
            FROM `{events_table}` e
            {ind_join}
            {chip_join}
            {industry_join}
            {board_join}
            WHERE COALESCE(e.`final_action`, e.`signal`) IN ('BUY','BUY_CONFIRM')
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新视图 %s（含筹码预计算）。", view)

    def _ensure_trade_metrics_table(self) -> None:
        columns = {
            "strategy_code": "VARCHAR(32) NOT NULL",
            "code": "VARCHAR(20) NOT NULL",
            "entry_date": "DATE NOT NULL",
            "entry_price": "DOUBLE NULL",
            "exit_date": "DATE NULL",
            "exit_price": "DOUBLE NULL",
            "atr_at_entry": "DOUBLE NULL",
            "pnl_pct": "DOUBLE NULL",
            "pnl_atr_ratio": "DOUBLE NULL",
            "holding_days": "INT NULL",
            "exit_reason": "VARCHAR(255) NULL",
        }
        table = TABLE_STRATEGY_TRADE_METRICS
        if not self._table_exists(table):
            self._create_table(table, columns, primary_key=("strategy_code", "code", "entry_date"))
            return
        self._add_missing_columns(table, columns)
        self._ensure_numeric_column(table, "pnl_pct", "DOUBLE NULL")
        self._ensure_numeric_column(table, "pnl_atr_ratio", "DOUBLE NULL")
        self._ensure_numeric_column(table, "holding_days", "INT NULL")
        self._ensure_varchar_length(table, "exit_reason", 255)

    def _ensure_chip_filter_table(self) -> None:
        columns = {
            "sig_date": "DATE NOT NULL",
            "code": "VARCHAR(20) NOT NULL",
            "announce_date": "DATE NULL",
            "gdhs_delta_pct": "DOUBLE NULL",
            "gdhs_delta_raw": "DOUBLE NULL",
            "chip_score": "DOUBLE NULL",
            "chip_reason": "VARCHAR(255) NULL",
            "chip_penalty": "DOUBLE NULL",
            "chip_note": "VARCHAR(255) NULL",
            "vol_ratio": "DOUBLE NULL",
            "age_days": "INT NULL",
            "deadzone_hit": "TINYINT(1) NULL",
            "stale_hit": "TINYINT(1) NULL",
            "updated_at": "DATETIME(6) NULL",
        }
        table = TABLE_STRATEGY_CHIP_FILTER
        if not self._table_exists(table):
            self._create_table(table, columns, primary_key=("sig_date", "code"))
            return
        self._add_missing_columns(table, columns)
        self._ensure_numeric_column(table, "chip_score", "DOUBLE NULL")
        self._ensure_numeric_column(table, "gdhs_delta_pct", "DOUBLE NULL")
        self._ensure_numeric_column(table, "gdhs_delta_raw", "DOUBLE NULL")
        self._ensure_numeric_column(table, "vol_ratio", "DOUBLE NULL")
        self._ensure_numeric_column(table, "age_days", "INT NULL")
        self._ensure_numeric_column(table, "deadzone_hit", "TINYINT(1) NULL")
        self._ensure_numeric_column(table, "stale_hit", "TINYINT(1) NULL")
        self._ensure_numeric_column(table, "chip_penalty", "DOUBLE NULL")
        self._ensure_varchar_length(table, "chip_reason", 255)
        self._ensure_varchar_length(table, "chip_note", 255)
        self._ensure_datetime_column(table, "updated_at")

    def _ensure_backtest_view(self) -> None:
        table = TABLE_STRATEGY_TRADE_METRICS
        view = VIEW_STRATEGY_BACKTEST
        if not self._table_exists(table):
            self._drop_relation(view)
            return
        stmt = text(
            f"""
            CREATE OR REPLACE VIEW `{view}` AS
            SELECT
              `strategy_code`,
              'WEEK' AS `period_type`,
              CAST(YEARWEEK(`exit_date`, 3) AS CHAR) AS `period_key`,
              COUNT(*) AS `trade_cnt`,
              1.0 * SUM(CASE WHEN `pnl_pct` > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS `win_rate`,
              AVG(`pnl_atr_ratio`) AS `avg_pnl_atr_ratio`,
              SUM(CASE WHEN `pnl_atr_ratio` > 1.5 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS `gt_1_5_ratio`
            FROM `{table}`
            WHERE `exit_date` IS NOT NULL
            GROUP BY `strategy_code`, YEARWEEK(`exit_date`, 3)
            UNION ALL
            SELECT
              `strategy_code`,
              'MONTH' AS `period_type`,
              CAST(DATE_FORMAT(`exit_date`, '%Y-%m') AS CHAR) AS `period_key`,
              COUNT(*) AS `trade_cnt`,
              1.0 * SUM(CASE WHEN `pnl_pct` > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS `win_rate`,
              AVG(`pnl_atr_ratio`) AS `avg_pnl_atr_ratio`,
              SUM(CASE WHEN `pnl_atr_ratio` > 1.5 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS `gt_1_5_ratio`
            FROM `{table}`
            WHERE `exit_date` IS NOT NULL
            GROUP BY `strategy_code`, DATE_FORMAT(`exit_date`, '%Y-%m')
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新回测视图 %s。", view)

    def _ensure_v_pnl_view(self) -> None:
        table = TABLE_STRATEGY_TRADE_METRICS
        view = VIEW_STRATEGY_PNL
        if not self._table_exists(table):
            self._drop_relation(view)
            return
        stmt = text(
            f"""
            CREATE OR REPLACE VIEW `{view}` AS
            SELECT
              `strategy_code`,
              `code`,
              COUNT(*) AS `trade_cnt`,
              SUM(CASE WHEN `pnl_pct` > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS `win_rate`,
              AVG(`pnl_atr_ratio`) AS `avg_pnl_atr_ratio`,
              AVG(`pnl_pct`) AS `avg_pnl_pct`
            FROM `{table}`
            WHERE `exit_date` IS NOT NULL
            GROUP BY `strategy_code`, `code`
            HAVING `avg_pnl_atr_ratio` > 1.5
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新视图 %s。", view)

    # ---------- Open monitor ----------
    def _ensure_open_monitor_run_table(self, table: str) -> None:
        columns = {
            "run_pk": "BIGINT NOT NULL AUTO_INCREMENT",
            "monitor_date": "DATE NOT NULL",
            "run_id": "VARCHAR(64) NOT NULL",
            "triggered_at": "DATETIME(6) NULL",
            "checked_at": "DATETIME(6) NULL",
            "status": "VARCHAR(32) NOT NULL DEFAULT 'RUNNING'",
            "error_msg": "TEXT NULL",
            "params_json": "TEXT NULL",
        }
        if not self._table_exists(table):
            self._create_table(table, columns, primary_key=("run_pk",))
        else:
            self._add_missing_columns(table, columns)
            self._drop_columns(table, ["env_index_snapshot_hash"])
        self._ensure_date_column(table, "monitor_date", not_null=True)
        self._ensure_datetime_column(table, "triggered_at")
        self._ensure_datetime_column(table, "checked_at")
        self._ensure_varchar_length(table, "run_id", 64)
        self._ensure_varchar_length(table, "status", 32)

        unique_name = "ux_open_monitor_run"
        if not self._index_exists(table, unique_name):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE UNIQUE INDEX `{unique_name}`
                        ON `{table}` (`monitor_date`, `run_id`)
                        """
                    )
                )
            self.logger.info("开盘监测运行表 %s 已新增唯一索引 %s。", table, unique_name)

    def _ensure_open_monitor_quote_table(self, table: str) -> None:
        columns = {
            "monitor_date": "DATE NOT NULL",
            "run_id": "VARCHAR(64) NOT NULL",
            "code": "VARCHAR(20) NOT NULL",
            "live_trade_date": "DATE NULL",
            "live_open": "DOUBLE NULL",
            "live_high": "DOUBLE NULL",
            "live_low": "DOUBLE NULL",
            "live_latest": "DOUBLE NULL",
            "live_volume": "DOUBLE NULL",
            "live_amount": "DOUBLE NULL",
        }
        if not self._table_exists(table):
            self._create_table(
                table,
                columns,
                primary_key=("monitor_date", "run_id", "code"),
            )
            return
        self._add_missing_columns(table, columns)
        self._drop_columns(table, ["run_pk"])
        self._ensure_varchar_length(table, "run_id", 64)
        self._ensure_date_column(table, "monitor_date", not_null=True)
        self._ensure_date_column(table, "live_trade_date", not_null=False)

        unique_name = "ux_open_monitor_quote_run"
        if not self._index_exists(table, unique_name):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE UNIQUE INDEX `{unique_name}`
                        ON `{table}` (`monitor_date`, `run_id`, `code`)
                        """
                    )
                )
            self.logger.info("行情快照表 %s 已新增唯一索引 %s。", table, unique_name)

    def _ensure_open_monitor_eval_table(self, table: str) -> None:
        columns = {
            "monitor_date": "DATE NOT NULL",
            "sig_date": "DATE NOT NULL",
            "run_id": "VARCHAR(64) NOT NULL",
            "asof_trade_date": "DATE NULL",
            "live_trade_date": "DATE NULL",
            "signal_age": "INT NULL",
            "valid_days": "INT NULL",
            "code": "VARCHAR(20) NOT NULL",
            "live_gap_pct": "DOUBLE NULL",
            "live_pct_change": "DOUBLE NULL",
            "live_intraday_vol_ratio": "DOUBLE NULL",
            "dev_ma5": "DOUBLE NULL",
            "dev_ma20": "DOUBLE NULL",
            "dev_ma5_atr": "DOUBLE NULL",
            "dev_ma20_atr": "DOUBLE NULL",
            "runup_from_sigclose": "DOUBLE NULL",
            "runup_from_sigclose_atr": "DOUBLE NULL",
            "runup_ref_price": "DOUBLE NULL",
            "runup_ref_source": "VARCHAR(32) NULL",
            "entry_exposure_cap": "DOUBLE NULL",
            "signal_strength": "DOUBLE NULL",
            "strength_delta": "DOUBLE NULL",
            "strength_trend": "VARCHAR(16) NULL",
            "strength_note": "VARCHAR(512) NULL",
            "signal_kind": "VARCHAR(16) NULL",
            "sig_signal": "VARCHAR(16) NULL",
            "sig_reason": "VARCHAR(255) NULL",
            "state": "VARCHAR(32) NULL",
            "status_reason": "VARCHAR(255) NULL",
            "action": "VARCHAR(16) NULL",
            "action_reason": "VARCHAR(255) NULL",
            "rule_hits_json": "TEXT NULL",
            "summary_line": "VARCHAR(512) NULL",
            "risk_tag": "VARCHAR(255) NULL",
            "risk_note": "VARCHAR(255) NULL",
            "snapshot_hash": "VARCHAR(64) NULL",
        }
        if not self._table_exists(table):
            self._create_table(
                table,
                columns,
                primary_key=("monitor_date", "sig_date", "code", "run_id"),
            )
        else:
            self._add_missing_columns(table, columns)
            self._drop_columns(
                table,
                [
                    "run_pk",
                    "checked_at",
                    "env_index_score",
                    "env_regime",
                    "env_position_hint",
                    "env_final_gate_action",
                    "env_index_snapshot_hash",
                    "env_weekly_asof_trade_date",
                    "env_weekly_risk_level",
                    "env_weekly_scene",
                    "env_weekly_structure_status",
                    "env_weekly_pattern_status",
                    "env_weekly_gate_action",
                    "env_weekly_gate_policy",
                ],
            )

        for col in ["code", "snapshot_hash", "run_id"]:
            self._ensure_varchar_length(table, col, 64 if col == "snapshot_hash" else 64)
        self._ensure_date_column(table, "monitor_date", not_null=True)
        self._ensure_date_column(table, "sig_date", not_null=True)
        self._ensure_date_column(table, "asof_trade_date", not_null=False)
        self._ensure_date_column(table, "live_trade_date", not_null=False)

        self._ensure_numeric_column(table, "live_intraday_vol_ratio", "DOUBLE NULL")
        self._ensure_numeric_column(table, "signal_strength", "DOUBLE NULL")
        self._ensure_numeric_column(table, "strength_delta", "DOUBLE NULL")

        self._ensure_open_monitor_indexes(table)

    def _ensure_open_monitor_indexes(self, table: str) -> None:
        unique_index = "ux_open_monitor_run"
        if not self._index_exists(table, unique_index):
            with self.engine.begin() as conn:
                try:
                    conn.execute(
                        text(
                            f"""
                            CREATE UNIQUE INDEX `{unique_index}`
                            ON `{table}` (`monitor_date`, `sig_date`, `code`, `run_id`)
                            """
                        )
                    )
                    self.logger.info("表 %s 已创建唯一索引 %s。", table, unique_index)
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("创建唯一索引 %s 失败：%s", unique_index, exc)

        index_name = "idx_open_monitor_strength_run"
        if not self._index_exists(table, index_name):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"CREATE INDEX `{index_name}` ON `{table}` (`monitor_date`, `code`, `run_id`)"
                    )
                )
            self.logger.info("表 %s 已新增索引 %s。", table, index_name)

        code_time_index = "idx_open_monitor_code_run"
        if not self._index_exists(table, code_time_index):
            with self.engine.begin() as conn:
                conn.execute(
                    text(f"CREATE INDEX `{code_time_index}` ON `{table}` (`code`, `run_id`)")
                )
            self.logger.info("表 %s 已新增索引 %s。", table, code_time_index)

    # ---------- Environment snapshots ----------
    def _ensure_env_snapshot_table(self, table: str) -> None:
        columns = {
            "run_pk": "BIGINT NOT NULL",
            "monitor_date": "DATE NOT NULL",
            "env_weekly_asof_trade_date": "DATE NULL",
            "env_daily_asof_trade_date": "DATE NULL",
            "env_final_gate_action": "VARCHAR(16) NULL",
            "env_final_cap_pct": "DOUBLE NULL",
            "env_final_reason_json": "TEXT NULL",
            "env_weekly_zone_id": "VARCHAR(32) NULL",
            "env_daily_zone_id": "VARCHAR(32) NULL",
            "env_live_override_action": "VARCHAR(16) NULL",
            "env_live_cap_multiplier": "DOUBLE NULL",
            "env_live_event_tags": "VARCHAR(255) NULL",
            "env_live_reason": "VARCHAR(255) NULL",
            "env_index_snapshot_hash": "CHAR(32) NULL",
        }
        if not self._table_exists(table):
            self._create_table(table, columns, primary_key=("run_pk",))
        else:
            self._add_missing_columns(table, columns)
            self._drop_columns(
                table,
                [
                    "run_id",
                    "checked_at",
                    "env_weekly_gate_policy",
                    "env_weekly_gate_action",
                    "env_weekly_risk_level",
                    "env_weekly_scene",
                    "env_index_score",
                    "env_regime",
                    "env_position_hint",
                    "env_index_code",
                    "env_index_asof_trade_date",
                    "env_index_live_trade_date",
                    "env_index_asof_close",
                    "env_index_asof_ma20",
                    "env_index_asof_ma60",
                    "env_index_asof_macd_hist",
                    "env_index_asof_atr14",
                    "env_index_live_open",
                    "env_index_live_high",
                    "env_index_live_low",
                    "env_index_live_latest",
                    "env_index_live_pct_change",
                    "env_index_live_volume",
                    "env_index_live_amount",
                    "env_index_dev_ma20_atr",
                    "env_index_gate_action",
                    "env_index_gate_reason",
                    "env_index_position_cap",
                ],
            )
        self._ensure_date_column(table, "monitor_date", not_null=True)
        self._ensure_date_column(table, "env_weekly_asof_trade_date", not_null=False)
        self._ensure_date_column(table, "env_daily_asof_trade_date", not_null=False)
        self._ensure_numeric_column(table, "env_final_cap_pct", "DOUBLE NULL")
        self._ensure_numeric_column(table, "env_live_cap_multiplier", "DOUBLE NULL")
        self._ensure_numeric_column(table, "run_pk", "BIGINT NOT NULL")
        self._ensure_varchar_length(table, "env_weekly_zone_id", 32)
        self._ensure_varchar_length(table, "env_daily_zone_id", 32)
        self._ensure_varchar_length(table, "env_live_override_action", 16)
        self._ensure_varchar_length(table, "env_live_event_tags", 255)
        self._ensure_varchar_length(table, "env_live_reason", 255)

        unique_name = "ux_env_snapshot_run"
        if not self._index_exists(table, unique_name):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE UNIQUE INDEX `{unique_name}`
                        ON `{table}` (`monitor_date`, `run_pk`)
                        """
                    )
                )
            self.logger.info("环境快照表 %s 已新增唯一索引 %s。", table, unique_name)

        hash_index = "idx_env_snapshot_hash"
        if not self._index_exists(table, hash_index):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE INDEX `{hash_index}`
                        ON `{table}` (`env_index_snapshot_hash`)
                        """
                    )
                )
            self.logger.info("环境快照表 %s 已新增索引 %s。", table, hash_index)

        run_idx = "idx_env_snapshot_run_pk"
        if not self._index_exists(table, run_idx):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE INDEX `{run_idx}`
                        ON `{table}` (`run_pk`)
                        """
                    )
                )
            self.logger.info("环境快照表 %s 已新增索引 %s。", table, run_idx)

    def _ensure_open_monitor_env_view(
        self,
        view: str,
        env_table: str,
        weekly_table: str,
        daily_table: str,
        run_table: str,
        quote_table: str,
    ) -> None:
        if not view:
            return
        if not (env_table and run_table):
            self._drop_relation(view)
            return
        if not self._table_exists(env_table):
            self._drop_relation(view)
            return
        if not weekly_table:
            self._drop_relation(view)
            return
        if not self._table_exists(weekly_table):
            self._drop_relation(view)
            return
        if not daily_table:
            self._drop_relation(view)
            return
        if not self._table_exists(daily_table):
            self._drop_relation(view)
            return
        if not self._table_exists(run_table):
            self._drop_relation(view)
            return
        if not quote_table:
            self._drop_relation(view)
            return
        if not self._table_exists(quote_table):
            self._drop_relation(view)
            return

        history_table = "history_index_daily_kline"
        has_history = self._table_exists(history_table)
        index_asof_close = (
            "idx.`close`" if has_history else "CAST(NULL AS DOUBLE)"
        )
        index_asof_join = ""
        if has_history:
            index_asof_join = f"""
            LEFT JOIN `{history_table}` idx
              ON idx.`date` = env.`env_daily_asof_trade_date`
             AND idx.`code` = '{WEEKLY_MARKET_BENCHMARK_CODE}'
            """

        pct_change_expr = f"""
            CASE
              WHEN q.`live_latest` IS NULL THEN NULL
              WHEN {index_asof_close} IS NULL THEN NULL
              WHEN {index_asof_close} = 0 THEN NULL
              ELSE (q.`live_latest` / {index_asof_close} - 1) * 100
            END
        """
        dev_ma20_atr_expr = """
            CASE
              WHEN q.`live_latest` IS NULL THEN NULL
              WHEN daily.`ma20` IS NULL THEN NULL
              WHEN daily.`atr14` IS NULL THEN NULL
              WHEN daily.`atr14` = 0 THEN NULL
              ELSE (q.`live_latest` - daily.`ma20`) / daily.`atr14`
            END
        """
        gate_action_expr = """
            CASE
              WHEN UPPER(COALESCE(daily.`regime`, '')) IN ('BREAKDOWN', 'BEAR_CONFIRMED')
                THEN 'STOP'
              WHEN UPPER(COALESCE(daily.`regime`, '')) = 'RISK_OFF'
                THEN 'WAIT'
              WHEN UPPER(COALESCE(daily.`regime`, '')) = 'PULLBACK'
                THEN CASE
                  WHEN daily.`position_hint` IS NOT NULL AND daily.`position_hint` <= 0.3
                    THEN 'WAIT'
                  ELSE 'ALLOW'
                END
              WHEN UPPER(COALESCE(daily.`regime`, '')) = 'RISK_ON'
                THEN 'ALLOW'
              WHEN daily.`position_hint` IS NOT NULL THEN CASE
                WHEN daily.`position_hint` <= 0 THEN 'STOP'
                WHEN daily.`position_hint` < 0.3 THEN 'WAIT'
                ELSE 'ALLOW'
              END
              ELSE NULL
            END
        """

        stmt = text(
            f"""
            CREATE OR REPLACE VIEW `{view}` AS
            SELECT
              env.`monitor_date`,
              r.`run_id`,
              env.`run_pk`,
              env.`env_index_snapshot_hash`,
              env.`env_final_gate_action`,
              env.`env_final_cap_pct`,
              env.`env_weekly_asof_trade_date`,
              env.`env_daily_asof_trade_date`,
              env.`env_final_reason_json`,
              env.`env_weekly_zone_id`,
              env.`env_daily_zone_id`,
              env.`env_live_override_action`,
              env.`env_live_cap_multiplier`,
              env.`env_live_event_tags`,
              env.`env_live_reason`,
              daily.`score` AS `env_index_score`,
              daily.`regime` AS `env_regime`,
              daily.`position_hint` AS `env_position_hint`,
              weekly.`weekly_gate_policy` AS `env_weekly_gate_policy`,
              weekly.`weekly_risk_level` AS `env_weekly_risk_level`,
              weekly.`weekly_scene_code` AS `env_weekly_scene`,
              weekly.`weekly_structure_status` AS `env_weekly_structure_status`,
              weekly.`weekly_pattern_status` AS `env_weekly_pattern_status`,
              weekly.`weekly_plan_a_exposure_cap` AS `env_weekly_plan_a_exposure_cap`,
              weekly.`weekly_key_levels_str` AS `env_weekly_key_levels_str`,
              weekly.`weekly_zone_score` AS `env_weekly_zone_score`,
              weekly.`weekly_exp_return_bucket` AS `env_weekly_exp_return_bucket`,
              weekly.`weekly_money_proxy` AS `env_weekly_money_proxy`,
              weekly.`weekly_tags` AS `env_weekly_tags`,
              weekly.`weekly_note` AS `env_weekly_note`,
              weekly.`weekly_gate_policy` AS `env_weekly_gate_action`,
              daily.`daily_zone_score` AS `env_daily_zone_score`,
              daily.`daily_cap_multiplier` AS `env_daily_cap_multiplier`,
              daily.`daily_zone_reason` AS `env_daily_zone_reason`,
              daily.`bb_pos` AS `env_daily_bb_pos`,
              daily.`bb_width` AS `env_daily_bb_width`,
              COALESCE(
                daily.`benchmark_code`,
                weekly.`benchmark_code`,
                '{WEEKLY_MARKET_BENCHMARK_CODE}'
              ) AS `env_index_code`,
              env.`env_daily_asof_trade_date` AS `env_index_asof_trade_date`,
              q.`live_trade_date` AS `env_index_live_trade_date`,
              {index_asof_close} AS `env_index_asof_close`,
              daily.`ma20` AS `env_index_asof_ma20`,
              daily.`ma60` AS `env_index_asof_ma60`,
              daily.`macd_hist` AS `env_index_asof_macd_hist`,
              daily.`atr14` AS `env_index_asof_atr14`,
              q.`live_open` AS `env_index_live_open`,
              q.`live_high` AS `env_index_live_high`,
              q.`live_low` AS `env_index_live_low`,
              q.`live_latest` AS `env_index_live_latest`,
              {pct_change_expr} AS `env_index_live_pct_change`,
              q.`live_volume` AS `env_index_live_volume`,
              q.`live_amount` AS `env_index_live_amount`,
              {dev_ma20_atr_expr} AS `env_index_dev_ma20_atr`,
              {gate_action_expr} AS `env_index_gate_action`,
              CONCAT(
                'regime=',
                COALESCE(daily.`regime`, ''),
                ' pos_hint=',
                COALESCE(daily.`position_hint`, '')
              ) AS `env_index_gate_reason`,
              daily.`position_hint` AS `env_index_position_cap`
            FROM `{env_table}` env
            LEFT JOIN `{run_table}` r
              ON env.`run_pk` = r.`run_pk`
            LEFT JOIN `{weekly_table}` weekly
              ON env.`env_weekly_asof_trade_date` = weekly.`weekly_asof_trade_date`
             AND weekly.`benchmark_code` = '{WEEKLY_MARKET_BENCHMARK_CODE}'
            LEFT JOIN `{daily_table}` daily
              ON env.`env_daily_asof_trade_date` = daily.`asof_trade_date`
             AND daily.`benchmark_code` = '{WEEKLY_MARKET_BENCHMARK_CODE}'
            LEFT JOIN `{quote_table}` q
              ON env.`monitor_date` = q.`monitor_date`
             AND r.`run_id` = q.`run_id`
             AND q.`code` = '{WEEKLY_MARKET_BENCHMARK_CODE}'
            {index_asof_join}
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新开盘监测环境视图 %s。", view)

    def _ensure_weekly_indicator_table(self, table: str) -> None:
        columns = {
            "weekly_asof_trade_date": "DATE NOT NULL",
            "benchmark_code": "VARCHAR(16) NOT NULL",
            "weekly_scene_code": "VARCHAR(32) NULL",
            "weekly_phase": "VARCHAR(32) NULL",
            "weekly_structure_status": "VARCHAR(32) NULL",
            "weekly_pattern_status": "VARCHAR(32) NULL",
            "weekly_risk_score": "DOUBLE NULL",
            "weekly_risk_level": "VARCHAR(16) NULL",
            "weekly_gate_policy": "VARCHAR(16) NULL",
            "weekly_plan_a_exposure_cap": "DOUBLE NULL",
            "weekly_key_levels_str": "VARCHAR(255) NULL",
            "weekly_zone_id": "VARCHAR(32) NULL",
            "weekly_zone_score": "INT NULL",
            "weekly_exp_return_bucket": "VARCHAR(16) NULL",
            "weekly_zone_reason": "VARCHAR(255) NULL",
            "weekly_money_proxy": "VARCHAR(255) NULL",
            "weekly_tags": "VARCHAR(255) NULL",
            "weekly_note": "VARCHAR(255) NULL",
            "weekly_plan_json": "TEXT NULL",
        }
        if not self._table_exists(table):
            self._create_table(
                table,
                columns,
                primary_key=("weekly_asof_trade_date", "benchmark_code"),
            )
        else:
            self._add_missing_columns(table, columns)
        self._ensure_date_column(table, "weekly_asof_trade_date", not_null=True)
        self._ensure_varchar_length(table, "benchmark_code", 16)

    def _ensure_daily_market_env_table(self, table: str) -> None:
        columns = {
            "asof_trade_date": "DATE NOT NULL",
            "benchmark_code": "VARCHAR(16) NOT NULL DEFAULT 'sh.000001'",
            "regime": "VARCHAR(32) NULL",
            "score": "DOUBLE NULL",
            "position_hint": "DOUBLE NULL",
            "ma20": "DOUBLE NULL",
            "ma60": "DOUBLE NULL",
            "ma250": "DOUBLE NULL",
            "macd_hist": "DOUBLE NULL",
            "atr14": "DOUBLE NULL",
            "dev_ma20_atr": "DOUBLE NULL",
            "bb_mid": "DOUBLE NULL",
            "bb_upper": "DOUBLE NULL",
            "bb_lower": "DOUBLE NULL",
            "bb_width": "DOUBLE NULL",
            "bb_pos": "DOUBLE NULL",
            "cycle_phase": "VARCHAR(32) NULL",
            "cycle_weekly_asof_trade_date": "DATE NULL",
            "cycle_weekly_scene_code": "VARCHAR(64) NULL",
            "breadth_pct_above_ma20": "DOUBLE NULL",
            "breadth_pct_above_ma60": "DOUBLE NULL",
            "breadth_risk_off_ratio": "DOUBLE NULL",
            "dispersion_score": "DOUBLE NULL",
            "daily_zone_id": "VARCHAR(32) NULL",
            "daily_zone_score": "INT NULL",
            "daily_cap_multiplier": "DOUBLE NULL",
            "daily_zone_reason": "VARCHAR(255) NULL",
            "components_json": "LONGTEXT NULL",
        }
        if not self._table_exists(table):
            self._create_table(
                table,
                columns,
                primary_key=("asof_trade_date", "benchmark_code"),
            )
        else:
            self._add_missing_columns(table, columns)
        self._ensure_date_column(table, "asof_trade_date", not_null=True)
        self._ensure_varchar_length(table, "benchmark_code", 16)

    def _ensure_open_monitor_view(
            self,
            view: str,
            wide_view: str,
            eval_table: str,
            env_view: str,
            quote_table: str,
            run_table: str,
    ) -> None:
        if not (wide_view and eval_table and env_view and run_table):
            return

        dim_stock_exists = self._table_exists("dim_stock_industry")
        board_dim_exists = self._table_exists("dim_stock_board_industry")
        stock_join = (
            "LEFT JOIN `dim_stock_industry` dsi ON e.`code` = dsi.`code`"
            if dim_stock_exists
            else ""
        )
        board_join = (
            "LEFT JOIN `dim_stock_board_industry` dsb ON e.`code` = dsb.`code`"
            if board_dim_exists
            else ""
        )
        name_expr = "dsi.`code_name`" if dim_stock_exists else "NULL"
        industry_expr = "dsi.`industry`" if dim_stock_exists else "NULL"
        board_name_expr = "dsb.`board_name`" if board_dim_exists else "NULL"
        board_code_expr = "dsb.`board_code`" if board_dim_exists else "NULL"
        quote_join = ""
        # 注意：eval 表只存评估/派生字段；盘口行情字段（open/high/low/latest/volume/amount）
        # 通常落在 quote 表（strategy_open_monitor_quote）中。
        live_open_expr = "NULL"
        live_high_expr = "NULL"
        live_low_expr = "NULL"
        live_latest_expr = "NULL"
        live_pct_expr = "e.`live_pct_change`"
        live_volume_expr = "NULL"
        live_amount_expr = "NULL"
        live_gap_expr = "e.`live_gap_pct`"
        live_intraday_expr = "e.`live_intraday_vol_ratio`"
        if quote_table and self._table_exists(quote_table):
            quote_join = (
                f"""
                LEFT JOIN `{quote_table}` q
                  ON e.`monitor_date` = q.`monitor_date`
                 AND e.`run_id` = q.`run_id`
                 AND e.`code` = q.`code`
                """
            )
            live_open_expr = "q.`live_open`"
            live_high_expr = "q.`live_high`"
            live_low_expr = "q.`live_low`"
            live_latest_expr = "q.`live_latest`"
            live_volume_expr = "q.`live_volume`"
            live_amount_expr = "q.`live_amount`"

        target_wide_view = wide_view or view
        stmt = text(
            f"""
            CREATE OR REPLACE VIEW `{target_wide_view}` AS
            SELECT
              e.`monitor_date`,
              e.`sig_date`,
              e.`run_id`,
              e.`code`,
              {name_expr} AS `name`,
              {industry_expr} AS `industry`,
              {board_name_expr} AS `board_name`,
              {board_code_expr} AS `board_code`,
              e.`asof_trade_date`,
              e.`live_trade_date`,
              e.`signal_age`,
              e.`valid_days`,
              {live_gap_expr} AS `live_gap_pct`,
              {live_pct_expr} AS `live_pct_change`,
              {live_intraday_expr} AS `live_intraday_vol_ratio`,
              e.`dev_ma5`,
              e.`dev_ma20`,
              e.`dev_ma5_atr`,
              e.`dev_ma20_atr`,
              e.`runup_from_sigclose`,
              e.`runup_from_sigclose_atr`,
              e.`runup_ref_price`,
              e.`runup_ref_source`,
              e.`entry_exposure_cap`,
              env.`env_index_score`,
              env.`env_regime`,
              env.`env_position_hint`,
              env.`env_final_gate_action`,
              env.`env_index_snapshot_hash`,
              e.`signal_strength`,
              e.`strength_delta`,
              e.`strength_trend`,
              e.`strength_note`,
              e.`signal_kind`,
              e.`sig_signal`,
              e.`sig_reason`,
              e.`state`,
              e.`status_reason`,
              e.`action`,
              e.`action_reason`,
              e.`rule_hits_json`,
              e.`summary_line`,
              e.`risk_tag`,
              e.`risk_note`,
              e.`snapshot_hash`,
              r.`run_pk`,
              r.`checked_at`,
              r.`status`,
              {live_open_expr} AS `live_open`,
              {live_high_expr} AS `live_high`,
              {live_low_expr} AS `live_low`,
              {live_latest_expr} AS `live_latest`,
              {live_volume_expr} AS `live_volume`,
              {live_amount_expr} AS `live_amount`,
              env.`env_weekly_asof_trade_date`,
              env.`env_weekly_risk_level`,
              env.`env_weekly_scene`,
              env.`env_weekly_gate_action`,
              env.`env_weekly_gate_policy`,
              env.`env_final_cap_pct`,
              env.`env_final_reason_json`,
              env.`env_index_code`,
              env.`env_index_asof_trade_date`,
              env.`env_index_live_trade_date`,
              env.`env_index_asof_close`,
              env.`env_index_asof_ma20`,
              env.`env_index_asof_ma60`,
              env.`env_index_asof_macd_hist`,
              env.`env_index_asof_atr14`,
              env.`env_index_live_open`,
              env.`env_index_live_high`,
              env.`env_index_live_low`,
              env.`env_index_live_latest`,
              env.`env_index_live_pct_change`,
              env.`env_index_live_volume`,
              env.`env_index_live_amount`,
              env.`env_index_dev_ma20_atr`,
              env.`env_index_gate_action`,
              env.`env_index_gate_reason`,
              env.`env_index_position_cap`
            FROM `{eval_table}` e
            LEFT JOIN `{env_view}` env
              ON e.`monitor_date` = env.`monitor_date`
             AND e.`run_id` = env.`run_id`
            LEFT JOIN `{run_table}` r
              ON e.`monitor_date` = r.`monitor_date`
             AND e.`run_id` = r.`run_id`
            {quote_join}
            {stock_join}
            {board_join}
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新开盘监测宽视图 %s。", target_wide_view)

        if view and view != target_wide_view:
            compact_columns = [
                "monitor_date",
                "sig_date",
                "run_id",
                "run_pk",
                "code",
                "name",
                "industry",
                "board_name",
                "board_code",
                "signal_kind",
                "sig_signal",
                "sig_reason",
                "live_trade_date",
                "live_open",
                "live_latest",
                "live_pct_change",
                "live_gap_pct",
                "live_intraday_vol_ratio",
                "state",
                "status_reason",
                "action",
                "action_reason",
                "entry_exposure_cap",
                "env_final_gate_action",
                "env_index_position_cap",
                "env_position_hint",
                "env_index_live_pct_change",
                "env_index_gate_action",
                "env_index_gate_reason",
                "rule_hits_json",
                "summary_line",
                "checked_at",
                "status",
            ]
            compact_select = ", ".join(f"`{col}`" for col in compact_columns)
            compact_stmt = text(
                f"""
                CREATE OR REPLACE VIEW `{view}` AS
                SELECT {compact_select}
                FROM `{target_wide_view}`
                """
            )
            with self.engine.begin() as conn:
                conn.execute(compact_stmt)
            self.logger.info("已创建/更新开盘监测精简视图 %s。", view)


def ensure_schema() -> None:
    db_config = DatabaseConfig.from_env()
    bootstrap_writer = MySQLWriter(db_config)
    try:
        SchemaManager(bootstrap_writer.engine, db_name=db_config.db_name).ensure_all()
    finally:
        bootstrap_writer.dispose()
