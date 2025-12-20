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
VIEW_A_SHARE_UNIVERSE = "a_share_universe"

# 统一策略信号体系表命名：按单一职责拆分
TABLE_STRATEGY_INDICATOR_DAILY = "strategy_indicator_daily"
TABLE_STRATEGY_SIGNAL_EVENTS = "strategy_signal_events"
TABLE_STRATEGY_SIGNAL_CANDIDATES = "strategy_signal_candidates"
VIEW_STRATEGY_SIGNAL_CANDIDATES = "v_strategy_signal_candidates"

# 开盘监测输出
TABLE_STRATEGY_OPEN_MONITOR_EVAL = "strategy_open_monitor_eval"
TABLE_STRATEGY_OPEN_MONITOR_ENV = "strategy_open_monitor_env"
TABLE_STRATEGY_OPEN_MONITOR_QUOTE = "strategy_open_monitor_quote"
VIEW_STRATEGY_OPEN_MONITOR_WIDE = "v_strategy_open_monitor_wide"
# 兼容旧宽表命名
VIEW_STRATEGY_OPEN_MONITOR_COMPAT = "strategy_open_monitor"

# 大盘/指数环境快照
TABLE_ENV_INDEX_SNAPSHOT = "strategy_env_index_snapshot"


@dataclass(frozen=True)
class TableNames:
    indicator_table: str
    signal_events_table: str
    candidates_table: str
    candidates_view: str
    candidates_as_view: bool
    open_monitor_eval_table: str
    env_snapshot_table: str
    env_index_snapshot_table: str
    open_monitor_view: str
    open_monitor_quote_table: str
    open_monitor_compat_view: str


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
        self._ensure_universe_view()

        self._ensure_indicator_table(tables.indicator_table)
        self._ensure_signal_events_table(tables.signal_events_table)
        if tables.candidates_as_view:
            self._ensure_candidates_view(
                tables.candidates_view, tables.signal_events_table, tables.indicator_table
            )
        else:
            self._ensure_candidates_table(tables.candidates_table)

        self._ensure_open_monitor_eval_table(tables.open_monitor_eval_table)
        self._ensure_open_monitor_quote_table(tables.open_monitor_quote_table)
        self._ensure_env_snapshot_table(tables.env_snapshot_table)
        self._ensure_env_index_snapshot_table(tables.env_index_snapshot_table)
        self._ensure_open_monitor_view(
            tables.open_monitor_view,
            tables.open_monitor_eval_table,
            tables.env_snapshot_table,
            tables.env_index_snapshot_table,
            tables.open_monitor_quote_table,
            tables.open_monitor_compat_view,
        )

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
        candidates_table = (
            str(strat_cfg.get("candidates_table", TABLE_STRATEGY_SIGNAL_CANDIDATES)).strip()
            or TABLE_STRATEGY_SIGNAL_CANDIDATES
        )
        candidates_view = (
            str(strat_cfg.get("candidates_view", VIEW_STRATEGY_SIGNAL_CANDIDATES)).strip()
            or VIEW_STRATEGY_SIGNAL_CANDIDATES
        )
        candidates_as_view = bool(strat_cfg.get("candidates_as_view", True))

        open_monitor_eval_table = (
            str(open_monitor_cfg.get("output_table", TABLE_STRATEGY_OPEN_MONITOR_EVAL)).strip()
            or TABLE_STRATEGY_OPEN_MONITOR_EVAL
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
        open_monitor_view = (
            str(
                open_monitor_cfg.get(
                    "open_monitor_view",
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
        open_monitor_compat_view = (
            str(
                open_monitor_cfg.get(
                    "open_monitor_compat_view",
                    VIEW_STRATEGY_OPEN_MONITOR_COMPAT,
                )
            ).strip()
            or VIEW_STRATEGY_OPEN_MONITOR_COMPAT
        )

        return TableNames(
            indicator_table=indicator_table,
            signal_events_table=signal_events_table,
            candidates_table=candidates_table,
            candidates_view=candidates_view,
            candidates_as_view=candidates_as_view,
            open_monitor_eval_table=open_monitor_eval_table,
            env_snapshot_table=env_snapshot_table,
            env_index_snapshot_table=env_index_snapshot_table,
            open_monitor_view=open_monitor_view,
            open_monitor_quote_table=open_monitor_quote_table,
            open_monitor_compat_view=open_monitor_compat_view,
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
                  CAST(NULL AS DOUBLE) AS `open`,
                  CAST(NULL AS DOUBLE) AS `high`,
                  CAST(NULL AS DOUBLE) AS `low`,
                  CAST(NULL AS DOUBLE) AS `close`,
                  CAST(NULL AS DOUBLE) AS `volume`,
                  CAST(NULL AS DOUBLE) AS `amount`,
                  CAST(NULL AS DOUBLE) AS `preclose`,
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
                  CAST(NULL AS DOUBLE) AS `open`,
                  CAST(NULL AS DOUBLE) AS `high`,
                  CAST(NULL AS DOUBLE) AS `low`,
                  CAST(NULL AS DOUBLE) AS `close`,
                  CAST(NULL AS DOUBLE) AS `volume`,
                  CAST(NULL AS DOUBLE) AS `amount`,
                  CAST(NULL AS DOUBLE) AS `preclose`,
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
            parts.append(
                f"SELECT '{name}' AS `index_name`, `date` AS `snapshot_date`, `code` FROM `{table}`"
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

    def _ensure_universe_view(self) -> None:
        if not (self._table_exists(VIEW_DIM_STOCK_BASIC) and self._table_exists(VIEW_FACT_STOCK_DAILY)):
            self.logger.debug(
                "依赖视图缺失（%s/%s），创建空视图 a_share_universe 以便占位。",
                VIEW_DIM_STOCK_BASIC,
                VIEW_FACT_STOCK_DAILY,
            )
            self._drop_relation(VIEW_A_SHARE_UNIVERSE)
            empty_stmt = text(
                f"""
                CREATE OR REPLACE VIEW `{VIEW_A_SHARE_UNIVERSE}` AS
                SELECT
                  CAST(NULL AS DATE) AS `date`,
                  CAST(NULL AS CHAR(20)) AS `code`,
                  CAST(NULL AS CHAR(64)) AS `code_name`,
                  CAST(NULL AS CHAR(8)) AS `tradeStatus`,
                  CAST(NULL AS DOUBLE) AS `amount`,
                  CAST(NULL AS DOUBLE) AS `volume`,
                  CAST(NULL AS DOUBLE) AS `open`,
                  CAST(NULL AS DOUBLE) AS `high`,
                  CAST(NULL AS DOUBLE) AS `low`,
                  CAST(NULL AS DOUBLE) AS `close`,
                  CAST(NULL AS DATE) AS `ipoDate`,
                  CAST(NULL AS CHAR(8)) AS `type`,
                  CAST(NULL AS CHAR(8)) AS `status`,
                  CAST(NULL AS SIGNED) AS `in_hs300`,
                  CAST(NULL AS SIGNED) AS `in_zz500`,
                  CAST(NULL AS SIGNED) AS `in_sz50`
                WHERE 1 = 0
                """
            )
            with self.engine.begin() as conn:
                conn.execute(empty_stmt)
            return

        dim_meta = set(self._column_meta(VIEW_DIM_STOCK_BASIC).keys())
        fact_meta = set(self._column_meta(VIEW_FACT_STOCK_DAILY).keys())
        if not {"code", "date"}.intersection(fact_meta) or "code" not in dim_meta:
            self.logger.warning("维度/事实视图缺少 code/date 列，跳过 a_share_universe 视图创建。")
            return

        date_ref = "trade_date" if "trade_date" in fact_meta else "date"
        date_expr = f"f.`{date_ref}`"
        trade_status_expr = "b.`tradeStatus`" if "tradeStatus" in dim_meta else "NULL"
        amount_expr = "f.`amount`" if "amount" in fact_meta else "NULL"
        volume_expr = "f.`volume`" if "volume" in fact_meta else "NULL"
        close_expr = "f.`close`" if "close" in fact_meta else "NULL"
        open_expr = "f.`open`" if "open" in fact_meta else "NULL"
        high_expr = "f.`high`" if "high" in fact_meta else "NULL"
        low_expr = "f.`low`" if "low" in fact_meta else "NULL"
        ipo_expr = "b.`ipoDate`" if "ipoDate" in dim_meta else "NULL"
        type_expr = "b.`type`" if "type" in dim_meta else "NULL"
        status_expr = "b.`status`" if "status" in dim_meta else "NULL"

        membership_exists = self._table_exists(VIEW_DIM_INDEX_MEMBERSHIP_SNAPSHOT)
        membership_select = ""
        membership_join = ""
        if membership_exists:
            membership_select = """
              ,im.`in_hs300`
              ,im.`in_zz500`
              ,im.`in_sz50`
            """
            membership_join = (
                f"""
                LEFT JOIN (
                  SELECT
                    im.`code`,
                    MAX(CASE WHEN im.`index_name` = 'hs300' THEN 1 ELSE 0 END) AS `in_hs300`,
                    MAX(CASE WHEN im.`index_name` = 'zz500' THEN 1 ELSE 0 END) AS `in_zz500`,
                    MAX(CASE WHEN im.`index_name` = 'sz50' THEN 1 ELSE 0 END) AS `in_sz50`
                  FROM `{VIEW_DIM_INDEX_MEMBERSHIP_SNAPSHOT}` im
                  JOIN (
                    SELECT `index_name`, MAX(`snapshot_date`) AS `snapshot_date`
                    FROM `{VIEW_DIM_INDEX_MEMBERSHIP_SNAPSHOT}`
                    GROUP BY `index_name`
                  ) latest_snapshot
                    ON im.`index_name` = latest_snapshot.`index_name`
                   AND im.`snapshot_date` = latest_snapshot.`snapshot_date`
                  GROUP BY im.`code`
                ) im ON f.`code` = im.`code`
                """
            )

        status_filter = "COALESCE(b.`tradeStatus`, '1') = '1'" if "tradeStatus" in dim_meta else "1 = 1"

        self._drop_relation(VIEW_A_SHARE_UNIVERSE)
        stmt = text(
            f"""
            CREATE OR REPLACE VIEW `{VIEW_A_SHARE_UNIVERSE}` AS
            SELECT
              {date_expr} AS `date`,
              f.`code`,
              b.`code_name`,
              {trade_status_expr} AS `tradeStatus`,
              {amount_expr} AS `amount`,
              {volume_expr} AS `volume`,
              {open_expr} AS `open`,
              {high_expr} AS `high`,
              {low_expr} AS `low`,
              {close_expr} AS `close`,
              {ipo_expr} AS `ipoDate`,
              {type_expr} AS `type`,
              {status_expr} AS `status`
              {membership_select}
            FROM `{VIEW_FACT_STOCK_DAILY}` f
            JOIN (
              SELECT `code`, MAX(`{date_ref}`) AS `latest_date`
              FROM `{VIEW_FACT_STOCK_DAILY}`
              GROUP BY `code`
            ) latest
              ON f.`code` = latest.`code` AND f.`{date_ref}` = latest.`latest_date`
            JOIN `{VIEW_DIM_STOCK_BASIC}` b ON f.`code` = b.`code`
            {membership_join}
            WHERE {status_filter}
              AND UPPER(COALESCE(b.`code_name`, '')) NOT LIKE '%ST%'
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新视图 %s。", VIEW_A_SHARE_UNIVERSE)

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
            "kdj_k": "DOUBLE NULL",
            "kdj_d": "DOUBLE NULL",
            "kdj_j": "DOUBLE NULL",
            "atr14": "DOUBLE NULL",
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
            "signal": "VARCHAR(10) NULL",
            "reason": "VARCHAR(255) NULL",
            "risk_tag": "VARCHAR(255) NULL",
            "risk_note": "VARCHAR(255) NULL",
            "stop_ref": "DOUBLE NULL",
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

    def _ensure_candidates_table(self, table: str) -> None:
        columns = {
            "sig_date": "DATE NOT NULL",
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
            self._create_table(table, columns, primary_key=("sig_date", "code"))
            return
        self._add_missing_columns(table, columns)

    def _ensure_candidates_view(
        self, view: str, events_table: str, indicator_table: str
    ) -> None:
        stmt = text(
            f"""
            CREATE OR REPLACE VIEW `{view}` AS
            SELECT
              e.`sig_date`,
              e.`code`,
              e.`strategy_code`,
              e.`signal` AS `sig_signal`,
              e.`reason` AS `sig_reason`,
              e.`reason`,
              DATEDIFF(CURDATE(), e.`sig_date`) AS `signal_age`,
              CASE
                WHEN e.`reason` LIKE '%回踩%' THEN 5
                ELSE 3
              END AS `valid_days`,
              ind.`close`,
              ind.`ma5`,ind.`ma20`,ind.`ma60`,ind.`ma250`,
              ind.`vol_ratio`,ind.`macd_hist`,ind.`kdj_k`,ind.`kdj_d`,ind.`atr14`,
              e.`stop_ref`,
              ind.`yearline_state`,e.`risk_tag`,e.`risk_note`,
              e.`extra_json`
            FROM `{events_table}` e
            LEFT JOIN `{indicator_table}` ind
              ON e.`sig_date` = ind.`trade_date` AND e.`code` = ind.`code`
            WHERE e.`signal` = 'BUY'
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新候选视图 %s。", view)

    # ---------- Open monitor ----------
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
            "live_gap_pct": "DOUBLE NULL",
            "live_pct_change": "DOUBLE NULL",
            "live_intraday_vol_ratio": "DOUBLE NULL",
        }
        if not self._table_exists(table):
            self._create_table(
                table,
                columns,
                primary_key=("monitor_date", "run_id", "code"),
            )
            return
        self._add_missing_columns(table, columns)
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
            "env_index_score": "DOUBLE NULL",
            "env_regime": "VARCHAR(32) NULL",
            "env_position_hint": "DOUBLE NULL",
            "env_final_gate_action": "VARCHAR(16) NULL",
            "env_index_snapshot_hash": "VARCHAR(32) NULL",
            "signal_strength": "DOUBLE NULL",
            "strength_delta": "DOUBLE NULL",
            "strength_trend": "VARCHAR(16) NULL",
            "strength_note": "VARCHAR(512) NULL",
            "signal_kind": "VARCHAR(16) NULL",
            "sig_signal": "VARCHAR(16) NULL",
            "sig_reason": "VARCHAR(255) NULL",
            "candidate_stage": "VARCHAR(16) NULL",
            "candidate_state": "VARCHAR(32) NULL",
            "candidate_status": "VARCHAR(32) NULL",
            "primary_status": "VARCHAR(32) NULL",
            "status_reason": "VARCHAR(255) NULL",
            "action": "VARCHAR(16) NULL",
            "action_reason": "VARCHAR(255) NULL",
            "status_tags_json": "TEXT NULL",
            "summary_line": "VARCHAR(512) NULL",
            "risk_tag": "VARCHAR(255) NULL",
            "risk_note": "VARCHAR(255) NULL",
            "checked_at": "DATETIME(6) NULL",
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

        for col in ["code", "snapshot_hash", "run_id", "env_index_snapshot_hash"]:
            self._ensure_varchar_length(table, col, 64 if col == "snapshot_hash" else 64)
        self._ensure_date_column(table, "monitor_date", not_null=True)
        self._ensure_date_column(table, "sig_date", not_null=True)
        self._ensure_date_column(table, "asof_trade_date", not_null=False)
        self._ensure_date_column(table, "live_trade_date", not_null=False)

        self._ensure_datetime_column(table, "checked_at")
        self._ensure_numeric_column(table, "live_intraday_vol_ratio", "DOUBLE NULL")
        self._ensure_numeric_column(table, "signal_strength", "DOUBLE NULL")
        self._ensure_numeric_column(table, "strength_delta", "DOUBLE NULL")

        self._ensure_open_monitor_indexes(table)

    def _cleanup_duplicate_snapshots(self, table: str) -> None:
        dedupe_cols = {"monitor_date", "sig_date", "code", "run_id", "checked_at"}
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
             AND t1.`run_id` = t2.`run_id`
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
        unique_index = "ux_open_monitor_run"
        if not self._index_exists(table, unique_index):
            self._cleanup_duplicate_snapshots(table)
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
            "monitor_date": "DATE NOT NULL",
            "run_id": "VARCHAR(64) NOT NULL",
            "checked_at": "DATETIME(6) NULL",
            "env_weekly_asof_trade_date": "DATE NULL",
            "env_weekly_risk_level": "VARCHAR(16) NULL",
            "env_weekly_scene": "VARCHAR(32) NULL",
            "env_weekly_gate_action": "VARCHAR(16) NULL",
            "env_weekly_gate_policy": "VARCHAR(16) NULL",
            "env_final_gate_action": "VARCHAR(16) NULL",
            "env_index_snapshot_hash": "VARCHAR(32) NULL",
        }
        if not self._table_exists(table):
            self._create_table(table, columns, primary_key=("monitor_date", "run_id"))
        else:
            self._add_missing_columns(table, columns)
        self._ensure_datetime_column(table, "checked_at")
        self._ensure_date_column(table, "monitor_date", not_null=True)
        self._ensure_date_column(table, "env_weekly_asof_trade_date", not_null=False)
        self._ensure_varchar_length(table, "run_id", 64)

        unique_name = "ux_env_snapshot_run"
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
            self.logger.info("环境快照表 %s 已新增唯一索引 %s。", table, unique_name)

    def _ensure_env_index_snapshot_table(self, table: str) -> None:
        columns = {
            "snapshot_hash": "VARCHAR(32) NOT NULL",
            "monitor_date": "DATE NULL",
            "run_id": "VARCHAR(64) NULL",
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
        self._ensure_date_column(table, "monitor_date", not_null=False)
        self._ensure_varchar_length(table, "run_id", 64)

        unique_name = "uk_env_index_snapshot"
        if not self._index_exists(table, unique_name):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE UNIQUE INDEX `{unique_name}`
                        ON `{table}` (`monitor_date`, `run_id`, `index_code`, `checked_at`)
                        """
                    )
                )
            self.logger.info("指数环境快照表 %s 已添加唯一索引 %s。", table, unique_name)

        run_idx = "idx_env_index_snapshot_run"
        if not self._index_exists(table, run_idx):
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE INDEX `{run_idx}`
                        ON `{table}` (`monitor_date`, `run_id`)
                        """
                    )
                )
            self.logger.info("指数环境快照表 %s 已新增索引 %s。", table, run_idx)

    def _ensure_open_monitor_view(
        self,
        view: str,
        eval_table: str,
        env_table: str,
        env_index_table: str,
        quote_table: str,
        compat_view: str,
    ) -> None:
        if not (view and eval_table and env_table and env_index_table):
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
        live_open_expr = "e.`live_open`"
        live_high_expr = "e.`live_high`"
        live_low_expr = "e.`live_low`"
        live_latest_expr = "e.`live_latest`"
        live_pct_expr = "e.`live_pct_change`"
        live_volume_expr = "e.`live_volume`"
        live_amount_expr = "e.`live_amount`"
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
            live_open_expr = "COALESCE(e.`live_open`, q.`live_open`)"
            live_high_expr = "COALESCE(e.`live_high`, q.`live_high`)"
            live_low_expr = "COALESCE(e.`live_low`, q.`live_low`)"
            live_latest_expr = "COALESCE(e.`live_latest`, q.`live_latest`)"
            live_pct_expr = "COALESCE(e.`live_pct_change`, q.`live_pct_change`)"
            live_volume_expr = "COALESCE(e.`live_volume`, q.`live_volume`)"
            live_amount_expr = "COALESCE(e.`live_amount`, q.`live_amount`)"
            live_gap_expr = "COALESCE(e.`live_gap_pct`, q.`live_gap_pct`)"
            live_intraday_expr = "COALESCE(e.`live_intraday_vol_ratio`, q.`live_intraday_vol_ratio`)"

        stmt = text(
            f"""
            CREATE OR REPLACE VIEW `{view}` AS
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
              e.`env_index_score`,
              e.`env_regime`,
              e.`env_position_hint`,
              COALESCE(e.`env_final_gate_action`, env.`env_final_gate_action`) AS `env_final_gate_action`,
              e.`env_index_snapshot_hash`,
              e.`signal_strength`,
              e.`strength_delta`,
              e.`strength_trend`,
              e.`strength_note`,
              e.`signal_kind`,
              e.`sig_signal`,
              e.`sig_reason`,
              e.`candidate_stage`,
              e.`candidate_state`,
              e.`candidate_status`,
              e.`primary_status`,
              e.`status_reason`,
              e.`action`,
              e.`action_reason`,
              e.`status_tags_json`,
              e.`summary_line`,
              e.`risk_tag`,
              e.`risk_note`,
              e.`checked_at`,
              e.`snapshot_hash`,
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
              idx.`asof_trade_date` AS `env_index_asof_trade_date`,
              idx.`live_trade_date` AS `env_index_live_trade_date`,
              idx.`asof_close` AS `env_index_asof_close`,
              idx.`asof_ma20` AS `env_index_asof_ma20`,
              idx.`asof_ma60` AS `env_index_asof_ma60`,
              idx.`asof_macd_hist` AS `env_index_asof_macd_hist`,
              idx.`asof_atr14` AS `env_index_asof_atr14`,
              idx.`live_open` AS `env_index_live_open`,
              idx.`live_high` AS `env_index_live_high`,
              idx.`live_low` AS `env_index_live_low`,
              idx.`live_latest` AS `env_index_live_latest`,
              idx.`live_pct_change` AS `env_index_live_pct_change`,
              idx.`live_volume` AS `env_index_live_volume`,
              idx.`live_amount` AS `env_index_live_amount`,
              idx.`dev_ma20_atr` AS `env_index_dev_ma20_atr`,
              idx.`gate_action` AS `env_index_gate_action`,
              idx.`gate_reason` AS `env_index_gate_reason`,
              idx.`position_cap` AS `env_index_position_cap`
            FROM `{eval_table}` e
            LEFT JOIN `{env_table}` env
              ON e.`monitor_date` = env.`monitor_date` AND e.`run_id` = env.`run_id`
            LEFT JOIN `{env_index_table}` idx
              ON e.`env_index_snapshot_hash` = idx.`snapshot_hash`
            {quote_join}
            {stock_join}
            {board_join}
            """
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        self.logger.info("已创建/更新开盘监测宽视图 %s。", view)

        if compat_view and compat_view != view:
            self._drop_relation(compat_view)
            compat_stmt = text(
                f"""
                CREATE OR REPLACE VIEW `{compat_view}` AS
                SELECT * FROM `{view}`
                """
            )
            with self.engine.begin() as conn:
                conn.execute(compat_stmt)
            self.logger.info("已创建/更新兼容视图 %s -> %s。", compat_view, view)


def ensure_schema() -> None:
    db_config = DatabaseConfig.from_env()
    bootstrap_writer = MySQLWriter(db_config)
    try:
        SchemaManager(bootstrap_writer.engine, db_name=db_config.db_name).ensure_all()
    finally:
        bootstrap_writer.dispose()
