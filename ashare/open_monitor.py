"""开盘监测：检查“前一交易日收盘信号”在今日开盘是否仍可执行。

目标：
- 读取 strategy_ma5_ma20_signals 中“最新交易日”的 BUY 信号（通常是昨天收盘跑出来的）。
- 在开盘/集合竞价阶段拉取实时行情（今开/最新价），做二次过滤：
  - 高开过多（追高风险/买不到合理价）
  - 低开破位（跌破 MA20 / 大幅低开）
  - 涨停（大概率买不到）

输出：
- 可选写入 MySQL：strategy_ma5_ma20_open_monitor（默认 append）
- 可选导出 CSV 到 output/open_monitor

注意：
- 该脚本“只做监测与清单输出”，不下单。
- 实时行情默认使用 Eastmoney push2 接口；如需测试 AkShare，可在 config.yaml 将 open_monitor.quote_source=akshare。
"""

from __future__ import annotations

import datetime as dt
import logging
import hashlib
import json
import math
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from sqlalchemy import bindparam, text

from .config import get_section
from .db import DatabaseConfig, MySQLWriter
from .utils.convert import to_float as _to_float
from .utils.logger import setup_logger
from .weekly_env_builder import WeeklyEnvironmentBuilder


SNAPSHOT_HASH_EXCLUDE = {
    "checked_at",
    "dedupe_bucket",
}


def _normalize_snapshot_value(value: Any) -> Any:  # noqa: ANN401
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 6)
    if isinstance(value, int):
        return value
    try:
        if hasattr(value, "item"):
            base = value.item()
            if isinstance(base, (float, int)):
                return _normalize_snapshot_value(base)
            return base
    except Exception:
        pass
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, dt.date):
        return value.isoformat()
    return value


def make_snapshot_hash(row: Dict[str, Any]) -> str:
    payload = {}
    for key, value in row.items():
        if key in SNAPSHOT_HASH_EXCLUDE:
            continue
        payload[key] = _normalize_snapshot_value(value)
    serialized = json.dumps(
        payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.md5(serialized.encode("utf-8")).hexdigest()


def _strip_baostock_prefix(code: str) -> str:
    code = str(code or "").strip()
    if code.startswith("sh.") or code.startswith("sz."):
        return code[3:]
    return code


def _to_baostock_code(exchange: str, symbol: str) -> str:
    ex = str(exchange or "").lower().strip()
    sym = str(symbol or "").strip()
    if ex in {"sh", "1"}:
        return f"sh.{sym}"
    if ex in {"sz", "0"}:
        return f"sz.{sym}"
    # fallback：猜测 6/9 为沪，0/3 为深
    if sym.startswith(("6", "9")):
        return f"sh.{sym}"
    return f"sz.{sym}"


def _to_eastmoney_secid(code: str) -> str:
    code = str(code or "").strip()
    if code.startswith("sh."):
        return f"1.{code[3:]}"
    if code.startswith("sz."):
        return f"0.{code[3:]}"
    # 尝试裸代码
    digits = _strip_baostock_prefix(code)
    if digits.startswith(("6", "9")):
        return f"1.{digits}"
    return f"0.{digits}"


@dataclass(frozen=True)
class OpenMonitorParams:
    """开盘监测参数（支持从 config.yaml 的 open_monitor 覆盖）。"""

    enabled: bool = True

    # 信号来源表：默认沿用 MA5-MA20 策略 signals_table
    signals_table: str = "strategy_ma5_ma20_signals"

    # 输出表：开盘检查结果
    output_table: str = "strategy_ma5_ma20_open_monitor"

    # 回看近 N 个交易日的 BUY 信号
    signal_lookback_days: int = 3

    # 行情来源：eastmoney / akshare（兼容：auto 将按 eastmoney 处理）
    quote_source: str = "eastmoney"

    # 候选有效期：回踩形态默认更长
    cross_valid_days: int = 3
    pullback_valid_days: int = 5

    # 核心过滤规则
    max_gap_up_pct: float = 0.05       # 今开相对昨收涨幅 > 5% → skip
    max_gap_up_atr_mult: float = 1.5   # 动态高开阈值：gap_up > min(max_gap_up_pct, atr_mult*ATR/昨收) → skip
    max_gap_down_pct: float = -0.03    # 今开相对昨收跌幅 < -3% → skip
    min_open_vs_ma20_pct: float = 0.0  # 今开 < MA20*(1+min_open_vs_ma20_pct) → skip（默认需站上 MA20）
    pullback_min_open_vs_ma20_pct: float = -0.01  # 回踩形态允许略低于 MA20 入场
    limit_up_trigger_pct: float = 9.7  # 涨跌幅 >= 9.7% → 视为涨停/接近涨停，skip

    # 追高过滤：入场价相对 MA5 乖离过大
    max_entry_vs_ma5_pct: float = 0.08  # 入场价 > MA5*(1+8%) → skip

    # 过期判定：信号后快速拉升
    expire_atr_mult: float = 1.2
    expire_pct_threshold: float = 0.07

    # 风控：开盘入场价为基准的 ATR 止损
    stop_atr_mult: float = 2.0         # trade_stop_ref = entry - stop_atr_mult*ATR

    # 情绪过滤：信号日（昨日收盘）如果是接近涨停的大阳线，次日默认不追
    signal_day_limit_up_pct: float = 0.095  # 9.5%（兼容“接近涨停”）

    # 输出控制
    write_to_db: bool = True

    # 增量写入：
    # - True：每次运行都 append（保留历史快照，便于对比）
    # - False：按 monitor_date+code 先删后写（只保留当天最新一份）
    incremental_write: bool = True

    # 增量导出：文件名带 checked_at 时间戳，避免同一天多次导出互相覆盖
    incremental_export_timestamp: bool = True

    export_csv: bool = True
    export_top_n: int = 100
    output_subdir: str = "open_monitor"
    interval_minutes: int = 5
    dedupe_bucket_minutes: int = 5

    # 环境快照表：存储周线计划等“批次级别”信息，避免在每条标的记录里重复。
    env_snapshot_table: str = "strategy_ma5_ma20_open_monitor_env"

    # 同一批次内同一 code 只保留“最新 date（信号日）”那条 BUY 信号。
    # 目的：避免同一批次出现重复 code（例如同一只股票在 12-09 与 12-11 都触发 BUY）。
    unique_code_latest_date_only: bool = True

    @classmethod
    def from_config(cls) -> "OpenMonitorParams":
        sec = get_section("open_monitor") or {}
        if not isinstance(sec, dict):
            sec = {}

        # 默认 signals_table 与策略保持一致
        strat = get_section("strategy_ma5_ma20_trend") or {}
        if isinstance(strat, dict):
            default_signals = strat.get("signals_table", cls.signals_table)
        else:
            default_signals = cls.signals_table

        logger = logging.getLogger(__name__)

        def _get_bool(key: str, default: bool) -> bool:
            val = sec.get(key, default)
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                return val.strip().lower() in {"1", "true", "yes", "y", "on"}
            return bool(val)

        def _get_float(key: str, default: float) -> float:
            raw = sec.get(key, default)
            parsed = _to_float(raw)
            return default if parsed is None else float(parsed)

        def _get_int(key: str, default: int) -> int:
            raw = sec.get(key, default)
            try:
                return int(raw)
            except Exception:
                return default

        def _normalize_ratio_pct(value: float, key: str) -> float:
            normalized = value / 100.0 if abs(value) > 1.5 else value
            if abs(value) > 1.5:
                logger.info(
                    "配置 %s 以百分数填写（%s），已按比例 %.4f 处理。",
                    key,
                    value,
                    normalized,
                )
            return normalized

        def _normalize_percent_value(value: float, key: str) -> float:
            normalized = value * 100.0 if abs(value) <= 1.5 else value
            if abs(value) <= 1.5:
                logger.info(
                    "配置 %s 以小数比例填写（%s），已按百分数 %.2f%% 处理。",
                    key,
                    value,
                    normalized,
                )
            return normalized

        quote_source = str(sec.get("quote_source", cls.quote_source)).strip().lower() or "auto"
        # 路线A：auto 也按 eastmoney 处理，避免误以为会优先 AkShare
        if quote_source == "auto":
            quote_source = "eastmoney"

        interval_minutes = _get_int("interval_minutes", cls.interval_minutes)

        dedupe_bucket_configured = sec.get("dedupe_bucket_minutes")
        dedupe_bucket_minutes = (
            _get_int("dedupe_bucket_minutes", interval_minutes)
            if dedupe_bucket_configured is not None
            else interval_minutes
        )

        return cls(
            enabled=_get_bool("enabled", cls.enabled),
            signals_table=str(sec.get("signals_table", default_signals)).strip() or default_signals,
            output_table=str(sec.get("output_table", cls.output_table)).strip() or cls.output_table,
            signal_lookback_days=_get_int("signal_lookback_days", cls.signal_lookback_days),
            quote_source=quote_source,
            cross_valid_days=_get_int("cross_valid_days", cls.cross_valid_days),
            pullback_valid_days=_get_int("pullback_valid_days", cls.pullback_valid_days),
            max_gap_up_pct=_normalize_ratio_pct(
                _get_float("max_gap_up_pct", cls.max_gap_up_pct), "max_gap_up_pct"
            ),
            max_gap_up_atr_mult=_get_float("max_gap_up_atr_mult", cls.max_gap_up_atr_mult),
            max_gap_down_pct=_normalize_ratio_pct(
                _get_float("max_gap_down_pct", cls.max_gap_down_pct), "max_gap_down_pct"
            ),
            min_open_vs_ma20_pct=_normalize_ratio_pct(
                _get_float("min_open_vs_ma20_pct", cls.min_open_vs_ma20_pct),
                "min_open_vs_ma20_pct",
            ),
            pullback_min_open_vs_ma20_pct=_normalize_ratio_pct(
                _get_float(
                    "pullback_min_open_vs_ma20_pct", cls.pullback_min_open_vs_ma20_pct
                ),
                "pullback_min_open_vs_ma20_pct",
            ),
            limit_up_trigger_pct=_normalize_percent_value(
                _get_float("limit_up_trigger_pct", cls.limit_up_trigger_pct),
                "limit_up_trigger_pct",
            ),

            max_entry_vs_ma5_pct=_normalize_ratio_pct(
                _get_float("max_entry_vs_ma5_pct", cls.max_entry_vs_ma5_pct),
                "max_entry_vs_ma5_pct",
            ),
            expire_atr_mult=_get_float("expire_atr_mult", cls.expire_atr_mult),
            expire_pct_threshold=_normalize_ratio_pct(
                _get_float("expire_pct_threshold", cls.expire_pct_threshold),
                "expire_pct_threshold",
            ),
            stop_atr_mult=_get_float("stop_atr_mult", cls.stop_atr_mult),
            signal_day_limit_up_pct=_normalize_ratio_pct(
                _get_float("signal_day_limit_up_pct", cls.signal_day_limit_up_pct),
                "signal_day_limit_up_pct",
            ),

            write_to_db=_get_bool("write_to_db", cls.write_to_db),
            incremental_write=_get_bool("incremental_write", cls.incremental_write),
            incremental_export_timestamp=_get_bool(
                "incremental_export_timestamp", cls.incremental_export_timestamp
            ),
            export_csv=_get_bool("export_csv", cls.export_csv),
            export_top_n=_get_int("export_top_n", cls.export_top_n),
            output_subdir=str(sec.get("output_subdir", cls.output_subdir)).strip() or cls.output_subdir,
            interval_minutes=interval_minutes,
            dedupe_bucket_minutes=dedupe_bucket_minutes,
            unique_code_latest_date_only=_get_bool(
                "unique_code_latest_date_only", cls.unique_code_latest_date_only
            ),
            env_snapshot_table=str(
                sec.get("env_snapshot_table", cls.env_snapshot_table)
            ).strip()
            or cls.env_snapshot_table,
        )


class MA5MA20OpenMonitorRunner:
    """开盘监测 Runner：读取前一交易日 BUY 信号 → 拉实时行情 → 输出可执行清单。"""

    def __init__(self) -> None:
        self.logger = setup_logger()
        self.params = OpenMonitorParams.from_config()
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())
        self.volume_ratio_threshold = self._resolve_volume_ratio_threshold()
        app_cfg = get_section("app") or {}
        self.index_codes = []
        if isinstance(app_cfg, dict):
            codes = app_cfg.get("index_codes", [])
            if isinstance(codes, (list, tuple)):
                self.index_codes = [str(c).strip() for c in codes if str(c).strip()]
        ak_cfg = get_section("akshare") or {}
        board_cfg = ak_cfg.get("board_industry", {}) if isinstance(ak_cfg, dict) else {}
        if not isinstance(board_cfg, dict):
            board_cfg = {}
        self.board_env_enabled = bool(board_cfg.get("enabled", False))
        self.board_spot_enabled = bool(board_cfg.get("spot_enabled", True))
        om_cfg = get_section("open_monitor") or {}
        threshold = _to_float(om_cfg.get("env_index_score_threshold"))
        self.env_index_score_threshold = threshold if threshold is not None else 2.0
        weekly_strength_threshold = _to_float(
            om_cfg.get("weekly_soft_gate_strength_threshold")
        )
        self.weekly_soft_gate_strength_threshold = (
            weekly_strength_threshold if weekly_strength_threshold is not None else 3.5
        )
        self.env_builder = WeeklyEnvironmentBuilder(
            db_writer=self.db_writer,
            logger=self.logger,
            index_codes=self.index_codes,
            board_env_enabled=self.board_env_enabled,
            board_spot_enabled=self.board_spot_enabled,
            env_index_score_threshold=self.env_index_score_threshold,
            weekly_soft_gate_strength_threshold=self.weekly_soft_gate_strength_threshold,
        )

    def _resolve_volume_ratio_threshold(self) -> float:
        strat = get_section("strategy_ma5_ma20_trend") or {}
        if isinstance(strat, dict):
            raw = strat.get("volume_ratio_threshold")
            parsed = _to_float(raw)
            if parsed is not None and parsed > 0:
                return float(parsed)
        return 1.5

    def _load_trading_calendar(self, start: dt.date, end: dt.date) -> bool:
        """加载并缓存交易日历，避免节假日误判。"""

        return self.env_builder.load_trading_calendar(start, end)

    @staticmethod
    def _calc_minutes_elapsed(now: dt.datetime) -> int:
        """计算当日已过去的交易分钟数（含午休处理）。"""

        start_am = dt.time(9, 30)
        end_am = dt.time(11, 30)
        start_pm = dt.time(13, 0)
        end_pm = dt.time(15, 0)

        t = now.time()
        if t < start_am:
            return 0
        if t <= end_am:
            delta = dt.datetime.combine(now.date(), t) - dt.datetime.combine(
                now.date(), start_am
            )
            return int(delta.total_seconds() // 60)
        if t < start_pm:
            return 120
        if t <= end_pm:
            delta = dt.datetime.combine(now.date(), t) - dt.datetime.combine(
                now.date(), start_pm
            )
            return 120 + int(delta.total_seconds() // 60)
        return 240

    def _calc_dedupe_bucket(self, ts: dt.datetime) -> str:
        bucket_minutes = max(int(self.params.dedupe_bucket_minutes or 5), 1)

        auction_start = dt.time(9, 15)
        lunch_break_start = dt.time(11, 30)
        lunch_break_end = dt.time(13, 0)
        market_close = dt.time(15, 0)

        t = ts.time()
        if t < auction_start:
            return "PREOPEN"
        if lunch_break_start <= t < lunch_break_end:
            return "BREAK"
        if t >= market_close:
            return "POSTCLOSE"

        minute_of_day = ts.hour * 60 + ts.minute
        bucket_minute = (minute_of_day // bucket_minutes) * bucket_minutes
        bucket_time = dt.datetime.combine(
            ts.date(), dt.time(bucket_minute // 60, bucket_minute % 60)
        )
        return bucket_time.strftime("%Y-%m-%d %H:%M")

    def _load_avg_volume(
        self, latest_trade_date: str, codes: List[str], window: int = 20
    ) -> Dict[str, float]:
        if not latest_trade_date or not codes:
            return {}

        table = self._daily_table()
        if not self._table_exists(table):
            return {}

        try:
            end_date = pd.to_datetime(latest_trade_date).date()
        except Exception:
            return {}

        start_date = end_date - dt.timedelta(days=max(window * 4, 60))
        stmt = text(
            f"""
            SELECT `date`,`code`,`volume`
            FROM `{table}`
            WHERE `code` IN :codes AND `date` BETWEEN :start_date AND :end_date
            """
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "codes": codes,
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取历史成交量失败，将跳过盘中量比：%s", exc)
            return {}

        if df.empty or "volume" not in df.columns:
            return {}

        df["code"] = df["code"].astype(str)
        df["volume"] = df["volume"].apply(_to_float)
        avg_map: Dict[str, float] = {}
        for code, grp in df.groupby("code", sort=False):
            top = grp.sort_values("date", ascending=False).head(window)
            volumes = top["volume"].dropna()
            if not volumes.empty:
                avg_map[code] = float(volumes.mean())
        return avg_map

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        if not self._table_exists(table) or self._column_exists(table, column):
            return

        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE `{table}` ADD COLUMN `{column}` {definition}"))
            self.logger.info("表 %s 已新增列 %s。", table, column)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("为表 %s 添加列 %s 失败：%s", table, column, exc)

    def _ensure_datetime_column(self, table: str, column: str) -> None:
        if not self._table_exists(table):
            return

        if not self._column_exists(table, column):
            try:
                with self.db_writer.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"ALTER TABLE `{table}` ADD COLUMN `{column}` DATETIME(6) NULL"
                        )
                    )
                self.logger.info("表 %s 已新增 DATETIME(6) 列 %s。", table, column)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("为表 %s 添加 DATETIME(6) 列 %s 失败：%s", table, column, exc)
            return

        data_type = ""
        column_type = ""
        try:
            stmt = text(
                """
                SELECT DATA_TYPE, COLUMN_TYPE
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table AND column_name = :column
                """
            )
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "schema": self.db_writer.config.db_name,
                        "table": table,
                        "column": column,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取列 %s.%s 类型失败：%s", table, column, exc)
            return

        if not df.empty:
            data_type = str(df.iloc[0].get("DATA_TYPE") or "").lower()
            column_type = str(df.iloc[0].get("COLUMN_TYPE") or "").lower()

        if data_type == "datetime" and "datetime(6)" in column_type:
            return

        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(
                    text(
                        f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` DATETIME(6) NULL"
                    )
                )
            self.logger.info(
                "表 %s.%s 列已转换为 DATETIME(6)，保证时间排序正确。", table, column
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "将列 %s.%s 转为 DATETIME(6) 失败（当前类型：%s），保留原类型：%s",
                table,
                column,
                column_type or "unknown",
                exc,
            )

    def _ensure_numeric_column(self, table: str, column: str, definition: str) -> None:
        if not self._table_exists(table):
            return

        if not self._column_exists(table, column):
            self._ensure_column(table, column, definition)
            return

        data_type = ""
        column_type = ""
        try:
            stmt = text(
                """
                SELECT DATA_TYPE, COLUMN_TYPE
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table AND column_name = :column
                """
            )
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "schema": self.db_writer.config.db_name,
                        "table": table,
                        "column": column,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取列 %s.%s 类型失败：%s", table, column, exc)
            return

        if not df.empty:
            data_type = str(df.iloc[0].get("DATA_TYPE") or "").lower()
            column_type = str(df.iloc[0].get("COLUMN_TYPE") or "").lower()

        numeric_types = {
            "double",
            "float",
            "decimal",
            "int",
            "bigint",
            "smallint",
            "tinyint",
        }

        if data_type in numeric_types:
            return

        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` {definition}"))
            self.logger.info(
                "表 %s.%s 列已调整为数值列 %s，避免类型漂移。",
                table,
                column,
                definition,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "调整列 %s.%s 为数值列失败（当前类型：%s），保留原类型：%s",
                table,
                column,
                column_type or "unknown",
                exc,
            )

    # -------------------------
    # DB helpers
    # -------------------------
    def _table_exists(self, table: str) -> bool:
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(text("SHOW TABLES LIKE :t"), conn, params={"t": table})
            return not df.empty
        except Exception:
            return False

    def _column_exists(self, table: str, column: str) -> bool:
        try:
            stmt = text(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table AND column_name = :column
                """
            )
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "schema": self.db_writer.config.db_name,
                        "table": table,
                        "column": column,
                    },
                )
            return not df.empty and bool(df.iloc[0].get("cnt", 0))
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("检查列 %s.%s 是否存在失败：%s", table, column, exc)
            return False

    def _index_exists(self, table: str, index: str) -> bool:
        try:
            stmt = text(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.statistics
                WHERE table_schema = :schema AND table_name = :table AND index_name = :index
                """
            )
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "schema": self.db_writer.config.db_name,
                        "table": table,
                        "index": index,
                    },
                )
            return not df.empty and bool(df.iloc[0].get("cnt", 0))
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("检查索引 %s.%s 是否存在失败：%s", table, index, exc)
            return False

    def _ensure_varchar_column(self, table: str, column: str, length: int) -> None:
        if not self._column_exists(table, column):
            return

        # utf8mb4 下单列 VARCHAR 最大约 16383 字符（受 65535 bytes 行大小限制影响）
        MYSQL_SAFE_VARCHAR_MAX = 16383

        try:
            stmt = text(
                """
                SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_TYPE
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table AND column_name = :column
                """
            )
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "schema": self.db_writer.config.db_name,
                        "table": table,
                        "column": column,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取列 %s.%s 元数据失败：%s", table, column, exc)
            return

        if df.empty:
            return

        row = df.iloc[0]
        data_type = str(row.get("DATA_TYPE") or "").lower()
        char_len = row.get("CHARACTER_MAXIMUM_LENGTH")
        column_type = str(row.get("COLUMN_TYPE") or "").lower()

        # information_schema 对 TEXT/LONGTEXT 会给出 65535 等“理论最大长度”，
        # 但这不代表能安全改成 VARCHAR(65535)（utf8mb4 下会触发 1074）。
        is_text_like = (
            ("text" in data_type)
            or ("blob" in data_type)
            or ("text" in column_type)
            or ("blob" in column_type)
        )
        is_varchar_like = data_type in {"varchar", "char"}

        # DATE/INT 等非字符串列没必要强行转 VARCHAR，且可能破坏语义。
        if not (is_text_like or is_varchar_like):
            return

        current_len = int(char_len or 0)
        safe_len = min(int(length), MYSQL_SAFE_VARCHAR_MAX)

        if safe_len <= 0:
            return

        if is_text_like:
            target_len = safe_len
        else:
            if current_len >= safe_len:
                return
            target_len = min(max(safe_len, current_len), MYSQL_SAFE_VARCHAR_MAX)

        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(
                    text(
                        f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` VARCHAR({target_len})"
                    )
                )
            self.logger.info(
                "表 %s.%s 列已调整为 VARCHAR(%s) 以支持索引。",
                table,
                column,
                target_len,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "调整列 %s.%s 类型为 VARCHAR 失败：%s", table, column, exc
            )

    def _cleanup_duplicate_snapshots(self, table: str) -> None:
        """清理历史重复快照，确保唯一索引可创建（仅保留最新 checked_at）。"""

        required_cols = {
            "monitor_date",
            "sig_date",
            "code",
            "dedupe_bucket",
            "checked_at",
        }
        for col in required_cols:
            if not self._column_exists(table, col):
                return

        try:
            with self.db_writer.engine.begin() as conn:
                res = conn.execute(
                    text(
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
                )
            # pymysql 对 DELETE 可能返回 -1，这里只做“有变化”提示
            if getattr(res, "rowcount", 0) and res.rowcount > 0:
                self.logger.info(
                    "表 %s 已清理 %s 条重复快照（保留最新 checked_at）。",
                    table,
                    res.rowcount,
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("清理表 %s 重复快照失败：%s", table, exc)

    def _ensure_indexable_columns(self, table: str) -> None:
        dedupe_columns = {
            "monitor_date": 32,
            "sig_date": 32,
            "code": 32,
            "snapshot_hash": 64,
            "dedupe_bucket": 32,
        }

        for column, length in dedupe_columns.items():
            self._ensure_varchar_column(table, column, length)

    def _ensure_snapshot_schema(self, table: str) -> None:
        if not table or not self._table_exists(table):
            return

        self._ensure_datetime_column(table, "checked_at")

        if not self._column_exists(table, "snapshot_hash"):
            try:
                with self.db_writer.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"ALTER TABLE `{table}` ADD COLUMN `snapshot_hash` VARCHAR(64)"
                        )
                    )
                self.logger.info("表 %s 已新增 snapshot_hash 列用于去重。", table)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("为表 %s 添加 snapshot_hash 列失败：%s", table, exc)

        if not self._column_exists(table, "dedupe_bucket"):
            try:
                with self.db_writer.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"ALTER TABLE `{table}` ADD COLUMN `dedupe_bucket` VARCHAR(32)"
                        )
                    )
                self.logger.info("表 %s 已新增 dedupe_bucket 列用于时间桶去重。", table)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("为表 %s 添加 dedupe_bucket 列失败：%s", table, exc)

        self._ensure_indexable_columns(table)

        # live_intraday_vol_ratio 在首次写入时可能因 dtype/object 被建成 TEXT，
        # 会导致 SQL 侧的筛选/排序出现隐式转换或字符串比较问题。
        # 这里强制修正为数值列，保持与代码中的 float 计算一致。
        self._ensure_numeric_column(table, "live_intraday_vol_ratio", "DOUBLE NULL")

        index_name = "ux_open_monitor_dedupe"
        if not self._index_exists(table, index_name):
            self._cleanup_duplicate_snapshots(table)

            try:
                with self.db_writer.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"""
                            CREATE UNIQUE INDEX `{index_name}`
                            ON `{table}` (`monitor_date`, `sig_date`, `code`, `dedupe_bucket`)
                            """
                        )
                    )
                self.logger.info("表 %s 已创建唯一索引 %s 用于幂等写入。", table, index_name)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("创建唯一索引 %s 失败：%s", index_name, exc)

    def _ensure_strength_schema(self, table: str) -> None:
        if not table or not self._table_exists(table):
            return

        self._ensure_datetime_column(table, "checked_at")

        self._ensure_numeric_column(table, "signal_strength", "DOUBLE NULL")
        self._ensure_numeric_column(table, "strength_delta", "DOUBLE NULL")
        self._ensure_column(table, "strength_trend", "VARCHAR(16) NULL")
        self._ensure_column(table, "strength_note", "VARCHAR(512) NULL")

    def _ensure_env_snapshot_schema(self, table: str) -> None:
        if not table:
            return

        if not self._table_exists(table):
            ddl = text(
                f"""
                CREATE TABLE `{table}` (
                    `monitor_date` VARCHAR(10) NOT NULL,
                    `dedupe_bucket` VARCHAR(32) NOT NULL,
                    `checked_at` DATETIME NULL,
                    `env_weekly_asof_trade_date` VARCHAR(10) NULL,
                    `env_weekly_risk_level` VARCHAR(16) NULL,
                    `env_weekly_scene` VARCHAR(32) NULL,
                    `env_weekly_gate_policy` VARCHAR(16) NULL,
                    `env_weekly_plan_json` TEXT NULL,
                    `env_weekly_plan_a` VARCHAR(255) NULL,
                    `env_weekly_plan_b` VARCHAR(255) NULL,
                    `env_weekly_plan_a_exposure_cap` DOUBLE NULL,
                    `env_weekly_bias` VARCHAR(16) NULL,
                    `env_weekly_status` VARCHAR(32) NULL,
                    `env_weekly_gating_enabled` TINYINT(1) NULL,
                    `env_weekly_tags` VARCHAR(255) NULL,
                    `env_weekly_money_proxy` VARCHAR(255) NULL,
                    `env_weekly_note` VARCHAR(255) NULL,
                    `env_regime` VARCHAR(32) NULL,
                    `env_position_hint` DOUBLE NULL,
                    PRIMARY KEY (`monitor_date`, `dedupe_bucket`)
                )
                """
            )
            try:
                with self.db_writer.engine.begin() as conn:
                    conn.execute(ddl)
                self.logger.info("已创建环境快照表 %s。", table)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("创建环境快照表 %s 失败：%s", table, exc)
                return

        self._ensure_datetime_column(table, "checked_at")
        for col, ddl in {
            "env_weekly_plan_a": "VARCHAR(255)",
            "env_weekly_plan_b": "VARCHAR(255)",
            "env_weekly_plan_a_exposure_cap": "DOUBLE",
            "env_weekly_tags": "VARCHAR(255)",
            "env_weekly_money_proxy": "VARCHAR(255)",
            "env_weekly_note": "VARCHAR(255)",
            "env_regime": "VARCHAR(32)",
            "env_position_hint": "DOUBLE",
            "env_weekly_gating_enabled": "TINYINT(1)",
            "env_weekly_gate_policy": "VARCHAR(16)",
        }.items():
            self._ensure_column(table, col, f"{ddl} NULL")

    def _persist_env_snapshot(
        self,
        env_context: dict[str, Any] | None,
        monitor_date: str,
        dedupe_bucket: str,
        checked_at: dt.datetime,
        env_weekly_gate_policy: str | None,
    ) -> None:
        if not env_context:
            return

        table = self.params.env_snapshot_table
        if not table:
            return

        payload = {}
        weekly_scenario = (
            env_context.get("weekly_scenario") if isinstance(env_context, dict) else {}
        )
        if not isinstance(weekly_scenario, dict):
            weekly_scenario = {}

        def _is_empty_env_value(value: Any) -> bool:
            if value is None:
                return True
            if isinstance(value, str):
                return value == ""
            if isinstance(value, dict):
                return len(value) == 0
            if isinstance(value, (list, tuple, set)):
                return len(value) == 0

            size = getattr(value, "size", None)
            if isinstance(size, int):
                return size == 0

            try:
                return len(value) == 0  # type: ignore[arg-type]
            except Exception:
                return False

        def _get_env(key: str) -> Any:  # noqa: ANN401
            if isinstance(env_context, dict):
                value = env_context.get(key, None)
                if not _is_empty_env_value(value):
                    return value
            return weekly_scenario.get(key)

        payload["monitor_date"] = monitor_date
        payload["dedupe_bucket"] = dedupe_bucket
        payload["checked_at"] = checked_at
        payload["env_weekly_asof_trade_date"] = _get_env("weekly_asof_trade_date")
        payload["env_weekly_risk_level"] = _get_env("weekly_risk_level")
        payload["env_weekly_scene"] = _get_env("weekly_scene_code")
        payload["env_weekly_gate_policy"] = env_weekly_gate_policy
        payload["env_weekly_plan_json"] = _get_env("weekly_plan_json")
        payload["env_weekly_plan_a"] = _get_env("weekly_plan_a")
        payload["env_weekly_plan_b"] = _get_env("weekly_plan_b")
        payload["env_weekly_plan_a_exposure_cap"] = _to_float(
            _get_env("weekly_plan_a_exposure_cap")
        )
        payload["env_weekly_bias"] = _get_env("weekly_bias")
        payload["env_weekly_status"] = _get_env("weekly_status")
        payload["env_weekly_gating_enabled"] = bool(
            _get_env("weekly_gating_enabled")
        )
        payload["env_weekly_tags"] = _get_env("weekly_tags")
        payload["env_weekly_money_proxy"] = _get_env("weekly_money_proxy")
        payload["env_weekly_note"] = _get_env("weekly_note")
        payload["env_regime"] = _get_env("regime")
        payload["env_position_hint"] = _to_float(_get_env("position_hint"))

        self._ensure_env_snapshot_schema(table)
        columns = list(payload.keys())
        update_cols = [c for c in columns if c not in {"monitor_date", "dedupe_bucket"}]
        col_clause = ", ".join(f"`{c}`" for c in columns)
        value_clause = ", ".join(f":{c}" for c in columns)
        update_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)
        stmt = text(
            f"""
            INSERT INTO `{table}` ({col_clause})
            VALUES ({value_clause})
            ON DUPLICATE KEY UPDATE {update_clause}
            """
        )

        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(stmt, payload)
            self.logger.info(
                "环境快照已写入表 %s（monitor_date=%s, dedupe_bucket=%s）",
                table,
                monitor_date,
                dedupe_bucket,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("写入环境快照失败：%s", exc)

    def _load_env_snapshot_context(
        self, monitor_date: str, dedupe_bucket: str | None = None
    ) -> dict[str, Any] | None:
        table = self.params.env_snapshot_table
        if not (table and monitor_date and self._table_exists(table)):
            return None

        self._ensure_env_snapshot_schema(table)

        def _load_latest(match_bucket: bool) -> pd.DataFrame:
            bucket_clause = "`dedupe_bucket` = :b AND" if match_bucket else ""
            stmt = text(
                f"""
                SELECT * FROM `{table}`
                WHERE {bucket_clause} `monitor_date` = :d
                ORDER BY `checked_at` DESC
                LIMIT 1
                """
            )

            params: dict[str, Any] = {"d": monitor_date}
            if match_bucket and dedupe_bucket is not None:
                params["b"] = dedupe_bucket
            elif match_bucket:
                return pd.DataFrame()

            with self.db_writer.engine.begin() as conn:
                return pd.read_sql_query(stmt, conn, params=params)

        try:
            if dedupe_bucket is None:
                df = _load_latest(match_bucket=False)
            else:
                df = _load_latest(match_bucket=True)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取环境快照失败：%s", exc)
            return None

        if df.empty:
            return None

        row = df.iloc[0]
        weekly_scenario = {
            "weekly_asof_trade_date": row.get("env_weekly_asof_trade_date"),
            "weekly_risk_level": row.get("env_weekly_risk_level"),
            "weekly_scene_code": row.get("env_weekly_scene"),
            "weekly_plan_json": row.get("env_weekly_plan_json"),
            "weekly_plan_a": row.get("env_weekly_plan_a"),
            "weekly_plan_b": row.get("env_weekly_plan_b"),
            "weekly_plan_a_exposure_cap": row.get("env_weekly_plan_a_exposure_cap"),
            "weekly_bias": row.get("env_weekly_bias"),
            "weekly_status": row.get("env_weekly_status"),
            "weekly_gating_enabled": row.get("env_weekly_gating_enabled"),
            "weekly_tags": row.get("env_weekly_tags"),
            "weekly_money_proxy": row.get("env_weekly_money_proxy"),
            "weekly_note": row.get("env_weekly_note"),
        }

        env_context: dict[str, Any] = {
            "weekly_scenario": weekly_scenario,
            "weekly_asof_trade_date": weekly_scenario.get("weekly_asof_trade_date"),
            "weekly_risk_level": weekly_scenario.get("weekly_risk_level"),
            "weekly_scene_code": weekly_scenario.get("weekly_scene_code"),
            "weekly_plan_json": weekly_scenario.get("weekly_plan_json"),
            "weekly_plan_a": weekly_scenario.get("weekly_plan_a"),
            "weekly_plan_b": weekly_scenario.get("weekly_plan_b"),
            "weekly_plan_a_exposure_cap": weekly_scenario.get(
                "weekly_plan_a_exposure_cap"
            ),
            "weekly_bias": weekly_scenario.get("weekly_bias"),
            "weekly_status": weekly_scenario.get("weekly_status"),
            "weekly_gating_enabled": bool(
                weekly_scenario.get("weekly_gating_enabled", False)
            ),
            "weekly_tags": weekly_scenario.get("weekly_tags"),
            "weekly_money_proxy": weekly_scenario.get("weekly_money_proxy"),
            "weekly_note": weekly_scenario.get("weekly_note"),
            "regime": row.get("env_regime"),
            "position_hint": row.get("env_position_hint"),
            "weekly_gate_policy": (
                row.get("env_weekly_gate_policy")
                or row.get("env_weekly_gate_action")
            ),
            "dedupe_bucket": row.get("dedupe_bucket"),
        }

        return env_context

    def load_env_snapshot_context(
        self,
        monitor_date: str,
        dedupe_bucket: str | None = None,
        *,
        allow_fallback: bool = True,
    ) -> dict[str, Any] | None:
        """公开的环境快照读取接口，必要时可回退到当日最新。"""

        env_context = self._load_env_snapshot_context(monitor_date, dedupe_bucket)
        if env_context:
            return env_context

        if allow_fallback and dedupe_bucket is not None:
            return self._load_env_snapshot_context(monitor_date, None)

        return None

    def _resolve_env_weekly_gate_policy(
        self, env_context: dict[str, Any] | None
    ) -> str | None:
        return self.env_builder.resolve_env_weekly_gate_policy(env_context)

    def _ensure_monitor_columns(self, table: str) -> None:
        if not table or not self._table_exists(table):
            return

        extra_columns = {
            "env_index_score": "DOUBLE",
            "env_regime": "VARCHAR(32)",
            "env_position_hint": "DOUBLE",
            "env_weekly_asof_trade_date": "VARCHAR(10)",
            "env_weekly_risk_level": "VARCHAR(16)",
            "env_weekly_scene": "VARCHAR(32)",
            "env_weekly_gate_action": "VARCHAR(16)",
            "risk_tag": "VARCHAR(255)",
            "risk_note": "VARCHAR(255)",
            "entry_exposure_cap": "DOUBLE",
            "sig_date": "VARCHAR(10)",
            "asof_trade_date": "VARCHAR(10)",
            "signal_kind": "VARCHAR(16)",
            "sig_close": "DOUBLE",
            "asof_close": "DOUBLE",
            "sig_ma5": "DOUBLE",
            "sig_ma20": "DOUBLE",
            "sig_ma60": "DOUBLE",
            "sig_ma250": "DOUBLE",
            "sig_vol_ratio": "DOUBLE",
            "sig_macd_hist": "DOUBLE",
            "sig_atr14": "DOUBLE",
            "sig_stop_ref": "DOUBLE",
            "sig_signal": "VARCHAR(16)",
            "sig_reason": "VARCHAR(255)",
            "asof_ma5": "DOUBLE",
            "asof_ma20": "DOUBLE",
            "asof_ma60": "DOUBLE",
            "asof_ma250": "DOUBLE",
            "asof_vol_ratio": "DOUBLE",
            "asof_macd_hist": "DOUBLE",
            "asof_atr14": "DOUBLE",
            "asof_stop_ref": "DOUBLE",
            "trade_stop_ref": "DOUBLE",
            "stop_loss_ref": "DOUBLE",
            "sig_kdj_k": "DOUBLE",
            "sig_kdj_d": "DOUBLE",
        }

        with self.db_writer.engine.begin() as conn:
            for col, ddl in extra_columns.items():
                if self._column_exists(table, col):
                    continue
                try:
                    conn.execute(text(f"ALTER TABLE `{table}` ADD COLUMN `{col}` {ddl}"))
                    self.logger.info("表 %s 已新增列 %s", table, col)
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("为表 %s 添加列 %s 失败：%s", table, col, exc)

        self._ensure_indexable_columns(table)

        index_name = "idx_open_monitor_strength_time"
        if not self._index_exists(table, index_name):
            try:
                with self.db_writer.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"CREATE INDEX `{index_name}` ON `{table}` (`monitor_date`, `code`, `checked_at`)"
                        )
                    )
                self.logger.info("表 %s 已新增索引 %s 加速强度历史查询。", table, index_name)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("创建索引 %s 失败：%s", index_name, exc)

        code_time_index = "idx_open_monitor_code_time"
        if not self._index_exists(table, code_time_index):
            try:
                with self.db_writer.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"CREATE INDEX `{code_time_index}` ON `{table}` (`code`, `checked_at`)"
                        )
                    )
                self.logger.info("表 %s 已新增索引 %s 以优化跨日强度查询。", table, code_time_index)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("创建索引 %s 失败：%s", code_time_index, exc)

    def _load_existing_snapshot_keys(
        self, table: str, monitor_date: str, codes: List[str], dedupe_bucket: str
    ) -> set[tuple[str, str, str, str]]:
        if not (
            table
            and monitor_date
            and codes
            and dedupe_bucket
            and self._table_exists(table)
        ):
            return set()

        stmt = text(
            f"""
            SELECT `monitor_date`, `sig_date`, `code`, `dedupe_bucket`
            FROM `{table}`
            WHERE `monitor_date` = :d AND `code` IN :codes AND `dedupe_bucket` = :b
        """
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={"d": monitor_date, "codes": codes, "b": dedupe_bucket},
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取已存在的 dedupe keys 失败：%s", exc)
            return set()

        existing: set[tuple[str, str, str, str]] = set()
        for _, row in df.iterrows():
            code = str(row.get("code") or "").strip()
            date_val = str(row.get("sig_date") or "").strip()
            monitor = str(row.get("monitor_date") or "").strip()
            bucket = str(row.get("dedupe_bucket") or "").strip()
            if code and date_val and monitor and bucket:
                existing.add((monitor, date_val, code, bucket))
        return existing

    def _delete_existing_bucket_rows(
        self, table: str, monitor_date: str, dedupe_bucket: str, codes: List[str]
    ) -> int:
        if not (
            table
            and monitor_date
            and dedupe_bucket
            and codes
            and self._table_exists(table)
        ):
            return 0

        stmt = text(
            "DELETE FROM `{table}` WHERE `monitor_date` = :d AND `dedupe_bucket` = :b AND `code` IN :codes".format(
                table=table
            )
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.db_writer.engine.begin() as conn:
                result = conn.execute(
                    stmt, {"d": monitor_date, "b": dedupe_bucket, "codes": codes}
                )
                return int(getattr(result, "rowcount", 0) or 0)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("覆盖写入时清理旧快照失败：%s", exc)
            return 0

    def _daily_table(self) -> str:
        """获取日线数据表名（用于补充计算“信号日涨幅”等信息）。"""

        strat = get_section("strategy_ma5_ma20_trend") or {}
        if isinstance(strat, dict):
            name = str(strat.get("daily_table") or "").strip()
            if name:
                return name
        return "history_daily_kline"

    def _load_signal_day_pct_change(self, signal_date: str, codes: List[str]) -> Dict[str, float]:
        """补充“信号日涨幅”（close vs 前一交易日 close）。

        用途：识别“信号日接近涨停/情绪极端”的场景，避免次日开盘追高。
        """

        if not signal_date or not codes:
            return {}

        daily = self._daily_table()
        if not self._table_exists(daily):
            return {}

        stmt = text(
            f"""
            SELECT `code`, `close`, `prev_close`
            FROM (
              SELECT
                `code`, `date`, `close`,
                LAG(`close`) OVER (PARTITION BY `code` ORDER BY `date`) AS `prev_close`
              FROM `{daily}`
              WHERE `code` IN :codes AND `date` <= :d
            ) t
            WHERE `date` = :d
            """
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": signal_date, "codes": codes})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取 %s 信号日涨幅失败（将跳过）：%s", daily, exc)
            return {}

        if df is None or df.empty:
            return {}

        out: Dict[str, float] = {}
        for _, row in df.iterrows():
            code = str(row.get("code") or "").strip()
            close = _to_float(row.get("close"))
            prev_close = _to_float(row.get("prev_close"))
            if not code or close is None or prev_close is None or prev_close <= 0:
                continue
            out[code] = (close - prev_close) / prev_close

        return out

    def _get_table_columns(self, table: str) -> List[str]:
        stmt = text(
            """
            SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t
            """
        )
        try:
            with self.db_writer.engine.begin() as conn:
                return pd.read_sql(stmt, conn, params={"t": table})["COLUMN_NAME"].tolist()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("无法获取 %s 列信息：%s", table, exc)
            return []

    def _is_trading_day(self, date_str: str, latest_trade_date: str | None = None) -> bool:
        """粗略判断是否为交易日（优先用日线表，其次用工作日）。"""

        try:
            d = dt.datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
        except Exception:  # noqa: BLE001
            return False

        if d.weekday() >= 5:
            return False

        target_str = d.isoformat()
        start = d - dt.timedelta(days=400)
        success = self._load_trading_calendar(start, d)
        calendar_range = self.env_builder.calendar_range
        if success and calendar_range and calendar_range[0] <= d <= calendar_range[1]:
            return target_str in self.env_builder.calendar_cache

        daily = self._daily_table()
        if not self._table_exists(daily):
            return True

        stmt = text(f"SELECT 1 FROM `{daily}` WHERE `date` = :d LIMIT 1")
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": str(d)})
            if not df.empty:
                return True
            if latest_trade_date and target_str > str(latest_trade_date)[:10]:
                return True
            return False
        except Exception:  # noqa: BLE001
            if latest_trade_date and target_str > str(latest_trade_date)[:10]:
                return True
            return False

    def _load_trade_age_map(
        self, latest_trade_date: str, min_date: str, monitor_date: str | None
    ) -> Dict[str, int]:
        """返回 {date_str: trading_day_age}，0 表示监控基准日。"""

        base_date = latest_trade_date
        monitor_str = str(monitor_date or "").strip()
        if monitor_str and monitor_str > latest_trade_date and self._is_trading_day(monitor_str, latest_trade_date):
            base_date = monitor_str

        daily = self._daily_table()
        if not self._table_exists(daily):
            # 兜底：没有日线表时，只能用 monitor_str 作为 age=0 的基准
            return {monitor_str: 0} if monitor_str else {}

        stmt = text(
            f"""
            SELECT DISTINCT CAST(`date` AS CHAR) AS d
            FROM `{daily}`
            WHERE `date` <= :base_date AND `date` >= :min_date
            ORDER BY `date` DESC
            """
        )
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"base_date": base_date, "min_date": min_date})
        except Exception:
            df = None

        dates = df["d"].dropna().astype(str).str[:10].tolist() if df is not None else []
        # 只在“确认为交易日/工作日盘中”时插入，避免周末误跑把周末插进去
        if (
            monitor_str
            and monitor_str not in dates
            and monitor_str > latest_trade_date
            and self._is_trading_day(monitor_str, latest_trade_date)
        ):
            dates.insert(0, monitor_str)

        if not dates:
            return {}

        return {d: i for i, d in enumerate(dates)}

    def _load_recent_buy_signals(self) -> Tuple[str | None, List[str], pd.DataFrame]:
        table = self.params.signals_table
        monitor_date = dt.date.today().isoformat()
        lookback = max(
            int(self.params.signal_lookback_days or 0),
            int(self.params.cross_valid_days or 0),
            int(self.params.pullback_valid_days or 0),
            1,
        )

        try:
            with self.db_writer.engine.begin() as conn:
                max_df = pd.read_sql_query(
                    text(f"SELECT MAX(`date`) AS max_date FROM `{table}`"),
                    conn,
                )
                dates_df = pd.read_sql_query(
                    text(
                        f"""
                        SELECT DISTINCT `date`
                        FROM `{table}`
                        WHERE `signal` = 'BUY'
                        ORDER BY `date` DESC
                        LIMIT :n
                        """
                    ),
                    conn,
                    params={"n": lookback},
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.error("读取 signals_table=%s 失败：%s", table, exc)
            return None, [], pd.DataFrame()

        if max_df.empty:
            return None, [], pd.DataFrame()

        max_date = max_df.iloc[0].get("max_date")
        if pd.isna(max_date) or not str(max_date).strip():
            return None, [], pd.DataFrame()

        latest_trade_date = str(max_date)[:10]
        if dates_df.empty:
            self.logger.info("%s 没有任何 BUY 信号，跳过开盘监测。", latest_trade_date)
            return latest_trade_date, [], pd.DataFrame()

        signal_dates = [str(v)[:10] for v in dates_df["date"].tolist() if str(v).strip()]
        self.logger.info(
            "回看最近 %s 个交易日（最新=%s）BUY 信号：%s", lookback, latest_trade_date, signal_dates
        )

        available_cols = set(self._get_table_columns(table))
        base_cols = [
            "date",
            "code",
            "close",
            "ma5",
            "ma20",
            "ma60",
            "ma250",
            "vol_ratio",
            "macd_hist",
            "kdj_k",
            "kdj_d",
            "atr14",
            "stop_ref",
            "signal",
            "reason",
        ]
        optional_cols = [
            "ret_10",
            "ret_20",
            "limit_up_cnt_20",
            "ma20_bias",
            "risk_tag",
            "risk_note",
        ]
        select_cols = base_cols + [c for c in optional_cols if c in available_cols]
        col_sql = ",".join([f"`{c}`" for c in select_cols])

        stmt = text(
            f"""
            SELECT {col_sql}
            FROM `{table}`
            WHERE `date` IN :dates AND `signal` = 'BUY'
            """
        ).bindparams(bindparam("dates", expanding=True))

        with self.db_writer.engine.begin() as conn:
            try:
                df = pd.read_sql_query(stmt, conn, params={"dates": signal_dates})
            except Exception as exc:  # noqa: BLE001
                self.logger.error("读取 %s BUY 信号失败：%s", table, exc)
                return latest_trade_date, signal_dates, pd.DataFrame()

        if df.empty:
            self.logger.info("%s 内无 BUY 信号，跳过开盘监测。", signal_dates)
            return latest_trade_date, signal_dates, df

        for col in optional_cols:
            if col not in df.columns:
                df[col] = None

        df["code"] = df["code"].astype(str)
        df["sig_date"] = df["date"].astype(str).str[:10]

        # 严格去重：同一 code 只保留最新信号日（date）那条记录。
        # 这能避免同一批次 open_monitor 出现重复 code（但信号日/入选原因不同）的情况。
        if self.params.unique_code_latest_date_only:
            before = len(df)
            df["_date_dt"] = pd.to_datetime(df["sig_date"], errors="coerce")
            df = df.sort_values(by=["code", "_date_dt"], ascending=[True, False])
            df = df.drop_duplicates(subset=["code"], keep="first")
            df = df.drop(columns=["_date_dt"], errors="ignore")
            dropped = before - len(df)
            if dropped > 0:
                self.logger.info(
                    "同一 code 多次触发 BUY：已按最新信号日去重 %s 条（保留 %s 条）。",
                    dropped,
                    len(df),
                )
            # 同步更新 signal_dates（仅用于日志展示/后续涨跌幅回补循环），避免误解。
            signal_dates = sorted(df["sig_date"].dropna().unique().tolist(), reverse=True)
        min_date = df["sig_date"].min()
        trade_age_map = self._load_trade_age_map(latest_trade_date, str(min_date), monitor_date)
        df["signal_age"] = df["sig_date"].map(trade_age_map)

        try:
            for d in signal_dates:
                codes = df.loc[df["sig_date"] == d, "code"].dropna().unique().tolist()
                pct_map = self._load_signal_day_pct_change(d, codes)
                mask = df["sig_date"] == d
                df.loc[mask, "_signal_day_pct_change"] = df.loc[mask, "code"].map(pct_map)
        except Exception:
            df["_signal_day_pct_change"] = None

        signal_prefix_map = {
            "close": "sig_close",
            "ma5": "sig_ma5",
            "ma20": "sig_ma20",
            "ma60": "sig_ma60",
            "ma250": "sig_ma250",
            "vol_ratio": "sig_vol_ratio",
            "macd_hist": "sig_macd_hist",
            "kdj_k": "sig_kdj_k",
            "kdj_d": "sig_kdj_d",
            "atr14": "sig_atr14",
            "stop_ref": "sig_stop_ref",
            "signal": "sig_signal",
            "reason": "sig_reason",
        }
        df = df.rename(columns={k: v for k, v in signal_prefix_map.items() if k in df.columns})
        for src, target in signal_prefix_map.items():
            if target not in df.columns:
                df[target] = None
        return latest_trade_date, signal_dates, df

    def _load_latest_snapshots(self, latest_trade_date: str, codes: List[str]) -> pd.DataFrame:
        if not latest_trade_date or not codes:
            return pd.DataFrame()

        table = self.params.signals_table
        stmt = text(
            f"""
            SELECT
              `date`,`code`,`close`,`ma5`,`ma20`,`ma60`,`ma250`,
              `vol_ratio`,`macd_hist`,`atr14`,`stop_ref`
            FROM `{table}`
            WHERE `date` = :d AND `code` IN :codes
            """
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": latest_trade_date, "codes": codes})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取最新指标失败，将跳过最新快照：%s", exc)
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        df["code"] = df["code"].astype(str)
        return df

    def _load_previous_strength(
        self, codes: List[str], as_of: dt.datetime | None = None
    ) -> Dict[str, float]:
        """读取历史信号强度，用于计算增减（支持跨天对比）。"""

        table = self.params.output_table
        if not codes or not self._table_exists(table):
            return {}

        if not self._column_exists(table, "signal_strength") or not self._column_exists(
            table, "checked_at"
        ):
            return {}

        stmt = text(
            f"""
            SELECT t1.`code`, t1.`signal_strength`
            FROM `{table}` t1
            JOIN (
                SELECT `code`, MAX(`checked_at`) AS latest_checked
                FROM `{table}`
                WHERE `code` IN :codes AND `signal_strength` IS NOT NULL {"AND `checked_at` < :as_of" if as_of else ""}
                GROUP BY `code`
            ) t2
              ON t1.`code` = t2.`code` AND t1.`checked_at` = t2.`latest_checked`
            WHERE t1.`code` IN :codes AND t1.`signal_strength` IS NOT NULL {"AND t1.`checked_at` < :as_of" if as_of else ""}
            """
        ).bindparams(bindparam("codes", expanding=True))

        params: dict[str, Any] = {"codes": codes}
        if as_of:
            params["as_of"] = as_of

        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params=params)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取历史信号强度失败，将跳过强度对比：%s", exc)
            return {}

        if df.empty or "code" not in df.columns:
            return {}

        df["code"] = df["code"].astype(str)
        strength_map: Dict[str, float] = {}
        for _, row in df.iterrows():
            score = _to_float(row.get("signal_strength"))
            if score is None:
                continue
            strength_map[str(row.get("code"))] = score
        return strength_map

    def _load_stock_industry_dim(self) -> pd.DataFrame:
        candidates = ["dim_stock_industry", "a_share_stock_industry"]
        for table in candidates:
            if not self._table_exists(table):
                continue
            try:
                with self.db_writer.engine.begin() as conn:
                    df = pd.read_sql_query(text(f"SELECT * FROM `{table}`"), conn)
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("读取 %s 失败：%s", table, exc)
                continue
            if not df.empty:
                df["code"] = df["code"].astype(str)
                return df
        return pd.DataFrame()

    def _load_board_constituent_dim(self) -> pd.DataFrame:
        table = "dim_stock_board_industry"
        if not self._table_exists(table):
            return pd.DataFrame()
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(text(f"SELECT * FROM `{table}`"), conn)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取 %s 失败：%s", table, exc)
            return pd.DataFrame()
        if not df.empty and "code" in df.columns:
            df["code"] = df["code"].astype(str)
        return df

    def _load_board_spot_strength(self) -> pd.DataFrame:
        return self.env_builder.load_board_spot_strength()

    def _load_index_trend(self, latest_trade_date: str) -> dict[str, Any]:
        return self.env_builder.load_index_trend(latest_trade_date)

    def _load_index_weekly_channel(self, latest_trade_date: str) -> dict[str, Any]:
        return self.env_builder.load_index_weekly_channel(latest_trade_date)

    def _build_weekly_scenario(
        self, weekly_payload: dict[str, Any], index_trend: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return self.env_builder.build_weekly_scenario(weekly_payload, index_trend)

    def _build_environment_context(self, latest_trade_date: str) -> dict[str, Any]:
        return self.env_builder.build_environment_context(latest_trade_date)

    def build_and_persist_env_snapshot(
        self,
        latest_trade_date: str,
        *,
        monitor_date: str | None = None,
        dedupe_bucket: str | None = None,
        checked_at: dt.datetime | None = None,
    ) -> dict[str, Any] | None:
        """计算周线环境并写入快照表（供低频任务调用）。"""

        if checked_at is None:
            checked_at = dt.datetime.now()
        if monitor_date is None:
            monitor_date = checked_at.date().isoformat()
        if dedupe_bucket is None:
            dedupe_bucket = self._calc_dedupe_bucket(checked_at)

        env_context = self._build_environment_context(latest_trade_date)
        weekly_gate_policy = self._resolve_env_weekly_gate_policy(env_context)
        if isinstance(env_context, dict):
            env_context["weekly_gate_policy"] = weekly_gate_policy

        self._persist_env_snapshot(
            env_context,
            monitor_date,
            dedupe_bucket,
            checked_at,
            weekly_gate_policy,
        )

        return env_context

    # -------------------------
    # Quote fetch
    # -------------------------
    def _fetch_quotes(self, codes: List[str]) -> pd.DataFrame:
        """获取实时行情。

        路线A：默认直接走东财（eastmoney）以避免 AkShare 全市场实时接口不稳定。
        - quote_source=eastmoney/auto：直接东财
        - quote_source=akshare：仅在显式指定时才调用 AkShare
        """

        source = (self.params.quote_source or "eastmoney").strip().lower()
        if source == "akshare":
            return self._fetch_quotes_akshare(codes)
        # 兼容：auto 视为 eastmoney
        return self._fetch_quotes_eastmoney(codes)

    def _fetch_quotes_akshare(self, codes: List[str]) -> pd.DataFrame:
        try:
            import akshare as ak  # type: ignore
        except Exception as exc:  # noqa: BLE001
            self.logger.info("AkShare 不可用（将回退）：%s", exc)
            return pd.DataFrame()

        digits = {_strip_baostock_prefix(c) for c in codes}
        try:
            spot = ak.stock_zh_a_spot_em()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("AkShare 行情拉取失败（将回退）：%s", exc)
            return pd.DataFrame()

        if spot is None or getattr(spot, "empty", True):
            return pd.DataFrame()

        rename_map = {
            "代码": "symbol",
            "名称": "name",
            "最新价": "latest",
            "涨跌幅": "pct_change",
            "今开": "open",
            "昨收": "prev_close",
        }
        for k in list(rename_map.keys()):
            if k not in spot.columns:
                rename_map.pop(k, None)

        spot = spot.rename(columns=rename_map)
        if "symbol" not in spot.columns:
            return pd.DataFrame()

        spot["symbol"] = spot["symbol"].astype(str)
        spot = spot[spot["symbol"].isin(digits)].copy()
        if spot.empty:
            return pd.DataFrame()

        out = pd.DataFrame()
        out["code"] = spot["symbol"].apply(lambda x: _to_baostock_code("auto", str(x)))
        out["symbol"] = spot["symbol"].astype(str)
        out["name"] = spot.get("name", pd.Series([""] * len(spot))).astype(str)
        out["open"] = spot.get("open", pd.Series([None] * len(spot))).apply(_to_float)
        out["latest"] = spot.get("latest", pd.Series([None] * len(spot))).apply(_to_float)
        out["prev_close"] = spot.get("prev_close", pd.Series([None] * len(spot))).apply(_to_float)
        out["pct_change"] = spot.get("pct_change", pd.Series([None] * len(spot))).apply(_to_float)

        mapping = {_strip_baostock_prefix(c): c for c in codes}
        out["code"] = out["symbol"].map(mapping).fillna(out["code"])
        return out.reset_index(drop=True)

    def _urlopen_json_no_proxy(self, url: str, *, timeout: int = 10, retries: int = 2) -> Dict[str, Any]:
        """访问东财接口并返回 JSON（默认不使用环境代理）。

        说明：urllib 默认会读取环境变量代理；这里强制 ProxyHandler({})，避免被 HTTP(S)_PROXY 影响。
        """

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://quote.eastmoney.com/",
            "Connection": "close",
        }
        req = urllib.request.Request(url, headers=headers)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        last_exc: Exception | None = None
        for i in range(retries + 1):
            try:
                with opener.open(req, timeout=timeout) as resp:
                    raw = resp.read().decode("utf-8", errors="ignore")
                return json.loads(raw) if raw else {}
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if i < retries:
                    time.sleep(0.5 * (2**i))
                    continue
                raise

    def _fetch_quotes_eastmoney(self, codes: List[str]) -> pd.DataFrame:
        if not codes:
            return pd.DataFrame()

        base_url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
        fields = "f2,f3,f4,f5,f6,f12,f14,f15,f16,f17,f18"
        secids = [_to_eastmoney_secid(c) for c in codes]

        batch_size = 80
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(secids), batch_size):
            part = secids[i : i + batch_size]
            query = {
                "fltt": "2",
                "invt": "2",
                "fields": fields,
                "secids": ",".join(part),
            }
            url = f"{base_url}?{urllib.parse.urlencode(query)}"
            try:
                payload = self._urlopen_json_no_proxy(url, timeout=10, retries=2)
            except Exception as exc:  # noqa: BLE001
                self.logger.error("Eastmoney 行情请求失败：%s", exc)
                continue

            data = (payload or {}).get("data") or {}
            diff = data.get("diff") or []
            if isinstance(diff, list):
                rows.extend([r for r in diff if isinstance(r, dict)])

        if not rows:
            return pd.DataFrame()

        out_rows: List[Dict[str, Any]] = []
        mapping = {_strip_baostock_prefix(c): c for c in codes}
        for r in rows:
            symbol = str(r.get("f12") or "").strip()
            name = str(r.get("f14") or "").strip()
            latest = _to_float(r.get("f2"))
            pct = _to_float(r.get("f3"))
            high = _to_float(r.get("f15"))
            low = _to_float(r.get("f16"))
            open_px = _to_float(r.get("f17"))
            prev_close = _to_float(r.get("f18"))
            # Eastmoney 成交量单位为“手”，统一转换为“股”口径
            volume = _to_float(r.get("f5"))
            volume = volume * 100 if volume is not None else None
            amount = _to_float(r.get("f6"))

            code_guess = _to_baostock_code("auto", symbol)
            code = mapping.get(symbol, code_guess)

            out_rows.append(
                {
                    "code": code,
                    "symbol": symbol,
                    "name": name,
                    "open": open_px,
                    "latest": latest,
                    "prev_close": prev_close,
                    "high": high,
                    "low": low,
                    "volume": volume,
                    "amount": amount,
                    "pct_change": pct,
                }
            )

        return pd.DataFrame(out_rows)

    # -------------------------
    # Evaluate
    # -------------------------
    def _is_pullback_signal(self, signal_reason: str) -> bool:
        reason_text = str(signal_reason or "")
        lower = reason_text.lower()
        return ("回踩" in reason_text) and (("ma20" in lower) or ("ma 20" in lower))

    def _evaluate(
        self,
        signals: pd.DataFrame,
        quotes: pd.DataFrame,
        latest_snapshots: pd.DataFrame,
        latest_trade_date: str,
        env_context: dict[str, Any] | None = None,
        *,
        checked_at: dt.datetime | None = None,
    ) -> pd.DataFrame:
        if signals.empty:
            return pd.DataFrame()

        q = quotes.copy()
        live_rename_map = {
            "open": "live_open",
            "high": "live_high",
            "low": "live_low",
            "latest": "live_latest",
            "volume": "live_volume",
            "amount": "live_amount",
            "pct_change": "live_pct_change",
            "gap_pct": "live_gap_pct",
            "intraday_vol_ratio": "live_intraday_vol_ratio",
            "prev_close": "prev_close",
        }
        q = q.rename(columns={k: v for k, v in live_rename_map.items() if k in q.columns})
        checked_at_ts = checked_at or dt.datetime.now()
        dedupe_bucket = self._calc_dedupe_bucket(checked_at_ts)
        avg_volume_map = self._load_avg_volume(
            latest_trade_date, signals["code"].dropna().astype(str).unique().tolist()
        )
        minutes_elapsed = self._calc_minutes_elapsed(checked_at_ts)
        total_minutes = 240
        if q.empty:
            out = signals.copy()
            out["monitor_date"] = dt.date.today().isoformat()
            out["live_open"] = None
            out["live_latest"] = None
            out["live_high"] = None
            out["live_low"] = None
            out["live_pct_change"] = None
            out["live_gap_pct"] = None
            out["live_volume"] = None
            out["live_amount"] = None
            out["live_intraday_vol_ratio"] = None
            out["asof_close"] = out.get("sig_close")
            out["asof_ma5"] = out.get("sig_ma5")
            out["asof_ma20"] = out.get("sig_ma20")
            out["asof_ma60"] = out.get("sig_ma60")
            out["asof_ma250"] = out.get("sig_ma250")
            out["asof_vol_ratio"] = out.get("sig_vol_ratio")
            out["asof_macd_hist"] = out.get("sig_macd_hist")
            out["asof_trade_date"] = out.get("sig_date")
            out["avg_volume_20"] = None
            out["action"] = "UNKNOWN"
            out["action_reason"] = "行情数据不可用"
            out["candidate_status"] = "UNKNOWN"
            out["status_reason"] = "行情数据不可用"
            out["checked_at"] = checked_at_ts
            out["dedupe_bucket"] = dedupe_bucket
            return out

        q["code"] = q["code"].astype(str)
        merged = signals.merge(q, on="code", how="left", suffixes=("", "_q"))

        env_context = env_context or {}
        industry_dim = self._load_stock_industry_dim()
        if not industry_dim.empty:
            rename_map = {}
            if "industryClassification" in industry_dim.columns:
                rename_map["industryClassification"] = "industry_classification"
            industry_dim = industry_dim.rename(columns=rename_map)
            if "industry" not in industry_dim.columns and "industry_classification" in industry_dim.columns:
                industry_dim["industry"] = industry_dim["industry_classification"]
            merged = merged.merge(
                industry_dim[[c for c in ["code", "industry", "industry_classification"] if c in industry_dim.columns]],
                on="code",
                how="left",
            )
        board_dim = self._load_board_constituent_dim()
        if not board_dim.empty:
            cols = [c for c in ["code", "board_name", "board_code"] if c in board_dim.columns]
            if cols:
                deduped = board_dim.drop_duplicates(subset=["code"], keep="first")
                merged = merged.merge(deduped[cols], on="code", how="left")

        board_map: dict[str, Any] = env_context.get("boards", {}) if isinstance(env_context, dict) else {}
        env_regime = env_context.get("regime") if isinstance(env_context, dict) else None
        env_position_hint = None
        if isinstance(env_context, dict):
            env_position_hint = _to_float(env_context.get("position_hint"))

        env_weekly_asof_trade_date = None
        env_weekly_risk_level = None
        env_weekly_gating_enabled = False
        env_weekly_plan_a = None
        env_weekly_plan_b = None
        env_weekly_scene = None
        env_weekly_plan_a_exposure_cap = None
        env_weekly_bias = None
        env_weekly_status = None
        if isinstance(env_context, dict):
            env_weekly_asof_trade_date = env_context.get("weekly_asof_trade_date")
            env_weekly_risk_level = env_context.get("weekly_risk_level")
            env_weekly_gating_enabled = bool(env_context.get("weekly_gating_enabled", False))
            env_weekly_plan_a = env_context.get("weekly_plan_a")
            env_weekly_plan_b = env_context.get("weekly_plan_b")
            env_weekly_scene = env_context.get("weekly_scene_code")
            env_weekly_plan_a_exposure_cap = _to_float(
                env_context.get("weekly_plan_a_exposure_cap")
            )
            env_weekly_bias = env_context.get("weekly_bias")
            env_weekly_status = env_context.get("weekly_status")
        index_score = None
        if isinstance(env_context, dict):
            index_section = env_context.get("index", {}) if isinstance(env_context.get("index"), dict) else {}
            index_score = index_section.get("score")
        idx_score_float = _to_float(index_score)

        if not latest_snapshots.empty:
            snap = latest_snapshots.copy()
            rename_map = {c: f"asof_{c}" for c in snap.columns if c not in {"code"}}
            snap = snap.rename(columns=rename_map)
            merged = merged.merge(snap, on="code", how="left")

        has_asof_cols = [c for c in ["asof_close", "asof_ma20"] if c in merged.columns]
        merged["_has_latest_snapshot"] = False
        if has_asof_cols:
            merged["_has_latest_snapshot"] = merged[has_asof_cols].notna().any(axis=1)

        if avg_volume_map:
            merged["avg_volume_20"] = merged["code"].map(avg_volume_map)
        else:
            merged["avg_volume_20"] = None

        float_cols = [
            "sig_close",
            "sig_ma5",
            "sig_ma20",
            "sig_ma60",
            "sig_ma250",
            "sig_vol_ratio",
            "sig_macd_hist",
            "sig_kdj_k",
            "sig_kdj_d",
            "sig_atr14",
            "sig_stop_ref",
            "asof_close",
            "asof_ma5",
            "asof_ma20",
            "asof_ma60",
            "asof_ma250",
            "asof_vol_ratio",
            "asof_macd_hist",
            "asof_atr14",
            "asof_stop_ref",
            "live_open",
            "live_latest",
            "live_high",
            "live_low",
            "live_pct_change",
            "live_volume",
            "live_amount",
            "avg_volume_20",
            "_signal_day_pct_change",
            "signal_strength",
            "strength_delta",
        ]
        for col in float_cols:
            if col in merged.columns:
                merged[col] = merged.get(col).apply(_to_float)

        def _normalize_intraday_volume(vol: float | None, avg_vol: float | None) -> float | None:
            if vol is None or avg_vol is None or avg_vol <= 0:
                return vol

            ratio = vol / avg_vol
            multiplied = (vol * 100.0) / avg_vol
            divided = (vol / 100.0) / avg_vol

            if ratio < 0.02 <= multiplied <= 200:
                self.logger.debug(
                    "检测到成交量单位可能为“手”，已按股处理（乘以 100）。"
                )
                return vol * 100.0

            if ratio > 200 >= divided >= 0.02:
                self.logger.debug(
                    "检测到成交量单位可能已按股存储，已按“手”还原（除以 100）。"
                )
                return vol / 100.0

            return vol

        def _calc_intraday_vol_ratio(row: pd.Series) -> float | None:
            vol = _normalize_intraday_volume(row.get("live_volume"), row.get("avg_volume_20"))
            avg_vol = row.get("avg_volume_20")
            effective_minutes = max(minutes_elapsed, 5)
            if (
                vol is None
                or avg_vol is None
                or avg_vol <= 0
                or minutes_elapsed <= 0
                or total_minutes <= 0
            ):
                return None

            scaled = (vol / effective_minutes) * total_minutes
            if scaled <= 0:
                return None
            return scaled / avg_vol

        merged["live_intraday_vol_ratio"] = merged.apply(
            _calc_intraday_vol_ratio, axis=1
        )
        if "asof_vol_ratio" not in merged.columns:
            merged["asof_vol_ratio"] = None

        def _calc_gap(row: pd.Series) -> float | None:
            ref_close = row.get("prev_close")
            if ref_close is None:
                ref_close = row.get("sig_close")

            px = row.get("live_open")
            if px is None or px <= 0:
                px = row.get("live_latest")

            if ref_close is None or px is None:
                return None
            if ref_close <= 0 or px <= 0:
                return None
            return (px - ref_close) / ref_close

        merged["live_gap_pct"] = merged.apply(_calc_gap, axis=1)

        max_up = self.params.max_gap_up_pct
        max_up_atr_mult = self.params.max_gap_up_atr_mult
        max_down = self.params.max_gap_down_pct
        min_vs_ma20 = self.params.min_open_vs_ma20_pct
        pullback_min_vs_ma20 = self.params.pullback_min_open_vs_ma20_pct
        limit_up_trigger = self.params.limit_up_trigger_pct

        max_entry_vs_ma5 = self.params.max_entry_vs_ma5_pct
        stop_atr_mult = self.params.stop_atr_mult
        signal_day_limit_up = self.params.signal_day_limit_up_pct

        expire_atr_mult = self.params.expire_atr_mult
        expire_pct_threshold = self.params.expire_pct_threshold
        cross_valid_days = self.params.cross_valid_days
        pullback_valid_days = self.params.pullback_valid_days
        vol_threshold = self.volume_ratio_threshold

        actions: List[str] = []
        reasons: List[str] = []
        statuses: List[str] = []
        status_reasons: List[str] = []
        signal_kinds: List[str] = []
        trade_stop_refs: List[float | None] = []
        sig_stop_refs: List[float | None] = []
        stop_loss_refs: List[float | None] = []
        valid_days_list: List[int | None] = []
        entry_exposure_caps: List[float | None] = []
        env_index_scores: List[float | None] = []
        env_regimes: List[str | None] = []
        env_position_hints: List[float | None] = []
        env_weekly_asof_trade_dates: List[str | None] = []
        env_weekly_risk_levels: List[str | None] = []
        env_weekly_scene_list: List[str | None] = []
        env_weekly_gate_action_list: List[str | None] = []
        board_statuses: List[str | None] = []
        board_ranks: List[float | None] = []
        board_chg_pcts: List[float | None] = []
        strength_scores: List[float | None] = []
        strength_deltas: List[float | None] = []
        strength_trends: List[str | None] = []
        strength_notes: List[str | None] = []
        asof_ma5_list: List[float | None] = []
        asof_ma20_list: List[float | None] = []
        asof_ma60_list: List[float | None] = []
        asof_ma250_list: List[float | None] = []
        asof_vol_ratio_list: List[float | None] = []
        asof_macd_hist_list: List[float | None] = []

        def _coalesce(row: pd.Series, *cols: str) -> float | None:
            for col in cols:
                val = row.get(col)
                if val is not None and not pd.isna(val):
                    return val
            return None

        def _dec(row: pd.Series, key: str) -> float | None:
            return _coalesce(row, f"asof_{key}", f"sig_{key}")

        def _px_now(row: pd.Series) -> float | None:
            return _coalesce(
                row,
                "live_latest",
                "live_open",
                "asof_close",
                "sig_close",
            )

        def _vol_ratio_dec(row: pd.Series) -> float | None:
            return _coalesce(
                row,
                "live_intraday_vol_ratio",
                "asof_vol_ratio",
                "sig_vol_ratio",
            )

        def _calc_signal_strength(
            row: pd.Series,
            price_now: float | None,
            vol_ratio_val: float | None = None,
            board_status: str | None = None,
            idx_score: float | None = None,
        ) -> tuple[float | None, str]:
            score = 0.0
            notes: list[str] = []

            ma5 = _dec(row, "ma5")
            ma20 = _dec(row, "ma20")
            ma60 = _dec(row, "ma60")
            if vol_ratio_val is None:
                vol_ratio_val = _vol_ratio_dec(row)
            atr14_val = _dec(row, "atr14")
            macd_hist_signal = _to_float(row.get("sig_macd_hist"))
            macd_hist_now = _to_float(row.get("live_macd_hist"))
            if macd_hist_now is None:
                macd_hist_now = _to_float(row.get("asof_macd_hist"))
            if macd_hist_now is None:
                macd_hist_now = macd_hist_signal

            if ma5 is not None and ma20 is not None:
                if ma5 > ma20:
                    score += 1.0
                    notes.append("MA5>MA20")
                else:
                    score -= 1.0
                    notes.append("MA5<MA20")

            if ma20 is not None and ma60 is not None:
                if ma20 > ma60:
                    score += 1.0
                    notes.append("MA20>MA60")
                else:
                    score -= 0.5
                    notes.append("MA20<=MA60")

            if price_now is not None and ma20 is not None:
                deviation = (price_now - ma20) / ma20
                if deviation >= 0:
                    score += 0.5
                    notes.append("价格站上MA20")
                else:
                    score -= 0.5
                    notes.append("价格跌破MA20")

                if atr14_val is not None and atr14_val > 0:
                    z_score = (price_now - ma20) / atr14_val
                    if -0.5 <= z_score <= 1.0:
                        score += 0.5
                        notes.append("乖离在ATR舒适区间")
                    elif z_score > 1.2:
                        score -= 0.5
                        notes.append("价格偏热")
                    elif z_score < -0.8:
                        score -= 0.5
                        notes.append("跌破回踩区间")

            if macd_hist_now is not None:
                if macd_hist_now > 0:
                    score += 0.5
                else:
                    score -= 0.5

            macd_delta = None
            if macd_hist_now is not None and macd_hist_signal is not None:
                macd_delta = macd_hist_now - macd_hist_signal

            if macd_delta is not None:
                if macd_delta > 0:
                    score += 0.5
                    notes.append("MACD扩张")
                elif macd_delta < 0:
                    score -= 0.5
                    notes.append("MACD收敛")

            if vol_ratio_val is not None:
                if vol_ratio_val >= vol_threshold:
                    score += 0.5
                    notes.append("量能放大")
                elif vol_ratio_val < 1:
                    if price_now is not None and ma20 is not None and price_now <= ma20:
                        score += 0.2
                        notes.append("回踩缩量")
                    else:
                        score -= 0.1
                        notes.append("量能偏弱")
                else:
                    score -= 0.1
                    notes.append("量能不足")

            if isinstance(board_status, str):
                status_norm = board_status.strip().lower()
                if status_norm == "strong":
                    score += 0.5
                    notes.append("板块强")
                elif status_norm == "weak":
                    score -= 0.5
                    notes.append("板块弱")

            if idx_score is not None:
                if idx_score >= self.env_index_score_threshold:
                    score += 0.5
                    notes.append("大盘顺风")
                else:
                    score -= 0.5
                    notes.append("大盘逆风")

            return round(score, 3), "；".join(notes)

        def _classify_strength_trend(curr: float | None, prev: float | None) -> str | None:
            if curr is None or prev is None:
                return None
            delta = curr - prev
            if delta >= 0.5:
                return "ENHANCING"
            if delta <= -0.5:
                return "WEAKENING"
            return "FLAT"

        codes_all = merged["code"].dropna().astype(str).unique().tolist()
        if not self.params.incremental_write:
            self.logger.warning(
                "incremental_write=False 会覆盖当日历史，strength_delta/strength_trend 可能缺乏对比基础"
            )
        prev_strength_map = self._load_previous_strength(codes_all, as_of=checked_at_ts)

        for _, row in merged.iterrows():
            action = "EXECUTE"
            reason = "OK"
            status = "ACTIVE"
            status_reason = "健康满足"

            risk_tag_raw = str(row.get("risk_tag") or "").strip()
            risk_note = str(row.get("risk_note") or "").strip()
            risk_tags_split = [t.strip() for t in risk_tag_raw.split("|") if t.strip()]

            open_px = row.get("live_open")
            latest_px = row.get("live_latest")
            entry_px = open_px
            ma20 = _dec(row, "ma20")
            ma5 = _dec(row, "ma5")
            ma60 = _dec(row, "ma60")
            ma250 = _dec(row, "ma250")
            vol_used = _vol_ratio_dec(row)
            macd_hist = _dec(row, "macd_hist")
            atr14 = _dec(row, "atr14")

            signal_stop_ref = _dec(row, "stop_ref")
            signal_close = row.get("sig_close")
            current_close = _coalesce(row, "asof_close", "sig_close")
            ref_close = row.get("prev_close")
            if ref_close is None:
                ref_close = signal_close

            gap = row.get("live_gap_pct")
            pct = row.get("live_pct_change")
            signal_day_pct = row.get("_signal_day_pct_change")
            signal_reason = str(row.get("sig_reason") or "")
            signal_age = row.get("signal_age")
            is_pullback = self._is_pullback_signal(signal_reason)

            used_latest_as_entry = False
            if entry_px is None or entry_px <= 0:
                entry_px = latest_px
                used_latest_as_entry = True
                if entry_px is None or entry_px <= 0:
                    action = "UNKNOWN"
                    reason = "无今开/最新价"
                else:
                    reason = "用最新价替代今开"

            trade_stop_ref = signal_stop_ref
            if entry_px is not None and atr14 is not None and atr14 > 0 and stop_atr_mult > 0:
                trade_stop_ref = entry_px - stop_atr_mult * atr14

            price_now = _px_now(row)

            board_candidates: list[str] = []
            for key in [
                row.get("board_code"),
                row.get("board_name"),
                row.get("industry"),
                row.get("industry_classification"),
            ]:
                if pd.notna(key):
                    key_norm = str(key).strip()
                    if key_norm and key_norm.lower() != "nan":
                        board_candidates.append(key_norm)
            board_info = None
            board_status = None
            board_rank = None
            board_chg = None
            for key in board_candidates:
                board_info = board_map.get(key)
                if board_info:
                    break
            if isinstance(board_info, dict):
                board_status = board_info.get("status")
                board_rank = _to_float(board_info.get("rank"))
                board_chg = _to_float(board_info.get("chg_pct"))

            # 规范化，避免大小写/空格导致后续判断失效
            if isinstance(board_status, str):
                board_status = board_status.strip().lower()

            strength_score, strength_note = _calc_signal_strength(
                row,
                price_now,
                vol_used,
                board_status=board_status,
                idx_score=idx_score_float,
            )
            prev_strength = prev_strength_map.get(str(row.get("code")))
            strength_delta = None
            if strength_score is not None and prev_strength is not None:
                strength_delta = strength_score - prev_strength
            strength_trend = _classify_strength_trend(strength_score, prev_strength)

            valid_days = pullback_valid_days if is_pullback else cross_valid_days

            # comment: feat: 止损参考价取 trade_stop_ref 与 signal_stop_ref 的更严格值 防止低开导致止损线被动放宽
            stop_loss_ref = None
            stop_candidates = [
                v for v in (trade_stop_ref, signal_stop_ref) if v is not None
            ]
            if stop_candidates:
                stop_loss_ref = max(stop_candidates)
            structural_failure_reason = None
            structural_downgrade_reason = None
            if ma5 is not None and ma20 is not None and ma5 < ma20:
                if is_pullback:
                    structural_downgrade_reason = "盘中结构观察：MA5 下穿 MA20（回踩形态降级观察）"
                else:
                    structural_failure_reason = "盘中结构失效：MA5 下穿 MA20"
            elif (
                price_now is not None
                and ma20 is not None
                and atr14 is not None
                and price_now < ma20 - 0.3 * atr14
            ):
                threshold = ma20 - 0.3 * atr14
                structural_failure_reason = (
                    f"盘中结构失效：最新价 {price_now:.2f} 低于 MA20-0.3ATR 阈值 {threshold:.2f}"
                )
            elif macd_hist is not None and macd_hist < 0:
                if is_pullback:
                    structural_downgrade_reason = "盘中结构观察：MACD 柱子转负（回踩形态降级观察）"
                else:
                    structural_failure_reason = "盘中结构失效：MACD 柱子转负"
            elif price_now is not None and stop_loss_ref is not None and price_now <= stop_loss_ref:
                stop_parts: list[str] = []
                if trade_stop_ref is not None:
                    stop_parts.append(f"entry 止损 {trade_stop_ref:.2f}")
                if signal_stop_ref is not None:
                    stop_parts.append(f"信号日止损 {signal_stop_ref:.2f}")
                stop_detail = "，".join(stop_parts)
                detail_suffix = f"（取较高者：{stop_detail}）" if stop_detail else ""
                structural_failure_reason = (
                    f"盘中结构失效：最新价 {price_now:.2f} 跌破止损参考价 {stop_loss_ref:.2f}{detail_suffix}"
                )

            if structural_failure_reason:
                status = "INVALID"
                status_reason = structural_failure_reason
                action = "SKIP"
                reason = structural_failure_reason

            if not structural_failure_reason:
                if structural_downgrade_reason and status == "ACTIVE":
                    status = "WAIT"
                    status_reason = structural_downgrade_reason
                if (
                    price_now is not None
                    and ma20 is not None
                    and vol_used is not None
                    and vol_used >= vol_threshold
                    and price_now < ma20
                ):
                    if is_pullback:
                        status = "WAIT"
                        status_reason = "价格跌破 MA20 且前一交易日放量（回踩形态观察）"
                    else:
                        status = "INVALID"
                        status_reason = "价格跌破 MA20 且前一交易日放量"
                elif valid_days > 0 and signal_age is not None and signal_age > valid_days:
                    if strength_trend == "ENHANCING" or (
                        strength_score is not None and strength_score >= 2
                    ):
                        status = "WAIT"
                        status_reason = (
                            f"超过有效期 {valid_days} 个交易日但强度走强，继续观察"
                        )
                    else:
                        status = "EXPIRED"
                        status_reason = f"超过有效期 {valid_days} 个交易日"
                elif (
                    price_now is not None
                    and ma5 is not None
                    and max_entry_vs_ma5 > 0
                    and price_now > ma5 * (1.0 + max_entry_vs_ma5)
                ):
                    status = "EXPIRED"
                    status_reason = "入场价/最新价相对 MA5 乖离过大"
                elif price_now is not None and signal_close is not None and price_now > signal_close:
                    gain = (price_now - signal_close) / signal_close
                    atr_base = atr14 if atr14 is not None else row.get("atr14")
                    atr_gain = None
                    if atr_base is not None and atr_base > 0:
                        atr_gain = (price_now - signal_close) / atr_base
                    if atr_gain is not None and atr_gain > expire_atr_mult:
                        status = "EXPIRED"
                        status_reason = f"信号后拉升超过 ATR×{expire_atr_mult:.2f}"
                    elif gain > expire_pct_threshold:
                        status = "EXPIRED"
                        status_reason = f"信号后涨幅 {gain*100:.2f}% 超过阈值"
                else:
                    trend_ok = (
                        current_close is not None
                        and ma60 is not None
                        and ma250 is not None
                        and ma20 is not None
                        and current_close > ma60
                        and current_close > ma250
                        and ma20 > ma60 > ma250
                    )
                    macd_ok = macd_hist is not None and macd_hist > 0
                    vol_ok = vol_used is not None and vol_used >= vol_threshold

                    if not trend_ok:
                        status = "INVALID"
                        status_reason = "多头排列/趋势破坏"
                    elif (not macd_ok) or (not vol_ok):
                        status = "WAIT"
                        weak_reasons = []
                        if not macd_ok:
                            weak_reasons.append("MACD 动能转弱")
                        if not vol_ok:
                            weak_reasons.append("量能不足")
                        status_reason = "；".join(weak_reasons) if weak_reasons else "继续观察"

            if status == "ACTIVE" and strength_score is not None:
                if strength_score < 0:
                    status = "WAIT"
                    status_reason = "信号强度偏弱，等待修复"
                elif strength_trend == "WEAKENING":
                    status = "WAIT"
                    status_reason = "信号强度走弱，等待确认"

            if status in {"INVALID", "EXPIRED"}:
                action = "SKIP"
                reason = status_reason
            elif status == "WAIT":
                action = "WAIT"
                reason = status_reason

            if action == "EXECUTE" and signal_day_pct is not None and signal_day_pct >= signal_day_limit_up:
                action = "SKIP"
                reason = f"信号日涨幅 {signal_day_pct*100:.2f}% 接近涨停，次日不追"

            if action == "EXECUTE" and pct is not None and pct >= limit_up_trigger:
                action = "SKIP"
                reason = f"涨幅 {pct:.2f}% 接近/达到涨停"

            if action == "EXECUTE" and gap is not None and gap > 0:
                gap_up_threshold = max_up
                atr_based = None
                if ref_close is not None and ref_close > 0 and atr14 is not None and atr14 > 0 and max_up_atr_mult > 0:
                    atr_based = max_up_atr_mult * atr14 / ref_close
                    if atr_based > 0:
                        gap_up_threshold = min(max_up, atr_based)

                if gap > gap_up_threshold:
                    action = "SKIP"
                    if atr_based is None:
                        reason = f"高开 {gap*100:.2f}% 超过阈值 {gap_up_threshold*100:.2f}%"
                    else:
                        reason = (
                            f"高开 {gap*100:.2f}% 超过阈值 {gap_up_threshold*100:.2f}%"
                            f"（min(固定{max_up*100:.2f}%, ATR×{max_up_atr_mult:.1f}={atr_based*100:.2f}% )）"
                        )

            if action == "EXECUTE" and gap is not None and gap < max_down:
                action = "SKIP"
                reason = f"低开 {gap*100:.2f}% 低于阈值 {max_down*100:.2f}%"

            if action == "EXECUTE" and (ma20 is not None) and (entry_px is not None):
                threshold = ma20 * (
                    1.0
                    + (
                        pullback_min_vs_ma20
                        if is_pullback
                        else min_vs_ma20
                    )
                )
                if entry_px < threshold:
                    action = "SKIP"
                    px_label = "入场价(最新)" if used_latest_as_entry else "入场价"
                    reason = f"{px_label} {entry_px:.2f} 跌破 MA20 阈值 {threshold:.2f}"

            if action == "EXECUTE" and (ma5 is not None) and (entry_px is not None) and (max_entry_vs_ma5 > 0):
                threshold_ma5 = ma5 * (1.0 + max_entry_vs_ma5)
                if entry_px > threshold_ma5:
                    action = "SKIP"
                    px_label = "入场价(最新)" if used_latest_as_entry else "入场价"
                    reason = (
                        f"{px_label} {entry_px:.2f} 高于 MA5 阈值 {threshold_ma5:.2f}"
                        f"（>{max_entry_vs_ma5*100:.2f}%）"
                    )

            if (
                idx_score_float is not None
                and action == "EXECUTE"
                and idx_score_float < self.env_index_score_threshold
            ):
                action = "WAIT"
                reason = (
                    f"大盘趋势分数 {idx_score_float:.1f} 低于阈值"
                    f" {self.env_index_score_threshold:.1f}，建议观望/减仓"
                )

            if board_status == "weak" and action == "EXECUTE":
                action = "WAIT"
                reason = "所属板块走势偏弱，建议降低优先级"
            elif board_status == "strong" and action == "WAIT" and action != "SKIP":
                rank_display = f"{board_rank:.0f}" if board_rank is not None else "-"
                reason = f"板块强势(rank={rank_display})，等待入场条件"

            if "MANIA" in risk_tags_split:
                action = "SKIP"
                status = "INVALID"
                status_reason = risk_note or "风险标签=MANIA：短期过热，默认不追"
                reason = status_reason
            elif risk_tags_split and action == "EXECUTE":
                action = "WAIT"
                status = "WAIT"
                note_text = risk_note or "存在风险标签，建议谨慎"
                reason = note_text
                status_reason = note_text

            weekly_gate_action = None
            entry_exposure_cap = env_weekly_plan_a_exposure_cap

            if env_regime:
                pos_hint_text = (
                    f"（仓位建议 ≤{env_position_hint*100:.0f}%）"
                    if env_position_hint is not None
                    else ""
                )
                breakdown_reason = f"大盘结构破位，执行跳过{pos_hint_text}"
                if env_regime == "BREAKDOWN":
                    action = "SKIP"
                    if status != "INVALID":
                        status = "WAIT"
                        status_reason = "市场环境 BREAKDOWN，暂不执行"
                    if reason and reason != "OK":
                        reason = breakdown_reason + "；" + reason
                    else:
                        reason = breakdown_reason
                elif env_regime == "RISK_OFF":
                    action = "WAIT"
                    if status != "INVALID":
                        status = "WAIT"
                        status_reason = "市场环境 RISK_OFF，轻仓观望"
                    if reason == "OK":
                        reason = f"大盘偏弱{pos_hint_text}"
                elif env_regime == "PULLBACK" and not risk_tags_split and reason == "OK":
                    reason = f"大盘处于回踩阶段{pos_hint_text}"

            if action == "EXECUTE" and status not in {"INVALID", "EXPIRED"}:
                plan_tail = ""
                if env_weekly_plan_a:
                    plan_tail = str(env_weekly_plan_a)
                if env_weekly_plan_b:
                    plan_b_text = str(env_weekly_plan_b)
                    plan_tail = f"{plan_tail}；{plan_b_text}" if plan_tail else plan_b_text
                exposure_cap_effective = entry_exposure_cap
                exposure_hint = (
                    f"（周线仓位≤{exposure_cap_effective*100:.0f}%）"
                    if exposure_cap_effective is not None
                    else ""
                )
                if not env_weekly_gating_enabled:
                    append_note = "周线数据不足，轻仓"
                    if reason and reason != "OK":
                        reason = f"{reason}；{append_note}"
                    else:
                        reason = append_note
                else:
                    if env_weekly_risk_level == "HIGH":
                        action = "WAIT"
                        status = "WAIT"
                        weekly_gate_action = "WAIT"
                        append_note = "周线风险高：观望/防守"
                        status_reason = f"{status_reason}；{append_note}" if status_reason else append_note
                        reason = f"{reason}；{append_note}" if reason and reason != "OK" else append_note
                    elif env_weekly_risk_level == "MEDIUM" and env_weekly_status == "FORMING":
                        soft_note = "周线MEDIUM+FORMING → 软门控限仓"
                        strong_enough = (
                            strength_score is not None
                            and strength_score >= self.weekly_soft_gate_strength_threshold
                        ) or board_status == "strong"
                        if strong_enough:
                            action = "EXECUTE_SMALL"
                            weekly_gate_action = "ALLOW_SMALL"
                            entry_exposure_cap = (
                                min(entry_exposure_cap, 0.2)
                                if entry_exposure_cap is not None
                                else 0.2
                            )
                            exposure_cap_effective = entry_exposure_cap
                            exposure_hint = (
                                f"（周线仓位≤{exposure_cap_effective*100:.0f}%）"
                                if exposure_cap_effective is not None
                                else ""
                            )
                            status_reason = (
                                f"{status_reason}；{soft_note}"
                                if status_reason
                                else soft_note
                            )
                            reason = (
                                f"{reason}；{soft_note}"
                                if reason and reason != "OK"
                                else soft_note
                            )
                            if plan_tail:
                                combined = (
                                    f"{reason}；{plan_tail}{exposure_hint}"
                                    if reason and reason != "OK"
                                    else f"{plan_tail}{exposure_hint}"
                                )
                                reason = combined[:255]
                        else:
                            action = "WAIT"
                            status = "WAIT"
                            weekly_gate_action = "WAIT"
                            status_reason = (
                                f"{status_reason}；{soft_note}"
                                if status_reason
                                else soft_note
                            )
                            reason = (
                                f"{reason}；{soft_note}"
                                if reason and reason != "OK"
                                else soft_note
                            )
                    elif env_weekly_risk_level == "MEDIUM":
                        if env_weekly_bias == "BEARISH" and env_weekly_status in {
                            "BREAKOUT_DOWN",
                            "CONFIRMED",
                            "FORMING",
                        }:
                            action = "WAIT"
                            status = "WAIT"
                            weekly_gate_action = "WAIT"
                            append_note = "周线偏空/未修复，等待"
                            status_reason = (
                                f"{status_reason}；{append_note}" if status_reason else append_note
                            )
                            reason = (
                                f"{reason}；{append_note}"
                                if reason and reason != "OK"
                                else append_note
                            )
                        else:
                            weekly_gate_action = "ALLOW"
                            if plan_tail:
                                combined = (
                                    f"{reason}；{plan_tail}{exposure_hint}"
                                    if reason and reason != "OK"
                                    else f"{plan_tail}{exposure_hint}"
                                )
                                reason = combined[:255]
                    elif env_weekly_risk_level == "LOW":
                        weekly_gate_action = "ALLOW"
                        if plan_tail:
                            combined = (
                                f"{reason}；{plan_tail}{exposure_hint}"
                                if reason and reason != "OK"
                                else f"{plan_tail}{exposure_hint}"
                            )
                            reason = combined[:255]

                    if weekly_gate_action is None:
                        weekly_gate_action = "ALLOW"

            env_regimes.append(env_regime)
            env_position_hints.append(env_position_hint)
            env_index_scores.append(idx_score_float)
            board_statuses.append(board_status)
            board_ranks.append(board_rank)
            board_chg_pcts.append(board_chg)

            strength_scores.append(_to_float(strength_score))
            strength_deltas.append(_to_float(strength_delta))
            strength_trends.append(strength_trend)
            strength_notes.append(strength_note or None)
            entry_exposure_caps.append(entry_exposure_cap)
            asof_ma5_list.append(_to_float(ma5))
            asof_ma20_list.append(_to_float(ma20))
            asof_ma60_list.append(_to_float(ma60))
            asof_ma250_list.append(_to_float(ma250))
            asof_vol_ratio_list.append(
                _to_float(_coalesce(row, "asof_vol_ratio", "sig_vol_ratio"))
            )
            asof_macd_hist_list.append(_to_float(macd_hist))

            env_weekly_asof_trade_dates.append(env_weekly_asof_trade_date)
            env_weekly_risk_levels.append(env_weekly_risk_level)
            env_weekly_scene_list.append(
                str(env_weekly_scene)[:32] if env_weekly_scene is not None else None
            )
            env_weekly_gate_action_list.append(weekly_gate_action)

            actions.append(action)
            reasons.append(reason)
            statuses.append(status)
            status_reasons.append(status_reason)
            signal_kinds.append("PULLBACK" if is_pullback else "CROSS")
            trade_stop_refs.append(_to_float(trade_stop_ref))
            sig_stop_refs.append(_to_float(signal_stop_ref))
            stop_loss_refs.append(_to_float(stop_loss_ref))
            valid_days_list.append(valid_days)

        merged["monitor_date"] = dt.date.today().isoformat()
        merged["action"] = actions
        merged["action_reason"] = reasons
        merged["candidate_status"] = statuses
        merged["status_reason"] = status_reasons
        merged["trade_stop_ref"] = trade_stop_refs
        merged["sig_stop_ref"] = sig_stop_refs
        merged["stop_loss_ref"] = stop_loss_refs
        merged["valid_days"] = valid_days_list
        merged["entry_exposure_cap"] = entry_exposure_caps
        merged["checked_at"] = checked_at_ts
        merged["dedupe_bucket"] = dedupe_bucket
        merged["env_index_score"] = env_index_scores
        merged["env_regime"] = env_regimes
        merged["env_position_hint"] = env_position_hints
        merged["env_weekly_asof_trade_date"] = env_weekly_asof_trade_dates
        merged["env_weekly_risk_level"] = env_weekly_risk_levels
        merged["env_weekly_scene"] = env_weekly_scene_list
        merged["env_weekly_gate_action"] = env_weekly_gate_action_list
        merged["board_status"] = board_statuses
        merged["board_rank"] = board_ranks
        merged["board_chg_pct"] = board_chg_pcts
        merged["signal_strength"] = strength_scores
        merged["strength_delta"] = strength_deltas
        merged["strength_trend"] = strength_trends
        merged["strength_note"] = strength_notes
        merged["signal_kind"] = signal_kinds

        def _resolve_asof_trade_date(row: pd.Series) -> str | None:
            has_latest = bool(row.get("_has_latest_snapshot"))
            sig_date_val = str(row.get("sig_date") or "").strip() or None
            return latest_trade_date if has_latest else sig_date_val

        merged["asof_trade_date"] = merged.apply(_resolve_asof_trade_date, axis=1)
        merged["asof_ma5"] = asof_ma5_list
        merged["asof_ma20"] = asof_ma20_list
        merged["asof_ma60"] = asof_ma60_list
        merged["asof_ma250"] = asof_ma250_list
        merged["asof_macd_hist"] = asof_macd_hist_list
        merged["asof_vol_ratio"] = asof_vol_ratio_list
        if "asof_close" not in merged.columns:
            merged["asof_close"] = merged.get("sig_close")

        if (
            "asof_ma250" in merged.columns
            and merged.get("asof_ma250") is not None
            and pd.to_numeric(merged.get("asof_ma250"), errors="coerce").isna().all()
        ):
            latest_ma250_series = None
            if "asof_ma250" in merged.columns:
                latest_ma250_series = pd.to_numeric(
                    merged.get("asof_ma250"), errors="coerce"
                )
            signal_ma250_series = pd.to_numeric(
                merged.get("sig_ma250"), errors="coerce"
            )
            has_ma250 = False
            if latest_ma250_series is not None:
                has_ma250 = latest_ma250_series.notna().any()
            if not has_ma250 and not signal_ma250_series.isna().all():
                has_ma250 = True
            if has_ma250:
                self.logger.warning(
                    "asof_ma250 计算为空：请检查 ma250 列合并/命名是否正确。"
                )

        keep_cols = [
            "monitor_date",
            "sig_date",
            "signal_age",
            "valid_days",
            "code",
            "name",
            "asof_trade_date",
            "avg_volume_20",
            "live_open",
            "live_high",
            "live_low",
            "live_latest",
            "live_volume",
            "live_amount",
            "live_gap_pct",
            "live_pct_change",
            "live_intraday_vol_ratio",
            "sig_close",
            "sig_ma5",
            "sig_ma20",
            "sig_ma60",
            "sig_ma250",
            "sig_vol_ratio",
            "sig_macd_hist",
            "sig_atr14",
            "sig_stop_ref",
            "stop_loss_ref",
            "asof_close",
            "asof_ma5",
            "asof_ma20",
            "asof_ma60",
            "asof_ma250",
            "asof_vol_ratio",
            "asof_macd_hist",
            "asof_atr14",
            "asof_stop_ref",
            "sig_kdj_k",
            "sig_kdj_d",
            "trade_stop_ref",
            "entry_exposure_cap",
            "env_index_score",
            "env_regime",
            "env_position_hint",
            "env_weekly_asof_trade_date",
            "env_weekly_risk_level",
            "env_weekly_scene",
            "env_weekly_gate_action",
            "industry",
            "board_name",
            "board_code",
            "board_status",
            "board_rank",
            "board_chg_pct",
            "signal_strength",
            "strength_delta",
            "strength_trend",
            "strength_note",
            "risk_tag",
            "risk_note",
            "signal_kind",
            "sig_signal",
            "sig_reason",
            "candidate_status",
            "status_reason",
            "action",
            "action_reason",
            "checked_at",
            "dedupe_bucket",
            "snapshot_hash",
        ]

        for col in keep_cols:
            if col not in merged.columns:
                merged[col] = None

        return merged[keep_cols].copy()

    # -------------------------
    # Persist & export
    # -------------------------
    def _persist_results(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        table = self.params.output_table
        if not self.params.write_to_db:
            return

        monitor_date = str(df.iloc[0].get("monitor_date") or "").strip()
        codes = df["code"].dropna().astype(str).unique().tolist()
        table_exists = self._table_exists(table)

        for col in ["signal_strength", "strength_delta"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "dedupe_bucket" not in df.columns:
            df["dedupe_bucket"] = None
        df["dedupe_bucket"] = df["dedupe_bucket"].fillna(
            self._calc_dedupe_bucket(dt.datetime.now())
        )

        for col in [
            "risk_tag",
            "risk_note",
            "sig_reason",
            "action_reason",
            "status_reason",
        ]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .fillna("")
                    .astype(str)
                    .str.slice(0, 250)
                )

        if table_exists:
            self._ensure_snapshot_schema(table)
            self._ensure_strength_schema(table)
            self._ensure_monitor_columns(table)

        df["snapshot_hash"] = df.apply(lambda row: make_snapshot_hash(row.to_dict()), axis=1)
        df = df.drop_duplicates(
            subset=["monitor_date", "sig_date", "code", "dedupe_bucket"]
        )

        # 增量模式：不删除旧记录，保留每次运行的历史快照（checked_at 会区分）
        if (not self.params.incremental_write) and monitor_date and codes and table_exists:
            delete_stmt = text(
                "DELETE FROM `{table}` WHERE `monitor_date` = :d AND `code` IN :codes".format(
                    table=table
                )
            ).bindparams(bindparam("codes", expanding=True))
            try:
                with self.db_writer.engine.begin() as conn:
                    conn.execute(delete_stmt, {"d": monitor_date, "codes": codes})
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("开盘监测表去重删除失败，将直接追加：%s", exc)

        if table_exists and monitor_date and codes:
            deleted_total = 0
            buckets = (
                df["dedupe_bucket"].dropna().astype(str).unique().tolist()
                if "dedupe_bucket" in df.columns
                else []
            )
            for bucket in buckets:
                if not bucket:
                    continue
                deleted_total += self._delete_existing_bucket_rows(
                    table, monitor_date, bucket, codes
                )
            if deleted_total > 0:
                self.logger.info("检测到同 bucket 旧快照：已覆盖删除 %s 条。", deleted_total)

        if df.empty:
            self.logger.info("本次开盘监测结果全部为重复快照，跳过写入。")
            return

        try:
            self.db_writer.write_dataframe(df, table, if_exists="append")
            self.logger.info("开盘监测结果已写入表 %s：%s 条", table, len(df))
            self._ensure_snapshot_schema(table)
            self._ensure_strength_schema(table)
        except Exception as exc:  # noqa: BLE001
            self.logger.error("写入开盘监测表失败：%s", exc)

    def _export_csv(self, df: pd.DataFrame) -> None:
        if df.empty or (not self.params.export_csv):
            return

        app_sec = get_section("app") or {}
        base_dir = "output"
        if isinstance(app_sec, dict):
            base_dir = str(app_sec.get("output_dir", base_dir))

        outdir = Path(base_dir) / self.params.output_subdir
        outdir.mkdir(parents=True, exist_ok=True)

        monitor_date = str(df.iloc[0].get("monitor_date") or dt.date.today().isoformat())

        # 增量导出：文件名带上 checked_at，避免同一天多次运行覆盖
        suffix = ""
        if self.params.incremental_export_timestamp:
            checked_at = str(df.iloc[0].get("checked_at") or "").strip()
            # checked_at 形如：2025-12-15 09:30:00.123456
            if checked_at and " " in checked_at:
                time_part = checked_at.split(" ", 1)[1].replace(":", "").replace(
                    ".", ""
                )
                if time_part:
                    suffix = f"_{time_part}"

        path = outdir / f"open_monitor_{monitor_date}{suffix}.csv"

        export_df = df.copy()
        export_df = export_df.drop(columns=["snapshot_hash"], errors="ignore")
        gap_col = "live_gap_pct" if "live_gap_pct" in export_df.columns else "gap_pct"
        export_df[gap_col] = export_df[gap_col].apply(_to_float)
        # CSV 里把“状态正常且可执行”放在最前面，方便你开盘快速扫一眼
        action_rank = {"EXECUTE": 0, "WAIT": 1, "SKIP": 2, "UNKNOWN": 3}
        status_rank = {"ACTIVE": 0, "WAIT": 1, "EXPIRED": 2, "INVALID": 3, "UNKNOWN": 4}
        export_df["_action_rank"] = export_df["action"].map(action_rank).fillna(99)
        export_df["_status_rank"] = export_df["candidate_status"].map(status_rank).fillna(99)
        export_df = export_df.sort_values(
            by=["_status_rank", "_action_rank", gap_col], ascending=[True, True, True]
        )
        export_df = export_df.drop(columns=["_action_rank", "_status_rank"], errors="ignore")
        if self.params.export_top_n > 0:
            export_df = export_df.head(self.params.export_top_n)

        export_df.to_csv(path, index=False, encoding="utf-8-sig")
        self.logger.info("开盘监测 CSV 已导出：%s", path)

    # -------------------------
    # Public run
    # -------------------------
    def run(self, *, force: bool = False) -> None:
        """执行开盘监测。

        - 默认遵循 config.yaml: open_monitor.enabled。
        - 当 force=True 时，即便 enabled=false 也会执行（用于单独运行脚本）。
        """

        if (not force) and (not self.params.enabled):
            self.logger.info("open_monitor.enabled=false，跳过开盘监测。")
            return

        if force and (not self.params.enabled):
            self.logger.info("open_monitor.enabled=false，但 force=True，仍将执行开盘监测。")

        checked_at = dt.datetime.now()
        monitor_date = checked_at.date().isoformat()
        dedupe_bucket = self._calc_dedupe_bucket(checked_at)
        env_snapshot_bucket = dedupe_bucket
        env_snapshot_matched_bucket = False

        latest_trade_date, signal_dates, signals = self._load_recent_buy_signals()
        if not latest_trade_date or signals.empty:
            return

        codes = signals["code"].dropna().astype(str).unique().tolist()
        self.logger.info("待监测标的数量：%s（信号日：%s）", len(codes), signal_dates)

        quotes = self._fetch_quotes(codes)
        if quotes.empty:
            self.logger.warning("未获取到任何实时行情，将输出 UNKNOWN 结果。")
        else:
            self.logger.info("实时行情已获取：%s 条", len(quotes))

        latest_snapshots = self._load_latest_snapshots(latest_trade_date, codes)
        env_context = self.load_env_snapshot_context(
            monitor_date, dedupe_bucket, allow_fallback=True
        )
        if env_context:
            loaded_bucket = str(env_context.get("dedupe_bucket") or "")
            env_snapshot_bucket = loaded_bucket or env_snapshot_bucket
            env_snapshot_matched_bucket = bool(loaded_bucket == str(dedupe_bucket))
            if env_snapshot_matched_bucket:
                self.logger.info(
                    "已加载环境快照（monitor_date=%s, dedupe_bucket=%s）",
                    monitor_date,
                    env_snapshot_bucket,
                )
            else:
                self.logger.info(
                    "已加载当日最新环境快照（monitor_date=%s，未匹配 dedupe_bucket=%s，实际=%s）",
                    monitor_date,
                    dedupe_bucket,
                    env_snapshot_bucket,
                )
        if not env_context:
            self.logger.warning(
                "未找到环境快照（monitor_date=%s, dedupe_bucket=%s），请先运行 run_index_weekly_channel 产出环境。",
                monitor_date,
                dedupe_bucket,
            )
            return
        weekly_scenario = env_context.get("weekly_scenario", {}) if isinstance(env_context, dict) else {}
        if isinstance(weekly_scenario, dict):
            struct_tags = ",".join(weekly_scenario.get("weekly_structure_tags", []) or [])
            confirm_tags = ",".join(weekly_scenario.get("weekly_confirm_tags", []) or [])
            scene_code = weekly_scenario.get("weekly_scene_code")
            bias = weekly_scenario.get("weekly_bias")
            status = weekly_scenario.get("weekly_status")
            key_levels_short = str(weekly_scenario.get("weekly_key_levels_str") or "")[:120]
            self.logger.info(
                "周线情景：asof=%s current_week_closed=%s risk=%s(%.1f) scene=%s bias=%s status=%s levels=%s 确认标签=%s",
                weekly_scenario.get("weekly_asof_trade_date"),
                weekly_scenario.get("weekly_current_week_closed"),
                weekly_scenario.get("weekly_risk_level"),
                _to_float(weekly_scenario.get("weekly_risk_score")) or 0.0,
                scene_code,
                bias,
                status,
                key_levels_short,
                confirm_tags,
            )
            plan_a = str(weekly_scenario.get("weekly_plan_a") or "").strip()[:200]
            plan_b = str(weekly_scenario.get("weekly_plan_b") or "").strip()[:200]
            if plan_a:
                self.logger.info("周线 PlanA：%s", plan_a)
            if plan_b:
                self.logger.info("周线 PlanB：%s", plan_b)
            plan_a_if = weekly_scenario.get("weekly_plan_a_if")
            plan_b_if = weekly_scenario.get("weekly_plan_b_if")
            plan_a_confirm = weekly_scenario.get("weekly_plan_a_confirm")
            plan_b_recover = weekly_scenario.get("weekly_plan_b_recover_if")
            self.logger.info(
                "周线 Plan tokens: A_if=%s A_then=%s A_confirm=%s B_if=%s B_recover=%s",
                str(plan_a_if or "")[:120],
                str(weekly_scenario.get("weekly_plan_a_then") or "")[:64],
                str(plan_a_confirm or "")[:64],
                str(plan_b_if or "")[:120],
                str(plan_b_recover or "")[:120],
            )
        result = self._evaluate(
            signals,
            quotes,
            latest_snapshots,
            latest_trade_date,
            env_context,
            checked_at=checked_at,
        )

        weekly_gate_policy = self._resolve_env_weekly_gate_policy(env_context)
        if isinstance(env_context, dict):
            env_context["weekly_gate_policy"] = weekly_gate_policy
        if not result.empty and "dedupe_bucket" in result.columns:
            dedupe_bucket_val = str(result.iloc[0].get("dedupe_bucket") or "").strip()
            dedupe_bucket = dedupe_bucket_val or dedupe_bucket
            env_snapshot_matched_bucket = bool(
                env_context
                and str(env_context.get("dedupe_bucket") or "") == str(dedupe_bucket)
            )

        if env_context and not env_snapshot_matched_bucket:
            self._persist_env_snapshot(
                env_context,
                monitor_date,
                dedupe_bucket or env_snapshot_bucket,
                checked_at,
                weekly_gate_policy,
            )

        if result.empty:
            return

        summary = result["action"].value_counts(dropna=False).to_dict()
        self.logger.info("开盘监测结果统计：%s", summary)

        exec_df = result[result["action"] == "EXECUTE"].copy()
        gap_col = "live_gap_pct" if "live_gap_pct" in exec_df.columns else "gap_pct"
        exec_df[gap_col] = exec_df[gap_col].apply(_to_float)
        exec_df = exec_df.sort_values(by=gap_col, ascending=True)
        top_n = min(30, len(exec_df))
        if top_n > 0:
            preview = exec_df[
                [
                    "code",
                    "name",
                    "sig_close",
                    "live_open",
                    "live_latest",
                    gap_col,
                    "action_reason",
                ]
            ].head(top_n)
            self.logger.info(
                "可执行清单 Top%s（按 gap 由小到大）：\n%s",
                top_n,
                preview.to_string(index=False),
            )

        wait_df = result[result["action"] == "WAIT"].copy()
        wait_df[gap_col] = wait_df[gap_col].apply(_to_float)
        wait_df = wait_df.sort_values(by=gap_col, ascending=True)
        wait_top = min(10, len(wait_df))
        if wait_top > 0:
            wait_preview = wait_df[
                [
                    "code",
                    "name",
                    "sig_close",
                    "live_open",
                    "live_latest",
                    gap_col,
                    "status_reason",
                ]
            ].head(wait_top)
            self.logger.info(
                "WAIT 观察清单 Top%s（按 gap 由小到大）：\n%s",
                wait_top,
                wait_preview.to_string(index=False),
            )

        self._persist_results(result)
        self._export_csv(result)
