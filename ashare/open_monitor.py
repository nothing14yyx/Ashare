"""开盘监测：检查“前一交易日收盘信号”在今日开盘是否仍可执行。

目标：
- 读取 strategy_signal_events 中“最新交易日”的 BUY 信号（通常是昨天收盘跑出来的）。
- 在开盘/集合竞价阶段拉取实时行情（今开/最新价），做二次过滤：
  - 高开过多（追高风险/买不到合理价）
  - 低开破位（跌破 MA20 / 大幅低开）
  - 涨停（大概率买不到）

输出：
- 可选写入 MySQL：strategy_open_monitor_eval、strategy_open_monitor_quote（默认 append）
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

from .baostock_core import BaostockDataFetcher
from .baostock_session import BaostockSession
from .config import get_section
from .db import DatabaseConfig, MySQLWriter
from .env_snapshot_utils import resolve_weekly_asof_date
from .ma5_ma20_trend_strategy import _atr, _macd
from .schema_manager import (
    TABLE_ENV_INDEX_SNAPSHOT,
    TABLE_STRATEGY_INDICATOR_DAILY,
    TABLE_STRATEGY_OPEN_MONITOR_ENV,
    TABLE_STRATEGY_OPEN_MONITOR_EVAL,
    TABLE_STRATEGY_OPEN_MONITOR_QUOTE,
    TABLE_STRATEGY_SIGNAL_EVENTS,
    STRATEGY_CODE_MA5_MA20_TREND,
    VIEW_STRATEGY_OPEN_MONITOR_WIDE,
)
from .utils.convert import to_float as _to_float
from .utils.logger import setup_logger
from .weekly_env_builder import WeeklyEnvironmentBuilder


SNAPSHOT_HASH_EXCLUDE = {
    "checked_at",
    "run_id",
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


def derive_index_gate_action(regime: str | None, position_hint: float | None) -> str | None:
    regime_norm = str(regime or "").strip().upper() or None
    pos_hint_val = _to_float(position_hint)

    if regime_norm in {"BREAKDOWN", "BEAR_CONFIRMED"}:
        return "STOP"

    if regime_norm == "RISK_OFF":
        return "WAIT"

    if regime_norm == "PULLBACK":
        if pos_hint_val is not None and pos_hint_val <= 0.3:
            return "WAIT"
        return "ALLOW"

    if regime_norm == "RISK_ON":
        return "ALLOW"

    if pos_hint_val is not None:
        if pos_hint_val <= 0:
            return "STOP"
        if pos_hint_val < 0.3:
            return "WAIT"
        return "ALLOW"

    return None


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
class RunupMetrics:
    runup_ref_price: float | None
    runup_ref_source: str | None
    runup_from_sigclose: float | None
    runup_from_sigclose_atr: float | None


def compute_runup_metrics(
    sig_close: float | None,
    *,
    asof_close: float | None,
    live_high: float | None,
    sig_atr14: float | None,
    eps: float = 1e-6,
) -> RunupMetrics:
    live_high_val = _to_float(live_high)
    asof_close_val = _to_float(asof_close)
    sig_close_val = _to_float(sig_close)
    sig_atr_val = _to_float(sig_atr14)

    ref_price = None
    ref_source = None
    if live_high_val is not None and asof_close_val is not None:
        ref_price = max(live_high_val, asof_close_val)
        ref_source = "max(asof_close, live_high)"
    elif live_high_val is not None:
        ref_price = live_high_val
        ref_source = "live_high"
    elif asof_close_val is not None:
        ref_price = asof_close_val
        ref_source = "asof_close"

    runup_from_sigclose = None
    runup_from_sigclose_atr = None
    if ref_price is not None and sig_close_val is not None:
        runup_from_sigclose = ref_price - sig_close_val
        if sig_atr_val is not None:
            runup_from_sigclose_atr = runup_from_sigclose / max(sig_atr_val, eps)

    return RunupMetrics(
        runup_ref_price=ref_price,
        runup_ref_source=ref_source,
        runup_from_sigclose=runup_from_sigclose,
        runup_from_sigclose_atr=runup_from_sigclose_atr,
    )


def evaluate_runup_breach(
    metrics: RunupMetrics,
    *,
    runup_atr_max: float | None,
    runup_atr_tol: float,
    dev_ma20_atr: float | None = None,
    dev_ma20_atr_min: float | None = None,
) -> tuple[bool, str | None]:
    if runup_atr_max is None or metrics.runup_from_sigclose_atr is None:
        return False, None

    if dev_ma20_atr_min is not None:
        if dev_ma20_atr is None or dev_ma20_atr < dev_ma20_atr_min:
            return False, None

    threshold = runup_atr_max + runup_atr_tol
    if metrics.runup_from_sigclose_atr > threshold:
        ref_price_display = (
            f"{metrics.runup_ref_price:.2f}" if metrics.runup_ref_price is not None else "N/A"
        )
        ref_source = metrics.runup_ref_source or "unknown"
        reason = (
            f"runup_ref_source={ref_source}, runup_ref_price={ref_price_display}, "
            f"runup_from_sigclose_atr={metrics.runup_from_sigclose_atr:.3f} "
            f"> runup_atr_max={runup_atr_max:.3f}+tol={runup_atr_tol:.3f}"
        )
        return True, reason
    return False, None


@dataclass(frozen=True)
class OpenMonitorParams:
    """开盘监测参数（支持从 config.yaml 的 open_monitor 覆盖）。"""

    enabled: bool = True

    # 信号来源表：默认使用 strategy_signal_events
    signal_events_table: str = TABLE_STRATEGY_SIGNAL_EVENTS
    indicator_table: str = TABLE_STRATEGY_INDICATOR_DAILY
    quote_table: str = TABLE_STRATEGY_OPEN_MONITOR_QUOTE
    strategy_code: str = STRATEGY_CODE_MA5_MA20_TREND

    # 输出表：开盘检查结果
    output_table: str = TABLE_STRATEGY_OPEN_MONITOR_EVAL
    open_monitor_view: str = VIEW_STRATEGY_OPEN_MONITOR_WIDE

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

    # 乖离/拉升阈值
    runup_atr_max: float = 1.2  # 信号后最大拉升 / ATR > runup_atr_max → skip
    runup_atr_tol: float = 0.02  # 运行容差：runup_from_sigclose_atr > runup_atr_max + tol
    pullback_runup_atr_max: float = 1.5  # 回踩形态使用更宽松的拉升阈值
    pullback_runup_dev_ma20_atr_min: float = 1.0  # 回踩形态仅在 dev_ma20_atr 超过该阈值时判定追高
    dev_ma5_atr_max: float = 2.0
    dev_ma20_atr_max: float = 2.5

    # 风控：开盘入场价为基准的 ATR 止损
    stop_atr_mult: float = 2.0         # trade_stop_ref = entry - stop_atr_mult*ATR

    # 情绪过滤：信号日（昨日收盘）如果是接近涨停的大阳线，次日默认不追
    signal_day_limit_up_pct: float = 0.095  # 9.5%（兼容“接近涨停”）

    # 输出控制
    write_to_db: bool = True

    # 指数快照配置
    index_code: str = "sh.000001"
    index_hist_lookback_days: int = 250

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
    run_id_minutes: int = 5

    # 环境快照表：存储周线计划等“批次级别”信息，避免在每条标的记录里重复。
    env_snapshot_table: str = TABLE_STRATEGY_OPEN_MONITOR_ENV

    # 指数环境快照表：按哈希去重存储单份指数环境，避免在事实表重复写入。
    env_index_snapshot_table: str = TABLE_ENV_INDEX_SNAPSHOT

    # 同一批次内同一 code 只保留“最新 date（信号日）”那条 BUY 信号。
    # 目的：避免同一批次出现重复 code（例如同一只股票在 12-09 与 12-11 都触发 BUY）。
    unique_code_latest_date_only: bool = True

    # 输出模式：FULL 保留全部字段，COMPACT 只保留核心字段
    output_mode: str = "FULL"

    # 环境快照持久化：与行情/评估表保持一致，默认写入
    persist_env_snapshot: bool = True

    @classmethod
    def from_config(cls) -> "OpenMonitorParams":
        sec = get_section("open_monitor") or {}
        if not isinstance(sec, dict):
            sec = {}

        # 默认信号/指标表与策略保持一致
        strat = get_section("strategy_ma5_ma20_trend") or {}
        if isinstance(strat, dict):
            default_indicator = strat.get("indicator_table", cls.indicator_table)
            default_events = (
                strat.get("signal_events_table")
                or strat.get("signals_table")
                or cls.signal_events_table
            )
            default_strategy_code = strat.get("strategy_code", STRATEGY_CODE_MA5_MA20_TREND)
        else:
            default_indicator = cls.indicator_table
            default_events = cls.signal_events_table
            default_strategy_code = STRATEGY_CODE_MA5_MA20_TREND

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

        run_id_configured = sec.get("run_id_minutes")
        run_id_minutes = (
            _get_int("run_id_minutes", interval_minutes)
            if run_id_configured is not None
            else interval_minutes
        )

        return cls(
            enabled=_get_bool("enabled", cls.enabled),
            signal_events_table=str(
                sec.get("signal_events_table", default_events)
            ).strip()
            or default_events,
            indicator_table=str(sec.get("indicator_table", default_indicator)).strip()
            or default_indicator,
            quote_table=str(sec.get("quote_table", cls.quote_table)).strip() or cls.quote_table,
            strategy_code=str(sec.get("strategy_code", default_strategy_code)).strip()
            or default_strategy_code,
            output_table=str(sec.get("output_table", cls.output_table)).strip() or cls.output_table,
            open_monitor_view=str(
                sec.get("open_monitor_view", cls.open_monitor_view)
            ).strip()
            or cls.open_monitor_view,
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
            index_code=str(sec.get("index_code", cls.index_code)).strip()
            or cls.index_code,
            index_hist_lookback_days=_get_int(
                "index_hist_lookback_days", cls.index_hist_lookback_days
            ),

            max_entry_vs_ma5_pct=_normalize_ratio_pct(
                _get_float("max_entry_vs_ma5_pct", cls.max_entry_vs_ma5_pct),
                "max_entry_vs_ma5_pct",
            ),
            runup_atr_max=_get_float("runup_atr_max", cls.runup_atr_max),
            runup_atr_tol=_get_float("runup_atr_tol", cls.runup_atr_tol),
            pullback_runup_atr_max=_get_float(
                "pullback_runup_atr_max", cls.pullback_runup_atr_max
            ),
            pullback_runup_dev_ma20_atr_min=_get_float(
                "pullback_runup_dev_ma20_atr_min", cls.pullback_runup_dev_ma20_atr_min
            ),
            dev_ma5_atr_max=_get_float("dev_ma5_atr_max", cls.dev_ma5_atr_max),
            dev_ma20_atr_max=_get_float("dev_ma20_atr_max", cls.dev_ma20_atr_max),
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
            run_id_minutes=run_id_minutes,
            unique_code_latest_date_only=_get_bool(
                "unique_code_latest_date_only", cls.unique_code_latest_date_only
            ),
            env_snapshot_table=str(
                sec.get("env_snapshot_table", cls.env_snapshot_table)
            ).strip()
            or cls.env_snapshot_table,
            env_index_snapshot_table=str(
                sec.get("env_index_snapshot_table", cls.env_index_snapshot_table)
            ).strip()
            or cls.env_index_snapshot_table,
            output_mode=str(sec.get("output_mode", cls.output_mode)).strip().upper()
            or cls.output_mode,
            persist_env_snapshot=_get_bool(
                "persist_env_snapshot", cls.persist_env_snapshot
            ),
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
        self.weekly_env_fallback_asof_date: str | None = None

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

    def _calc_run_id(self, ts: dt.datetime) -> str:
        window_minutes = max(int(self.params.run_id_minutes or 5), 1)

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
        slot_minute = (minute_of_day // window_minutes) * window_minutes
        slot_time = dt.datetime.combine(ts.date(), dt.time(slot_minute // 60, slot_minute % 60))
        return slot_time.strftime("%Y-%m-%d %H:%M")

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

    def _persist_env_snapshot(
        self,
        env_context: dict[str, Any] | None,
        monitor_date: str,
        run_id: str,
        checked_at: dt.datetime,
        env_weekly_gate_policy: str | None,
    ) -> None:
        if not env_context:
            return

        table = self.params.env_snapshot_table
        if not table:
            return

        monitor_dt = pd.to_datetime(monitor_date, errors="coerce")
        monitor_date_val = monitor_dt.date() if not pd.isna(monitor_dt) else monitor_date

        payload: dict[str, Any] = {}
        weekly_scenario = env_context.get("weekly_scenario") if isinstance(env_context, dict) else {}
        if not isinstance(weekly_scenario, dict):
            weekly_scenario = {}

        def _get_env(key: str) -> Any:  # noqa: ANN401
            if isinstance(env_context, dict) and env_context.get(key) not in (None, ""):
                return env_context.get(key)
            return weekly_scenario.get(key)

        payload["monitor_date"] = monitor_date_val
        payload["run_id"] = run_id
        payload["checked_at"] = checked_at
        env_weekly_asof = _get_env("weekly_asof_trade_date")
        if env_weekly_asof is not None:
            parsed_weekly = pd.to_datetime(env_weekly_asof, errors="coerce")
            env_weekly_asof = parsed_weekly.date() if not pd.isna(parsed_weekly) else env_weekly_asof
        payload["env_weekly_asof_trade_date"] = env_weekly_asof
        payload["env_weekly_risk_level"] = _get_env("weekly_risk_level")
        payload["env_weekly_scene"] = _get_env("weekly_scene_code")
        payload["env_weekly_gate_policy"] = env_weekly_gate_policy
        payload["env_weekly_gate_action"] = _get_env("weekly_gate_policy") or env_weekly_gate_policy
        index_snapshot = {}
        if isinstance(env_context, dict):
            raw_index_snapshot = env_context.get("index_intraday")
            if isinstance(raw_index_snapshot, dict):
                index_snapshot = raw_index_snapshot
        env_index_hash = index_snapshot.get("env_index_snapshot_hash")
        payload["env_index_snapshot_hash"] = env_index_hash
        payload["env_final_gate_action"] = self._merge_gate_actions(
            env_weekly_gate_policy, index_snapshot.get("env_index_gate_action")
        )

        if not self._table_exists(table):
            self.logger.error("环境快照表 %s 不存在，已跳过写入。", table)
            return
        columns = list(payload.keys())
        update_cols = [c for c in columns if c not in {"monitor_date", "run_id"}]
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
                "环境快照已写入表 %s（monitor_date=%s, run_id=%s）",
                table,
                monitor_date,
                run_id,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("写入环境快照失败：%s", exc)

    def _persist_index_snapshot(
        self,
        snapshot: dict[str, Any],
        *,
        table: str,
    ) -> str | None:
        if not snapshot or not table:
            return None

        if not self._table_exists(table):
            self.logger.error("指数环境快照表 %s 不存在，已跳过写入。", table)
            return None

        for date_col in ["monitor_date", "asof_trade_date", "live_trade_date"]:
            if date_col in snapshot:
                parsed = pd.to_datetime(snapshot.get(date_col), errors="coerce")
                snapshot[date_col] = parsed.date() if not pd.isna(parsed) else snapshot.get(date_col)

        columns = list(snapshot.keys())
        if "snapshot_hash" not in columns:
            return None

        col_clause = ", ".join(f"`{c}`" for c in columns)
        value_clause = ", ".join(f":{c}" for c in columns)
        stmt = text(
            f"""
            INSERT INTO `{table}` ({col_clause})
            VALUES ({value_clause})
            ON DUPLICATE KEY UPDATE `snapshot_hash` = `snapshot_hash`
            """
        )

        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(stmt, snapshot)
            self.logger.info(
                "指数环境快照已写入表 %s（hash=%s）。",
                table,
                snapshot.get("snapshot_hash"),
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("写入指数环境快照失败：%s", exc)
            return None

        return str(snapshot.get("snapshot_hash") or "")

    def _load_index_snapshot_by_hash(self, snapshot_hash: str | None) -> dict[str, Any]:
        if not snapshot_hash:
            return {}
        table = self.params.env_index_snapshot_table
        if not (table and self._table_exists(table)):
            return {}
        stmt = text(
            f"""
            SELECT *
            FROM `{table}`
            WHERE `snapshot_hash` = :h
            ORDER BY `checked_at` DESC
            LIMIT 1
            """
        )
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"h": snapshot_hash})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取指数环境快照失败：%s", exc)
            return {}
        if df.empty:
            return {}
        raw = df.iloc[0].to_dict()
        normalized = {
            "env_index_snapshot_hash": raw.get("snapshot_hash"),
            "env_index_code": raw.get("index_code"),
            "env_index_asof_trade_date": raw.get("asof_trade_date"),
            "env_index_live_trade_date": raw.get("live_trade_date"),
            "env_index_asof_close": raw.get("asof_close"),
            "env_index_asof_ma20": raw.get("asof_ma20"),
            "env_index_asof_ma60": raw.get("asof_ma60"),
            "env_index_asof_macd_hist": raw.get("asof_macd_hist"),
            "env_index_asof_atr14": raw.get("asof_atr14"),
            "env_index_live_open": raw.get("live_open"),
            "env_index_live_high": raw.get("live_high"),
            "env_index_live_low": raw.get("live_low"),
            "env_index_live_latest": raw.get("live_latest"),
            "env_index_live_pct_change": raw.get("live_pct_change"),
            "env_index_live_volume": raw.get("live_volume"),
            "env_index_live_amount": raw.get("live_amount"),
            "env_index_dev_ma20_atr": raw.get("dev_ma20_atr"),
            "env_index_gate_action": raw.get("gate_action"),
            "env_index_gate_reason": raw.get("gate_reason"),
            "env_index_position_cap": raw.get("position_cap"),
            "checked_at": raw.get("checked_at"),
        }
        return normalized

    def _load_env_snapshot_context(
        self, monitor_date: str, run_id: str | None = None
    ) -> dict[str, Any] | None:
        table = self.params.env_snapshot_table
        if not (table and monitor_date and self._table_exists(table)):
            return None

        def _load_latest(match_run_id: bool) -> pd.DataFrame:
            run_clause = "`run_id` = :b AND" if match_run_id else ""
            stmt = text(
                f"""
                SELECT * FROM `{table}`
                WHERE {run_clause} `monitor_date` = :d
                ORDER BY `checked_at` DESC
                LIMIT 1
                """
            )

            params: dict[str, Any] = {"d": monitor_date}
            if match_run_id and run_id is not None:
                params["b"] = run_id
            elif match_run_id:
                return pd.DataFrame()

            with self.db_writer.engine.begin() as conn:
                return pd.read_sql_query(stmt, conn, params=params)

        try:
            if run_id is None:
                df = _load_latest(match_run_id=False)
            else:
                df = _load_latest(match_run_id=True)
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
            "weekly_structure_status": row.get("env_weekly_structure_status"),
            "weekly_pattern_status": row.get("env_weekly_pattern_status"),
            "weekly_gating_enabled": row.get("env_weekly_gating_enabled"),
            "weekly_tags": row.get("env_weekly_tags"),
            "weekly_money_proxy": row.get("env_weekly_money_proxy"),
            "weekly_note": row.get("env_weekly_note"),
        }

        def _is_empty_env_value(value: Any) -> bool:
            if value is None:
                return True
            if isinstance(value, str):
                return value.strip() == ""
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

        raw_plan_json = row.get("env_weekly_plan_json")
        plan_dict: dict[str, Any] = {}
        if isinstance(raw_plan_json, str):
            plan_text = raw_plan_json.strip()
            if plan_text:
                try:
                    parsed = json.loads(plan_text)
                    if isinstance(parsed, dict):
                        plan_dict = parsed
                except Exception as exc:  # noqa: BLE001
                    self.logger.debug("解析 env_weekly_plan_json 失败：%s", exc)
        elif isinstance(raw_plan_json, dict):
            plan_dict = raw_plan_json

        def _backfill(target_key: str, source_key: str | None = None) -> None:
            if not isinstance(plan_dict, dict):
                return
            if not _is_empty_env_value(weekly_scenario.get(target_key)):
                return
            plan_key = source_key or target_key
            value = plan_dict.get(plan_key)
            if _is_empty_env_value(value):
                return
            weekly_scenario[target_key] = value

        _backfill("weekly_current_week_closed")
        _backfill("weekly_risk_score")
        _backfill("weekly_key_levels_str")
        _backfill("weekly_plan_a_if")
        _backfill("weekly_plan_a_then")
        _backfill("weekly_plan_a_confirm")
        _backfill("weekly_plan_b_if")
        _backfill("weekly_plan_b_then")
        _backfill("weekly_plan_b_recover_if")
        _backfill("weekly_structure_tags")
        _backfill("weekly_confirm_tags")
        _backfill("weekly_money_tags")
        _backfill("weekly_direction_confirmed")
        _backfill("weekly_key_levels")
        _backfill("weekly_structure_status")
        _backfill("weekly_pattern_status")
        _backfill("weekly_status", "weekly_structure_status")

        index_snapshot_hash = row.get("env_index_snapshot_hash")
        loaded_index_snapshot = self._load_index_snapshot_by_hash(index_snapshot_hash)

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
            "weekly_structure_status": weekly_scenario.get("weekly_structure_status")
            or weekly_scenario.get("weekly_status"),
            "weekly_pattern_status": weekly_scenario.get("weekly_pattern_status"),
            "weekly_direction_confirmed": weekly_scenario.get(
                "weekly_direction_confirmed"
            ),
            "weekly_money_tags": weekly_scenario.get("weekly_money_tags"),
            "weekly_gating_enabled": bool(
                weekly_scenario.get("weekly_gating_enabled", False)
            ),
            "weekly_tags": weekly_scenario.get("weekly_tags"),
            "weekly_money_proxy": weekly_scenario.get("weekly_money_proxy"),
            "weekly_note": weekly_scenario.get("weekly_note"),
            "regime": row.get("env_regime"),
            "position_hint": row.get("env_position_hint"),
            "position_hint_raw": row.get("env_position_hint_raw"),
            "effective_position_hint": row.get("env_position_hint"),
            "weekly_gate_policy": (
                row.get("env_weekly_gate_policy")
                or row.get("env_weekly_gate_action")
            ),
            "run_id": row.get("run_id"),
            "env_index_snapshot_hash": index_snapshot_hash,
        }
        index_snapshot = loaded_index_snapshot or {
            "env_index_code": row.get("env_index_code"),
            "env_index_asof_trade_date": row.get("env_index_asof_trade_date"),
            "env_index_asof_close": row.get("env_index_asof_close"),
            "env_index_asof_ma20": row.get("env_index_asof_ma20"),
            "env_index_asof_ma60": row.get("env_index_asof_ma60"),
            "env_index_asof_macd_hist": row.get("env_index_asof_macd_hist"),
            "env_index_asof_atr14": row.get("env_index_asof_atr14"),
            "env_index_live_trade_date": row.get("env_index_live_trade_date"),
            "env_index_live_open": row.get("env_index_live_open"),
            "env_index_live_high": row.get("env_index_live_high"),
            "env_index_live_low": row.get("env_index_live_low"),
            "env_index_live_latest": row.get("env_index_live_latest"),
            "env_index_live_pct_change": row.get("env_index_live_pct_change"),
            "env_index_live_volume": row.get("env_index_live_volume"),
            "env_index_live_amount": row.get("env_index_live_amount"),
            "env_index_dev_ma20_atr": row.get("env_index_dev_ma20_atr"),
            "env_index_gate_action": row.get("env_index_gate_action"),
            "env_index_gate_reason": row.get("env_index_gate_reason"),
            "env_index_position_cap": row.get("env_index_position_cap"),
            "env_final_gate_action": row.get("env_final_gate_action"),
            "env_index_snapshot_hash": index_snapshot_hash,
        }
        env_context["index_intraday"] = index_snapshot
        if env_context.get("position_hint") is None:
            env_context["position_hint"] = index_snapshot.get("env_index_position_cap")
            env_context["effective_position_hint"] = env_context["position_hint"]

        return env_context

    def load_env_snapshot_context(
        self,
        monitor_date: str,
        run_id: str | None = None,
        *,
        allow_fallback: bool = True,
    ) -> dict[str, Any] | None:
        """公开的环境快照读取接口，必要时可回退到当日最新。"""

        self.weekly_env_fallback_asof_date = None
        env_context = self._load_env_snapshot_context(monitor_date, run_id)
        if env_context:
            return env_context

        if allow_fallback and run_id is not None:
            env_context = self._load_env_snapshot_context(monitor_date, None)
            if env_context:
                return env_context

        if allow_fallback:
            try:
                asof_date = resolve_weekly_asof_date(include_current_week=False)
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("解析周线 asof_date 失败：%s", exc)
                return None

            if not asof_date:
                return None

            self.logger.info(
                "最近已收盘周 asof_date=%s",
                asof_date,
            )

            env_context = self._load_env_snapshot_context(
                asof_date, f"WEEKLY_{asof_date}"
            )
            if env_context:
                self.weekly_env_fallback_asof_date = asof_date
                return env_context

            env_context = self._load_env_snapshot_context(asof_date, None)
            if env_context:
                self.weekly_env_fallback_asof_date = asof_date
                return env_context

        return None

    def _resolve_env_weekly_gate_policy(
        self, env_context: dict[str, Any] | None
    ) -> str | None:
        return self.env_builder.resolve_env_weekly_gate_policy(env_context)

    def _load_existing_snapshot_keys(
        self, table: str, monitor_date: str, codes: List[str], run_id: str
    ) -> set[tuple[str, str, str, str]]:
        if not (
            table
            and monitor_date
            and codes
            and run_id
            and self._table_exists(table)
        ):
            return set()

        stmt = text(
            f"""
            SELECT `monitor_date`, `sig_date`, `code`, `run_id`
            FROM `{table}`
            WHERE `monitor_date` = :d AND `code` IN :codes AND `run_id` = :b
        """
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={"d": monitor_date, "codes": codes, "b": run_id},
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取已存在的 run_id 键失败：%s", exc)
            return set()

        existing: set[tuple[str, str, str, str]] = set()
        for _, row in df.iterrows():
            code = str(row.get("code") or "").strip()
            date_val = str(row.get("sig_date") or "").strip()
            monitor = str(row.get("monitor_date") or "").strip()
            run_id_val = str(row.get("run_id") or "").strip()
            if code and date_val and monitor and run_id_val:
                existing.add((monitor, date_val, code, run_id_val))
        return existing

    def _delete_existing_run_rows(
        self, table: str, monitor_date: str, run_id: str, codes: List[str]
    ) -> int:
        if not (
            table
            and monitor_date
            and run_id
            and codes
            and self._table_exists(table)
        ):
            return 0

        stmt = text(
            "DELETE FROM `{table}` WHERE `monitor_date` = :d AND `run_id` = :b AND `code` IN :codes".format(
                table=table
            )
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.db_writer.engine.begin() as conn:
                result = conn.execute(
                    stmt, {"d": monitor_date, "b": run_id, "codes": codes}
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
        events_table = self.params.signal_events_table
        indicator_table = self.params.indicator_table
        monitor_date = dt.date.today().isoformat()
        # 严格最近 N 个交易日窗口：只由 signal_lookback_days 决定
        lookback = max(int(self.params.signal_lookback_days or 0), 1)

        try:
            with self.db_writer.engine.begin() as conn:
                max_df = pd.read_sql_query(
                    text(
                        f"""
                        SELECT MAX(`sig_date`) AS max_date
                        FROM `{events_table}`
                        WHERE `strategy_code` = :strategy
                        """
                    ),
                    conn,
                    params={"strategy": self.params.strategy_code},
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.error("读取 signal_events_table=%s 失败：%s", events_table, exc)
            return None, [], pd.DataFrame()

        if max_df.empty:
            return None, [], pd.DataFrame()

        max_date = max_df.iloc[0].get("max_date")
        if pd.isna(max_date) or not str(max_date).strip():
            return None, [], pd.DataFrame()

        latest_trade_date = str(max_date)[:10]

        # 1) 先取严格“最近 N 个交易日窗口”（优先日线表；无日线表则回退信号/指标表的日期序列）
        base_table = self._daily_table()
        date_col = "date"
        if not self._table_exists(base_table):
            if self._table_exists(indicator_table):
                base_table = indicator_table
                date_col = "trade_date"
            else:
                base_table = events_table
                date_col = "sig_date"

        try:
            with self.db_writer.engine.begin() as conn:
                trade_dates_df = pd.read_sql_query(
                    text(
                        f"""
                        SELECT DISTINCT CAST(`{date_col}` AS CHAR) AS d
                        FROM `{base_table}`
                        WHERE `{date_col}` <= :base_date
                        ORDER BY `{date_col}` DESC
                        LIMIT :n
                        """
                    ),
                    conn,
                    params={"base_date": latest_trade_date, "n": lookback},
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.error("读取最近交易日窗口失败：%s", exc)
            return latest_trade_date, [], pd.DataFrame()

        trade_dates = (
            trade_dates_df["d"].dropna().astype(str).str[:10].tolist()
            if trade_dates_df is not None and not trade_dates_df.empty
            else []
        )
        if not trade_dates:
            self.logger.info(
                "%s 无法获得最近 %s 个交易日窗口，跳过开盘监测。", latest_trade_date, lookback
            )
            return latest_trade_date, [], pd.DataFrame()

        window_latest = trade_dates[0]
        window_earliest = trade_dates[-1]

        # 2) 只在该交易日窗口内筛 BUY 信号日（避免 BUY 稀疏导致日期漂移到更早）
        try:
            with self.db_writer.engine.begin() as conn:
                buy_dates_df = pd.read_sql_query(
                    text(
                        f"""
                        SELECT DISTINCT `sig_date`
                        FROM `{events_table}`
                        WHERE `signal` = 'BUY'
                          AND `strategy_code` = :strategy
                          AND `sig_date` <= :latest
                          AND `sig_date` >= :earliest
                        ORDER BY `sig_date` DESC
                        """
                    ),
                    conn,
                    params={
                        "latest": window_latest,
                        "earliest": window_earliest,
                        "strategy": self.params.strategy_code,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.error("读取窗口内 BUY 信号日期失败：%s", exc)
            return latest_trade_date, [], pd.DataFrame()

        if buy_dates_df.empty:
            self.logger.info(
                "最近 %s 个交易日窗口（%s~%s，最新=%s）内无 BUY 信号，跳过开盘监测。",
                lookback,
                window_earliest,
                window_latest,
                latest_trade_date,
            )
            return latest_trade_date, [], pd.DataFrame()

        signal_dates = [str(v)[:10] for v in buy_dates_df["sig_date"].tolist() if str(v).strip()]
        self.logger.info(
            "回看最近 %s 个交易日窗口（%s~%s，最新=%s）BUY 信号：%s",
            lookback,
            window_earliest,
            window_latest,
            latest_trade_date,
            signal_dates,
        )

        events_stmt = text(
            f"""
            SELECT
              `sig_date`,
              `code`,
              `signal`,
              `reason`,
              `risk_tag`,
              `risk_note`,
              `stop_ref`
            FROM `{events_table}`
            WHERE `sig_date` IN :dates
              AND `signal` = 'BUY'
              AND `strategy_code` = :strategy
            """
        ).bindparams(bindparam("dates", expanding=True))

        with self.db_writer.engine.begin() as conn:
            try:
                events_df = pd.read_sql_query(
                    events_stmt,
                    conn,
                    params={"dates": signal_dates, "strategy": self.params.strategy_code},
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.error("读取 %s BUY 信号失败：%s", events_table, exc)
                return latest_trade_date, signal_dates, pd.DataFrame()

        if events_df.empty:
            self.logger.info("%s 内无 BUY 信号，跳过开盘监测。", signal_dates)
            return latest_trade_date, signal_dates, events_df

        events_df["code"] = events_df["code"].astype(str)
        events_df["sig_date"] = pd.to_datetime(events_df["sig_date"], errors="coerce")

        indicator_cols = [
            "trade_date",
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
            "ret_10",
            "ret_20",
            "limit_up_cnt_20",
            "ma20_bias",
            "yearline_state",
        ]
        ind_df = pd.DataFrame()
        if indicator_table and self._table_exists(indicator_table):
            try:
                with self.db_writer.engine.begin() as conn:
                    ind_df = pd.read_sql_query(
                        text(
                            f"""
                            SELECT {",".join(f"`{c}`" for c in indicator_cols)}
                            FROM `{indicator_table}`
                            WHERE `trade_date` IN :dates AND `code` IN :codes
                            """
                        ).bindparams(bindparam("dates", expanding=True), bindparam("codes", expanding=True)),
                        conn,
                        params={
                            "dates": signal_dates,
                            "codes": events_df["code"].dropna().astype(str).unique().tolist(),
                        },
                    )
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("读取指标表 %s 失败，将跳过指标合并：%s", indicator_table, exc)
                ind_df = pd.DataFrame()

        merged = events_df.copy()
        if not ind_df.empty:
            ind_df = ind_df.rename(columns={"trade_date": "sig_date"})
            ind_df["sig_date"] = pd.to_datetime(ind_df["sig_date"], errors="coerce")
            merged = merged.merge(ind_df, on=["sig_date", "code"], how="left")

        base_cols = [
            "sig_date",
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
            "yearline_state",
        ]
        optional_cols = [
            "ret_10",
            "ret_20",
            "limit_up_cnt_20",
            "ma20_bias",
            "risk_tag",
            "risk_note",
        ]
        for col in base_cols + optional_cols:
            if col not in merged.columns:
                merged[col] = None

        merged["sig_date"] = merged["sig_date"].dt.strftime("%Y-%m-%d")

        # 严格去重：同一 code 只保留最新信号日（date）那条记录。
        # 这能避免同一批次 open_monitor 出现重复 code（但信号日/入选原因不同）的情况。
        if self.params.unique_code_latest_date_only:
            before = len(merged)
            merged["_date_dt"] = pd.to_datetime(merged["sig_date"], errors="coerce")
            merged = merged.sort_values(by=["code", "_date_dt"], ascending=[True, False])
            merged = merged.drop_duplicates(subset=["code"], keep="first")
            merged = merged.drop(columns=["_date_dt"], errors="ignore")
            dropped = before - len(merged)
            if dropped > 0:
                self.logger.info(
                    "同一 code 多次触发 BUY：已按最新信号日去重 %s 条（保留 %s 条）。",
                    dropped,
                    len(merged),
                )
            # 同步更新 signal_dates（仅用于日志展示/后续涨跌幅回补循环），避免误解。
            signal_dates = sorted(merged["sig_date"].dropna().unique().tolist(), reverse=True)
        min_date = merged["sig_date"].min()
        trade_age_map = self._load_trade_age_map(latest_trade_date, str(min_date), monitor_date)
        merged["signal_age"] = merged["sig_date"].map(trade_age_map)

        try:
            for d in signal_dates:
                codes = merged.loc[merged["sig_date"] == d, "code"].dropna().unique().tolist()
                pct_map = self._load_signal_day_pct_change(d, codes)
                mask = merged["sig_date"] == d
                merged.loc[mask, "_signal_day_pct_change"] = merged.loc[mask, "code"].map(pct_map)
        except Exception:
            merged["_signal_day_pct_change"] = None

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
        df = merged.rename(columns={k: v for k, v in signal_prefix_map.items() if k in merged.columns})
        for src, target in signal_prefix_map.items():
            if target not in df.columns:
                df[target] = None
        return latest_trade_date, signal_dates, df

    def _load_latest_snapshots(self, latest_trade_date: str, codes: List[str]) -> pd.DataFrame:
        if not latest_trade_date or not codes:
            return pd.DataFrame()

        indicator_table = self.params.indicator_table
        events_table = self.params.signal_events_table
        if not indicator_table or not self._table_exists(indicator_table):
            return pd.DataFrame()

        codes = [str(c) for c in codes if str(c).strip()]
        if not codes:
            return pd.DataFrame()

        indicator_cols = [
            "trade_date",
            "code",
            "close",
            "ma5",
            "ma20",
            "ma60",
            "ma250",
            "vol_ratio",
            "macd_hist",
            "atr14",
        ]
        indicator_stmt = (
            text(
                f"""
                SELECT {",".join(f"`{c}`" for c in indicator_cols)}
                FROM `{indicator_table}`
                WHERE `trade_date` = :d AND `code` IN :codes
                """
            ).bindparams(bindparam("codes", expanding=True))
        )
        try:
            with self.db_writer.engine.begin() as conn:
                ind_df = pd.read_sql_query(
                    indicator_stmt,
                    conn,
                    params={"d": latest_trade_date, "codes": codes},
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取最新指标失败，将跳过最新快照：%s", exc)
            return pd.DataFrame()

        if ind_df is None or ind_df.empty:
            return pd.DataFrame()

        stop_df = pd.DataFrame()
        if events_table and self._table_exists(events_table):
            try:
                with self.db_writer.engine.begin() as conn:
                    stop_df = pd.read_sql_query(
                        text(
                            f"""
                            SELECT `sig_date`,`code`,`stop_ref`
                            FROM `{events_table}`
                            WHERE `sig_date` = :d
                              AND `strategy_code` = :strategy
                              AND `code` IN :codes
                            """
                        ).bindparams(bindparam("codes", expanding=True)),
                        conn,
                        params={
                            "d": latest_trade_date,
                            "codes": codes,
                            "strategy": self.params.strategy_code,
                        },
                    )
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("读取 stop_ref 失败：%s", exc)
                stop_df = pd.DataFrame()

        ind_df["code"] = ind_df["code"].astype(str)
        merged = ind_df.copy()
        if not stop_df.empty:
            stop_df = stop_df.rename(columns={"sig_date": "trade_date"})
            stop_df["trade_date"] = pd.to_datetime(stop_df["trade_date"]).dt.strftime("%Y-%m-%d")
            merged["trade_date"] = pd.to_datetime(merged["trade_date"]).dt.strftime("%Y-%m-%d")
            merged = merged.merge(stop_df, on=["trade_date", "code"], how="left")
        merged["trade_date"] = pd.to_datetime(merged["trade_date"]).dt.strftime("%Y-%m-%d")
        return merged

    def _load_index_history(self, latest_trade_date: str) -> dict[str, Any]:
        code = str(self.params.index_code or "").strip()
        if not code or not latest_trade_date:
            return {}

        table = "history_index_daily_kline"
        lookback = max(int(self.params.index_hist_lookback_days or 0), 1)
        df = pd.DataFrame()
        start_date = None
        try:
            end_date = pd.to_datetime(latest_trade_date).date()
            start_date = (end_date - dt.timedelta(days=lookback * 3)).isoformat()
        except Exception:
            end_date = None

        if self._table_exists(table) and end_date is not None and start_date is not None:
            stmt = text(
                f"""
                SELECT `date`,`open`,`high`,`low`,`close`,`volume`,`amount`
                FROM `{table}`
                WHERE `code` = :code AND `date` BETWEEN :start_date AND :end_date
                ORDER BY `date`
                """
            )
            try:
                with self.db_writer.engine.begin() as conn:
                    df = pd.read_sql_query(
                        stmt,
                        conn,
                        params={
                            "code": code,
                            "start_date": start_date,
                            "end_date": latest_trade_date,
                        },
                    )
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("读取指数日线失败，将尝试 Baostock：%s", exc)

        if df.empty and end_date is not None and start_date is not None:
            try:
                client = BaostockDataFetcher(BaostockSession())
                df = client.get_kline(code, start_date, latest_trade_date)
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("Baostock 指数日线兜底失败：%s", exc)

        if df.empty:
            return {"index_code": code}

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.sort_values("date")
        if len(df) > lookback:
            df = df.tail(lookback)

        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "preclose" not in df.columns:
            df["preclose"] = df["close"].shift(1)

        dif, dea, hist = _macd(df["close"])
        df["macd_hist"] = hist
        df["atr14"] = _atr(df["high"], df["low"], df["preclose"])
        df["ma20"] = df["close"].rolling(20, min_periods=1).mean()
        df["ma60"] = df["close"].rolling(60, min_periods=1).mean()

        asof_row = df.iloc[-1]
        asof_trade_date = asof_row.get("date")
        asof_trade_date_str = (
            asof_trade_date.date().isoformat() if not pd.isna(asof_trade_date) else None
        )

        return {
            "index_code": code,
            "asof_trade_date": asof_trade_date_str,
            "asof_close": _to_float(asof_row.get("close")),
            "asof_ma20": _to_float(asof_row.get("ma20")),
            "asof_ma60": _to_float(asof_row.get("ma60")),
            "asof_macd_hist": _to_float(asof_row.get("macd_hist")),
            "asof_atr14": _to_float(asof_row.get("atr14")),
            "history": df,
        }

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

    def _load_board_spot_strength(
        self, latest_trade_date: str, checked_at: dt.datetime | None = None
    ) -> pd.DataFrame:
        return self.env_builder.load_board_spot_strength(latest_trade_date, checked_at)

    def _load_index_trend(self, latest_trade_date: str) -> dict[str, Any]:
        return self.env_builder.load_index_trend(latest_trade_date)

    def _load_index_weekly_channel(self, latest_trade_date: str) -> dict[str, Any]:
        return self.env_builder.load_index_weekly_channel(latest_trade_date)

    def _build_weekly_scenario(
        self, weekly_payload: dict[str, Any], index_trend: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return self.env_builder.build_weekly_scenario(weekly_payload, index_trend)

    def _build_environment_context(
        self, latest_trade_date: str, *, checked_at: dt.datetime | None = None
    ) -> dict[str, Any]:
        return self.env_builder.build_environment_context(latest_trade_date, checked_at=checked_at)

    def build_and_persist_env_snapshot(
        self,
        latest_trade_date: str,
        *,
        monitor_date: str | None = None,
        run_id: str | None = None,
        checked_at: dt.datetime | None = None,
    ) -> dict[str, Any] | None:
        """计算周线环境并写入快照表（供低频任务调用）。"""

        if checked_at is None:
            checked_at = dt.datetime.now()
        if monitor_date is None:
            monitor_date = checked_at.date().isoformat()
        if run_id is None:
            run_id = self._calc_run_id(checked_at)

        env_context = self._build_environment_context(
            latest_trade_date, checked_at=checked_at
        )
        weekly_gate_policy = self._resolve_env_weekly_gate_policy(env_context)
        if isinstance(env_context, dict):
            env_context["weekly_gate_policy"] = weekly_gate_policy

        self._persist_env_snapshot(
            env_context,
            monitor_date,
            run_id,
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

    def _fetch_index_live_quote(self) -> dict[str, Any]:
        code = str(self.params.index_code or "").strip()
        if not code:
            return {}
        df = self._fetch_quotes([code])
        if df.empty:
            return {"index_code": code}
        row = df.iloc[0].to_dict()
        row["index_code"] = code
        row["live_trade_date"] = dt.date.today().isoformat()
        return row

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
    @staticmethod
    def _merge_gate_actions(*actions: str | None) -> str | None:
        severity = {
            "STOP": 3,
            "WAIT": 2,
            "ALLOW_SMALL": 1,
            "ALLOW": 0,
            "NOT_APPLIED": -1,
            None: -1,
        }
        normalized = []
        for action in actions:
            if action is None:
                normalized.append((severity[None], None))
                continue
            action_norm = str(action).strip().upper()
            if action_norm == "GO":
                action_norm = "ALLOW"
            score = severity.get(action_norm, 0)
            normalized.append((score, action_norm))
        if not normalized:
            return None
        normalized.sort(key=lambda x: x[0], reverse=True)
        top_action = normalized[0][1]
        if top_action == "GO":
            return "ALLOW"
        return top_action

    def _build_index_env_snapshot(
        self,
        asof_indicators: dict[str, Any],
        live_quote: dict[str, Any],
        env_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot: dict[str, Any] = {}
        if isinstance(asof_indicators, dict):
            snapshot.update(
                {
                    "env_index_code": asof_indicators.get("index_code"),
                    "env_index_asof_trade_date": asof_indicators.get("asof_trade_date"),
                    "env_index_asof_close": _to_float(asof_indicators.get("asof_close")),
                    "env_index_asof_ma20": _to_float(asof_indicators.get("asof_ma20")),
                    "env_index_asof_ma60": _to_float(asof_indicators.get("asof_ma60")),
                    "env_index_asof_macd_hist": _to_float(
                        asof_indicators.get("asof_macd_hist")
                    ),
                    "env_index_asof_atr14": _to_float(asof_indicators.get("asof_atr14")),
                }
            )
        if isinstance(live_quote, dict):
            snapshot.update(
                {
                    "env_index_live_trade_date": live_quote.get("live_trade_date")
                    or dt.date.today().isoformat(),
                    "env_index_live_open": _to_float(live_quote.get("live_open") or live_quote.get("open")),
                    "env_index_live_high": _to_float(live_quote.get("live_high") or live_quote.get("high")),
                    "env_index_live_low": _to_float(live_quote.get("live_low") or live_quote.get("low")),
                    "env_index_live_latest": _to_float(
                        live_quote.get("live_latest") or live_quote.get("latest")
                    ),
                    "env_index_live_pct_change": _to_float(
                        live_quote.get("live_pct_change") or live_quote.get("pct_change")
                    ),
                    "env_index_live_volume": _to_float(
                        live_quote.get("live_volume") or live_quote.get("volume")
                    ),
                    "env_index_live_amount": _to_float(
                        live_quote.get("live_amount") or live_quote.get("amount")
                    ),
                }
            )

        live_latest = snapshot.get("env_index_live_latest")
        asof_ma20 = snapshot.get("env_index_asof_ma20")
        asof_ma60 = snapshot.get("env_index_asof_ma60")
        asof_atr14 = snapshot.get("env_index_asof_atr14")
        asof_macd_hist = snapshot.get("env_index_asof_macd_hist")

        dev_ma20_atr = None
        if (
            live_latest is not None
            and asof_ma20 is not None
            and asof_atr14 is not None
            and asof_atr14 != 0
        ):
            dev_ma20_atr = (live_latest - asof_ma20) / max(asof_atr14, 1e-6)

        gate_reason_parts: list[str] = []
        if live_latest is not None and asof_ma60 is not None and live_latest < asof_ma60:
            gate_reason_parts.append("指数低于MA60")

        if (
            live_latest is not None
            and asof_ma20 is not None
            and asof_ma60 is not None
            and live_latest < asof_ma20
            and asof_ma20 < asof_ma60
        ):
            gate_reason_parts.append("指数跌破MA20且MA20<MA60")

        if asof_macd_hist is not None and asof_macd_hist < 0:
            gate_reason_parts.append("MACD柱子为负")

        regime = None
        pos_hint = None
        if isinstance(env_context, dict):
            regime = env_context.get("regime")
            pos_hint = env_context.get("position_hint_raw") or env_context.get("position_hint")

        gate_action = derive_index_gate_action(regime, pos_hint) or "ALLOW"
        regime_norm = str(regime or "").strip().upper()
        pos_hint_val = _to_float(pos_hint)
        if regime_norm or pos_hint_val is not None:
            pos_hint_display = (
                f"{pos_hint_val:.2f}" if pos_hint_val is not None else "-"
            )
            gate_reason_parts.insert(
                0, f"regime={regime_norm or '-'} pos_hint={pos_hint_display}"
            )

        position_cap_map = {"ALLOW": 1.0, "ALLOW_SMALL": 0.5, "WAIT": 0.5, "STOP": 0.0}
        position_cap = position_cap_map.get(gate_action, 1.0)
        if pos_hint_val is not None:
            position_cap = min(position_cap, max(pos_hint_val, 0.0))

        snapshot["env_index_dev_ma20_atr"] = _to_float(dev_ma20_atr)
        snapshot["env_index_gate_action"] = gate_action
        snapshot["env_index_gate_reason"] = "；".join(gate_reason_parts) or None
        snapshot["env_index_position_cap"] = position_cap
        return snapshot

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
        run_id = self._calc_run_id(checked_at_ts)
        avg_volume_map = self._load_avg_volume(
            latest_trade_date, signals["code"].dropna().astype(str).unique().tolist()
        )
        minutes_elapsed = self._calc_minutes_elapsed(checked_at_ts)
        total_minutes = 240
        if q.empty:
            out = signals.copy()
            out["monitor_date"] = checked_at_ts.date().isoformat()
            out["live_trade_date"] = out["monitor_date"]
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
            out["asof_atr14"] = out.get("sig_atr14")
            out["asof_stop_ref"] = out.get("sig_stop_ref")
            out["asof_trade_date"] = out.get("sig_date")
            out["avg_volume_20"] = None
            out["dev_ma5"] = None
            out["dev_ma20"] = None
            out["dev_ma5_atr"] = None
            out["dev_ma20_atr"] = None
            out["runup_from_sigclose"] = None
            out["runup_from_sigclose_atr"] = None
            out["runup_ref_price"] = None
            out["runup_ref_source"] = None
            out["status_tags_json"] = None
            out["primary_status"] = None
            out["env_index_snapshot_hash"] = None
            out["env_final_gate_action"] = None
            out["summary_line"] = "INACTIVE/UNKNOWN UNKNOWN | 行情数据不可用"
            out["action"] = "UNKNOWN"
            out["action_reason"] = "行情数据不可用"
            out["candidate_status"] = "UNKNOWN"
            out["status_reason"] = "行情数据不可用"
            out["checked_at"] = checked_at_ts
            out["run_id"] = run_id
            return out

        q["code"] = q["code"].astype(str)
        merged = signals.merge(q, on="code", how="left", suffixes=("", "_q"))

        env_context = env_context or {}
        if not env_context.get("index"):
            env_context["index"] = self._load_index_trend(latest_trade_date)
        if not env_context.get("boards"):
            board_df = self._load_board_spot_strength(latest_trade_date, checked_at)
            board_map: dict[str, Any] = {}
            if not board_df.empty and "board_name" in board_df.columns:
                total = len(board_df)
                for _, row in board_df.iterrows():
                    name = str(row.get("board_name") or "").strip()
                    code = str(row.get("board_code") or "").strip()
                    rank = _to_float(row.get("rank"))
                    pct = _to_float(row.get("chg_pct"))
                    status = "neutral"
                    if total > 0 and rank:
                        if rank <= max(1, int(total * 0.2)):
                            status = "strong"
                        elif rank >= max(1, int(total * 0.8)):
                            status = "weak"
                    payload = {"rank": rank, "chg_pct": pct, "status": status}
                    for key in [name, code]:
                        key_norm = str(key).strip()
                        if key_norm:
                            board_map[key_norm] = payload
            env_context["boards"] = board_map
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
        env_position_hint_raw = None
        env_position_hint = None
        if isinstance(env_context, dict):
            env_position_hint_raw = _to_float(env_context.get("position_hint_raw"))
            env_position_hint = _to_float(
                env_context.get("effective_position_hint")
            )
            if env_position_hint is None:
                env_position_hint = _to_float(env_context.get("position_hint"))

        index_intraday: dict[str, Any] = {}
        if isinstance(env_context, dict):
            raw_index = env_context.get("index_intraday") or env_context.get("env_index")
            if isinstance(raw_index, dict):
                index_intraday = raw_index

        env_weekly_asof_trade_date = None
        env_weekly_risk_level = None
        env_weekly_gating_enabled = False
        env_weekly_plan_a = None
        env_weekly_plan_b = None
        env_weekly_scene = None
        env_weekly_plan_a_exposure_cap = None
        env_weekly_bias = None
        env_weekly_status = None
        env_weekly_structure_status = None
        env_weekly_pattern_status = None
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
            env_weekly_structure_status = env_context.get("weekly_structure_status")
            env_weekly_pattern_status = env_context.get("weekly_pattern_status")
            env_weekly_status = env_weekly_structure_status or env_context.get("weekly_status")
        if env_weekly_pattern_status is None:
            env_weekly_pattern_status = env_weekly_status
        env_index_code = index_intraday.get("env_index_code")
        env_index_asof_trade_date = index_intraday.get("env_index_asof_trade_date")
        env_index_asof_close = _to_float(index_intraday.get("env_index_asof_close"))
        env_index_asof_ma20 = _to_float(index_intraday.get("env_index_asof_ma20"))
        env_index_asof_ma60 = _to_float(index_intraday.get("env_index_asof_ma60"))
        env_index_asof_macd_hist = _to_float(
            index_intraday.get("env_index_asof_macd_hist")
        )
        env_index_asof_atr14 = _to_float(index_intraday.get("env_index_asof_atr14"))
        env_index_live_trade_date = index_intraday.get("env_index_live_trade_date")
        env_index_live_open = _to_float(index_intraday.get("env_index_live_open"))
        env_index_live_high = _to_float(index_intraday.get("env_index_live_high"))
        env_index_live_low = _to_float(index_intraday.get("env_index_live_low"))
        env_index_live_latest = _to_float(index_intraday.get("env_index_live_latest"))
        env_index_live_pct_change = _to_float(
            index_intraday.get("env_index_live_pct_change")
        )
        env_index_live_volume = _to_float(index_intraday.get("env_index_live_volume"))
        env_index_live_amount = _to_float(index_intraday.get("env_index_live_amount"))
        env_index_dev_ma20_atr = _to_float(index_intraday.get("env_index_dev_ma20_atr"))
        env_index_gate_action = (
            str(index_intraday.get("env_index_gate_action")).strip().upper()
            if index_intraday.get("env_index_gate_action") is not None
            else None
        )
        if env_index_gate_action == "GO":
            env_index_gate_action = "ALLOW"
        env_index_gate_reason = index_intraday.get("env_index_gate_reason")
        env_index_position_cap = _to_float(index_intraday.get("env_index_position_cap"))

        if env_position_hint is None:
            env_position_hint = env_position_hint_raw

        if env_weekly_gating_enabled and env_weekly_plan_a_exposure_cap is not None:
            if env_position_hint is None:
                env_position_hint = env_weekly_plan_a_exposure_cap
            else:
                env_position_hint = min(env_position_hint, env_weekly_plan_a_exposure_cap)
        index_score = None
        if isinstance(env_context, dict):
            index_section = env_context.get("index", {}) if isinstance(env_context.get("index"), dict) else {}
            index_score = index_section.get("score")
        idx_score_float = _to_float(index_score)

        if not latest_snapshots.empty:
            snap = latest_snapshots.copy()
            snap["_has_latest_snapshot"] = True
            rename_map = {
                c: f"asof_{c}"
                for c in snap.columns
                if c not in {"code", "_has_latest_snapshot"}
            }
            snap = snap.rename(columns=rename_map)
            merged = merged.merge(snap, on="code", how="left")

        if "_has_latest_snapshot" not in merged.columns:
            merged["_has_latest_snapshot"] = False
        else:
            merged["_has_latest_snapshot"] = merged["_has_latest_snapshot"].eq(True)

        if "asof_close" in merged.columns and "sig_close" in merged.columns:
            merged["asof_close"] = merged["asof_close"].fillna(merged.get("sig_close"))

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
            "prev_close",
            "avg_volume_20",
            "_signal_day_pct_change",
            "signal_strength",
            "strength_delta",
        ]
        for col in float_cols:
            if col in merged.columns:
                merged[col] = merged.get(col).apply(_to_float)

        def _resolve_ref_close(row: pd.Series) -> tuple[str, float] | tuple[None, None]:
            for key in ("prev_close", "asof_close", "sig_close"):
                val = row.get(key)
                if val is not None and val > 0:
                    return key, float(val)
            return None, None

        def _resolve_live_pct_change(row: pd.Series) -> float | None:
            latest_px = row.get("live_latest")
            base_key, base_close = _resolve_ref_close(row)

            reported_pct = row.get("live_pct_change")
            if base_close is None or latest_px is None or latest_px <= 0:
                return reported_pct

            computed_pct = (latest_px / base_close - 1.0) * 100.0
            if reported_pct is not None and abs(computed_pct - reported_pct) > 0.5:
                base_label = base_key or "unknown"
                self.logger.debug(
                    "live_pct_change 偏差超过 0.5%%，按计算值覆盖：code=%s latest=%.4f %s=%.4f reported=%.4f computed=%.4f",
                    row.get("code"),
                    latest_px,
                    base_label,
                    base_close,
                    reported_pct,
                    computed_pct,
                )
            return computed_pct

        merged["live_pct_change"] = merged.apply(_resolve_live_pct_change, axis=1)

        def _infer_volume_scale_factor(df: pd.DataFrame) -> float:
            if "live_volume" not in df.columns or "avg_volume_20" not in df.columns:
                return 1.0

            sample = df[["live_volume", "avg_volume_20"]].dropna()
            sample = sample[(sample["live_volume"] > 0) & (sample["avg_volume_20"] > 0)]
            if sample.empty:
                return 1.0

            ratio = (sample["live_volume"] / sample["avg_volume_20"]).median()
            ratio_mul = ((sample["live_volume"] * 100.0) / sample["avg_volume_20"]).median()
            ratio_div = ((sample["live_volume"] / 100.0) / sample["avg_volume_20"]).median()

            if ratio < 0.02 <= ratio_mul <= 200:
                return 100.0
            if ratio > 200 >= ratio_div >= 0.02:
                return 0.01
            return 1.0

        live_vol_scale = _infer_volume_scale_factor(merged)
        if live_vol_scale != 1.0 and "live_volume" in merged.columns:
            self.logger.debug(
                "统一成交量口径：live_volume *= %.2f（用于匹配 avg_volume_20）。",
                live_vol_scale,
            )
            merged["live_volume"] = merged["live_volume"].apply(
                lambda x: None if x is None else x * live_vol_scale
            )

        def _calc_intraday_vol_ratio(row: pd.Series) -> float | None:
            vol = row.get("live_volume")
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
            _, ref_close = _resolve_ref_close(row)

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
        runup_atr_max = self.params.runup_atr_max
        dev_ma5_atr_max = self.params.dev_ma5_atr_max
        dev_ma20_atr_max = self.params.dev_ma20_atr_max
        cross_valid_days = self.params.cross_valid_days
        pullback_valid_days = self.params.pullback_valid_days
        vol_threshold = self.volume_ratio_threshold
        weekly_gate_policy = self._resolve_env_weekly_gate_policy(env_context)

        actions: List[str] = []
        reasons: List[str] = []
        statuses: List[str] = []
        status_reasons: List[str] = []
        signal_kinds: List[str] = []
        trade_stop_refs: List[float | None] = []
        sig_stop_refs: List[float | None] = []
        effective_stop_refs: List[float | None] = []
        valid_days_list: List[int | None] = []
        entry_exposure_caps: List[float | None] = []
        env_index_scores: List[float | None] = []
        env_regimes: List[str | None] = []
        env_position_hints: List[float | None] = []
        env_weekly_asof_trade_dates: List[str | None] = []
        env_weekly_risk_levels: List[str | None] = []
        env_weekly_scene_list: List[str | None] = []
        env_weekly_structure_statuses: List[str | None] = []
        env_weekly_pattern_statuses: List[str | None] = []
        env_weekly_gate_action_list: List[str | None] = []
        env_final_gate_action_list: List[str | None] = []
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
        asof_atr14_list: List[float | None] = []
        asof_stop_ref_list: List[float | None] = []
        dev_ma5_list: List[float | None] = []
        dev_ma20_list: List[float | None] = []
        dev_ma5_atr_list: List[float | None] = []
        dev_ma20_atr_list: List[float | None] = []
        runup_from_sigclose_list: List[float | None] = []
        runup_from_sigclose_atr_list: List[float | None] = []
        runup_ref_price_list: List[float | None] = []
        runup_ref_source_list: List[str | None] = []
        primary_statuses: List[str | None] = []
        status_tags_json_list: List[str | None] = []
        candidate_stages: List[str] = []
        candidate_states: List[str] = []
        summary_lines: List[str | None] = []
        env_index_snapshot_hashes: List[str | None] = []

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
        eps = 1e-6

        status_priority = ["INVALID", "RUNUP_TOO_LARGE", "OVEREXTENDED", "STALE"]

        for idx, row in merged.iterrows():
            action = "EXECUTE"
            reason = "OK"
            candidate_status = "ACTIVE"
            status_reason = "结构/时效通过"
            status_hits: list[tuple[str, str]] = []

            risk_tag_raw = str(row.get("risk_tag") or "").strip()
            risk_note = str(row.get("risk_note") or "").strip()
            risk_tags_split = [t.strip() for t in risk_tag_raw.split("|") if t.strip()]
            weekly_gate_action = weekly_gate_policy or "NOT_APPLIED"

            pct = row.get("live_pct_change")
            live_intraday_vol_ratio = row.get("live_intraday_vol_ratio")

            if (
                pct is not None
                and pct <= -4.0
                and live_intraday_vol_ratio is not None
                and live_intraday_vol_ratio >= 1.2
            ):
                if "BIG_DROP" not in risk_tags_split:
                    risk_tags_split.append("BIG_DROP")
                detail = (
                    f"暴跌放量风险：跌幅 {pct:.2f}%"
                    f"（阈值≤-4.00%），盘中量比 {live_intraday_vol_ratio:.2f}"
                    "（阈值≥1.20）"
                )
                risk_note = f"{risk_note}|{detail}" if risk_note else detail

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
            _, ref_close = _resolve_ref_close(row)

            gap = row.get("live_gap_pct")
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
            sig_atr14_val = _to_float(row.get("sig_atr14"))

            trade_stop_ref_val = _to_float(trade_stop_ref)
            asof_stop_ref_val = _to_float(row.get("asof_stop_ref"))
            signal_stop_ref_val = _to_float(signal_stop_ref)

            effective_stop_ref = next(
                (
                    val
                    for val in [
                        trade_stop_ref_val,
                        asof_stop_ref_val,
                        signal_stop_ref_val,
                    ]
                    if val is not None
                ),
                None,
            )
            live_price_for_dev = _to_float(latest_px) if latest_px is not None else _to_float(price_now)
            dev_ma5 = None
            dev_ma20 = None
            dev_ma5_atr = None
            dev_ma20_atr = None
            if live_price_for_dev is not None and ma5 is not None:
                dev_ma5 = live_price_for_dev - ma5
                if atr14 is not None:
                    dev_ma5_atr = dev_ma5 / max(atr14, eps)
            if live_price_for_dev is not None and ma20 is not None:
                dev_ma20 = live_price_for_dev - ma20
                if atr14 is not None:
                    dev_ma20_atr = dev_ma20 / max(atr14, eps)

            sig_close_val = _to_float(signal_close)
            runup_metrics = compute_runup_metrics(
                sig_close_val,
                asof_close=_coalesce(row, "asof_close", "sig_close"),
                live_high=row.get("live_high"),
                sig_atr14=sig_atr14_val if sig_atr14_val is not None else atr14,
                eps=eps,
            )
            runup_from_sigclose = runup_metrics.runup_from_sigclose
            runup_from_sigclose_atr = runup_metrics.runup_from_sigclose_atr
            runup_ref_price_val = runup_metrics.runup_ref_price
            runup_ref_source_val = runup_metrics.runup_ref_source

            runup_atr_cap = runup_atr_max
            dev_ma20_gate = None
            if is_pullback:
                runup_atr_cap = (
                    self.params.pullback_runup_atr_max
                    if self.params.pullback_runup_atr_max is not None
                    else runup_atr_max
                )
                dev_ma20_gate = self.params.pullback_runup_dev_ma20_atr_min
            runup_triggered, runup_detail = evaluate_runup_breach(
                runup_metrics,
                runup_atr_max=runup_atr_cap,
                runup_atr_tol=self.params.runup_atr_tol,
                dev_ma20_atr=dev_ma20_atr,
                dev_ma20_atr_min=dev_ma20_gate,
            )

            if valid_days > 0 and signal_age is not None and signal_age > valid_days:
                status_hits.append(
                    ("STALE", f"signal_age={int(signal_age)} > valid_days={valid_days}")
                )

            invalid_reason = None
            if macd_hist is not None and macd_hist <= 0:
                invalid_reason = f"MACD 柱子转负：asof_macd_hist={macd_hist:.4f} ≤ 0"
            elif price_now is not None and ma20 is not None and price_now < ma20:
                invalid_reason = (
                    f"跌破MA20：price_now={price_now:.2f} < asof_ma20={ma20:.2f}"
                )
            elif ma5 is not None and ma20 is not None and ma5 < ma20:
                invalid_reason = f"盘中结构失效：MA5({ma5:.2f}) < MA20({ma20:.2f})"
            elif (
                price_now is not None
                and effective_stop_ref is not None
                and price_now <= effective_stop_ref
            ):
                stop_parts: list[str] = []
                if trade_stop_ref_val is not None:
                    stop_parts.append(f"entry止损={trade_stop_ref_val:.2f}")
                if asof_stop_ref_val is not None:
                    stop_parts.append(f"昨收止损={asof_stop_ref_val:.2f}")
                if signal_stop_ref_val is not None:
                    stop_parts.append(f"信号日止损={signal_stop_ref_val:.2f}")
                stop_detail = "，".join(stop_parts)
                invalid_reason = (
                    f"触发止损：price_now={price_now:.2f} <= effective_stop_ref={effective_stop_ref:.2f}"
                )
                if stop_detail:
                    invalid_reason = f"{invalid_reason}（参考：{stop_detail}）"
            elif (
                current_close is not None
                and ma60 is not None
                and ma250 is not None
                and ma20 is not None
                and (current_close <= ma60 or current_close <= ma250 or ma20 <= ma60)
            ):
                invalid_reason = "多头排列/趋势破坏"

            if invalid_reason:
                status_hits.append(("INVALID", invalid_reason))

            if runup_triggered and runup_detail:
                status_hits.append(("RUNUP_TOO_LARGE", runup_detail))

            overextend_reasons: list[str] = []
            if dev_ma5_atr is not None and dev_ma5_atr > dev_ma5_atr_max:
                overextend_reasons.append(
                    f"dev_ma5_atr={dev_ma5_atr:.2f} > {dev_ma5_atr_max:.2f}"
                )
            if dev_ma20_atr is not None and dev_ma20_atr > dev_ma20_atr_max:
                overextend_reasons.append(
                    f"dev_ma20_atr={dev_ma20_atr:.2f} > {dev_ma20_atr_max:.2f}"
                )
            if overextend_reasons:
                status_hits.append(("OVEREXTENDED", "；".join(overextend_reasons)))

            if "MANIA" in risk_tags_split:
                mania_reason = risk_note or "风险标签=MANIA：短期过热，默认不追"
                status_hits.append(("INVALID", mania_reason))

            primary_state = next(
                (
                    state
                    for state in status_priority
                    if any(hit[0] == state for hit in status_hits)
                ),
                "OK",
            )
            primary_reason = next(
                (hit[1] for hit in status_hits if hit[0] == primary_state),
                "结构/时效通过",
            )
            status_tags_json_value = json.dumps(
                {
                    "primary": {"state": primary_state, "reason": primary_reason},
                    "hits": [{"state": s, "reason": r} for s, r in status_hits],
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )

            candidate_stage = "ACTIVE" if primary_state == "OK" else "INACTIVE"
            candidate_state = primary_state
            candidate_status = "ACTIVE" if candidate_stage == "ACTIVE" else candidate_state
            status_reason = primary_reason or "结构/时效通过"

            if candidate_status != "ACTIVE":
                action = "SKIP"
                if (
                    runup_triggered
                    and candidate_status != "RUNUP_TOO_LARGE"
                    and runup_detail
                ):
                    status_reason = f"{status_reason}；{runup_detail}"
                reason = status_reason
            else:
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
                        reason = (
                            f"高开 {gap*100:.2f}% 超过阈值 {gap_up_threshold*100:.2f}%"
                            if atr_based is None
                            else (
                                f"高开 {gap*100:.2f}% 超过阈值 {gap_up_threshold*100:.2f}%"
                                f"（min(固定{max_up*100:.2f}%, ATR×{max_up_atr_mult:.1f}={atr_based*100:.2f}% )）"
                            )
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

                if action == "EXECUTE" and strength_score is not None:
                    if strength_score < 0:
                        action = "WAIT"
                        status_reason = "信号强度<0，等待修复"
                        reason = status_reason
                    elif strength_trend == "WEAKENING":
                        action = "WAIT"
                        status_reason = "信号强度走弱，等待确认"

                if vol_used is not None and vol_used < vol_threshold and action == "EXECUTE":
                    action = "WAIT"
                    status_reason = (
                        f"量能不足：{vol_used:.2f} < 量比阈值 {vol_threshold:.2f}"
                    )
                    reason = status_reason

                if (
                    idx_score_float is not None
                    and action == "EXECUTE"
                    and idx_score_float < self.env_index_score_threshold
                ):
                    action = "WAIT"
                    status_reason = (
                        f"大盘趋势分数 {idx_score_float:.1f} 低于阈值 {self.env_index_score_threshold:.1f}，观望"
                    )
                    reason = status_reason

                if board_status == "weak" and action == "EXECUTE":
                    action = "WAIT"
                    status_reason = "所属板块走势偏弱，建议降低优先级"
                    reason = status_reason
                elif board_status == "strong" and action == "WAIT":
                    rank_display = f"{board_rank:.0f}" if board_rank is not None else "-"
                    status_reason = f"板块强势(rank={rank_display})，等待入场条件"
                    reason = status_reason

                if risk_tags_split and action == "EXECUTE":
                    action = "WAIT"
                    note_text = risk_note or "存在风险标签，建议谨慎"
                    reason = note_text
                    status_reason = note_text

            risk_tag_value = "|".join(risk_tags_split) if risk_tags_split else None
            merged.at[idx, "risk_tag"] = risk_tag_value
            merged.at[idx, "risk_note"] = risk_note or None

            cap_candidates: list[tuple[str, float]] = []
            if env_position_hint is not None:
                cap_candidates.append(("env_position_hint", env_position_hint))
            if env_weekly_plan_a_exposure_cap is not None:
                cap_candidates.append(("weekly_plan_a_cap", env_weekly_plan_a_exposure_cap))
            if env_index_position_cap is not None:
                cap_candidates.append(("env_index_position_cap", env_index_position_cap))
            entry_exposure_cap = None
            if cap_candidates:
                entry_exposure_cap = min(val for _, val in cap_candidates)

            if env_regime:
                pos_hint_text = (
                    f"（仓位建议 ≤{env_position_hint*100:.0f}%）"
                    if env_position_hint is not None
                    else ""
                )
                breakdown_reason = f"大盘结构破位，执行跳过{pos_hint_text}"
                if env_regime == "BREAKDOWN":
                    action = "SKIP"
                    if candidate_status == "ACTIVE":
                        status_reason = "市场环境 BREAKDOWN，暂不执行"
                    if reason and reason != "OK":
                        reason = breakdown_reason + "；" + reason
                    else:
                        reason = breakdown_reason
                elif env_regime == "RISK_OFF":
                    action = "WAIT"
                    if candidate_status == "ACTIVE":
                        status_reason = "市场环境 RISK_OFF，轻仓观望"
                    if reason == "OK":
                        reason = f"大盘偏弱{pos_hint_text}"
                elif env_regime == "PULLBACK" and not risk_tags_split and reason == "OK":
                    reason = f"大盘处于回踩阶段{pos_hint_text}"

            if action == "EXECUTE" and candidate_status == "ACTIVE":
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
                        weekly_gate_action = "WAIT"
                        append_note = "周线风险高：观望/防守"
                        status_reason = f"{status_reason}；{append_note}" if status_reason else append_note
                        reason = f"{reason}；{append_note}" if reason and reason != "OK" else append_note
                    elif env_weekly_risk_level == "MEDIUM" and (
                        env_weekly_structure_status or env_weekly_status
                    ) == "FORMING":
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
                            cap_candidates.append(
                                ("weekly_soft_gate_cap", _to_float(entry_exposure_cap) or 0.0)
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
                        if env_weekly_bias == "BEARISH" and env_weekly_pattern_status in {
                            "BREAKOUT_DOWN",
                            "CONFIRMED",
                            "FORMING",
                        }:
                            action = "WAIT"
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

            final_gate_action = self._merge_gate_actions(
                weekly_gate_action, env_index_gate_action
            )
            gate_notes: list[str] = []
            if env_index_gate_action:
                gate_notes.append(f"指数门控={env_index_gate_action}")
            if env_index_gate_reason:
                gate_notes.append(str(env_index_gate_reason))
            gate_note_text = "；".join([t for t in gate_notes if t])

            if final_gate_action == "STOP":
                gate_reason_text = gate_note_text or "指数门控=STOP"
                if action in {"EXECUTE", "EXECUTE_SMALL"}:
                    action = "WAIT"
                if candidate_status == "ACTIVE":
                    status_reason = (
                        f"{status_reason}；{gate_reason_text}"
                        if status_reason
                        else gate_reason_text
                    )
                if reason and reason != "OK":
                    reason = f"{reason}；{gate_reason_text}"
                else:
                    reason = gate_reason_text
            elif final_gate_action == "WAIT" and action == "EXECUTE":
                wait_text = gate_note_text or "指数门控=WAIT"
                action = "WAIT"
                status_reason = (
                    f"{status_reason}；{wait_text}" if status_reason else wait_text
                )
                reason = f"{reason}；{wait_text}" if reason and reason != "OK" else wait_text

            if candidate_status == "ACTIVE" and action in {"SKIP", "WAIT"}:
                status_reason = reason

            exposure_chain = None
            if candidate_status == "ACTIVE":
                chain_parts = [f"{name}={val*100:.0f}%" for name, val in cap_candidates]
                if entry_exposure_cap is not None:
                    chain_tail = f"{entry_exposure_cap*100:.0f}%"
                    exposure_chain = (
                        f"entry_exposure_cap=min({', '.join(chain_parts)}) => {chain_tail}"
                        if chain_parts
                        else f"entry_exposure_cap={chain_tail}"
                    )
                elif chain_parts:
                    exposure_chain = f"entry_exposure_cap=min({', '.join(chain_parts)})"
                else:
                    exposure_chain = "entry_exposure_cap=无显式上限（未提供环境上限）"

            if exposure_chain:
                if reason and reason != "OK":
                    reason = f"{reason}；{exposure_chain}"
                else:
                    reason = exposure_chain

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
            asof_atr14_list.append(_to_float(atr14))
            asof_stop_ref_list.append(_to_float(_coalesce(row, "asof_stop_ref", "sig_stop_ref")))
            dev_ma5_list.append(_to_float(dev_ma5))
            dev_ma20_list.append(_to_float(dev_ma20))
            dev_ma5_atr_list.append(_to_float(dev_ma5_atr))
            dev_ma20_atr_list.append(_to_float(dev_ma20_atr))
            runup_from_sigclose_list.append(_to_float(runup_from_sigclose))
            runup_from_sigclose_atr_list.append(_to_float(runup_from_sigclose_atr))
            runup_ref_price_list.append(_to_float(runup_ref_price_val))
            runup_ref_source_list.append(runup_ref_source_val)
            primary_statuses.append(primary_state)
            status_tags_json_list.append(status_tags_json_value)
            candidate_stages.append(candidate_stage)
            candidate_states.append(candidate_state)
            env_index_snapshot_hashes.append(index_intraday.get("env_index_snapshot_hash"))
            env_final_gate_action_list.append(final_gate_action)

            vol_ratio_val = _to_float(live_intraday_vol_ratio)
            dev_ma20_atr_val = _to_float(dev_ma20_atr)
            runup_atr_val = _to_float(runup_from_sigclose_atr)
            vol_ratio_txt = f"{vol_ratio_val:.2f}" if vol_ratio_val is not None else "-"
            dev_ma20_atr_txt = f"{dev_ma20_atr_val:.2f}" if dev_ma20_atr_val is not None else "-"
            runup_atr_txt = f"{runup_atr_val:.2f}" if runup_atr_val is not None else "-"
            board_txt = board_status or "-"
            gate_txt = final_gate_action or weekly_gate_action or "NOT_APPLIED"
            index_pct_txt = (
                f"{env_index_live_pct_change:.2f}%"
                if env_index_live_pct_change is not None
                else "-"
            )
            index_gate_txt = env_index_gate_action or "-"
            index_dev_txt = (
                f"{env_index_dev_ma20_atr:.2f}"
                if env_index_dev_ma20_atr is not None
                else "-"
            )
            summary_line = (
                f"{candidate_stage}/{candidate_state} {action}"
                f" | 量比={vol_ratio_txt}"
                f" | 偏离20ATR={dev_ma20_atr_txt}"
                f" | runupATR={runup_atr_txt}"
                f" | 板块={board_txt}"
                f" | 门控={gate_txt}"
                f" | 指数={index_pct_txt}/{index_gate_txt}/{index_dev_txt}"
            )
            summary_lines.append(summary_line[:512])

            env_weekly_asof_trade_dates.append(env_weekly_asof_trade_date)
            env_weekly_risk_levels.append(env_weekly_risk_level)
            env_weekly_scene_list.append(
                str(env_weekly_scene)[:32] if env_weekly_scene is not None else None
            )
            env_weekly_structure_statuses.append(
                env_weekly_structure_status or env_weekly_status
            )
            env_weekly_pattern_statuses.append(env_weekly_pattern_status)
            env_weekly_gate_action_list.append(weekly_gate_action)

            actions.append(action)
            reasons.append(reason)
            statuses.append(candidate_status)
            status_reasons.append(status_reason)
            signal_kinds.append("PULLBACK" if is_pullback else "CROSS")
            trade_stop_refs.append(trade_stop_ref_val)
            sig_stop_refs.append(_to_float(signal_stop_ref_val))
            effective_stop_refs.append(_to_float(effective_stop_ref))
            valid_days_list.append(valid_days)

        merged["monitor_date"] = checked_at_ts.date().isoformat()
        merged["live_trade_date"] = merged["monitor_date"]
        merged["action"] = actions
        merged["action_reason"] = reasons
        merged["candidate_status"] = statuses
        merged["candidate_stage"] = candidate_stages
        merged["candidate_state"] = candidate_states
        merged["status_reason"] = status_reasons
        merged["trade_stop_ref"] = trade_stop_refs
        merged["sig_stop_ref"] = sig_stop_refs
        merged["effective_stop_ref"] = effective_stop_refs
        merged["valid_days"] = valid_days_list
        merged["entry_exposure_cap"] = entry_exposure_caps
        merged["checked_at"] = checked_at_ts
        merged["run_id"] = run_id
        merged["env_index_score"] = env_index_scores
        merged["env_regime"] = env_regimes
        merged["env_position_hint"] = env_position_hints
        merged["env_weekly_asof_trade_date"] = env_weekly_asof_trade_dates
        merged["env_weekly_risk_level"] = env_weekly_risk_levels
        merged["env_weekly_scene"] = env_weekly_scene_list
        merged["env_weekly_structure_status"] = env_weekly_structure_statuses
        merged["env_weekly_pattern_status"] = env_weekly_pattern_statuses
        merged["env_weekly_gate_action"] = env_weekly_gate_action_list
        merged["env_final_gate_action"] = env_final_gate_action_list
        merged["env_index_snapshot_hash"] = env_index_snapshot_hashes
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
        merged["asof_atr14"] = asof_atr14_list
        merged["asof_stop_ref"] = asof_stop_ref_list
        merged["dev_ma5"] = dev_ma5_list
        merged["dev_ma20"] = dev_ma20_list
        merged["dev_ma5_atr"] = dev_ma5_atr_list
        merged["dev_ma20_atr"] = dev_ma20_atr_list
        merged["runup_from_sigclose"] = runup_from_sigclose_list
        merged["runup_from_sigclose_atr"] = runup_from_sigclose_atr_list
        merged["runup_ref_price"] = runup_ref_price_list
        merged["runup_ref_source"] = runup_ref_source_list
        merged["status_tags_json"] = status_tags_json_list
        merged["primary_status"] = primary_statuses
        merged["summary_line"] = summary_lines
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

        full_keep_cols = [
            "monitor_date",
            "sig_date",
            "signal_age",
            "valid_days",
            "code",
            "name",
            "asof_trade_date",
            "live_trade_date",
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
            "effective_stop_ref",
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
            "dev_ma5",
            "dev_ma20",
            "dev_ma5_atr",
            "dev_ma20_atr",
            "runup_from_sigclose",
            "runup_from_sigclose_atr",
            "runup_ref_price",
            "runup_ref_source",
            "entry_exposure_cap",
            "env_index_score",
            "env_regime",
            "env_position_hint",
            "env_index_snapshot_hash",
            "env_final_gate_action",
            "env_weekly_asof_trade_date",
            "env_weekly_risk_level",
            "env_weekly_scene",
            "env_weekly_structure_status",
            "env_weekly_pattern_status",
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
            "status_tags_json",
            "primary_status",
            "summary_line",
            "signal_kind",
            "sig_signal",
            "sig_reason",
            "candidate_status",
            "status_reason",
            "action",
            "action_reason",
            "checked_at",
            "run_id",
            "snapshot_hash",
        ]

        compact_keep_cols = [
            "monitor_date",
            "sig_date",
            "asof_trade_date",
            "live_trade_date",
            "signal_age",
            "valid_days",
            "code",
            "name",
            "live_open",
            "live_latest",
            "live_high",
            "live_low",
            "live_pct_change",
            "live_gap_pct",
            "live_intraday_vol_ratio",
            "sig_close",
            "sig_ma5",
            "sig_ma20",
            "sig_ma60",
            "sig_atr14",
            "sig_vol_ratio",
            "sig_macd_hist",
            "sig_stop_ref",
            "trade_stop_ref",
            "dev_ma5",
            "dev_ma20",
            "dev_ma5_atr",
            "dev_ma20_atr",
            "runup_from_sigclose",
            "runup_from_sigclose_atr",
            "runup_ref_price",
            "runup_ref_source",
            "effective_stop_ref",
            "env_regime",
            "env_position_hint",
            "env_index_score",
            "env_index_snapshot_hash",
            "env_final_gate_action",
            "env_weekly_asof_trade_date",
            "env_weekly_risk_level",
            "env_weekly_scene",
            "env_weekly_structure_status",
            "env_weekly_pattern_status",
            "env_weekly_gate_action",
            "candidate_stage",
            "candidate_state",
            "candidate_status",
            "status_reason",
            "action",
            "action_reason",
            "signal_strength",
            "strength_note",
            "risk_tag",
            "risk_note",
            "status_tags_json",
            "primary_status",
            "summary_line",
            "checked_at",
            "run_id",
            "snapshot_hash",
        ]

        output_mode = (self.params.output_mode or "COMPACT").upper()
        keep_cols = full_keep_cols if output_mode == "FULL" else compact_keep_cols

        for col in keep_cols:
            if col not in merged.columns:
                merged[col] = None

        return merged[keep_cols].copy()

    # -------------------------
    # Persist & export
    # -------------------------
    def _persist_quote_snapshots(self, df: pd.DataFrame) -> None:
        if df.empty or not self.params.write_to_db:
            return
        table = self.params.quote_table
        if not table:
            return

        keep_cols = [
            "monitor_date",
            "run_id",
            "code",
            "live_trade_date",
            "live_open",
            "live_high",
            "live_low",
            "live_latest",
            "live_volume",
            "live_amount",
            "live_gap_pct",
            "live_pct_change",
            "live_intraday_vol_ratio",
        ]
        for col in keep_cols:
            if col not in df.columns:
                df[col] = None
        quotes = df[keep_cols].copy()
        quotes["monitor_date"] = pd.to_datetime(quotes["monitor_date"]).dt.date
        quotes["live_trade_date"] = pd.to_datetime(quotes["live_trade_date"]).dt.date
        quotes["run_id"] = quotes["run_id"].fillna(
            self._calc_run_id(dt.datetime.now())
        )
        quotes["code"] = quotes["code"].astype(str)

        monitor_dates = quotes["monitor_date"].dropna().astype(str).unique().tolist()
        if self._table_exists(table) and monitor_dates:
            for monitor in monitor_dates:
                run_ids = (
                    quotes.loc[quotes["monitor_date"].astype(str) == monitor, "run_id"]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()
                )
                for run_id in run_ids:
                    run_id_codes = quotes.loc[
                        (quotes["monitor_date"].astype(str) == monitor)
                        & (quotes["run_id"].astype(str) == run_id),
                        "code",
                    ].dropna().astype(str).unique().tolist()
                    if run_id_codes:
                        self._delete_existing_run_rows(
                            table,
                            monitor,
                            run_id,
                            run_id_codes,
                        )

        try:
            self.db_writer.write_dataframe(quotes, table, if_exists="append")
            self.logger.info("开盘监测实时行情快照已写入表 %s：%s 条", table, len(quotes))
        except Exception as exc:  # noqa: BLE001
            self.logger.error("写入开盘监测行情快照失败：%s", exc)

    def _persist_results(self, df: pd.DataFrame) -> None:
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

        if "run_id" not in df.columns:
            df["run_id"] = None
        df["run_id"] = df["run_id"].fillna(
            self._calc_run_id(dt.datetime.now())
        )

        for col in ["monitor_date", "sig_date", "asof_trade_date", "live_trade_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        monitor_date_val = df["monitor_date"].iloc[0] if "monitor_date" in df.columns and not df.empty else None
        monitor_date = monitor_date_val.isoformat() if monitor_date_val is not None else ""

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

        if "summary_line" in df.columns:
            df["summary_line"] = (
                df["summary_line"].fillna("").astype(str).str.slice(0, 512)
            )

        if "status_tags_json" in df.columns:
            df["status_tags_json"] = (
                df["status_tags_json"].fillna("").astype(str).str.slice(0, 4000)
            )
        if "primary_status" in df.columns:
            df["primary_status"] = (
                df["primary_status"].fillna("").astype(str).str.slice(0, 64)
            )
        else:
            if "status_tags_json" in df.columns:
                try:
                    df["primary_status"] = df["status_tags_json"].apply(
                        lambda x: (json.loads(x).get("primary", {}) or {}).get("state")
                        if isinstance(x, str) and x.strip()
                        else None
                    )
                except Exception:
                    df["primary_status"] = None

        df["snapshot_hash"] = df.apply(lambda row: make_snapshot_hash(row.to_dict()), axis=1)
        df = df.drop_duplicates(
            subset=["monitor_date", "sig_date", "code", "run_id"]
        )

        self._persist_quote_snapshots(df)

        if table_columns:
            keep_cols = [c for c in df.columns if c in table_columns]
            df = df[keep_cols]

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
            run_ids = (
                df["run_id"].dropna().astype(str).unique().tolist()
                if "run_id" in df.columns
                else []
            )
            for run_id_val in run_ids:
                if not run_id_val:
                    continue
                deleted_total += self._delete_existing_run_rows(
                    table, monitor_date, run_id_val, codes
                )
            if deleted_total > 0:
                self.logger.info("检测到同 run_id 旧快照：已覆盖删除 %s 条。", deleted_total)

        if df.empty:
            self.logger.info("本次开盘监测结果全部为重复快照，跳过写入。")
            return

        try:
            self.db_writer.write_dataframe(df, table, if_exists="append")
            self.logger.info("开盘监测结果已写入表 %s：%s 条", table, len(df))
        except Exception as exc:  # noqa: BLE001
            self.logger.error("写入开盘监测表失败：%s", exc)

    def _load_open_monitor_view_data(
        self, monitor_date: str, run_id: str | None
    ) -> pd.DataFrame:
        view = self.params.open_monitor_view
        if not (view and monitor_date and self._table_exists(view)):
            return pd.DataFrame()

        clauses = ["`monitor_date` = :d"]
        params: dict[str, Any] = {"d": monitor_date}
        if run_id:
            clauses.append("`run_id` = :b")
            params["b"] = run_id
        where_clause = " AND ".join(clauses)
        stmt = text(
            f"""
            SELECT *
            FROM `{view}`
            WHERE {where_clause}
            ORDER BY `checked_at` DESC, `sig_date` DESC, `code`
            """
        )
        try:
            with self.db_writer.engine.begin() as conn:
                return pd.read_sql_query(stmt, conn, params=params)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取开盘监测视图 %s 失败：%s", view, exc)
            return pd.DataFrame()

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
        export_df["_action_rank"] = export_df["action"].map(action_rank).fillna(99)

        if "candidate_stage" in export_df.columns and "candidate_state" in export_df.columns:
            stage_rank = {"ACTIVE": 0, "INACTIVE": 1}
            state_rank = {
                "OK": 0,
                "OVEREXTENDED": 1,
                "RUNUP_TOO_LARGE": 2,
                "INVALID": 3,
                "STALE": 4,
                "UNKNOWN": 9,
            }
            export_df["_stage_rank"] = export_df["candidate_stage"].map(stage_rank).fillna(99)
            export_df["_state_rank"] = export_df["candidate_state"].map(state_rank).fillna(99)
            export_df = export_df.sort_values(
                by=["_stage_rank", "_action_rank", "_state_rank", gap_col],
                ascending=[True, True, True, True],
            )
            export_df = export_df.drop(
                columns=["_action_rank", "_stage_rank", "_state_rank"],
                errors="ignore",
            )
        else:
            status_rank = {
                "ACTIVE": 0,
                "OVEREXTENDED": 1,
                "RUNUP_TOO_LARGE": 2,
                "INVALID": 3,
                "STALE": 4,
                "UNKNOWN": 5,
            }
            export_df["_status_rank"] = (
                export_df["candidate_status"].map(status_rank).fillna(99)
            )
            export_df = export_df.sort_values(
                by=["_status_rank", "_action_rank", gap_col],
                ascending=[True, True, True],
            )
            export_df = export_df.drop(
                columns=["_action_rank", "_status_rank"], errors="ignore"
            )
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
        run_id = self._calc_run_id(checked_at)
        env_snapshot_run_id = run_id
        env_snapshot_matched_run_id = False

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
            monitor_date, run_id, allow_fallback=True
        )
        if env_context:
            loaded_run_id = str(env_context.get("run_id") or "")
            env_snapshot_run_id = loaded_run_id or env_snapshot_run_id
            env_snapshot_matched_run_id = bool(loaded_run_id == str(run_id))
            if self.weekly_env_fallback_asof_date:
                self.logger.info(
                    "已加载周线回退环境（asof_date=%s，run_id=%s）",
                    self.weekly_env_fallback_asof_date,
                    env_snapshot_run_id,
                )
            elif env_snapshot_matched_run_id:
                self.logger.info(
                    "已加载环境快照（monitor_date=%s, run_id=%s）",
                    monitor_date,
                    env_snapshot_run_id,
                )
            else:
                self.logger.info(
                    "已加载当日最新环境快照（monitor_date=%s，未匹配 run_id=%s，实际=%s）",
                    monitor_date,
                    run_id,
                    env_snapshot_run_id,
                )
        if not env_context:
            self.logger.warning(
                "未找到环境快照（monitor_date=%s, run_id=%s），请先运行 run_index_weekly_channel 产出环境。",
                monitor_date,
                run_id,
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
            plan_tokens = [
                plan_a_if,
                weekly_scenario.get("weekly_plan_a_then"),
                plan_a_confirm,
                plan_b_if,
                plan_b_recover,
            ]
            if any(str(token or "").strip() for token in plan_tokens):
                self.logger.info(
                    "周线 Plan tokens: A_if=%s A_then=%s A_confirm=%s B_if=%s B_recover=%s",
                    str(plan_a_if or "")[:120],
                    str(weekly_scenario.get("weekly_plan_a_then") or "")[:64],
                    str(plan_a_confirm or "")[:64],
                    str(plan_b_if or "")[:120],
                    str(plan_b_recover or "")[:120],
                )
        index_history = self._load_index_history(latest_trade_date)
        index_live_quote = self._fetch_index_live_quote()
        index_env_snapshot = self._build_index_env_snapshot(
            index_history, index_live_quote, env_context
        )
        env_index_snapshot_hash = None
        if index_env_snapshot:
            index_snapshot_payload = {
                "monitor_date": monitor_date,
                "run_id": run_id,
                "checked_at": checked_at,
                "index_code": index_env_snapshot.get("env_index_code"),
                "asof_trade_date": index_env_snapshot.get("env_index_asof_trade_date"),
                "live_trade_date": index_env_snapshot.get("env_index_live_trade_date"),
                "asof_close": _to_float(
                    index_env_snapshot.get("env_index_asof_close")
                ),
                "asof_ma20": _to_float(index_env_snapshot.get("env_index_asof_ma20")),
                "asof_ma60": _to_float(index_env_snapshot.get("env_index_asof_ma60")),
                "asof_macd_hist": _to_float(
                    index_env_snapshot.get("env_index_asof_macd_hist")
                ),
                "asof_atr14": _to_float(index_env_snapshot.get("env_index_asof_atr14")),
                "live_open": _to_float(index_env_snapshot.get("env_index_live_open")),
                "live_high": _to_float(index_env_snapshot.get("env_index_live_high")),
                "live_low": _to_float(index_env_snapshot.get("env_index_live_low")),
                "live_latest": _to_float(
                    index_env_snapshot.get("env_index_live_latest")
                ),
                "live_pct_change": _to_float(
                    index_env_snapshot.get("env_index_live_pct_change")
                ),
                "live_volume": _to_float(index_env_snapshot.get("env_index_live_volume")),
                "live_amount": _to_float(index_env_snapshot.get("env_index_live_amount")),
                "dev_ma20_atr": _to_float(
                    index_env_snapshot.get("env_index_dev_ma20_atr")
                ),
                "gate_action": index_env_snapshot.get("env_index_gate_action"),
                "gate_reason": index_env_snapshot.get("env_index_gate_reason"),
                "position_cap": _to_float(
                    index_env_snapshot.get("env_index_position_cap")
                ),
            }
            index_snapshot_payload["snapshot_hash"] = make_snapshot_hash(index_snapshot_payload)
            env_index_snapshot_hash = self._persist_index_snapshot(
                index_snapshot_payload,
                table=self.params.env_index_snapshot_table,
            ) or index_snapshot_payload["snapshot_hash"]
            index_env_snapshot["env_index_snapshot_hash"] = env_index_snapshot_hash
        if isinstance(env_context, dict):
            env_context["index_intraday"] = index_env_snapshot
            env_context["env_index_snapshot_hash"] = env_index_snapshot_hash
        if index_env_snapshot:
            gate_action = index_env_snapshot.get("env_index_gate_action")
            gate_reason = index_env_snapshot.get("env_index_gate_reason") or "-"
            live_pct = _to_float(index_env_snapshot.get("env_index_live_pct_change"))
            dev_ma20_atr = _to_float(index_env_snapshot.get("env_index_dev_ma20_atr"))
            self.logger.info(
                "指数快照：code=%s asof=%s live=%s pct=%.2f%% dev_ma20_atr=%s 门控=%s（%s）",
                index_env_snapshot.get("env_index_code"),
                index_env_snapshot.get("env_index_asof_trade_date"),
                index_env_snapshot.get("env_index_live_trade_date"),
                0.0 if live_pct is None else live_pct,
                f"{dev_ma20_atr:.2f}" if dev_ma20_atr is not None else "-",
                gate_action or "-",
                gate_reason,
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
        if not result.empty and "run_id" in result.columns:
            run_id_val = str(result.iloc[0].get("run_id") or "").strip()
            run_id = run_id_val or run_id
            env_snapshot_matched_run_id = bool(
                env_context
                and str(env_context.get("run_id") or "") == str(run_id)
            )

        if (
            self.params.persist_env_snapshot
            and env_context
            and not env_snapshot_matched_run_id
        ):
            self._persist_env_snapshot(
                env_context,
                monitor_date,
                run_id or env_snapshot_run_id,
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
        export_df = self._load_open_monitor_view_data(monitor_date, run_id)
        if export_df.empty:
            export_df = result
        self._export_csv(export_df)
