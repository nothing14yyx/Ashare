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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd
from sqlalchemy import bindparam, text

from .baostock_core import BaostockDataFetcher
from .baostock_session import BaostockSession
from .config import get_section
from .db import DatabaseConfig, MySQLWriter
from .ma5_ma20_trend_strategy import _atr, _macd
from .schema_manager import (
    TABLE_ENV_INDEX_SNAPSHOT,
    TABLE_STRATEGY_OPEN_MONITOR_ENV,
    TABLE_STRATEGY_OPEN_MONITOR_EVAL,
    TABLE_STRATEGY_OPEN_MONITOR_QUOTE,
    TABLE_STRATEGY_SIGNAL_EVENTS,
    STRATEGY_CODE_MA5_MA20_TREND,
    VIEW_STRATEGY_OPEN_MONITOR,
    VIEW_STRATEGY_OPEN_MONITOR_WIDE,
    VIEW_STRATEGY_READY_SIGNALS,
)
from .utils.convert import to_float as _to_float
from .utils.logger import setup_logger
from .weekly_env_builder import WeeklyEnvironmentBuilder
from .monitor_rules import MonitorRuleConfig, build_default_monitor_rules
from .open_monitor_quotes import (
    fetch_quotes_akshare,
    fetch_quotes_eastmoney,
    normalize_quotes_columns,
)
from .open_monitor_rules import (
    DecisionContext,
    DecisionResult,
    MarketEnvironment,
    Rule,
    RuleEngine,
    RuleHit,
    RuleResult,
)


SNAPSHOT_HASH_EXCLUDE = {
    "checked_at",
    "run_id",
}

READY_SIGNALS_REQUIRED_COLS = (
    "sig_date",
    "code",
    "strategy_code",
    "close",
    "ma20",
    "atr14",
)



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

    # 信号输入：只接受 ready_signals_view（由 SchemaManager 负责生成/维护）
    ready_signals_view: str = VIEW_STRATEGY_READY_SIGNALS
    # 契约与 fail-fast（默认开启）
    strict_ready_signals_required: bool = True
    strict_quotes: bool = True
    quote_table: str = TABLE_STRATEGY_OPEN_MONITOR_QUOTE
    strategy_code: str = STRATEGY_CODE_MA5_MA20_TREND

    # 输出表：开盘检查结果
    output_table: str = TABLE_STRATEGY_OPEN_MONITOR_EVAL
    open_monitor_view: str = VIEW_STRATEGY_OPEN_MONITOR
    open_monitor_wide_view: str = VIEW_STRATEGY_OPEN_MONITOR_WIDE

    # 回看近 N 个交易日的 BUY 信号
    signal_lookback_days: int = 3

    # 行情来源：eastmoney / akshare（兼容：auto 将按 eastmoney 处理）
    quote_source: str = "eastmoney"

    # 候选有效期：回踩形态默认更长
    cross_valid_days: int = 3
    pullback_valid_days: int = 5

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
        # 默认策略 code 与 strategy_ma5_ma20_trend 保持一致
        default_strategy_code = STRATEGY_CODE_MA5_MA20_TREND

        strat = get_section("strategy_ma5_ma20_trend") or {}
        if isinstance(strat, dict):
            default_strategy_code = strat.get("strategy_code", STRATEGY_CODE_MA5_MA20_TREND)
        else:
            default_strategy_code = STRATEGY_CODE_MA5_MA20_TREND

        logger = logging.getLogger(__name__)

        def _get_bool(key: str, default: bool) -> bool:
            val = sec.get(key, default)
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                return val.strip().lower() in {"1", "true", "yes", "y", "on"}
            return bool(val)

        def _get_int(key: str, default: int) -> int:
            raw = sec.get(key, default)
            try:
                return int(raw)
            except Exception:
                return default

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
            ready_signals_view=str(
                sec.get("ready_signals_view", cls.ready_signals_view)
            ).strip()
            or cls.ready_signals_view,
            strict_ready_signals_required=_get_bool(
                "strict_ready_signals_required", cls.strict_ready_signals_required
            ),
            strict_quotes=_get_bool("strict_quotes", cls.strict_quotes),
            quote_table=str(sec.get("quote_table", cls.quote_table)).strip() or cls.quote_table,
            strategy_code=str(sec.get("strategy_code", default_strategy_code)).strip()
            or default_strategy_code,
            output_table=str(sec.get("output_table", cls.output_table)).strip() or cls.output_table,
            open_monitor_view=str(
                sec.get("open_monitor_view", cls.open_monitor_view)
            ).strip()
            or cls.open_monitor_view,
            open_monitor_wide_view=str(
                sec.get("open_monitor_wide_view", cls.open_monitor_wide_view)
            ).strip()
            or cls.open_monitor_wide_view,
            signal_lookback_days=_get_int("signal_lookback_days", cls.signal_lookback_days),
            quote_source=quote_source,
            cross_valid_days=_get_int("cross_valid_days", cls.cross_valid_days),
            pullback_valid_days=_get_int("pullback_valid_days", cls.pullback_valid_days),
            index_code=str(sec.get("index_code", cls.index_code)).strip()
            or cls.index_code,
            index_hist_lookback_days=_get_int(
                "index_hist_lookback_days", cls.index_hist_lookback_days
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
        self.rule_config = MonitorRuleConfig.from_config(
            get_section("open_monitor"),
            logger=self.logger,
        )
        self.rules = build_default_monitor_rules(
            self.rule_config,
            Rule=Rule,
            RuleResult=RuleResult,
        )
        self.rule_engine = RuleEngine(self._merge_gate_actions)
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
        self.env_builder = WeeklyEnvironmentBuilder(
            db_writer=self.db_writer,
            logger=self.logger,
            index_codes=self.index_codes,
            board_env_enabled=self.board_env_enabled,
            board_spot_enabled=self.board_spot_enabled,
            env_index_score_threshold=self.rule_config.env_index_score_threshold,
            weekly_soft_gate_strength_threshold=self.rule_config.weekly_soft_gate_strength_threshold,
        )
        self.weekly_env_fallback_asof_date: str | None = None
        self._ready_signals_used: bool = False

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

        weekly_gate_action = (
            _get_env("weekly_gate_action")
            or _get_env("weekly_gate_policy")
            or env_weekly_gate_policy
        )
        payload["env_weekly_gate_action"] = weekly_gate_action
        index_snapshot = {}
        if isinstance(env_context, dict):
            raw_index_snapshot = env_context.get("index_intraday")
            if isinstance(raw_index_snapshot, dict):
                index_snapshot = raw_index_snapshot
        env_index_hash = index_snapshot.get("env_index_snapshot_hash")
        payload["env_index_snapshot_hash"] = env_index_hash
        payload["env_final_gate_action"] = env_context.get("env_final_gate_action")
        if payload["env_final_gate_action"] is None:
            self.logger.error(
                "环境快照缺少 env_final_gate_action，已跳过写入（monitor_date=%s, run_id=%s）。",
                monitor_date,
                run_id,
            )
            return
        cap_candidates = [
            env_context.get("env_final_cap_pct") if isinstance(env_context, dict) else None,
            env_context.get("effective_position_hint") if isinstance(env_context, dict) else None,
            env_context.get("position_hint") if isinstance(env_context, dict) else None,
            index_snapshot.get("env_index_position_cap") if isinstance(index_snapshot, dict) else None,
        ]
        payload["env_final_cap_pct"] = next(
            ( _to_float(c) for c in cap_candidates if _to_float(c) is not None),
            None,
        )
        payload["env_final_reason_json"] = env_context.get("env_final_reason_json")

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
        if run_id is None:
            self.logger.error("读取环境快照时缺少 run_id（monitor_date=%s）。", monitor_date)
            return None

        stmt = text(
            f"""
            SELECT * FROM `{table}`
            WHERE `run_id` = :b AND `monitor_date` = :d
            ORDER BY `checked_at` DESC
            LIMIT 1
            """
        )

        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": monitor_date, "b": run_id})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取环境快照失败：%s", exc)
            return None

        if df.empty:
            self.logger.error("未找到环境快照（monitor_date=%s, run_id=%s）。", monitor_date, run_id)
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
            "env_final_gate_action": row.get("env_final_gate_action"),
            "env_final_cap_pct": row.get("env_final_cap_pct"),
            "env_final_reason_json": row.get("env_final_reason_json"),
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
    ) -> dict[str, Any] | None:
        """公开的环境快照读取接口，严格按 monitor_date + run_id 匹配。"""

        self.weekly_env_fallback_asof_date = None
        return self._load_env_snapshot_context(monitor_date, run_id)

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

    def _resolve_latest_trade_date(
        self,
        *,
        signals: pd.DataFrame | None = None,
        ready_view: str | None = None,
    ) -> str | None:
        """统一推导 latest_trade_date，避免对基础数据表（如指标表）的隐性依赖。

        优先级：
        1) signals 中的 asof_trade_date / sig_date
        2) 日线表（_daily_table）MAX(date)
        3) ready_signals_view MAX(sig_date)
        """

        if isinstance(signals, pd.DataFrame) and not signals.empty:
            for col in ("asof_trade_date", "sig_date"):
                if col in signals.columns:
                    s = pd.to_datetime(signals[col], errors="coerce").dropna()
                    if not s.empty:
                        return s.max().date().isoformat()

        daily = self._daily_table()
        if daily and self._table_exists(daily):
            try:
                with self.db_writer.engine.begin() as conn:
                    df = pd.read_sql_query(
                        text(f"SELECT MAX(`date`) AS latest_trade_date FROM `{daily}`"),
                        conn,
                    )
                if df is not None and not df.empty:
                    v = df.iloc[0].get("latest_trade_date")
                    ts = pd.to_datetime(v, errors="coerce")
                    if pd.notna(ts):
                        return ts.date().isoformat()
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("推导 latest_trade_date 失败（daily_table）：%s", exc)

        view = ready_view or str(self.params.ready_signals_view or "").strip()
        if view and self._table_exists(view):
            try:
                with self.db_writer.engine.begin() as conn:
                    df = pd.read_sql_query(
                        text(f"SELECT MAX(`sig_date`) AS latest_sig_date FROM `{view}`"),
                        conn,
                    )
                if df is not None and not df.empty:
                    v = df.iloc[0].get("latest_sig_date")
                    ts = pd.to_datetime(v, errors="coerce")
                    if pd.notna(ts):
                        return ts.date().isoformat()
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("推导 latest_trade_date 失败（ready_view）：%s", exc)

        return None

    def _load_recent_buy_signals(self) -> Tuple[str | None, List[str], pd.DataFrame]:
        """从 ready_signals_view 读取最近 BUY 信号（严格模式，无旧逻辑回退）。

        约定：
        - open_monitor 只接受数据库视图 v_strategy_ready_signals 作为信号输入；
        - 不再回退读取基础表并在 Python 端手动 merge。
        """

        view = str(self.params.ready_signals_view or "").strip()
        strict_ready = bool(getattr(self.params, "strict_ready_signals_required", True))
        if not view:
            msg = "未配置 ready_signals_view"
            if strict_ready:
                raise RuntimeError(msg)
            self.logger.error("%s，已跳过开盘监测。", msg)
            return None, [], pd.DataFrame()
        if not self._table_exists(view):
            msg = f"ready_signals_view={view} 不存在"
            if strict_ready:
                raise RuntimeError(msg)
            self.logger.error(
                "%s，已跳过开盘监测；请先确保 SchemaManager.ensure_all 已执行或检查配置。",
                msg,
            )
            return None, [], pd.DataFrame()

        required_view_cols = set(READY_SIGNALS_REQUIRED_COLS)
        view_cols = set(self._get_table_columns(view))
        missing_view_cols = sorted(required_view_cols - view_cols)
        if missing_view_cols:
            msg = f"ready_signals_view `{view}` 缺少关键列：{missing_view_cols}"
            if strict_ready:
                raise RuntimeError(msg)
            self.logger.error("%s，已跳过开盘监测。", msg)
            return None, [], pd.DataFrame()

        # 严格模式，无旧逻辑回退
        self._ready_signals_used = True

        monitor_date = dt.date.today().isoformat()
        lookback = max(int(self.params.signal_lookback_days or 0), 1)

        # 1) 推导最新交易日（优先 daily_table；再回退 view.sig_date）
        latest_trade_date = self._resolve_latest_trade_date(ready_view=view)
        if not latest_trade_date:
            self.logger.error("无法推导 latest_trade_date（daily_table/view 均不可用），已跳过开盘监测。")
            return None, [], pd.DataFrame()

        # 2) 取严格“最近 N 个交易日窗口”（优先日线表；若无日线表则退化为信号日序列）
        base_table = self._daily_table()
        date_col = "date"
        if not self._table_exists(base_table):
            base_table = view
            date_col = "sig_date"

        # 取最近 N 个交易日窗口（含 latest_trade_date）
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
            self.logger.info("%s 无法获得最近 %s 个交易日窗口，跳过开盘监测。", latest_trade_date, lookback)
            return latest_trade_date, [], pd.DataFrame()

        window_latest = trade_dates[0]
        window_earliest = trade_dates[-1]

        # 3) 只在该交易日窗口内筛 BUY 信号日（避免 BUY 稀疏导致日期漂移到更早）
        try:
            with self.db_writer.engine.begin() as conn:
                buy_dates_df = pd.read_sql_query(
                    text(
                        f"""
                        SELECT DISTINCT `sig_date`
                        FROM `{view}`
                        WHERE `strategy_code` = :strategy
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

        # 4) 读取 view 中对应日期的 BUY 信号宽表（view 本身已限定 BUY/BUY_CONFIRM）
        stmt = text(
            f"""
            SELECT *
            FROM `{view}`
            WHERE `sig_date` IN :dates
              AND `strategy_code` = :strategy
            """
        ).bindparams(bindparam("dates", expanding=True))

        with self.db_writer.engine.begin() as conn:
            try:
                events_df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={"dates": signal_dates, "strategy": self.params.strategy_code},
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.error("读取 %s BUY 信号失败：%s", view, exc)
                return latest_trade_date, signal_dates, pd.DataFrame()

        if events_df.empty:
            self.logger.info("%s 内无 BUY 信号，跳过开盘监测。", signal_dates)
            return latest_trade_date, signal_dates, events_df

        # feat: 统一字段口径由 ready_signals_view 保证，避免 Python 端回填

        if "code" in events_df.columns:
            events_df["code"] = events_df["code"].astype(str)
        if "sig_date" in events_df.columns:
            events_df["sig_date"] = pd.to_datetime(events_df["sig_date"], errors="coerce")

        # 必要列校验（缺少关键字段直接终止，避免 silent wrong）
        required_cols = [
            "sig_date",
            "code",
            "strategy_code",
            "signal",
            "close",
            "ma20",
            "atr14",
        ]
        missing = [c for c in required_cols if c not in events_df.columns]
        if missing:
            msg = f"ready_signals_view `{view}` 缺少关键列：{missing}"
            if strict_ready:
                raise RuntimeError(msg)
            self.logger.error("%s，已跳过开盘监测。", msg)
            return latest_trade_date, signal_dates, pd.DataFrame()

        # 输出列补齐（保持下游 _prepare_monitor_frame/_evaluate 依赖稳定）
        base_cols = [
            "sig_date",
            "code",
            "strategy_code",
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
            "final_action",
            "final_reason",
            "final_cap",
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
            "reason",
            "yearline_state",
            "risk_tag",
            "risk_note",
            "industry",
            "board_name",
            "board_code",
            "industry_classification",
        ]
        for col in base_cols:
            if col not in events_df.columns:
                events_df[col] = None

        events_df["sig_date"] = events_df["sig_date"].dt.strftime("%Y-%m-%d")

        # 严格去重：同一 code 只保留最新信号日那条记录。
        if self.params.unique_code_latest_date_only:
            before = len(events_df)
            events_df["_date_dt"] = pd.to_datetime(events_df["sig_date"], errors="coerce")
            events_df = events_df.sort_values(by=["code", "_date_dt"], ascending=[True, False])
            events_df = events_df.drop_duplicates(subset=["code"], keep="first")
            events_df = events_df.drop(columns=["_date_dt"], errors="ignore")
            dropped = before - len(events_df)
            if dropped > 0:
                self.logger.info(
                    "同一 code 多次触发 BUY：已按最新信号日去重 %s 条（保留 %s 条）。",
                    dropped,
                    len(events_df),
                )
            signal_dates = sorted(events_df["sig_date"].dropna().unique().tolist(), reverse=True)

        min_date = events_df["sig_date"].min()
        trade_age_map = self._load_trade_age_map(latest_trade_date, str(min_date), monitor_date)
        events_df["signal_age"] = events_df["sig_date"].map(trade_age_map)

        try:
            for d in signal_dates:
                codes = events_df.loc[events_df["sig_date"] == d, "code"].dropna().unique().tolist()
                pct_map = self._load_signal_day_pct_change(d, codes)
                mask = events_df["sig_date"] == d
                events_df.loc[mask, "_signal_day_pct_change"] = events_df.loc[mask, "code"].map(pct_map)
        except Exception:
            events_df["_signal_day_pct_change"] = None

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
            "final_action": "sig_final_action",
            "final_reason": "sig_final_reason",
            "final_cap": "sig_final_cap",
            "macd_event": "sig_macd_event",
            "chip_score": "sig_chip_score",
            "gdhs_delta_pct": "sig_gdhs_delta_pct",
            "gdhs_announce_date": "sig_gdhs_announce_date",
            "chip_reason": "sig_chip_reason",
            "chip_penalty": "sig_chip_penalty",
            "chip_note": "sig_chip_note",
            "age_days": "sig_chip_age_days",
            "deadzone_hit": "sig_chip_deadzone",
            "stale_hit": "sig_chip_stale",
            "fear_score": "sig_fear_score",
            "wave_type": "sig_wave_type",
            "reason": "sig_reason",
        }
        df = events_df.rename(columns={k: v for k, v in signal_prefix_map.items() if k in events_df.columns})
        for src, target in signal_prefix_map.items():
            if target not in df.columns:
                df[target] = None

        return latest_trade_date, signal_dates, df

    def _load_latest_snapshots(self, latest_trade_date: str, codes: List[str]) -> pd.DataFrame:
        if not latest_trade_date or not codes:
            return pd.DataFrame()

        daily_table = self._daily_table()
        if not daily_table or not self._table_exists(daily_table):
            return pd.DataFrame()

        codes = [str(c) for c in codes if str(c).strip()]
        if not codes:
            return pd.DataFrame()

        stmt = (
            text(
                f"""
                SELECT CAST(`date` AS CHAR) AS trade_date, `code`, `close`
                FROM `{daily_table}`
                WHERE `date` = :d AND `code` IN :codes
                """
            ).bindparams(bindparam("codes", expanding=True))
        )
        try:
            with self.db_writer.engine.begin() as conn:
                snap_df = pd.read_sql_query(stmt, conn, params={"d": latest_trade_date, "codes": codes})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取日线快照失败，将跳过最新快照：%s", exc)
            return pd.DataFrame()

        if snap_df is None or snap_df.empty:
            return pd.DataFrame()

        snap_df["code"] = snap_df["code"].astype(str)
        merged = snap_df.copy()
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

        env_context, _, _ = self._attach_index_snapshot(
            latest_trade_date,
            monitor_date,
            run_id,
            checked_at,
            env_context,
        )

        self.env_builder._finalize_env_directives(
            env_context, weekly_gate_policy=weekly_gate_policy
        )

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
        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        return fetch_quotes_akshare(codes, strict_quotes=strict_quotes, logger=self.logger)

    def _fetch_quotes_eastmoney(self, codes: List[str]) -> pd.DataFrame:
        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        return fetch_quotes_eastmoney(codes, strict_quotes=strict_quotes, logger=self.logger)

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

    def _attach_index_snapshot(
        self,
        latest_trade_date: str,
        monitor_date: str,
        run_id: str,
        checked_at: dt.datetime,
        env_context: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any], str | None]:
        ctx = env_context if isinstance(env_context, dict) else {}
        index_history = self._load_index_history(latest_trade_date)
        index_live_quote = self._fetch_index_live_quote()
        index_env_snapshot = self._build_index_env_snapshot(
            index_history, index_live_quote, ctx
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
                "asof_close": _to_float(index_env_snapshot.get("env_index_asof_close")),
                "asof_ma20": _to_float(index_env_snapshot.get("env_index_asof_ma20")),
                "asof_ma60": _to_float(index_env_snapshot.get("env_index_asof_ma60")),
                "asof_macd_hist": _to_float(index_env_snapshot.get("env_index_asof_macd_hist")),
                "asof_atr14": _to_float(index_env_snapshot.get("env_index_asof_atr14")),
                "live_open": _to_float(index_env_snapshot.get("env_index_live_open")),
                "live_high": _to_float(index_env_snapshot.get("env_index_live_high")),
                "live_low": _to_float(index_env_snapshot.get("env_index_live_low")),
                "live_latest": _to_float(index_env_snapshot.get("env_index_live_latest")),
                "live_pct_change": _to_float(index_env_snapshot.get("env_index_live_pct_change")),
                "live_volume": _to_float(index_env_snapshot.get("env_index_live_volume")),
                "live_amount": _to_float(index_env_snapshot.get("env_index_live_amount")),
                "dev_ma20_atr": _to_float(index_env_snapshot.get("env_index_dev_ma20_atr")),
                "gate_action": index_env_snapshot.get("env_index_gate_action"),
                "gate_reason": index_env_snapshot.get("env_index_gate_reason"),
                "position_cap": _to_float(index_env_snapshot.get("env_index_position_cap")),
            }
            index_snapshot_payload["snapshot_hash"] = make_snapshot_hash(index_snapshot_payload)
            env_index_snapshot_hash = self._persist_index_snapshot(
                index_snapshot_payload,
                table=self.params.env_index_snapshot_table,
            ) or index_snapshot_payload["snapshot_hash"]
            index_env_snapshot["env_index_snapshot_hash"] = env_index_snapshot_hash

        if isinstance(ctx, dict):
            ctx["index_intraday"] = index_env_snapshot
            ctx["env_index_snapshot_hash"] = env_index_snapshot_hash

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

        return ctx or env_context, index_env_snapshot, env_index_snapshot_hash

    def _is_pullback_signal(self, signal_reason: str) -> bool:
        reason_text = str(signal_reason or "")
        lower = reason_text.lower()
        return ("回踩" in reason_text) and (("ma20" in lower) or ("ma 20" in lower))


    def _parse_market_environment(self, env_context: dict[str, Any] | None) -> MarketEnvironment:
        return MarketEnvironment.from_snapshot(env_context)


    def _prepare_monitor_frame(
        self,
        signals: pd.DataFrame,
        quotes: pd.DataFrame,
        latest_snapshots: pd.DataFrame,
        latest_trade_date: str,
        checked_at_ts: dt.datetime,
    ) -> tuple[pd.DataFrame, dict[str, float] | None, int, int]:
        """准备 open_monitor 评估所需的宽表输入（严格模式）。

        约定：
        - signals 来自 ready_signals_view，已包含指标/筹码/行业板块等静态字段；
        - 本函数只负责 quotes 标准化 + 合并 + 最新快照补齐（asof_*）。
        """

        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        if quotes is None or getattr(quotes, "empty", True):
            q = pd.DataFrame(
                columns=[
                    "code",
                    "live_open",
                    "live_high",
                    "live_low",
                    "live_latest",
                    "live_volume",
                    "live_amount",
                    "live_pct_change",
                    "live_gap_pct",
                    "live_intraday_vol_ratio",
                    "prev_close",
                ]
            )
        else:
            q = quotes.copy()
            if "code" not in q.columns:
                msg = "quotes 缺少 code 列"
                if strict_quotes:
                    raise RuntimeError(msg)
                self.logger.error("%s（strict_quotes=false，将跳过行情合并）", msg)
                q["code"] = ""
            q["code"] = q["code"].astype(str)
            required = [
                "live_open",
                "live_high",
                "live_low",
                "live_latest",
                "live_volume",
                "live_amount",
            ]
            missing = [c for c in required if c not in q.columns]
            if missing:
                msg = f"quotes 缺少统一行情列：{missing}"
                if strict_quotes:
                    raise RuntimeError(msg)
                self.logger.error("%s（strict_quotes=false，将补空列）", msg)
                for c in missing:
                    q[c] = None

        merged = signals.copy()
        merged["code"] = merged["code"].astype(str)
        merged = merged.merge(q, on="code", how="left", suffixes=("", "_q"))

        # 最新快照始终合并（asof_* 仅作为参考，不影响信号日指标字段 sig_*）
        if not latest_snapshots.empty:
            snap = latest_snapshots.copy()
            snap["_has_latest_snapshot"] = True
            rename_map = {c: f"asof_{c}" for c in snap.columns if c not in {"code", "_has_latest_snapshot"}}
            snap = snap.rename(columns=rename_map)
            merged = merged.merge(snap, on="code", how="left")

        if "_has_latest_snapshot" not in merged.columns:
            merged["_has_latest_snapshot"] = False
        else:
            merged["_has_latest_snapshot"] = merged["_has_latest_snapshot"].eq(True)

        for col in [
            "asof_trade_date",
            "asof_close",
            "asof_ma5",
            "asof_ma20",
            "asof_ma60",
            "asof_ma250",
            "asof_vol_ratio",
            "asof_macd_hist",
            "asof_atr14",
            "asof_stop_ref",
        ]:
            if col not in merged.columns:
                merged[col] = None

        avg_volume_map = self._load_avg_volume(
            latest_trade_date, merged["code"].dropna().astype(str).unique().tolist()
        )
        minutes_elapsed = self._calc_minutes_elapsed(checked_at_ts)
        total_minutes = 240
        merged["avg_volume_20"] = merged["code"].map(avg_volume_map) if avg_volume_map else None

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
            "live_open",
            "live_latest",
            "live_high",
            "live_low",
            "live_pct_change",
            "live_volume",
            "live_amount",
            "prev_close",
        ]
        for col in float_cols:
            if col in merged.columns:
                merged[col] = merged.get(col).apply(_to_float)

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
            merged["live_volume"] = merged["live_volume"].apply(lambda x: None if x is None else x * live_vol_scale)

        return merged, avg_volume_map, minutes_elapsed, total_minutes
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

        checked_at_ts = checked_at or dt.datetime.now()
        run_id = self._calc_run_id(checked_at_ts)
        monitor_date = checked_at_ts.date().isoformat()
        run_id_norm = str(run_id or "").strip().upper()
        if not getattr(self, "_ready_signals_used", False):
            self.logger.error(
                "未启用 ready_signals_view（严格模式），已终止评估（monitor_date=%s, run_id=%s）。",
                monitor_date,
                run_id,
            )
            return pd.DataFrame()

        env = self._parse_market_environment(env_context)

        if env.gate_action is None:
            self.logger.error(
                "环境快照缺少 env_final_gate_action，已终止评估（monitor_date=%s, run_id=%s）。",
                monitor_date,
                run_id,
            )
            return pd.DataFrame()
        if env.position_cap_pct is None:
            self.logger.error(
                "环境快照缺少 env_final_cap_pct，已终止评估（monitor_date=%s, run_id=%s）。",
                monitor_date,
                run_id,
            )
            return pd.DataFrame()

        merged, avg_volume_map, minutes_elapsed, total_minutes = self._prepare_monitor_frame(
            signals,
            quotes,
            latest_snapshots,
            latest_trade_date,
            checked_at_ts,
        )

        def _resolve_ref_close(row: pd.Series) -> float | None:
            for key in ("prev_close", "sig_close"):
                val = _to_float(row.get(key))
                if val is not None and val > 0:
                    return val
            return None

        live_gap_list: List[float | None] = []
        live_pct_change_list: List[float | None] = []
        live_intraday_vol_list: List[float | None] = []
        dev_ma5_list: List[float | None] = []
        dev_ma20_list: List[float | None] = []
        dev_ma5_atr_list: List[float | None] = []
        dev_ma20_atr_list: List[float | None] = []
        runup_from_sigclose_list: List[float | None] = []
        runup_from_sigclose_atr_list: List[float | None] = []
        runup_ref_price_list: List[float | None] = []
        runup_ref_source_list: List[str | None] = []
        actions: List[str] = []
        action_reasons: List[str] = []
        states: List[str] = []
        status_reasons: List[str] = []
        signal_kinds: List[str] = []
        valid_days_list: List[int | None] = []
        entry_exposure_caps: List[float | None] = []
        trade_stop_refs: List[float | None] = []
        sig_stop_refs: List[float | None] = []
        effective_stop_refs: List[float | None] = []
        rule_hits_json_list: List[str | None] = []
        summary_lines: List[str | None] = []
        env_index_snapshot_hashes: List[str | None] = []
        env_final_gate_action_list: List[str | None] = []
        env_regimes: List[str | None] = []
        env_position_hints: List[float | None] = []
        env_weekly_asof_trade_dates: List[str | None] = []
        env_weekly_risk_levels: List[str | None] = []
        env_weekly_scene_list: List[str | None] = []

        max_up = self.rule_config.max_gap_up_pct
        max_up_atr_mult = self.rule_config.max_gap_up_atr_mult
        max_down = self.rule_config.max_gap_down_pct
        min_vs_ma20 = self.rule_config.min_open_vs_ma20_pct
        pullback_min_vs_ma20 = self.rule_config.pullback_min_open_vs_ma20_pct
        limit_up_trigger = self.rule_config.limit_up_trigger_pct
        stop_atr_mult = self.rule_config.stop_atr_mult
        runup_atr_max = self.rule_config.runup_atr_max
        pullback_runup_atr_max = self.rule_config.pullback_runup_atr_max
        pullback_runup_dev_ma20_atr_min = self.rule_config.pullback_runup_dev_ma20_atr_min
        runup_atr_tol = self.rule_config.runup_atr_tol
        cross_valid_days = self.params.cross_valid_days
        pullback_valid_days = self.params.pullback_valid_days

        for _, row in merged.iterrows():
            sig_reason_text = str(row.get("sig_reason") or row.get("reason") or "")
            is_pullback = self._is_pullback_signal(sig_reason_text)
            valid_days = pullback_valid_days if is_pullback else cross_valid_days
            valid_days_list.append(valid_days)
            sig_stop_ref = _to_float(row.get("sig_stop_ref"))
            sig_stop_refs.append(sig_stop_ref)

            price_now = _to_float(row.get("live_open")) or _to_float(row.get("live_latest"))
            ref_close = _resolve_ref_close(row)
            if price_now is None:
                if run_id_norm == "PREOPEN" and ref_close is not None:
                    price_now = ref_close
                elif run_id_norm == "POSTCLOSE":
                    price_now = _to_float(row.get("live_latest")) or ref_close or _to_float(row.get("sig_close"))

            live_gap = None
            live_pct = None
            if ref_close and price_now:
                live_gap = (price_now - ref_close) / ref_close
                live_pct = (price_now / ref_close - 1.0) * 100.0
            live_gap_list.append(live_gap)
            live_pct_change_list.append(live_pct)

            avg_vol_20 = avg_volume_map.get(str(row.get("code"))) if avg_volume_map else None
            live_vol = _to_float(row.get("live_volume"))
            live_intraday = None
            if (
                    avg_vol_20 is not None
                    and avg_vol_20 > 0
                    and live_vol is not None
                    and live_vol > 0
                    and minutes_elapsed > 0
            ):
                scaled = (live_vol / max(minutes_elapsed, 1)) * total_minutes
                live_intraday = scaled / avg_vol_20
            live_intraday_vol_list.append(live_intraday)

            sig_ma5 = _to_float(row.get("sig_ma5"))
            sig_ma20 = _to_float(row.get("sig_ma20"))
            sig_atr14 = _to_float(row.get("sig_atr14"))

            dev_ma5 = None
            dev_ma20 = None
            dev_ma5_atr = None
            dev_ma20_atr = None
            if price_now is not None:
                if sig_ma5:
                    dev_ma5 = price_now - sig_ma5
                if sig_ma20:
                    dev_ma20 = price_now - sig_ma20
                if sig_atr14 and sig_atr14 != 0:
                    if sig_ma5 is not None:
                        dev_ma5_atr = (price_now - sig_ma5) / sig_atr14
                    if sig_ma20 is not None:
                        dev_ma20_atr = (price_now - sig_ma20) / sig_atr14
            dev_ma5_list.append(dev_ma5)
            dev_ma20_list.append(dev_ma20)
            dev_ma5_atr_list.append(dev_ma5_atr)
            dev_ma20_atr_list.append(dev_ma20_atr)

            ref_asof_close = _to_float(row.get("asof_close")) or ref_close or _to_float(row.get("sig_close"))
            runup_metrics = compute_runup_metrics(
                _to_float(row.get("sig_close")),
                asof_close=ref_asof_close,
                live_high=_to_float(row.get("live_high")) or price_now,
                sig_atr14=sig_atr14,
            )
            runup_from_sigclose_list.append(runup_metrics.runup_from_sigclose)
            runup_from_sigclose_atr_list.append(runup_metrics.runup_from_sigclose_atr)
            runup_ref_price_list.append(runup_metrics.runup_ref_price)
            runup_ref_source_list.append(runup_metrics.runup_ref_source)

            threshold_gap_up = max_up
            if sig_atr14 and sig_atr14 > 0 and ref_close:
                atr_gap = max_up_atr_mult * sig_atr14 / ref_close
                threshold_gap_up = min(max_up, atr_gap)

            ma20_thresh = pullback_min_vs_ma20 if is_pullback else min_vs_ma20

            chip_score = _to_float(row.get("sig_chip_score"))

            breach = False
            breach_reason = None
            if price_now is not None:
                dev_ma20_atr_val = dev_ma20_atr
                runup_limit = pullback_runup_atr_max if is_pullback else runup_atr_max
                breach, breach_reason = evaluate_runup_breach(
                    runup_metrics,
                    runup_atr_max=runup_limit,
                    runup_atr_tol=runup_atr_tol,
                    dev_ma20_atr=dev_ma20_atr_val,
                    dev_ma20_atr_min=pullback_runup_dev_ma20_atr_min if is_pullback else None,
                )

            signal_age = None
            raw_signal_age = row.get("signal_age")
            if raw_signal_age is not None:
                try:
                    if not pd.isna(raw_signal_age):
                        signal_age = int(float(raw_signal_age))
                except Exception:
                    signal_age = None

            ctx = DecisionContext(
                entry_exposure_cap=env.position_cap_pct,
                env=env,
                chip_score=chip_score,
                price_now=price_now,
                live_gap=live_gap,
                live_pct=live_pct,
                threshold_gap_up=threshold_gap_up,
                max_gap_down=max_down,
                sig_ma20=sig_ma20,
                ma20_thresh=ma20_thresh,
                signal_age=signal_age,
                valid_days=valid_days,
                limit_up_trigger=limit_up_trigger,
                runup_breach=breach,
                runup_breach_reason=breach_reason,
            )
            self.rule_engine.apply(ctx, self.rules)


            result = ctx.export_result()

            action = result.action
            action_reason = result.action_reason


            trade_stop_ref = None
            if price_now is not None and sig_atr14 is not None:
                trade_stop_ref = price_now - stop_atr_mult * sig_atr14

            entry_cap = result.entry_exposure_cap
            if entry_cap is None:
                entry_cap = env.position_cap_pct
            sig_final_cap = _to_float(row.get("final_cap"))
            if sig_final_cap is not None:
                entry_cap = sig_final_cap if entry_cap is None else min(entry_cap, sig_final_cap)
            entry_exposure_caps.append(entry_cap)
            trade_stop_refs.append(trade_stop_ref)
            effective_stop_refs.append(trade_stop_ref)

            states.append(result.state)
            status_reasons.append(result.status_reason)
            actions.append(action)
            action_reasons.append(action_reason)
            signal_kinds.append("PULLBACK" if is_pullback else "CROSS")
            rule_hits_json_list.append(result.rule_hits_json)
            summary_lines.append(result.summary_line)
            env_index_snapshot_hashes.append(env.index_snapshot_hash)
            env_final_gate_action_list.append(result.env_gate_action or env.gate_action)
            env_regimes.append(env.regime)
            env_position_hints.append(env.position_hint)
            env_weekly_asof_trade_dates.append(env.weekly_asof_trade_date)
            env_weekly_risk_levels.append(env.weekly_risk_level)
            env_weekly_scene_list.append(env.weekly_scene)

        merged["monitor_date"] = monitor_date
        merged["live_trade_date"] = monitor_date
        merged["live_gap_pct"] = live_gap_list
        merged["live_pct_change"] = live_pct_change_list
        merged["live_intraday_vol_ratio"] = live_intraday_vol_list
        merged["action"] = actions
        merged["action_reason"] = action_reasons
        merged["state"] = states
        merged["status_reason"] = status_reasons
        merged["signal_kind"] = signal_kinds
        merged["signal_age"] = merged.get("signal_age")
        merged["valid_days"] = valid_days_list
        merged["trade_stop_ref"] = trade_stop_refs
        merged["effective_stop_ref"] = effective_stop_refs
        merged["entry_exposure_cap"] = entry_exposure_caps
        merged["checked_at"] = checked_at_ts
        merged["run_id"] = run_id
        merged["env_regime"] = env_regimes
        merged["env_position_hint"] = env_position_hints
        merged["env_weekly_asof_trade_date"] = env_weekly_asof_trade_dates
        merged["env_weekly_risk_level"] = env_weekly_risk_levels
        merged["env_weekly_scene"] = env_weekly_scene_list
        merged["env_weekly_structure_status"] = None
        merged["env_weekly_pattern_status"] = None
        merged["env_weekly_gate_action"] = None
        merged["env_final_gate_action"] = env_final_gate_action_list
        merged["env_index_snapshot_hash"] = env_index_snapshot_hashes
        merged["runup_from_sigclose"] = runup_from_sigclose_list
        merged["runup_from_sigclose_atr"] = runup_from_sigclose_atr_list
        merged["runup_ref_price"] = runup_ref_price_list
        merged["runup_ref_source"] = runup_ref_source_list
        merged["dev_ma5"] = dev_ma5_list
        merged["dev_ma20"] = dev_ma20_list
        merged["dev_ma5_atr"] = dev_ma5_atr_list
        merged["dev_ma20_atr"] = dev_ma20_atr_list
        merged["rule_hits_json"] = rule_hits_json_list
        merged["summary_line"] = summary_lines

        # ---- 这里：修复 fillna object downcast FutureWarning（核心改动）----
        def _coalesce_numeric_into(target: str, fallback: str) -> None:
            if target not in merged.columns:
                merged[target] = None
            if fallback not in merged.columns:
                return
            left = pd.to_numeric(merged[target], errors="coerce")
            right = pd.to_numeric(merged[fallback], errors="coerce")
            merged[target] = left.fillna(right)

        if "asof_trade_date" in merged.columns and "sig_date" in merged.columns:
            # 用 where 合并，避免 object fillna 触发 downcast warning
            mask = merged["asof_trade_date"].notna()
            merged["asof_trade_date"] = merged["asof_trade_date"].where(mask, merged["sig_date"])

        _coalesce_numeric_into("asof_close", "sig_close")
        _coalesce_numeric_into("asof_ma5", "sig_ma5")
        _coalesce_numeric_into("asof_ma20", "sig_ma20")
        _coalesce_numeric_into("asof_ma60", "sig_ma60")
        _coalesce_numeric_into("asof_ma250", "sig_ma250")
        _coalesce_numeric_into("asof_vol_ratio", "sig_vol_ratio")
        _coalesce_numeric_into("asof_macd_hist", "sig_macd_hist")
        _coalesce_numeric_into("asof_atr14", "sig_atr14")
        _coalesce_numeric_into("asof_stop_ref", "sig_stop_ref")
        # ---- 修复结束 ----

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
            "rule_hits_json",
            "summary_line",
            "signal_kind",
            "sig_signal",
            "sig_reason",
            "state",
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
            "state",
            "status_reason",
            "action",
            "action_reason",
            "signal_strength",
            "strength_note",
            "risk_tag",
            "risk_note",
            "rule_hits_json",
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

        merged["snapshot_hash"] = merged.apply(lambda row: make_snapshot_hash(row.to_dict()), axis=1)
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

        if "rule_hits_json" in df.columns:
            df["rule_hits_json"] = (
                df["rule_hits_json"].fillna("").astype(str).str.slice(0, 4000)
            )

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
        view = self.params.open_monitor_view or self.params.open_monitor_wide_view
        fallback_view = self.params.open_monitor_wide_view
        if not (view and monitor_date and self._table_exists(view)):
            if fallback_view and self._table_exists(fallback_view):
                view = fallback_view
            else:
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

        state_rank = {
            "OK": 0,
            "OVEREXTENDED": 1,
            "RUNUP_TOO_LARGE": 2,
            "INVALID": 3,
            "STALE": 4,
            "UNKNOWN": 9,
        }
        if "state" in export_df.columns:
            export_df["_state_rank"] = export_df["state"].map(state_rank).fillna(99)
        else:
            export_df["_state_rank"] = 99
        export_df = export_df.sort_values(
            by=["_action_rank", "_state_rank", gap_col],
            ascending=[True, True, True],
        )
        export_df = export_df.drop(
            columns=["_action_rank", "_state_rank"],
            errors="ignore",
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

        latest_trade_date, signal_dates, signals = self._load_recent_buy_signals()
        if not latest_trade_date or signals.empty:
            return

        codes = signals["code"].dropna().astype(str).unique().tolist()
        self.logger.info("待监测标的数量：%s（信号日：%s）", len(codes), signal_dates)

        quotes = self._fetch_quotes(codes)
        if quotes is None or quotes.empty:
            self.logger.warning("未获取到任何实时行情，将输出 UNKNOWN 结果。")
        else:
            self.logger.info("实时行情已获取：%s 条", len(quotes))

        latest_snapshots = self._load_latest_snapshots(latest_trade_date, codes)
        env_context = self.load_env_snapshot_context(monitor_date, run_id)
        if not env_context:
            self.logger.error(
                "未找到环境快照（monitor_date=%s, run_id=%s），本次开盘监测终止。",
                monitor_date,
                run_id,
            )
            return

        env_final_gate_action = env_context.get("env_final_gate_action") if isinstance(env_context, dict) else None
        self.logger.info(
            "已加载环境快照（monitor_date=%s, run_id=%s, gate=%s）。",
            monitor_date,
            run_id,
            env_final_gate_action,
        )
        # feat: 周线环境日志从 env_final_reason_json 回填关键字段，避免出现大量 None
        reason_ctx = {}
        if isinstance(env_context, dict):
            reason_json = env_context.get("env_final_reason_json")
            if isinstance(reason_json, str) and reason_json.strip():
                try:
                    reason_ctx = json.loads(reason_json)
                except Exception:
                    reason_ctx = {}

        weekly_scenario = env_context.get("weekly_scenario", {}) if isinstance(env_context, dict) else {}
        if isinstance(weekly_scenario, dict):
            struct_tags = ",".join(weekly_scenario.get("weekly_structure_tags", []) or [])
            confirm_tags = ",".join(weekly_scenario.get("weekly_confirm_tags", []) or [])

            asof_trade_date = weekly_scenario.get("weekly_asof_trade_date") or reason_ctx.get("weekly_asof_trade_date")
            current_week_closed = weekly_scenario.get("weekly_current_week_closed")
            if current_week_closed is None:
                current_week_closed = reason_ctx.get("weekly_current_week_closed")

            risk_level = weekly_scenario.get("weekly_risk_level") or reason_ctx.get("weekly_risk_level")
            risk_score = _to_float(weekly_scenario.get("weekly_risk_score"))
            if risk_score is None:
                risk_score = _to_float(reason_ctx.get("weekly_risk_score"))
            risk_score = risk_score or 0.0

            scene_code = weekly_scenario.get("weekly_scene_code") or reason_ctx.get("weekly_scene_code")

            bias = weekly_scenario.get("weekly_bias")
            if bias is None:
                tags_str = reason_ctx.get("weekly_tags")
                if isinstance(tags_str, str):
                    if "BIAS_BULLISH" in tags_str:
                        bias = "BULLISH"
                    elif "BIAS_BEARISH" in tags_str:
                        bias = "BEARISH"

            status = weekly_scenario.get("weekly_status")
            if status is None:
                status = (
                    weekly_scenario.get("weekly_pattern_status")
                    or reason_ctx.get("weekly_pattern_status")
                    or reason_ctx.get("weekly_structure_status")
                )

            key_levels_str = weekly_scenario.get("weekly_key_levels_str") or reason_ctx.get("weekly_key_levels_str")
            key_levels_short = str(key_levels_str or "")[:120]

            if not confirm_tags:
                tags_str = reason_ctx.get("weekly_tags")
                if isinstance(tags_str, str) and tags_str.strip():
                    confirm_tags = ",".join([t.strip() for t in tags_str.replace(";", ",").split(",") if t.strip()])

            self.logger.info(
                "周线情景：asof=%s current_week_closed=%s risk=%s(%.1f) scene=%s bias=%s status=%s levels=%s 确认标签=%s",
                asof_trade_date,
                current_week_closed,
                risk_level,
                risk_score,
                scene_code,
                bias,
                status,
                key_levels_short,
                confirm_tags,
            )
            weekly_note = str(weekly_scenario.get("weekly_note") or reason_ctx.get("weekly_note") or "").strip()
            if weekly_note:
                self.logger.info("周线备注：%s", weekly_note[:200])
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
        result = self._evaluate(
            signals,
            quotes,
            latest_snapshots,
            latest_trade_date,
            env_context,
            checked_at=checked_at,
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
            preview_disp = preview.copy()
            if gap_col.endswith("_pct") and gap_col in preview_disp.columns:
                def _fmt_pct(v):
                    try:
                        fv = float(v)
                    except Exception:
                        return ""
                    if math.isnan(fv):
                        return ""
                    return f"{fv * 100:.3f}%"

                preview_disp[gap_col] = preview_disp[gap_col].apply(_fmt_pct)
            self.logger.info(
                "可执行清单 Top%s（按 gap 由小到大）：\n%s",
                top_n,
                preview_disp.to_string(index=False),
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
            wait_preview_disp = wait_preview.copy()
            if gap_col.endswith("_pct") and gap_col in wait_preview_disp.columns:
                def _fmt_pct(v):
                    try:
                        fv = float(v)
                    except Exception:
                        return ""
                    if math.isnan(fv):
                        return ""
                    return f"{fv * 100:.3f}%"

                wait_preview_disp[gap_col] = wait_preview_disp[gap_col].apply(_fmt_pct)
            self.logger.info(
                "WAIT 观察清单 Top%s（按 gap 由小到大）：\n%s",
                wait_top,
                wait_preview_disp.to_string(index=False),
            )

        self._persist_results(result)
        export_df = self._load_open_monitor_view_data(monitor_date, run_id)
        if export_df.empty:
            export_df = result
        self._export_csv(export_df)
