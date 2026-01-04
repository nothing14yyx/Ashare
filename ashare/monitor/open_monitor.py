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
import json
import logging
import math
import re
from dataclasses import dataclass, replace
from typing import Any, Callable, List

import pandas as pd

from ashare.core.config import get_section
from ashare.core.db import DatabaseConfig, MySQLWriter
from ashare.monitor.monitor_rules import MonitorRuleConfig, build_default_monitor_rules
from ashare.monitor.open_monitor_env import OpenMonitorEnvService
from ashare.monitor.low_suck_detector import detect_low_suck_signal
from ashare.monitor.open_monitor_eval import (
    OpenMonitorEvaluator,
    merge_gate_actions,
)
from ashare.monitor.open_monitor_market_data import OpenMonitorMarketData
from ashare.monitor.open_monitor_repo import OpenMonitorRepository, calc_run_id, make_snapshot_hash
from ashare.monitor.open_monitor_rules import Rule, RuleEngine, RuleResult
from ashare.indicators.market_indicator_builder import MarketIndicatorBuilder
from ashare.core.schema_manager import (
    STRATEGY_CODE_MA5_MA20_TREND,
    TABLE_STRATEGY_DAILY_MARKET_ENV,
    TABLE_STRATEGY_OPEN_MONITOR_ENV,
    TABLE_STRATEGY_OPEN_MONITOR_EVAL,
    TABLE_STRATEGY_OPEN_MONITOR_MINUTE,
    TABLE_STRATEGY_OPEN_MONITOR_QUOTE,
    TABLE_STRATEGY_OPEN_MONITOR_RUN,
    TABLE_STRATEGY_WEEKLY_MARKET_ENV,
    WEEKLY_MARKET_BENCHMARK_CODE,
    VIEW_STRATEGY_OPEN_MONITOR,
    VIEW_STRATEGY_OPEN_MONITOR_ENV,
    VIEW_STRATEGY_OPEN_MONITOR_WIDE,
    TABLE_STRATEGY_READY_SIGNALS,
)
from ashare.utils.convert import to_float as _to_float
from ashare.utils.logger import setup_logger
from ashare.indicators.weekly_env_builder import WeeklyEnvironmentBuilder


@dataclass(frozen=True)
class OpenMonitorParams:
    """开盘监测参数（支持从 config.yaml 的 open_monitor 覆盖）。"""

    enabled: bool = True
    # 运行时上下文（由 Runner 填充；用于记录行情抓取时间）
    checked_at: dt.datetime | None = None
    monitor_date: str | None = None

    # 信号输入：只接受 ready_signals_view（由 SchemaManager 负责生成/维护）
    ready_signals_view: str = TABLE_STRATEGY_READY_SIGNALS
    # 契约与 fail-fast（默认开启）
    strict_ready_signals_required: bool = True
    strict_quotes: bool = True
    quote_table: str = TABLE_STRATEGY_OPEN_MONITOR_QUOTE
    strategy_code: str = STRATEGY_CODE_MA5_MA20_TREND

    # 输出表：开盘检查结果
    output_table: str = TABLE_STRATEGY_OPEN_MONITOR_EVAL
    run_table: str = TABLE_STRATEGY_OPEN_MONITOR_RUN
    minute_table: str = TABLE_STRATEGY_OPEN_MONITOR_MINUTE
    open_monitor_view: str = VIEW_STRATEGY_OPEN_MONITOR
    open_monitor_wide_view: str = VIEW_STRATEGY_OPEN_MONITOR_WIDE
    open_monitor_env_view: str = VIEW_STRATEGY_OPEN_MONITOR_ENV

    # 回看近 N 个交易日的 BUY 信号
    signal_lookback_days: int = 3

    # 行情来源：eastmoney / akshare（兼容：auto 将按 eastmoney 处理）
    quote_source: str = "eastmoney"

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

    export_csv: bool = False
    export_top_n: int = 100
    output_subdir: str = "open_monitor"
    interval_minutes: int = 5
    run_id_minutes: int = 5
    run_id_use_seconds: bool = False

    # 环境表：存储周线计划等“批次级别”信息，避免在每条标的记录里重复。
    open_monitor_env_table: str = TABLE_STRATEGY_OPEN_MONITOR_ENV

    weekly_indicator_table: str = TABLE_STRATEGY_WEEKLY_MARKET_ENV
    daily_indicator_table: str = TABLE_STRATEGY_DAILY_MARKET_ENV
    weekly_benchmark_code: str = WEEKLY_MARKET_BENCHMARK_CODE

    # 同一批次内同一 code 只保留“最新 date（信号日）”那条 BUY 信号。
    # 目的：避免同一批次出现重复 code（例如同一只股票在 12-09 与 12-11 都触发 BUY）。
    unique_code_latest_date_only: bool = True

    # 输出模式：FULL 保留全部字段，COMPACT 只保留核心字段
    output_mode: str = "FULL"

    # 环境快照持久化：与行情/评估表保持一致，默认写入
    persist_env_snapshot: bool = True
    # 盘中关键位突破：触达/站稳/回踩阈值
    live_breakout_high_eps: float = 0.002
    live_breakout_latest_eps: float = 0.001
    live_retest_pct: float = 0.003
    live_retest_atr_mult: float = 0.5
    # 分时拉取保护
    minute_fetch_timeout_sec: float = 5.0
    minute_fetch_skip_non_trading_day: bool = True

    def validate(self, logger: logging.Logger) -> "OpenMonitorParams":
        params = self

        def _reset(field: str, reason: str) -> None:
            nonlocal params
            default_val = getattr(OpenMonitorParams, field)
            logger.warning(
                "open_monitor param %s invalid (%s); fallback=%s",
                field,
                reason,
                default_val,
            )
            params = replace(params, **{field: default_val})

        def _ensure_min_int(field: str, min_val: int) -> None:
            nonlocal params
            val = getattr(params, field)
            try:
                val_int = int(val)
            except Exception:
                _reset(field, "not int")
                return
            if val_int < min_val:
                _reset(field, f"<{min_val}")
                return
            if val_int != val:
                params = replace(params, **{field: val_int})

        def _ensure_float_range(field: str, min_val: float, max_val: float) -> None:
            nonlocal params
            val = getattr(params, field)
            try:
                val_float = float(val)
            except Exception:
                _reset(field, "not float")
                return
            if (not math.isfinite(val_float)) or val_float < min_val or val_float > max_val:
                _reset(field, f"range {min_val}..{max_val}")
                return
            if val_float != val:
                params = replace(params, **{field: val_float})

        _ensure_min_int("signal_lookback_days", 1)
        _ensure_min_int("interval_minutes", 1)
        _ensure_min_int("run_id_minutes", 1)
        _ensure_min_int("export_top_n", 0)

        if params.run_id_minutes < params.interval_minutes:
            logger.warning(
                "open_monitor param run_id_minutes < interval_minutes; clamp to %s",
                params.interval_minutes,
            )
            params = replace(params, run_id_minutes=params.interval_minutes)

        mode = str(params.output_mode or "").strip().upper()
        if mode not in {"FULL", "COMPACT"}:
            _reset("output_mode", "invalid enum")
        elif mode != params.output_mode:
            params = replace(params, output_mode=mode)

        quote_source = str(params.quote_source or "").strip().lower()
        if quote_source == "auto":
            logger.warning("open_monitor quote_source=auto; using eastmoney")
            quote_source = "eastmoney"
        if quote_source not in {"eastmoney", "akshare"}:
            _reset("quote_source", "invalid enum")
        elif quote_source != params.quote_source:
            params = replace(params, quote_source=quote_source)

        _ensure_float_range("live_breakout_high_eps", 0.0, 0.1)
        _ensure_float_range("live_breakout_latest_eps", 0.0, 0.1)
        _ensure_float_range("live_retest_pct", 0.0, 0.1)
        _ensure_float_range("live_retest_atr_mult", 0.0, 10.0)
        _ensure_float_range("minute_fetch_timeout_sec", 0.0, 120.0)

        name_pattern = re.compile(r"^[A-Za-z0-9_]+$")
        name_fields = [
            "ready_signals_view",
            "quote_table",
            "output_table",
            "run_table",
            "minute_table",
            "open_monitor_view",
            "open_monitor_wide_view",
            "open_monitor_env_view",
            "open_monitor_env_table",
            "weekly_indicator_table",
            "daily_indicator_table",
        ]
        for field in name_fields:
            val = getattr(params, field)
            if not isinstance(val, str) or not name_pattern.match(val):
                _reset(field, "invalid name")

        return params

    @classmethod
    def from_config(cls) -> "OpenMonitorParams":
        sec = get_section("open_monitor") or {}
        if not isinstance(sec, dict):
            sec = {}
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

        def _get_float(key: str, default: float) -> float:
            raw = sec.get(key, default)
            try:
                return float(raw)
            except Exception:
                return default

        quote_source = str(sec.get("quote_source", cls.quote_source)).strip().lower() or "auto"

        interval_minutes = _get_int("interval_minutes", cls.interval_minutes)

        run_id_configured = sec.get("run_id_minutes")
        run_id_minutes = (
            _get_int("run_id_minutes", interval_minutes)
            if run_id_configured is not None
            else interval_minutes
        )

        params = cls(
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
            run_table=str(sec.get("run_table", cls.run_table)).strip() or cls.run_table,
            minute_table=str(sec.get("minute_table", cls.minute_table)).strip()
                         or cls.minute_table,
            open_monitor_view=str(
                sec.get("open_monitor_view", cls.open_monitor_view)
            ).strip()
                              or cls.open_monitor_view,
            open_monitor_env_view=str(
                sec.get("open_monitor_env_view", cls.open_monitor_env_view)
            ).strip()
                                  or cls.open_monitor_env_view,
            open_monitor_wide_view=str(
                sec.get("open_monitor_wide_view", cls.open_monitor_wide_view)
            ).strip()
                                   or cls.open_monitor_wide_view,
            signal_lookback_days=_get_int("signal_lookback_days", cls.signal_lookback_days),
            quote_source=quote_source,
            index_code=str(sec.get("index_code", cls.index_code)).strip() or cls.index_code,
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
            output_subdir=str(sec.get("output_subdir", cls.output_subdir)).strip()
                          or cls.output_subdir,
            interval_minutes=interval_minutes,
            run_id_minutes=run_id_minutes,
            run_id_use_seconds=_get_bool("run_id_use_seconds", cls.run_id_use_seconds),
            unique_code_latest_date_only=_get_bool(
                "unique_code_latest_date_only", cls.unique_code_latest_date_only
            ),
            open_monitor_env_table=str(
                sec.get("open_monitor_env_table", cls.open_monitor_env_table)
            ).strip()
            or cls.open_monitor_env_table,
            weekly_indicator_table=cls.weekly_indicator_table,
            daily_indicator_table=str(
                sec.get("daily_indicator_table", cls.daily_indicator_table)
            ).strip()
            or cls.daily_indicator_table,
            weekly_benchmark_code=cls.weekly_benchmark_code,
            output_mode=str(sec.get("output_mode", cls.output_mode)).strip().upper()
                        or cls.output_mode,
            persist_env_snapshot=_get_bool("persist_env_snapshot", cls.persist_env_snapshot),
            live_breakout_high_eps=_get_float(
                "live_breakout_high_eps", cls.live_breakout_high_eps
            ),
            live_breakout_latest_eps=_get_float(
                "live_breakout_latest_eps", cls.live_breakout_latest_eps
            ),
            live_retest_pct=_get_float("live_retest_pct", cls.live_retest_pct),
            live_retest_atr_mult=_get_float(
                "live_retest_atr_mult", cls.live_retest_atr_mult
            ),
            minute_fetch_timeout_sec=_get_float(
                "minute_fetch_timeout_sec", cls.minute_fetch_timeout_sec
            ),
            minute_fetch_skip_non_trading_day=_get_bool(
                "minute_fetch_skip_non_trading_day",
                cls.minute_fetch_skip_non_trading_day,
            ),
        )
        return params.validate(logger)


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
        self.rule_engine = RuleEngine(merge_gate_actions)
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())

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

        self.repo = OpenMonitorRepository(self.db_writer.engine, self.logger, self.params)
        self.market_data = OpenMonitorMarketData(self.logger, self.params)
        self.env_service = OpenMonitorEnvService(
            self.repo,
            self.logger,
            self.params,
            self.env_builder,
            MarketIndicatorBuilder(
                env_builder=self.env_builder,
                logger=self.logger,
            ),
        )
        self.evaluator = OpenMonitorEvaluator(
            self.logger, self.params, self.rule_engine, self.rule_config, self.rules
        )

    def _calc_run_id(self, ts: dt.datetime) -> str:
        return calc_run_id(ts, self.params.run_id_minutes, self.params.run_id_use_seconds)

    def _build_run_params_json(self) -> str:
        payload = {
            "signal_lookback_days": self.params.signal_lookback_days,
            "index_code": self.params.index_code,
            "index_hist_lookback_days": self.params.index_hist_lookback_days,
            "quote_source": self.params.quote_source,
            "interval_minutes": self.params.interval_minutes,
            "run_id_minutes": self.params.run_id_minutes,
            "run_id_use_seconds": self.params.run_id_use_seconds,
            "strategy_code": self.params.strategy_code,
            "ready_signals_view": self.params.ready_signals_view,
        }
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    def build_and_persist_open_monitor_env(
        self,
        latest_trade_date: str,
        *,
        monitor_date: str | None = None,
        run_id: str | None = None,
        run_pk: int | None = None,
        checked_at: dt.datetime | None = None,
        allow_auto_compute: bool = False,
        fetch_index_live_quote: Callable[[], dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        return self.env_service.build_and_persist_open_monitor_env(
            latest_trade_date,
            monitor_date=monitor_date,
            run_id=run_id,
            run_pk=run_pk,
            checked_at=checked_at,
            allow_auto_compute=allow_auto_compute,
            fetch_index_live_quote=(
                self.market_data.fetch_index_live_quote
                if fetch_index_live_quote is None
                else fetch_index_live_quote
            ),
        )

    def load_open_monitor_env_context(
            self,
            monitor_date: str,
            run_pk: int | None = None,
    ) -> dict[str, Any] | None:
        return self.env_service.load_open_monitor_env_context(monitor_date, run_pk)

    def run(self, *, force: bool = False, checked_at: dt.datetime | None = None) -> None:
        """执行开盘监测。

        - 默认遵循 config.yaml: open_monitor.enabled。
        - 当 force=True 时，即便 enabled=false 也会执行（用于单独运行脚本）。
        """

        if (not force) and (not self.params.enabled):
            self.logger.info("open_monitor.enabled=false，跳过开盘监测。")
            return

        if force and (not self.params.enabled):
            self.logger.info("open_monitor.enabled=false，但 force=True，仍将执行开盘监测。")

        checked_at = checked_at or dt.datetime.now()
        monitor_date = self.repo.resolve_monitor_trade_date(checked_at)
        biz_ts = dt.datetime.combine(dt.date.fromisoformat(monitor_date), checked_at.time())

        # 将本次运行时上下文透传给行情层（用于补齐 live_trade_date）
        self.params = replace(self.params, checked_at=checked_at, monitor_date=monitor_date)
        self.repo.params = self.params
        self.market_data.params = self.params
        run_id = self._calc_run_id(biz_ts)

        latest_trade_date, signal_dates, signals = self.repo.load_recent_buy_signals()
        if latest_trade_date and (signals is not None) and (not signals.empty):
            codes = signals["code"].dropna().astype(str).unique().tolist()
            asof_df = self.repo.load_latest_indicators(latest_trade_date, codes)
            if asof_df is not None and not asof_df.empty:
                signals = signals.merge(asof_df, on="code", how="left")
                if "avg_volume_20" in signals.columns and "asof_avg_volume_20" in signals.columns:
                    signals["avg_volume_20"] = signals["avg_volume_20"].fillna(
                        signals["asof_avg_volume_20"]
                    )
                    signals = signals.drop(columns=["asof_avg_volume_20"])
            if "asof_trade_date" not in signals.columns:
                signals["asof_trade_date"] = latest_trade_date
            else:
                signals["asof_trade_date"] = signals["asof_trade_date"].fillna(
                    latest_trade_date
                )

        run_id_norm = str(run_id or "").strip()
        run_stage = run_id_norm.split(" ", 1)[0] if " " in run_id_norm else ""
        if run_stage not in {"PREOPEN", "BREAK", "POSTCLOSE"}:
            run_stage = "INTRADAY"

        fetch_index_quote = None
        dedup_sig = None
        signals_sig = None
        index_sig = None
        if latest_trade_date and (signals is not None) and (not signals.empty):
            codes = signals["code"].dropna().astype(str).unique().tolist()
            signal_dates_sorted = sorted(
                [str(d) for d in (signal_dates or []) if str(d).strip()]
            )
            signals_payload = {
                "latest_trade_date": latest_trade_date,
                "signal_dates": signal_dates_sorted,
                "codes": sorted(codes),
            }
            signals_sig = make_snapshot_hash(signals_payload)

            index_live_quote = self.market_data.fetch_index_live_quote()
            if isinstance(index_live_quote, dict) and index_live_quote:
                fetch_index_quote = lambda: dict(index_live_quote)
                index_payload = {
                    "index_code": str(self.params.index_code or "").strip() or None,
                    "live_trade_date": index_live_quote.get("live_trade_date"),
                    "live_open": _to_float(index_live_quote.get("live_open") or index_live_quote.get("open")),
                    "live_high": _to_float(index_live_quote.get("live_high") or index_live_quote.get("high")),
                    "live_low": _to_float(index_live_quote.get("live_low") or index_live_quote.get("low")),
                    "live_latest": _to_float(index_live_quote.get("live_latest") or index_live_quote.get("latest")),
                    "live_pct_change": _to_float(
                        index_live_quote.get("live_pct_change")
                        or index_live_quote.get("pct_change")
                    ),
                    "live_volume": _to_float(
                        index_live_quote.get("live_volume") or index_live_quote.get("volume")
                    ),
                    "live_amount": _to_float(
                        index_live_quote.get("live_amount") or index_live_quote.get("amount")
                    ),
                }
                index_sig = make_snapshot_hash(index_payload)

            if index_sig:
                dedup_sig = make_snapshot_hash(
                    {
                        "dedup_stage": run_stage,
                        "signals_sig": signals_sig,
                        "index_sig": index_sig,
                    }
                )
                prev = self.repo.load_latest_run_context_by_stage(
                    monitor_date, stage=run_stage
                )
                if (not self.params.run_id_use_seconds) and prev and prev.get("dedup_sig") == dedup_sig and prev.get("run_id"):
                    run_id = str(prev["run_id"])

        run_params_json = self._build_run_params_json()
        try:
            run_params = json.loads(run_params_json)
        except Exception:
            run_params = {"raw_params_json": run_params_json}
        if isinstance(run_params, dict):
            run_params.setdefault("dedup_stage", run_stage)
            if dedup_sig:
                run_params["dedup_sig"] = dedup_sig
            if signals_sig:
                run_params["signals_sig"] = signals_sig
            if index_sig:
                run_params["index_sig"] = index_sig

        run_pk = self.repo.ensure_run_context(
            monitor_date,
            run_id,
            checked_at=checked_at,
            triggered_at=checked_at,
            run_stage=run_stage,
            params_json=run_params,
        )
        if run_pk is None:
            self.logger.error("未获取 run_pk，无法继续执行开盘监测。")
            return

        if not latest_trade_date or signals.empty:
            return


        codes = signals["code"].dropna().astype(str).unique().tolist()
        self.logger.info("待监测标的数量：%s（信号日：%s）", len(codes), signal_dates)

        env_context = self.build_and_persist_open_monitor_env(
            latest_trade_date,
            monitor_date=monitor_date,
            run_id=run_id,
            run_pk=run_pk,
            checked_at=checked_at,
            allow_auto_compute=True,
            fetch_index_live_quote=fetch_index_quote,
        )
        if not env_context:
            self.logger.error(
                "未构建环境快照（monitor_date=%s, run_id=%s），本次开盘监测终止。",
                monitor_date,
                run_id,
            )
            return

        quotes = self.market_data.fetch_quotes(codes)
        if quotes is None or quotes.empty:
            self.logger.warning("未获取到任何实时行情，将输出 UNKNOWN 结果。")
        else:
            self.logger.info("实时行情已获取：%s 条", len(quotes))
        
        # === 新增：低吸信号检测 ===
        low_suck_map = {}
        minute_frames = []
        if self.params.enabled and self.rule_config.enable_low_suck_bonus:
            self.logger.info("开始执行低吸信号扫描 (%s 只标的)...", len(codes))
            # 为了获取昨收，先建立一个 code -> close 映射
            ref_close_map = {}
            sig_close_map = {}
            sig_date_map = {}
            strategy_code_map = {}
            if signals is not None and not signals.empty:
                # 优先用信号日的前一交易日收盘价
                for _, row in signals.iterrows():
                    c = str(row.get("code"))
                    sig_close = _to_float(row.get("sig_close"))
                    if sig_close:
                        sig_close_map[c] = sig_close
                    prev_close = _to_float(row.get("_signal_day_prev_close"))
                    if prev_close:
                        ref_close_map[c] = prev_close
                    sig_date_map[c] = row.get("sig_date")
                    if "strategy_code" in signals.columns:
                        sc = str(row.get("strategy_code") or "").strip()
                        if sc:
                            strategy_code_map[c] = sc
            
            # 补漏：如果 signals 里没有，从 quotes 里找 prev_close
            if not quotes.empty and "prev_close" in quotes.columns:
                for _, row in quotes.iterrows():
                    c = str(row.get("code"))
                    if c in ref_close_map:
                        continue
                    pcl = _to_float(row.get("prev_close"))
                    if pcl:
                        ref_close_map[c] = pcl

            # 再补漏：如果仍缺失，使用信号日收盘价兜底
            for c, cl in sig_close_map.items():
                if c not in ref_close_map and cl:
                    ref_close_map[c] = cl

            for code in codes:
                try:
                    target_date = sig_date_map.get(code) or latest_trade_date or monitor_date
                    target_date_str = (
                        pd.to_datetime(target_date, errors="coerce").date().isoformat()
                        if target_date
                        else None
                    )
                    if (
                        target_date_str
                        and self.params.minute_fetch_skip_non_trading_day
                        and not self.repo._is_trading_day(target_date_str, latest_trade_date)
                    ):
                        low_suck_map[code] = {
                            "strength": "NONE",
                            "reason": "non_trading_day",
                            "score": 0,
                        }
                        continue

                    minute_df = self.market_data.fetch_minute_data(
                        code,
                        trade_date=target_date_str,
                        timeout_sec=self.params.minute_fetch_timeout_sec,
                    )
                    ref_close = ref_close_map.get(code)
                    ls_res = detect_low_suck_signal(minute_df, ref_close_yesterday=ref_close)
                    if ls_res.get("strength") != "NONE":
                        self.logger.info(f"[{code}] 低吸评估: {ls_res['strength']} (分={ls_res['score']}) - {ls_res['reason']}")
                    low_suck_map[code] = ls_res

                    if minute_df is not None and not minute_df.empty:
                        df_min = minute_df.copy()
                        if "time" in df_min.columns and "minute_time" not in df_min.columns:
                            df_min = df_min.rename(columns={"time": "minute_time"})
                        df_min["monitor_date"] = monitor_date
                        df_min["run_pk"] = run_pk
                        df_min["strategy_code"] = strategy_code_map.get(
                            code
                        ) or self.params.strategy_code
                        df_min["sig_date"] = sig_date_map.get(code)
                        df_min["code"] = code
                        if "minute_time" in df_min.columns:
                            df_min["minute_date"] = pd.to_datetime(
                                df_min["minute_time"], errors="coerce"
                            ).dt.date
                        if target_date_str and target_date_str != dt.date.today().isoformat():
                            df_min["source"] = "akshare_em_hist"
                        else:
                            df_min["source"] = "akshare_em"
                        minute_frames.append(df_min)
                except Exception as e:
                    self.logger.warning(f"低吸检测异常 {code}: {e}")

        env_instruction = {
            "gate_status": env_context.get("env_final_gate_action"),
            "position_cap": env_context.get("env_final_cap_pct"),
            "reason": env_context.get("env_final_reason_json"),
        }
        if minute_frames:
            minute_df = pd.concat(minute_frames, ignore_index=True)
            self.repo.persist_minute_snapshots(minute_df)
        reason_matrix = None
        if env_instruction.get("reason"):
            try:
                reason_payload = json.loads(env_instruction["reason"])
                if isinstance(reason_payload, dict):
                    reason_matrix = reason_payload.get("risk_emotion_matrix")
            except Exception:
                reason_matrix = None
        env_payload = {
            "env_final_gate_action": env_instruction.get("gate_status"),
            "env_final_cap_pct": env_instruction.get("position_cap"),
            "env_final_reason_json": env_instruction.get("reason"),
            "env_weekly_gate_action": env_context.get("weekly_gate_action")
            or env_context.get("weekly_gate_policy"),
            "weekly_risk_level": env_context.get("weekly_risk_level"),
            "weekly_scene_code": env_context.get("weekly_scene_code"),
            "index_score": env_context.get("index_score"),
            "regime": env_context.get("regime"),
            "regime_raw": env_context.get("regime_raw"),
            "position_hint": env_context.get("position_hint"),
        }
        env_final_gate_action = env_instruction.get("gate_status")
        self.logger.info(
            "已构建环境快照（monitor_date=%s, run_id=%s, gate=%s, cap=%s, regime=%s(raw=%s), weekly_risk=%s, matrix=%s）。",
            monitor_date,
            run_id,
            env_final_gate_action,
            env_instruction.get("position_cap"),
            env_context.get("regime"),
            env_context.get("regime_raw"),
            env_context.get("weekly_risk_level"),
            reason_matrix,
        )
        prev_strength_map = self.repo.load_previous_strength_by_monitor_date(
            codes, monitor_date
        )
        result = self.evaluator.evaluate(
            signals,
            quotes,
            env_payload,
            checked_at=checked_at,
            monitor_date=monitor_date,
            run_id=run_id,
            run_pk=run_pk,
            ready_signals_used=self.repo.ready_signals_used,
            previous_strength_map=prev_strength_map,
            low_suck_map=low_suck_map,  # Pass the map here
        )

        if result.empty:
            return
        ranked_df, rank_meta = self.evaluator.build_rank_frame(result, env_context)
        if rank_meta:
            self.logger.info(
                "展示排序权重(不影响 gate/action/入库)：market_weight=%.2f（%s） board_weight=%s stock_quality=%s",
                float(rank_meta.get("market_weight") or 0.0),
                str(rank_meta.get("market_note") or "-")[:120],
                str(rank_meta.get("board_weight_map") or "-")[:120],
                str(rank_meta.get("stock_quality_source") or "-")[:120],
            )

        summary = result["action"].value_counts(dropna=False).to_dict()
        self.logger.info("开盘监测结果统计：%s", summary)

        exec_df = ranked_df[ranked_df["action"] == "EXECUTE"].copy()
        gap_col = "live_gap_pct" if "live_gap_pct" in exec_df.columns else "gap_pct"
        exec_df[gap_col] = exec_df[gap_col].apply(_to_float)
        if "final_rank_score" in exec_df.columns:
            exec_df["final_rank_score"] = pd.to_numeric(exec_df["final_rank_score"], errors="coerce")
            exec_df = exec_df.sort_values(
                by=["final_rank_score", gap_col],
                ascending=[False, True],
            )
        else:
            exec_df = exec_df.sort_values(by=gap_col, ascending=True)
        top_n = min(30, len(exec_df))
        if top_n > 0:
            preview_cols = [
                "code",
                "name",
                "sig_close",
                "live_open",
                "live_latest",
                gap_col,
                "board_status",
                "signal_strength",
                "final_rank_score",
                "action_reason",
            ]
            preview_cols = [c for c in preview_cols if c in exec_df.columns]
            preview = exec_df[preview_cols].head(top_n)
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
                "可执行清单 Top%s（按 final_rank_score 优先，其次 gap）：\n%s",
                top_n,
                preview_disp.to_string(index=False),
            )

        wait_df = ranked_df[ranked_df["action"] == "WAIT"].copy()
        wait_df[gap_col] = wait_df[gap_col].apply(_to_float)
        if "final_rank_score" in wait_df.columns:
            wait_df["final_rank_score"] = pd.to_numeric(wait_df["final_rank_score"], errors="coerce")
            wait_df = wait_df.sort_values(
                by=["final_rank_score", gap_col],
                ascending=[False, True],
            )
        else:
            wait_df = wait_df.sort_values(by=gap_col, ascending=True)
        wait_top = min(10, len(wait_df))
        if wait_top > 0:
            wait_cols = [
                "code",
                "name",
                "sig_close",
                "live_open",
                "live_latest",
                gap_col,
                "board_status",
                "signal_strength",
                "final_rank_score",
                "status_reason",
            ]
            wait_cols = [c for c in wait_cols if c in wait_df.columns]
            wait_preview = wait_df[wait_cols].head(wait_top)
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
                "WAIT 观察清单 Top%s（按 final_rank_score 优先，其次 gap）：\n%s",
                wait_top,
                wait_preview_disp.to_string(index=False),
            )

        self.repo.persist_results(result)
        # 导出功能已停用，数据已保存在数据库中

