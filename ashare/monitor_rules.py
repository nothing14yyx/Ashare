from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, List, Mapping

from .utils.convert import to_float as _to_float


@dataclass(frozen=True)
class MonitorRuleConfig:
    """开盘监测规则参数集中管理。

    说明：
    - 所有阈值集中到此处，open_monitor 只负责拼装上下文并执行规则。
    - 运行时动态值（例如 threshold_gap_up / ma20_thresh）由 DecisionContext 计算并携带。
    """

    # --- thresholds ---
    max_gap_up_pct: float = 0.05
    max_gap_up_atr_mult: float = 1.5
    max_gap_down_pct: float = -0.03
    min_open_vs_ma20_pct: float = 0.0
    pullback_min_open_vs_ma20_pct: float = -0.01
    below_ma20_tol_pct: float = 0.002
    limit_up_trigger_pct: float = 9.7
    max_entry_vs_ma5_pct: float = 0.08
    runup_atr_max: float = 1.2
    runup_atr_tol: float = 0.02
    pullback_runup_atr_max: float = 1.5
    pullback_runup_dev_ma20_atr_min: float = 1.0
    dev_ma5_atr_max: float = 2.0
    dev_ma20_atr_max: float = 2.5
    stop_atr_mult: float = 2.0
    signal_day_limit_up_pct: float = 0.095
    env_index_score_threshold: float = 2.0
    weekly_soft_gate_strength_threshold: float = 3.5

    # --- switches ---
    enable_env_gate: bool = True
    enable_chip_score: bool = True
    enable_quote_missing: bool = True
    enable_gap_up: bool = True
    enable_gap_down: bool = True
    enable_below_ma20: bool = True
    enable_limit_up: bool = True
    enable_runup_breach: bool = True

    enable_signal_expired: bool = True

    # --- thresholds ---
    chip_score_wait_threshold: float = -0.5

    chip_score_allow_small_cap: float = 0.2

    # --- actions & reasons ---
    env_stop_action: str = "SKIP"
    env_stop_reason: str = "环境阻断"
    env_wait_action: str = "WAIT"
    env_wait_reason: str = "环境等待"
    chip_score_reason: str = "筹码评分<=-0.5"
    quote_missing_action: str = "SKIP"
    quote_missing_reason: str = "行情数据不可用"
    gap_up_action: str = "SKIP"
    gap_up_reason: str = "高开过阈值"
    gap_down_action: str = "SKIP"
    gap_down_reason: str = "低开破位"
    below_ma20_action: str = "SKIP"
    below_ma20_reason: str = "未站上MA20要求"
    limit_up_action: str = "SKIP"
    limit_up_reason: str = "涨停不可成交"
    runup_breach_action: str = "WAIT"
    runup_breach_fallback_reason: str = "拉升过快"


    signal_expired_action: str = "SKIP"
    signal_expired_reason: str = "信号已过期"

    # --- severity ---
    sev_env_stop: int = 100
    sev_env_wait: int = 90
    sev_signal_expired: int = 85
    sev_chip_score: int = 80
    sev_quote_missing: int = 75
    sev_gap_up: int = 70
    sev_gap_down: int = 70
    sev_below_ma20: int = 60
    sev_limit_up: int = 60
    sev_runup_breach: int = 55

    @classmethod
    def from_config(
        cls,
        cfg: Mapping[str, Any] | None,
        *,
        logger: logging.Logger | None = None,
    ) -> "MonitorRuleConfig":
        cfg = cfg or {}
        defaults = cls()

        def _get_bool(key: str, default: bool) -> bool:
            raw = cfg.get(key, default)
            if isinstance(raw, bool):
                return raw
            if isinstance(raw, str):
                return raw.strip().lower() in {"1", "true", "yes", "y", "on"}
            return bool(raw)

        def _get_float(key: str, default: float) -> float:
            parsed = _to_float(cfg.get(key, default))
            return default if parsed is None else float(parsed)

        def _normalize_ratio_pct(value: float, key: str) -> float:
            normalized = value / 100.0 if abs(value) > 1.5 else value
            if logger and abs(value) > 1.5:
                logger.info(
                    "配置 %s 以百分数填写（%s），已按比例 %.4f 处理。",
                    key,
                    value,
                    normalized,
                )
            return normalized

        def _normalize_percent_value(value: float, key: str) -> float:
            normalized = value * 100.0 if abs(value) <= 1.5 else value
            if logger and abs(value) <= 1.5:
                logger.info(
                    "配置 %s 以小数比例填写（%s），已按百分数 %.2f%% 处理。",
                    key,
                    value,
                    normalized,
                )
            return normalized

        return cls(
            max_gap_up_pct=_normalize_ratio_pct(
                _get_float("max_gap_up_pct", defaults.max_gap_up_pct),
                "max_gap_up_pct",
            ),
            max_gap_up_atr_mult=_get_float("max_gap_up_atr_mult", defaults.max_gap_up_atr_mult),
            max_gap_down_pct=_normalize_ratio_pct(
                _get_float("max_gap_down_pct", defaults.max_gap_down_pct),
                "max_gap_down_pct",
            ),
            min_open_vs_ma20_pct=_normalize_ratio_pct(
                _get_float("min_open_vs_ma20_pct", defaults.min_open_vs_ma20_pct),
                "min_open_vs_ma20_pct",
            ),
            pullback_min_open_vs_ma20_pct=_normalize_ratio_pct(
                _get_float("pullback_min_open_vs_ma20_pct", defaults.pullback_min_open_vs_ma20_pct),
                "pullback_min_open_vs_ma20_pct",
            ),
            below_ma20_tol_pct=_normalize_ratio_pct(
                _get_float("below_ma20_tol_pct", defaults.below_ma20_tol_pct),
                "below_ma20_tol_pct",
            ),
            limit_up_trigger_pct=_normalize_percent_value(
                _get_float("limit_up_trigger_pct", defaults.limit_up_trigger_pct),
                "limit_up_trigger_pct",
            ),
            max_entry_vs_ma5_pct=_normalize_ratio_pct(
                _get_float("max_entry_vs_ma5_pct", defaults.max_entry_vs_ma5_pct),
                "max_entry_vs_ma5_pct",
            ),
            runup_atr_max=_get_float("runup_atr_max", defaults.runup_atr_max),
            runup_atr_tol=_get_float("runup_atr_tol", defaults.runup_atr_tol),
            pullback_runup_atr_max=_get_float(
                "pullback_runup_atr_max", defaults.pullback_runup_atr_max
            ),
            pullback_runup_dev_ma20_atr_min=_get_float(
                "pullback_runup_dev_ma20_atr_min", defaults.pullback_runup_dev_ma20_atr_min
            ),
            dev_ma5_atr_max=_get_float("dev_ma5_atr_max", defaults.dev_ma5_atr_max),
            dev_ma20_atr_max=_get_float("dev_ma20_atr_max", defaults.dev_ma20_atr_max),
            stop_atr_mult=_get_float("stop_atr_mult", defaults.stop_atr_mult),
            signal_day_limit_up_pct=_normalize_ratio_pct(
                _get_float("signal_day_limit_up_pct", defaults.signal_day_limit_up_pct),
                "signal_day_limit_up_pct",
            ),
            env_index_score_threshold=_get_float(
                "env_index_score_threshold",
                defaults.env_index_score_threshold,
            ),
            weekly_soft_gate_strength_threshold=_get_float(
                "weekly_soft_gate_strength_threshold",
                defaults.weekly_soft_gate_strength_threshold,
            ),
            enable_env_gate=_get_bool("enable_env_gate", defaults.enable_env_gate),
            enable_chip_score=_get_bool("enable_chip_score", defaults.enable_chip_score),
            enable_quote_missing=_get_bool("enable_quote_missing", defaults.enable_quote_missing),
            enable_gap_up=_get_bool("enable_gap_up", defaults.enable_gap_up),
            enable_gap_down=_get_bool("enable_gap_down", defaults.enable_gap_down),
            enable_below_ma20=_get_bool("enable_below_ma20", defaults.enable_below_ma20),
            enable_limit_up=_get_bool("enable_limit_up", defaults.enable_limit_up),
            enable_runup_breach=_get_bool("enable_runup_breach", defaults.enable_runup_breach),
            enable_signal_expired=_get_bool(
                "enable_signal_expired", defaults.enable_signal_expired
            ),
            chip_score_wait_threshold=_get_float(
                "chip_score_wait_threshold", defaults.chip_score_wait_threshold
            ),
            chip_score_allow_small_cap=_get_float(
                "chip_score_allow_small_cap", defaults.chip_score_allow_small_cap
            ),
            env_stop_action=str(cfg.get("env_stop_action", defaults.env_stop_action)).strip()
            or defaults.env_stop_action,
            env_stop_reason=str(cfg.get("env_stop_reason", defaults.env_stop_reason)).strip()
            or defaults.env_stop_reason,
            env_wait_action=str(cfg.get("env_wait_action", defaults.env_wait_action)).strip()
            or defaults.env_wait_action,
            env_wait_reason=str(cfg.get("env_wait_reason", defaults.env_wait_reason)).strip()
            or defaults.env_wait_reason,
            chip_score_reason=str(cfg.get("chip_score_reason", defaults.chip_score_reason)).strip()
            or defaults.chip_score_reason,
            quote_missing_action=str(
                cfg.get("quote_missing_action", defaults.quote_missing_action)
            ).strip()
            or defaults.quote_missing_action,
            quote_missing_reason=str(
                cfg.get("quote_missing_reason", defaults.quote_missing_reason)
            ).strip()
            or defaults.quote_missing_reason,
            gap_up_action=str(cfg.get("gap_up_action", defaults.gap_up_action)).strip()
            or defaults.gap_up_action,
            gap_up_reason=str(cfg.get("gap_up_reason", defaults.gap_up_reason)).strip()
            or defaults.gap_up_reason,
            gap_down_action=str(cfg.get("gap_down_action", defaults.gap_down_action)).strip()
            or defaults.gap_down_action,
            gap_down_reason=str(cfg.get("gap_down_reason", defaults.gap_down_reason)).strip()
            or defaults.gap_down_reason,
            below_ma20_action=str(
                cfg.get("below_ma20_action", defaults.below_ma20_action)
            ).strip()
            or defaults.below_ma20_action,
            below_ma20_reason=str(
                cfg.get("below_ma20_reason", defaults.below_ma20_reason)
            ).strip()
            or defaults.below_ma20_reason,
            limit_up_action=str(cfg.get("limit_up_action", defaults.limit_up_action)).strip()
            or defaults.limit_up_action,
            limit_up_reason=str(cfg.get("limit_up_reason", defaults.limit_up_reason)).strip()
            or defaults.limit_up_reason,
            runup_breach_action=str(
                cfg.get("runup_breach_action", defaults.runup_breach_action)
            ).strip()
            or defaults.runup_breach_action,
            runup_breach_fallback_reason=str(
                cfg.get("runup_breach_fallback_reason", defaults.runup_breach_fallback_reason)
            ).strip()
            or defaults.runup_breach_fallback_reason,
            signal_expired_action=str(
                cfg.get("signal_expired_action", defaults.signal_expired_action)
            ).strip()
            or defaults.signal_expired_action,
            signal_expired_reason=str(
                cfg.get("signal_expired_reason", defaults.signal_expired_reason)
            ).strip()
            or defaults.signal_expired_reason,
            sev_env_stop=int(cfg.get("sev_env_stop", defaults.sev_env_stop)),
            sev_env_wait=int(cfg.get("sev_env_wait", defaults.sev_env_wait)),
            sev_signal_expired=int(cfg.get("sev_signal_expired", defaults.sev_signal_expired)),
            sev_chip_score=int(cfg.get("sev_chip_score", defaults.sev_chip_score)),
            sev_quote_missing=int(cfg.get("sev_quote_missing", defaults.sev_quote_missing)),
            sev_gap_up=int(cfg.get("sev_gap_up", defaults.sev_gap_up)),
            sev_gap_down=int(cfg.get("sev_gap_down", defaults.sev_gap_down)),
            sev_below_ma20=int(cfg.get("sev_below_ma20", defaults.sev_below_ma20)),
            sev_limit_up=int(cfg.get("sev_limit_up", defaults.sev_limit_up)),
            sev_runup_breach=int(cfg.get("sev_runup_breach", defaults.sev_runup_breach)),
        )


def build_default_monitor_rules(
    config: MonitorRuleConfig,
    *,
    Rule: Any,
    RuleResult: Any,
) -> List[Any]:
    """构建默认的开盘监测“硬门控规则”。

    说明：
    - 通过注入 Rule / RuleResult 类型，避免 monitor_rules 直接 import open_monitor 引发循环依赖。
    - predicate 依赖 DecisionContext 上的字段（由 open_monitor 在 per-row 构造 ctx 时填充）。
    """

    def _chip_effective(ctx: Any) -> bool:
        chip_score = getattr(ctx, "chip_score", None)
        if chip_score is None:
            return False
        chip_age = getattr(ctx, "chip_age_days", None)
        if chip_age is None:
            return False
        if chip_age > 45:
            return False
        if getattr(ctx, "chip_stale_hit", None):
            return False
        chip_reason = str(getattr(ctx, "chip_reason", "") or "").strip()
        if chip_reason.startswith("DATA_"):
            return False
        if "OUTLIER" in chip_reason.upper():
            return False
        return True

    return [
        Rule(
            id="ENV_STOP",
            category="ACTION",
            severity=config.sev_env_stop,
            predicate=lambda ctx: bool(
                config.enable_env_gate
                and getattr(ctx, "env", None)
                and getattr(getattr(ctx, "env"), "gate_action", None) == "STOP"
            ),
            effect=lambda ctx: RuleResult(
                reason=config.env_stop_reason,
                action_override=config.env_stop_action,
            ),
        ),
        Rule(
            id="ENV_WAIT",
            category="ACTION",
            severity=config.sev_env_wait,
            predicate=lambda ctx: bool(
                config.enable_env_gate
                and getattr(ctx, "env", None)
                and getattr(getattr(ctx, "env"), "gate_action", None) == "WAIT"
            ),
            effect=lambda ctx: RuleResult(
                reason=config.env_wait_reason,
                action_override=config.env_wait_action,
            ),
        ),
        Rule(
            id="CHIP_SCORE_NEG",
            category="ACTION",
            severity=config.sev_chip_score,
            predicate=lambda ctx: bool(
                config.enable_chip_score
                and getattr(ctx, "chip_score", None) is not None
                and _chip_effective(ctx)
                and getattr(ctx, "chip_score") < config.chip_score_wait_threshold
            ),
            effect=lambda ctx: (
                RuleResult(
                    reason=config.chip_score_reason,
                    cap_override=config.chip_score_allow_small_cap,
                )
            ),
        ),
        Rule(
            id="QUOTE_MISSING",
            category="ACTION",
            severity=config.sev_quote_missing,
            predicate=lambda ctx: bool(config.enable_quote_missing and getattr(ctx, "price_now", None) is None),
            effect=lambda ctx: RuleResult(
                reason=config.quote_missing_reason,
                action_override=config.quote_missing_action,
            ),
        ),
        Rule(
            id="GAP_UP_TOO_MUCH",
            category="ACTION",
            severity=config.sev_gap_up,
            predicate=lambda ctx: bool(
                config.enable_gap_up
                and getattr(ctx, "live_gap", None) is not None
                and getattr(ctx, "threshold_gap_up", None) is not None
                and getattr(ctx, "live_gap") > getattr(ctx, "threshold_gap_up")
            ),
            effect=lambda ctx: RuleResult(
                reason=config.gap_up_reason,
                action_override=config.gap_up_action,
            ),
        ),
        Rule(
            id="GAP_DOWN_BREAK",
            category="ACTION",
            severity=config.sev_gap_down,
            predicate=lambda ctx: bool(
                config.enable_gap_down
                and getattr(ctx, "live_gap", None) is not None
                and getattr(ctx, "max_gap_down", None) is not None
                and getattr(ctx, "live_gap") < getattr(ctx, "max_gap_down")
            ),
            effect=lambda ctx: RuleResult(
                reason=config.gap_down_reason,
                action_override=config.gap_down_action,
            ),
        ),
        Rule(
            id="BELOW_MA20_REQ",
            category="ACTION",
            severity=config.sev_below_ma20,
            predicate=lambda ctx: bool(
                config.enable_below_ma20
                and getattr(ctx, "price_now", None) is not None
                and getattr(ctx, "sig_ma20", None) is not None
                and getattr(ctx, "ma20_thresh", None) is not None
                and getattr(ctx, "price_now") < getattr(ctx, "sig_ma20") * (1 + getattr(ctx, "ma20_thresh") - config.below_ma20_tol_pct)
            ),
            effect=lambda ctx: RuleResult(
                reason=config.below_ma20_reason,
                action_override=config.below_ma20_action,
            ),
        ),
        Rule(
            id="LIMIT_UP",
            category="ACTION",
            severity=config.sev_limit_up,
            predicate=lambda ctx: bool(
                config.enable_limit_up
                and getattr(ctx, "live_pct", None) is not None
                and getattr(ctx, "limit_up_trigger", None) is not None
                and getattr(ctx, "live_pct") >= getattr(ctx, "limit_up_trigger")
            ),
            effect=lambda ctx: RuleResult(
                reason=config.limit_up_reason,
                action_override=config.limit_up_action,
            ),
        ),
        Rule(
            id="RUNUP_BREACH",
            category="ACTION",
            severity=config.sev_runup_breach,
            predicate=lambda ctx: bool(config.enable_runup_breach and bool(getattr(ctx, "runup_breach", False))),
            effect=lambda ctx: RuleResult(
                reason=getattr(ctx, "runup_breach_reason", None) or config.runup_breach_fallback_reason,
                action_override=config.runup_breach_action,
            ),
        ),
    ]
