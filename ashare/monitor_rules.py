from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List


@dataclass(frozen=True)
class MonitorRuleConfig:
    """开盘监测规则参数集中管理。

    说明：
    - 运行时阈值（例如 threshold_gap_up / ma20_thresh）由 DecisionContext 计算并携带；
      本配置主要收口“固定阈值/开关/动作/文案/严重级别”。
    - 通过集中参数，可以让 open_monitor._evaluate 更像“纯 pipeline runner”。
    """

    # --- switches ---
    enable_env_gate: bool = True
    enable_chip_score: bool = True
    enable_quote_missing: bool = True
    enable_gap_up: bool = True
    enable_gap_down: bool = True
    enable_below_ma20: bool = True
    enable_limit_up: bool = True
    enable_runup_breach: bool = True

    # --- thresholds ---
    chip_score_wait_threshold: float = 0.0

    # --- actions & reasons ---
    env_stop_action: str = "SKIP"
    env_stop_reason: str = "环境阻断"
    env_wait_action: str = "WAIT"
    env_wait_reason: str = "环境等待"
    chip_score_action: str = "WAIT"
    chip_score_reason: str = "筹码评分<0"
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

    # --- severity ---
    sev_env_stop: int = 100
    sev_env_wait: int = 90
    sev_chip_score: int = 80
    sev_quote_missing: int = 75
    sev_gap_up: int = 70
    sev_gap_down: int = 70
    sev_below_ma20: int = 60
    sev_limit_up: int = 60
    sev_runup_breach: int = 55


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
                and getattr(ctx, "chip_score") < config.chip_score_wait_threshold
            ),
            effect=lambda ctx: RuleResult(
                reason=config.chip_score_reason,
                action_override=config.chip_score_action,
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
                and getattr(ctx, "price_now") < getattr(ctx, "sig_ma20") * (1 + getattr(ctx, "ma20_thresh"))
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
