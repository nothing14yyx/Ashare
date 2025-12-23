from __future__ import annotations

"""open_monitor 的规则/决策引擎。

说明：
- 将 Rule/DecisionContext/RuleEngine 等纯逻辑从 open_monitor.py 拆分出来，避免巨石文件继续膨胀；
- 规则集合（build_default_monitor_rules）仍由 monitor_rules.py 负责（通过类型注入避免循环依赖）。
"""

import json
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class MarketEnvironment:
    """开盘监测所需的核心环境信息。

    说明：
    - gate_action/position_cap_pct/reason_json 是决策层的主要输入；
    - raw 保留原始 env_context，便于落库/排查，但规则与决策应优先使用结构化字段。
    """

    gate_action: str | None = None  # STOP / WAIT / ALLOW
    position_cap_pct: float | None = None
    reason_json: Any | None = None
    index_snapshot_hash: str | None = None
    regime: str | None = None
    position_hint: float | None = None
    weekly_asof_trade_date: str | None = None
    weekly_risk_level: str | None = None
    weekly_scene: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuleHit:
    name: str
    category: str  # STRUCTURE / EXECUTION / ENV
    severity: int
    reason: str
    action_override: str | None = None
    state_override: str | None = None
    cap_override: float | None = None

    def to_dict(self) -> dict[str, Any]:  # noqa: ANN401
        payload: dict[str, Any] = {
            "name": self.name,
            "category": self.category,
            "severity": self.severity,
            "reason": self.reason,
        }
        if self.action_override is not None:
            payload["action_override"] = self.action_override
        if self.state_override is not None:
            payload["state_override"] = self.state_override
        if self.cap_override is not None:
            payload["cap_override"] = self.cap_override
        return payload


@dataclass(frozen=True)
class Rule:
    id: str
    category: str  # STRUCTURE / ACTION / ENV_OVERLAY
    severity: int
    predicate: Callable[["DecisionContext"], bool]
    effect: Callable[["DecisionContext"], "RuleResult"]


@dataclass(frozen=True)
class RuleResult:
    reason: str
    action_override: str | None = None
    state_override: str | None = None
    cap_override: float | None = None
    env_gate_action: str | None = None
    env_position_cap: float | None = None


ACTION_PRIORITY = ["STOP", "SKIP", "REDUCE_50%", "WAIT", "EXECUTE", "UNKNOWN"]


def _action_rank(val: str | None) -> int:
    if val is None:
        return len(ACTION_PRIORITY)
    val_norm = str(val).strip().upper()
    try:
        return ACTION_PRIORITY.index(val_norm)
    except ValueError:
        return len(ACTION_PRIORITY)


@dataclass
class DecisionContext:
    state: str = "OK"
    state_reason: str = "结构/时效通过"
    state_severity: int = 0
    action: str = "EXECUTE"
    action_reason: str = "OK"
    action_rank: int = _action_rank("EXECUTE")
    entry_exposure_cap: float | None = None
    env_gate_action: str | None = None
    env_gate_reason: str | None = None
    env_position_cap: float | None = None

    # --- inputs for RuleEngine predicates (optional) ---
    env: MarketEnvironment | None = None
    chip_score: float | None = None
    price_now: float | None = None
    live_gap: float | None = None
    live_pct: float | None = None
    threshold_gap_up: float | None = None
    max_gap_down: float | None = None
    sig_ma20: float | None = None
    ma20_thresh: float | None = None
    limit_up_trigger: float | None = None
    runup_breach: bool = False
    runup_breach_reason: str | None = None

    rule_hits: list[RuleHit] = field(default_factory=list)

    def record_hit(
        self,
        rule: Rule,
        result: RuleResult,
    ) -> None:
        cap_override = result.cap_override
        if result.env_position_cap is not None:
            cap_override = (
                result.env_position_cap
                if cap_override is None
                else min(cap_override, result.env_position_cap)
            )
        self.rule_hits.append(
            RuleHit(
                name=rule.id,
                category=rule.category,
                severity=rule.severity,
                reason=result.reason,
                action_override=result.action_override,
                state_override=result.state_override,
                cap_override=cap_override,
            )
        )

    def apply_state(self, rule: Rule, result: RuleResult) -> None:
        target_state = result.state_override or rule.id
        if rule.severity >= self.state_severity:
            self.state = target_state
            self.state_reason = result.reason
            self.state_severity = rule.severity
        self.record_hit(rule, result)

    def apply_action(self, rule: Rule, result: RuleResult) -> None:
        if result.cap_override is not None:
            if self.entry_exposure_cap is None:
                self.entry_exposure_cap = result.cap_override
            else:
                self.entry_exposure_cap = min(self.entry_exposure_cap, result.cap_override)
        target_action = result.action_override or self.action
        target_rank = _action_rank(target_action)
        if target_rank < self.action_rank:
            self.action = target_action
            self.action_rank = target_rank
            self.action_reason = result.reason
        elif target_rank == self.action_rank and self.action_reason in {"", None, "OK"}:
            self.action_reason = result.reason
        self.record_hit(rule, result)

    def apply_env_overlay(
        self,
        rule: Rule,
        result: RuleResult,
        merge_gate_actions: Callable[..., str | None],
    ) -> None:
        if result.env_gate_action is None and result.env_position_cap is None:
            self.record_hit(rule, result)
            return
        next_gate = merge_gate_actions(
            self.env_gate_action,
            result.env_gate_action,
            prev_reason=self.env_gate_reason,
            next_reason=result.reason,
        )
        if next_gate != self.env_gate_action:
            self.env_gate_action = next_gate
            self.env_gate_reason = result.reason
        if result.env_position_cap is not None:
            if self.env_position_cap is None:
                self.env_position_cap = result.env_position_cap
            else:
                self.env_position_cap = min(self.env_position_cap, result.env_position_cap)
        self.record_hit(rule, result)

    def export_action_reason(self) -> str:
        if self.action_reason and self.action_reason != "OK":
            return str(self.action_reason)
        if self.env_gate_reason:
            return str(self.env_gate_reason)
        if self.state_reason and self.state_reason != "结构/时效通过":
            return str(self.state_reason)
        return "OK"

    def export_state_reason(self) -> str:
        if self.state_reason:
            return str(self.state_reason)
        return "结构/时效通过"

    def export_rule_hits_json(self) -> str | None:
        if not self.rule_hits:
            return None
        payload = [hit.to_dict() for hit in self.rule_hits]
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    def finalize_env_overlay(
        self,
        merge_gate_actions: Callable[..., str | None],
        *,
        derive_gate_action: Callable[[str | None, float | None], str | None] | None = None,
    ) -> None:
        if self.env is None:
            return
        gate_from_env = self.env.gate_action
        derived_gate = None
        if derive_gate_action is not None:
            derived_gate = derive_gate_action(self.env.regime, self.env.position_hint)
        if derived_gate and derived_gate != gate_from_env:
            gate_from_env = merge_gate_actions(
                gate_from_env,
                derived_gate,
                prev_reason="env",
                next_reason="derived",
            )
        if gate_from_env:
            self.env_gate_action = merge_gate_actions(
                self.env_gate_action,
                gate_from_env,
                prev_reason=self.env_gate_reason,
                next_reason="env",
            )
        if self.env.position_cap_pct is not None:
            cap = float(self.env.position_cap_pct)
            if self.env_position_cap is None:
                self.env_position_cap = cap
            else:
                self.env_position_cap = min(self.env_position_cap, cap)

    def export_env_gate_action(self) -> str | None:
        return self.env_gate_action

    def export_env_cap_pct(self) -> float | None:
        return self.env_position_cap


@dataclass(frozen=True)
class DecisionResult:
    state: str
    state_reason: str
    action: str
    action_reason: str
    entry_exposure_cap: float | None
    env_gate_action: str | None
    env_position_cap: float | None
    rule_hits_json: str | None = None


class RuleEngine:
    def __init__(self, rules: list[Rule]) -> None:
        self.rules = list(rules or [])

    def evaluate(
        self,
        ctx: DecisionContext,
        *,
        merge_gate_actions: Callable[..., str | None],
        derive_gate_action: Callable[[str | None, float | None], str | None] | None = None,
    ) -> DecisionResult:
        if not self.rules:
            ctx.finalize_env_overlay(
                merge_gate_actions,
                derive_gate_action=derive_gate_action,
            )
            return DecisionResult(
                state=ctx.state,
                state_reason=ctx.export_state_reason(),
                action=ctx.action,
                action_reason=ctx.export_action_reason(),
                entry_exposure_cap=ctx.entry_exposure_cap,
                env_gate_action=ctx.export_env_gate_action(),
                env_position_cap=ctx.export_env_cap_pct(),
                rule_hits_json=ctx.export_rule_hits_json(),
            )

        # 1) STRUCTURE gates
        for rule in self.rules:
            if rule.category != "STRUCTURE":
                continue
            if not rule.predicate(ctx):
                continue
            result = rule.effect(ctx)
            ctx.apply_state(rule, result)

        # 2) ACTION rules
        for rule in self.rules:
            if rule.category != "ACTION":
