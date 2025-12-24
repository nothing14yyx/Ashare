from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable

# 注意：open_monitor_rules 是“唯一规则引擎真源”。
# open_monitor.py 仅做 orchestrator：拼装 ctx / 拉行情 / 落库 / 导出。


ACTION_PRIORITY = ["STOP", "SKIP", "REDUCE_50%", "WAIT", "EXECUTE", "UNKNOWN"]


def _action_rank(val: str | None) -> int:
    if val is None:
        return len(ACTION_PRIORITY)
    val_norm = str(val).strip().upper()
    try:
        return ACTION_PRIORITY.index(val_norm)
    except ValueError:
        return len(ACTION_PRIORITY)


@dataclass(frozen=True)
class MarketEnvironment:
    """开盘监测所需的核心环境信息。"""

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

    @classmethod
    def from_snapshot(cls, snapshot: dict[str, Any] | None) -> "MarketEnvironment":
        """从环境快照 dict 解析出强类型 MarketEnvironment。

        约定快照 key：
        - env_final_gate_action / env_final_cap_pct / env_final_reason_json
        - env_index_snapshot_hash / regime / position_hint
        - weekly_asof_trade_date / weekly_risk_level / weekly_scene_code

        如果 snapshot 非 dict 或字段缺失，则返回尽可能完整的对象，缺失字段保持 None。
        """

        if not isinstance(snapshot, dict):
            return cls()

        def _to_float(val: Any) -> float | None:
            if val is None:
                return None
            if isinstance(val, bool):
                return float(val)
            if isinstance(val, (int, float)):
                fv = float(val)
                if fv != fv:  # NaN
                    return None
                return fv
            try:
                s = str(val).strip()
                if not s:
                    return None
                fv = float(s)
                if fv != fv:
                    return None
                return fv
            except Exception:
                return None

        gate_action = snapshot.get("env_final_gate_action")
        if gate_action is None:
            gate_action = snapshot.get("gate_action")
        if gate_action is not None:
            gate_action = str(gate_action).strip().upper() or None

        position_cap = snapshot.get("env_final_cap_pct")
        if position_cap is None:
            position_cap = snapshot.get("position_cap_pct")

        weekly_scene = snapshot.get("weekly_scene_code")
        if weekly_scene is None:
            weekly_scene = snapshot.get("weekly_scene")

        return cls(
            gate_action=gate_action,
            position_cap_pct=_to_float(position_cap),
            reason_json=snapshot.get("env_final_reason_json", snapshot.get("reason_json")),
            index_snapshot_hash=snapshot.get("env_index_snapshot_hash", snapshot.get("index_snapshot_hash")),
            regime=snapshot.get("regime"),
            position_hint=_to_float(snapshot.get("position_hint")),
            weekly_asof_trade_date=snapshot.get("weekly_asof_trade_date"),
            weekly_risk_level=snapshot.get("weekly_risk_level"),
            weekly_scene=weekly_scene,
            raw=snapshot,
        )


@dataclass(frozen=True)
class RuleHit:
    name: str
    category: str  # STRUCTURE / ACTION / ENV_OVERLAY
    severity: int
    reason: str
    action_override: str | None = None
    state_override: str | None = None
    cap_override: float | None = None

    def to_dict(self) -> dict[str, Any]:
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


@dataclass
class DecisionContext:
    """每一行信号的决策上下文。"""

    # --- decision outputs ---
    state: str = "OK"
    state_reason: str = "结构/时效通过"
    state_severity: int = 0

    action: str = "EXECUTE"
    action_reason: str = "OK"
    action_rank: int = _action_rank("EXECUTE")

    entry_exposure_cap: float | None = None

    # env_overlay 输出（独立于 env.gate_action；env.gate_action 由 ACTION 类规则处理）
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


    signal_age: int | None = None
    valid_days: int | None = None
    rule_hits: list[RuleHit] = field(default_factory=list)

    def record_hit(self, rule: Rule, result: RuleResult) -> None:
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
        if result.env_gate_action:
            self.env_gate_action = merge_gate_actions(self.env_gate_action, result.env_gate_action)
            gate_note = result.reason or ""
            if self.env_gate_reason and gate_note:
                self.env_gate_reason = f"{self.env_gate_reason}; {gate_note}"
            elif gate_note:
                self.env_gate_reason = gate_note

        if result.env_position_cap is not None:
            if self.env_position_cap is None:
                self.env_position_cap = result.env_position_cap
            else:
                self.env_position_cap = min(self.env_position_cap, result.env_position_cap)

        self.record_hit(rule, result)

    def finalize_env_overlay(self, merge_gate_actions: Callable[..., str | None]) -> None:
        if self.env_position_cap is not None:
            if self.entry_exposure_cap is None:
                self.entry_exposure_cap = self.env_position_cap
            else:
                self.entry_exposure_cap = min(self.entry_exposure_cap, self.env_position_cap)

        final_gate = merge_gate_actions(self.env_gate_action)
        if final_gate is not None:
            self.env_gate_action = final_gate

        if final_gate in {"STOP", "WAIT"} and _action_rank(self.action) > _action_rank("WAIT"):
            self.action = "WAIT"
            self.action_rank = _action_rank("WAIT")
            if self.action_reason in {"", None, "OK"} and self.env_gate_reason:
                self.action_reason = self.env_gate_reason

        if not self.rule_hits:
            self.rule_hits.append(
                RuleHit(
                    name="OK",
                    category="FINAL",
                    severity=0,
                    reason="OK",
                )
            )

    def export_result(self) -> "DecisionResult":
        state = self.state
        if self.action == "SKIP":
            state = "INVALID"
        elif self.action == "WAIT":
            state = "PENDING"
        elif self.action == "UNKNOWN":
            state = "UNKNOWN"

        rule_hits_json = json.dumps(
            [hit.to_dict() for hit in self.rule_hits],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        status_reason = self.action_reason
        summary_line = f"{state} {self.action} | {self.action_reason}"

        return DecisionResult(
            state=state,
            action=self.action,
            action_reason=self.action_reason,
            status_reason=status_reason,
            entry_exposure_cap=self.entry_exposure_cap,
            env_gate_action=self.env_gate_action,
            rule_hits_json=rule_hits_json,
            summary_line=summary_line,
        )


@dataclass(frozen=True)
class DecisionResult:
    state: str
    action: str
    action_reason: str
    status_reason: str
    entry_exposure_cap: float | None
    env_gate_action: str | None
    rule_hits_json: str
    summary_line: str


class RuleEngine:
    """开盘监测规则引擎（唯一实现）。"""

    def __init__(self, merge_gate_actions: Callable[..., str | None]) -> None:
        self.merge_gate_actions = merge_gate_actions

    def apply(self, ctx: DecisionContext, rules: list[Rule]) -> None:
        by_category: dict[str, list[Rule]] = {
            "STRUCTURE": [],
            "ACTION": [],
            "ENV_OVERLAY": [],
        }
        for rule in rules:
            bucket = by_category.get(rule.category)
            if bucket is not None:
                bucket.append(rule)

        for category in ("STRUCTURE", "ACTION", "ENV_OVERLAY"):
            for rule in sorted(by_category[category], key=lambda r: r.severity, reverse=True):
                if rule.predicate(ctx):
                    result = rule.effect(ctx)
                    if category == "STRUCTURE":
                        ctx.apply_state(rule, result)
                    elif category == "ACTION":
                        ctx.apply_action(rule, result)
                    else:
                        ctx.apply_env_overlay(rule, result, self.merge_gate_actions)

        ctx.finalize_env_overlay(self.merge_gate_actions)
