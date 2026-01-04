import json

from ashare.monitor.open_monitor_eval import merge_gate_actions
from ashare.monitor.open_monitor_rules import (
    DecisionContext,
    MarketEnvironment,
    Rule,
    RuleEngine,
    RuleResult,
    _action_rank,
)


def test_action_rank_priority():
    assert _action_rank("STOP") < _action_rank("WAIT")
    assert _action_rank("SKIP") < _action_rank("EXECUTE")
    assert _action_rank("UNKNOWN") > _action_rank("EXECUTE")


def test_market_environment_from_snapshot():
    snapshot = {
        "env_final_gate_action": " wait ",
        "env_final_cap_pct": "0.5",
        "env_final_reason_json": {"k": "v"},
        "env_index_snapshot_hash": "abc",
        "index_score": "3.2",
        "regime": "RISK_ON",
        "position_hint": "0.7",
        "weekly_asof_trade_date": "2025-01-03",
        "weekly_risk_level": "LOW",
        "weekly_scene_code": "SCENE_A",
    }
    env = MarketEnvironment.from_snapshot(snapshot)
    assert env.gate_action == "WAIT"
    assert env.position_cap_pct == 0.5
    assert env.index_snapshot_hash == "abc"
    assert env.score == 3.2
    assert env.position_hint == 0.7
    assert env.weekly_scene == "SCENE_A"


def test_rule_engine_action_override():
    rules = [
        Rule(
            id="QUOTE_MISSING",
            category="ACTION",
            severity=10,
            predicate=lambda ctx: True,
            effect=lambda ctx: RuleResult(reason="no quote", action_override="SKIP"),
        )
    ]
    ctx = DecisionContext(entry_exposure_cap=1.0)
    engine = RuleEngine(merge_gate_actions)
    engine.apply(ctx, rules)
    result = ctx.export_result()
    assert result.action == "SKIP"
    assert result.state == "INVALID"
    assert result.entry_exposure_cap == 0.0
    hits = json.loads(result.rule_hits_json)
    assert hits[0]["name"] == "QUOTE_MISSING"


def test_env_overlay_wait_promotes_action():
    rules = [
        Rule(
            id="ENV_WAIT",
            category="ENV_OVERLAY",
            severity=5,
            predicate=lambda ctx: True,
            effect=lambda ctx: RuleResult(reason="env", env_gate_action="WAIT"),
        )
    ]
    ctx = DecisionContext(entry_exposure_cap=0.8)
    engine = RuleEngine(merge_gate_actions)
    engine.apply(ctx, rules)
    result = ctx.export_result()
    assert result.action == "WAIT"
    assert result.state == "PENDING"
