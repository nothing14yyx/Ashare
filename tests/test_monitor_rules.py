from ashare.monitor.monitor_rules import MonitorRuleConfig, build_default_monitor_rules
from ashare.monitor.open_monitor_eval import merge_gate_actions
from ashare.monitor.open_monitor_rules import DecisionContext, RuleEngine, Rule, RuleResult


def test_monitor_rule_config_normalizes_percentages():
    cfg = MonitorRuleConfig.from_config(
        {
            "max_gap_up_pct": 5,
            "limit_up_trigger_pct": 0.1,
            "enable_gap_up": "false",
        }
    )
    assert abs(cfg.max_gap_up_pct - 0.05) < 1e-6
    assert abs(cfg.limit_up_trigger_pct - 10.0) < 1e-6
    assert cfg.enable_gap_up is False


def test_build_default_rules_quote_missing():
    cfg = MonitorRuleConfig()
    rules = build_default_monitor_rules(cfg, Rule=Rule, RuleResult=RuleResult)
    ctx = DecisionContext(price_now=None)
    engine = RuleEngine(merge_gate_actions)
    engine.apply(ctx, rules)
    result = ctx.export_result()
    assert result.action == cfg.quote_missing_action
    assert cfg.quote_missing_reason in result.action_reason
