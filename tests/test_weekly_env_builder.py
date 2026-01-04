from ashare.indicators.weekly_env_builder import WeeklyEnvironmentBuilder


def test_merge_gate_actions():
    assert WeeklyEnvironmentBuilder._merge_gate_actions("ALLOW", "WAIT") == "WAIT"
    assert WeeklyEnvironmentBuilder._merge_gate_actions("go") == "ALLOW"


def test_resolve_env_weekly_gate_policy():
    env_context = {
        "weekly_gating_enabled": True,
        "weekly_risk_level": "MEDIUM",
        "weekly_structure_status": "FORMING",
    }
    policy = WeeklyEnvironmentBuilder.resolve_env_weekly_gate_policy(env_context)
    assert policy == "ALLOW_SMALL"


def test_derive_gate_action():
    assert WeeklyEnvironmentBuilder._derive_gate_action("BREAKDOWN", 0.5) == "STOP"
    assert WeeklyEnvironmentBuilder._derive_gate_action("RISK_OFF", 0.5) == "WAIT"
    assert WeeklyEnvironmentBuilder._derive_gate_action("RISK_ON", 0.2) == "ALLOW"


def test_resolve_weekly_zone():
    scenario = {
        "weekly_risk_level": "HIGH",
        "weekly_plan_a_exposure_cap": 0.2,
        "weekly_direction_confirmed": False,
    }
    zone = WeeklyEnvironmentBuilder._resolve_weekly_zone(scenario, "WAIT", "")
    assert zone["weekly_zone_id"] == "WZ0_RISK_OFF"
    assert zone["weekly_zone_score"] == 10
