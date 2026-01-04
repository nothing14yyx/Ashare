import json

import pandas as pd

from ashare.monitor.open_monitor_eval import (
    OpenMonitorEvaluator,
    compute_runup_metrics,
    evaluate_runup_breach,
    merge_gate_actions,
)


def test_merge_gate_actions_priority():
    assert merge_gate_actions("ALLOW", "WAIT") == "WAIT"
    assert merge_gate_actions("GO") == "ALLOW"
    assert merge_gate_actions("stop", "allow") == "STOP"


def test_compute_runup_metrics():
    metrics = compute_runup_metrics(10, asof_close=12, live_high=11, sig_atr14=1)
    assert metrics.runup_ref_price == 12
    assert metrics.runup_ref_source == "max(asof_close, live_high)"
    assert metrics.runup_from_sigclose == 2
    assert metrics.runup_from_sigclose_atr == 2


def test_evaluate_runup_breach():
    metrics = compute_runup_metrics(10, asof_close=12, live_high=12, sig_atr14=1)
    breached, reason = evaluate_runup_breach(metrics, runup_atr_max=1.0, runup_atr_tol=0.1)
    assert breached is True
    assert "runup_from_sigclose_atr" in (reason or "")


def test_extract_rule_hit_names():
    hits = [{"name": "limit_up"}, {"id": "runup_breach"}]
    raw = json.dumps(hits)
    names = OpenMonitorEvaluator._extract_rule_hit_names(raw)
    assert "LIMIT_UP" in names
    assert "RUNUP_BREACH" in names


def test_build_board_map_from_strength():
    df = pd.DataFrame(
        [
            {"board_name": "A", "board_code": "001", "rank": 1, "chg_pct": 2.0},
            {"board_name": "B", "board_code": "002", "rank": 10, "chg_pct": -1.0},
        ]
    )
    board_map = OpenMonitorEvaluator.build_board_map_from_strength(df)
    assert board_map["A"]["status"] == "strong"
    assert board_map["002"]["status"] in {"weak", "neutral"}
