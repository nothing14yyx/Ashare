import pytest

from .open_monitor import RunupMetrics, compute_runup_metrics, evaluate_runup_breach


def test_runup_metrics_respects_max_source_and_consistency():
    metrics = compute_runup_metrics(
        sig_close=10.0,
        asof_close=11.0,
        live_high=10.5,
        sig_atr14=2.0,
    )
    assert metrics.runup_ref_source == "max(asof_close, live_high)"
    assert metrics.runup_ref_price == pytest.approx(11.0)
    assert metrics.runup_ref_price >= 10.5
    assert metrics.runup_ref_price >= 11.0
    assert metrics.runup_from_sigclose == pytest.approx(1.0)
    assert metrics.runup_from_sigclose_atr == pytest.approx(0.5)
    assert metrics.runup_from_sigclose == metrics.runup_ref_price - 10.0


def test_runup_metrics_asof_fallback_and_atr_ratio():
    metrics = compute_runup_metrics(
        sig_close=9.5,
        asof_close=9.8,
        live_high=None,
        sig_atr14=0.5,
    )
    assert metrics.runup_ref_source == "asof_close"
    assert metrics.runup_ref_price == pytest.approx(9.8)
    assert metrics.runup_from_sigclose == pytest.approx(0.3)
    assert metrics.runup_from_sigclose_atr == pytest.approx(0.6)


def test_runup_tolerance_and_pullback_gate_prevent_flapping():
    near_threshold_metrics = RunupMetrics(
        runup_ref_price=12.0,
        runup_ref_source="asof_close",
        runup_from_sigclose=1.0,
        runup_from_sigclose_atr=1.2059,
    )
    triggered, reason = evaluate_runup_breach(
        near_threshold_metrics,
        runup_atr_max=1.2,
        runup_atr_tol=0.05,
    )
    assert triggered is False
    assert reason is None

    gated_metrics = RunupMetrics(
        runup_ref_price=12.5,
        runup_ref_source="max(asof_close, live_high)",
        runup_from_sigclose=1.5,
        runup_from_sigclose_atr=1.6,
    )
    triggered, _ = evaluate_runup_breach(
        gated_metrics,
        runup_atr_max=1.5,
        runup_atr_tol=0.02,
        dev_ma20_atr=0.8,
        dev_ma20_atr_min=1.0,
    )
    assert triggered is False

    triggered, reason = evaluate_runup_breach(
        gated_metrics,
        runup_atr_max=1.5,
        runup_atr_tol=0.02,
        dev_ma20_atr=1.2,
        dev_ma20_atr_min=1.0,
    )
    assert triggered is True
    assert "runup_from_sigclose_atr=1.600" in reason
