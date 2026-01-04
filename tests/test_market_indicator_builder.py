import pandas as pd

from ashare.indicators.market_indicator_builder import MarketIndicatorBuilder


def test_resolve_daily_regime_pullback_fast_drop():
    df = pd.DataFrame(
        {
            "status": ["PULLBACK", "RISK_ON"],
            "score_raw": [2, 3],
            "pullback_mode": ["FAST_DROP", None],
        }
    )
    result = MarketIndicatorBuilder._resolve_daily_regime(df)
    assert result["regime"] == "PULLBACK"
    assert abs(result["position_hint"] - 0.3) < 1e-6


def test_resolve_breadth_metrics():
    df = pd.DataFrame(
        {
            "ma20": [1, 1, None],
            "ma60": [1, None, 1],
            "above_ma20": [True, False, False],
            "above_ma60": [True, False, True],
            "risk_off_flag": [True, False, True],
            "macd_hist": [1, 1, 1],
            "dev_ma20_atr": [0.1, 0.2, 0.3],
        }
    )
    result = MarketIndicatorBuilder._resolve_breadth_metrics(df)
    assert abs(result["breadth_pct_above_ma20"] - 0.5) < 1e-6
    assert abs(result["breadth_pct_above_ma60"] - 1.0) < 1e-6
    assert result["breadth_risk_off_ratio"] is not None


def test_resolve_daily_zone_breakdown():
    row = pd.Series({"regime": "BREAKDOWN", "bb_pos": 0.5, "bb_width": 0.1})
    zone = MarketIndicatorBuilder._resolve_daily_zone(row)
    assert zone["daily_zone_id"] == "DZ_BREAKDOWN"
    assert zone["daily_cap_multiplier"] == 0.2
