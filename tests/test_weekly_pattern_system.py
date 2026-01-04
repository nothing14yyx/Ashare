import pandas as pd

from ashare.indicators.weekly_pattern_system import _clip_text, _fit_regression, WeeklyPlanSystem


def test_clip_text():
    text = "a b  c"
    assert _clip_text(text, 3) == "a b"


def test_fit_regression():
    slope, intercept = _fit_regression([(0, 1.0), (1, 2.0)])
    assert slope is not None
    assert intercept is not None


def test_prepare_df_sorts_and_adds_columns():
    system = WeeklyPlanSystem()
    bars = [
        {"week_end": "2025-01-10", "open": 2, "high": 3, "low": 1, "close": 2.5, "volume": 100},
        {"week_end": "2025-01-03", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 90},
    ]
    df = system._prepare_df(bars)
    assert not df.empty
    assert df.iloc[0]["week_end"] < df.iloc[1]["week_end"]
    assert "ma_fast" in df.columns
    assert "atr14" in df.columns
