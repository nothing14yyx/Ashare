import pandas as pd

from ashare.indicators.weekly_channel_regime import _to_weekly_ohlcv, WeeklyChannelClassifier


def test_to_weekly_ohlcv_aggregates():
    df = pd.DataFrame(
        [
            {"code": "sh.000001", "date": "2025-01-06", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100},
            {"code": "sh.000001", "date": "2025-01-07", "open": 2, "high": 3, "low": 2, "close": 3, "volume": 150},
            {"code": "sh.000001", "date": "2025-01-13", "open": 3, "high": 4, "low": 3, "close": 4, "volume": 200},
            {"code": "sh.000001", "date": "2025-01-14", "open": 4, "high": 5, "low": 4, "close": 5, "volume": 250},
        ]
    )
    wk = _to_weekly_ohlcv(df)
    assert len(wk) == 2
    assert wk.iloc[0]["volume"] == 250
    assert wk.iloc[1]["volume"] == 450


def test_weekly_channel_classifier_returns_payload():
    df = pd.DataFrame(
        [
            {"code": "sh.000001", "date": "2025-01-06", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100},
            {"code": "sh.000001", "date": "2025-01-07", "open": 2, "high": 3, "low": 2, "close": 3, "volume": 150},
            {"code": "sh.000001", "date": "2025-01-08", "open": 3, "high": 4, "low": 3, "close": 3.5, "volume": 120},
            {"code": "sh.000001", "date": "2025-01-09", "open": 3.5, "high": 4, "low": 3, "close": 3.2, "volume": 130},
            {"code": "sh.000001", "date": "2025-01-10", "open": 3.2, "high": 4, "low": 3, "close": 3.8, "volume": 140},
            {"code": "sh.000001", "date": "2025-01-13", "open": 3.8, "high": 4.2, "low": 3.7, "close": 4.0, "volume": 160},
        ]
    )
    classifier = WeeklyChannelClassifier(lrc_length=3, ma_fast=2, ma_slow=3)
    result = classifier.classify(df)
    assert result.state is not None
    assert "sh.000001" in result.detail
