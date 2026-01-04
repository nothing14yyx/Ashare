import pandas as pd

from ashare.indicators.market_regime import MarketRegimeClassifier


def test_market_regime_risk_on():
    dates = pd.date_range("2025-01-01", periods=6, freq="D")
    df = pd.DataFrame(
        {
            "code": ["sh.000001"] * len(dates),
            "date": dates,
            "close": [10, 10.5, 11, 11.5, 12, 12.5],
        }
    )
    classifier = MarketRegimeClassifier(breakdown_window=3, effective_breakdown_days=2)
    result = classifier.classify(df, short=2, mid=3, long=4)
    assert result.regime in {"RISK_ON", "PULLBACK"}
    assert result.position_hint is not None
