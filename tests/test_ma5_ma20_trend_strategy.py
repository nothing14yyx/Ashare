import pandas as pd

from ashare.strategies.ma5_ma20_trend_strategy import _atr, _macd, _rsi, _split_exchange_symbol


def test_split_exchange_symbol():
    assert _split_exchange_symbol("sh.600000") == ("sh", "600000")
    assert _split_exchange_symbol("600000") == ("", "600000")
    assert _split_exchange_symbol("") == ("", "")


def test_macd_and_atr_shapes():
    close = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    dif, dea, hist = _macd(close)
    assert len(dif) == len(close)
    assert len(dea) == len(close)
    assert len(hist) == len(close)

    high = pd.Series([2, 3, 4, 5, 6])
    low = pd.Series([1, 2, 3, 4, 5])
    preclose = pd.Series([1, 2, 3, 4, 5])
    atr = _atr(high, low, preclose)
    assert len(atr) == len(high)


def test_rsi_range():
    close = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4])
    rsi = _rsi(close)
    assert rsi.dropna().between(0, 100).all()
