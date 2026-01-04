import pandas as pd

from ashare.indicators.indicator_utils import consecutive_true


def test_consecutive_true():
    series = pd.Series([True, True, False, True, False, True, True, True])
    result = consecutive_true(series).tolist()
    assert result == [1, 2, 0, 1, 0, 1, 2, 3]
