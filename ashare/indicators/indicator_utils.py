from __future__ import annotations

import pandas as pd


def _ema(series: pd.Series, span: int) -> pd.Series:
    """指数移动平均（EMA）。"""
    return series.ewm(span=span, adjust=False).mean()


def macd(
    close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """MACD: DIF/DEA/HIST(2*(DIF-DEA))."""
    dif = _ema(close, fast) - _ema(close, slow)
    dea = _ema(dif, signal)
    hist = 2 * (dif - dea)
    return dif, dea, hist


def atr(
    high: pd.Series, low: pd.Series, preclose: pd.Series, n: int = 14
) -> pd.Series:
    """ATR: 平均真实波幅（n 日）。"""
    tr1 = (high - low).abs()
    tr2 = (high - preclose).abs()
    tr3 = (low - preclose).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=1).mean().rename("atr14")


def consecutive_true(mask: pd.Series) -> pd.Series:
    """计算布尔序列的连续为 True 的累积天数。

    参数
    ----
    mask: pd.Series
        需要计算连续 True 段长度的布尔序列。

    返回
    ----
    pd.Series
        与 mask 等长的整数序列，表示当前位置连续为 True 的天数，False 位置为 0。
    """

    mask = mask.fillna(False).astype(bool)
    changes = (mask != mask.shift()).cumsum()
    streak = mask.groupby(changes).cumcount().add(1)
    return streak.where(mask, 0)
