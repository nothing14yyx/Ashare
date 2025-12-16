from __future__ import annotations

import pandas as pd


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
