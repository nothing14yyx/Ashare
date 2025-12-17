from __future__ import annotations

"""周线下跌/上升通道（线性回归通道）+ 30/60 周均线的情景分类。

设计目标：
- 尽量把“手动画通道”的主观部分，替换成可重复的计算方法；
- 输出“情景（state）+ 仓位提示（position_hint）+ 通道位置百分比（chan_pos）+ 关键价位
  （upper/lower/MA30/MA60）”，便于在 open_monitor / 报告里展示或做过滤。

注意：
- 这里不做艾略特波浪计数（主观性强），只保留可客观落地的条件判断；
- 通道用线性回归中轴 + 残差标准差*dev 的上下轨。
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


def _to_weekly_ohlcv(df_daily: pd.DataFrame) -> pd.DataFrame:
    """把日线 OHLCV 按 code 聚合成周线（周五作为周收盘）。

    期望列：code,date,open,high,low,close,volume,(optional amount)
    返回列：code,week_end,open,high,low,close,volume,(optional amount)
    """

    if df_daily is None or df_daily.empty:
        return pd.DataFrame(columns=["code", "week_end", "open", "high", "low", "close", "volume"])

    work = df_daily.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    keep = [c for c in ["code", "date", "open", "high", "low", "close", "volume", "amount"] if c in work.columns]
    work = work[keep].dropna(subset=["code", "date"]).copy()
    if work.empty:
        return pd.DataFrame(columns=["code", "week_end", "open", "high", "low", "close", "volume"])

    out_parts = []
    for code, grp in work.groupby("code", sort=False):
        g = grp.sort_values("date").set_index("date")
        agg = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
        if "amount" in g.columns:
            agg["amount"] = "sum"

        wk = g.resample("W-FRI").agg(agg).dropna(subset=["open", "high", "low", "close"]).reset_index()
        wk["code"] = str(code)
        wk = wk.rename(columns={"date": "week_end"})
        out_parts.append(wk)

    if not out_parts:
        return pd.DataFrame(columns=["code", "week_end", "open", "high", "low", "close", "volume"])

    out = pd.concat(out_parts, ignore_index=True)
    out = out.sort_values(["code", "week_end"]).reset_index(drop=True)
    return out


def _sma(s: pd.Series, n: int) -> pd.Series:
    n = max(int(n), 1)
    return s.rolling(n, min_periods=n).mean()


def _linear_regression_channel(
    close: pd.Series,
    *,
    length: int = 52,
    dev: float = 2.0,
) -> pd.DataFrame:
    """线性回归通道：center + dev*std(residual) 的上下轨。

    - length: 回归窗口（周）
    - dev: 标准差倍数
    """

    close = pd.to_numeric(close, errors="coerce").astype(float)
    n = len(close)
    center = np.full(n, np.nan, dtype=float)
    upper = np.full(n, np.nan, dtype=float)
    lower = np.full(n, np.nan, dtype=float)
    slope = np.full(n, np.nan, dtype=float)

    length = max(int(length), 2)
    x = np.arange(length, dtype=float)

    for i in range(length - 1, n):
        y = close.iloc[i - length + 1 : i + 1].to_numpy(dtype=float)
        if np.any(np.isnan(y)):
            continue
        a, b = np.polyfit(x, y, 1)  # y = a*x + b
        y_hat = a * x + b
        resid = y - y_hat
        sigma = float(np.std(resid, ddof=1)) if length > 2 else 0.0

        center[i] = y_hat[-1]
        upper[i] = y_hat[-1] + float(dev) * sigma
        lower[i] = y_hat[-1] - float(dev) * sigma
        slope[i] = a

    return pd.DataFrame(
        {
            "lrc_center": center,
            "lrc_upper": upper,
            "lrc_lower": lower,
            "lrc_slope": slope,
        },
        index=close.index,
    )


@dataclass
class WeeklyChannelResult:
    state: Optional[str]
    position_hint: Optional[float]
    detail: Dict[str, Dict[str, Any]]
    context: Dict[str, Any]

    def to_payload(self) -> Dict[str, Any]:
        payload = {
            "state": self.state,
            "position_hint": self.position_hint,
            "detail": self.detail,
        }
        payload.update(self.context)
        return payload


class WeeklyChannelClassifier:
    """周线通道情景分类器（指数/个股均可）。"""

    def __init__(
        self,
        *,
        lrc_length: int = 52,
        lrc_dev: float = 2.0,
        touch_chan_eps: float = 0.005,
        near_lower_eps: float = 0.02,
        near_ma_eps: float = 0.01,
        ma_fast: int = 30,
        ma_slow: int = 60,
        primary_code: str = "sh.000001",
    ) -> None:
        self.lrc_length = max(int(lrc_length), 2)
        self.lrc_dev = float(lrc_dev)
        self.touch_chan_eps = float(touch_chan_eps)
        self.near_lower_eps = float(near_lower_eps)
        self.near_ma_eps = float(near_ma_eps)
        self.ma_fast = max(int(ma_fast), 1)
        self.ma_slow = max(int(ma_slow), 1)
        self.primary_code = str(primary_code or "").strip() or "sh.000001"

    @staticmethod
    def _near(x: float | None, y: float | None, eps: float) -> bool:
        if x is None or y is None:
            return False
        if pd.isna(x) or pd.isna(y) or y == 0:
            return False
        return abs(float(x) - float(y)) / abs(float(y)) <= float(eps)

    def _classify_one(self, wk: pd.DataFrame) -> Dict[str, Any]:
        # 分组后的 wk 可能继承了全表的原始 index（不是从 0 开始的连续 RangeIndex）
        # 若直接 concat(lrc.reset_index(drop=True)) 会发生 index 对齐错位，导致出现
        # close/week_end 为 None 但通道列有值的“幽灵行”。
        wk = wk.copy().reset_index(drop=True)
        wk["ma30"] = _sma(wk["close"], self.ma_fast)
        wk["ma60"] = _sma(wk["close"], self.ma_slow)
        wk["vol_ma20"] = _sma(wk["volume"], 20)
        lrc = _linear_regression_channel(wk["close"], length=self.lrc_length, dev=self.lrc_dev)
        # wk 已 reset_index(drop=True)，lrc 的 index 与 wk 完全一致，直接 concat 即可
        wk = pd.concat([wk, lrc], axis=1)

        last = wk.iloc[-1]
        close = float(last.get("close")) if pd.notna(last.get("close")) else None
        high = float(last.get("high")) if pd.notna(last.get("high")) else None
        low = float(last.get("low")) if pd.notna(last.get("low")) else None
        upper = float(last.get("lrc_upper")) if pd.notna(last.get("lrc_upper")) else None
        lower = float(last.get("lrc_lower")) if pd.notna(last.get("lrc_lower")) else None
        slope = float(last.get("lrc_slope")) if pd.notna(last.get("lrc_slope")) else None
        ma30 = float(last.get("ma30")) if pd.notna(last.get("ma30")) else None
        ma60 = float(last.get("ma60")) if pd.notna(last.get("ma60")) else None
        volume = float(last.get("volume")) if pd.notna(last.get("volume")) else None
        amount = float(last.get("amount")) if pd.notna(last.get("amount")) else None
        vol_ma20 = float(last.get("vol_ma20")) if pd.notna(last.get("vol_ma20")) else None

        wk_vol_ratio_20 = None
        if volume is not None and vol_ma20 not in (None, 0):
            wk_vol_ratio_20 = volume / vol_ma20 if vol_ma20 else None

        prev_high = None
        if len(wk) >= 2:
            prev_row = wk.iloc[-2]
            prev_high = float(prev_row.get("high")) if pd.notna(prev_row.get("high")) else None

        slope_change_4w = None
        slope_shift = wk["lrc_slope"].shift(4)
        if not slope_shift.empty:
            last_slope = wk["lrc_slope"].iloc[-1]
            prev_slope = slope_shift.iloc[-1]
            if pd.notna(last_slope) and pd.notna(prev_slope):
                slope_change_4w = float(last_slope - prev_slope)

        state = "INSIDE_CHANNEL"
        note = "仍在通道内运行，等待周收盘选择方向"

        chan_pos = None
        chan_pos_clamped = None
        if close is not None and upper is not None and lower is not None and upper > lower:
            chan_pos = (close - lower) / (upper - lower)
            chan_pos_clamped = min(max(chan_pos, 0.0), 1.0)

        near_lower = (
            lower is not None
            and (
                (close is not None and self._near(close, lower, self.near_lower_eps))
                or (low is not None and low <= lower * (1.0 + self.touch_chan_eps))
            )
        )
        near_ma30 = self._near(close, ma30, self.near_ma_eps) or (
            ma30 is not None and low is not None and low <= ma30 * (1.0 + self.near_ma_eps)
        )

        if upper is not None and close is not None and close > upper:
            state = "CHANNEL_BREAKOUT_UP"
            note = "周收盘价上破通道上轨"
        elif lower is not None and close is not None and close < lower:
            state = "CHANNEL_BREAKDOWN"
            note = "周收盘价跌破通道下轨"
        elif near_lower and near_ma30:
            state = "LOWER_RAIL_NEAR_MA30_ZONE"
            note = "触及/接近下轨且在30周线附近（反弹观察区）"
        elif near_lower:
            state = "NEAR_LOWER_RAIL"
            note = "靠近通道下轨（下轨反弹观察区）"
        elif (
            self._near(close, ma60, self.near_ma_eps)
            or (ma60 is not None and low is not None and low <= ma60 * (1.0 + self.near_ma_eps))
        ):
            state = "EXTREME_NEAR_MA60_ZONE"
            note = "回踩到60周线附近（更深调整观察区）"

        position_hint_map = {
            "CHANNEL_BREAKOUT_UP": 0.8,
            "INSIDE_CHANNEL": 0.6,
            "NEAR_LOWER_RAIL": 0.5,
            "LOWER_RAIL_NEAR_MA30_ZONE": 0.4,
            "EXTREME_NEAR_MA60_ZONE": 0.2,
            "CHANNEL_BREAKDOWN": 0.0,
        }

        position_hint = chan_pos_clamped
        if state == "CHANNEL_BREAKOUT_UP":
            base = position_hint_map.get(state)
            position_hint = base if position_hint is None else max(position_hint, base)
        elif state == "CHANNEL_BREAKDOWN":
            position_hint = 0.0
        elif state == "EXTREME_NEAR_MA60_ZONE":
            position_hint = 0.3 if position_hint is None else min(position_hint, 0.3)
        elif state == "LOWER_RAIL_NEAR_MA30_ZONE":
            position_hint = 0.4 if position_hint is None else min(position_hint, 0.4)
        elif state == "NEAR_LOWER_RAIL":
            position_hint = 0.5 if position_hint is None else min(position_hint, 0.5)
        elif position_hint is None:
            position_hint = position_hint_map.get(state)

        week_end = last.get("week_end")
        week_end_str = None
        if pd.notna(week_end):
            try:
                week_end_str = pd.to_datetime(week_end).date().isoformat()
            except Exception:
                week_end_str = str(week_end)[:10]

        return {
            "week_end": week_end_str,
            "close": close,
            "ma30": ma30,
            "ma60": ma60,
            "chan_upper": upper,
            "chan_lower": lower,
            "chan_slope": slope,
            "chan_pos": chan_pos,
            "high": high,
            "low": low,
            "wk_volume": volume,
            "wk_amount": amount,
            "wk_vol_ma20": vol_ma20,
            "wk_vol_ratio_20": wk_vol_ratio_20,
            "slope_change_4w": slope_change_4w,
            "prev_high": prev_high,
            "channel_dir": (
                "DOWN" if (slope is not None and slope < 0) else "UP" if (slope is not None and slope > 0) else None
            ),
            "state": state,
            "position_hint": position_hint,
            "note": note,
        }

    def classify(self, df_daily: pd.DataFrame) -> WeeklyChannelResult:
        if df_daily is None or df_daily.empty:
            return WeeklyChannelResult(state=None, position_hint=None, detail={}, context={})

        wk = _to_weekly_ohlcv(df_daily)
        if wk.empty:
            return WeeklyChannelResult(state=None, position_hint=None, detail={}, context={})

        detail: Dict[str, Dict[str, Any]] = {}
        for code, grp in wk.groupby("code", sort=False):
            grp = grp.sort_values("week_end").reset_index(drop=True)
            if len(grp) < max(self.ma_slow, self.lrc_length):
                # 数据不足时仍输出最新值，但 state/通道可能为 None
                payload = self._classify_one(grp)
            else:
                payload = self._classify_one(grp)
            detail[str(code)] = payload

        # 选一个“主参考指数”输出整体 state（便于 open_monitor 直接引用）
        primary = self.primary_code
        if primary not in detail and detail:
            primary = sorted(detail.keys())[0]
        primary_payload = detail.get(primary, {}) if detail else {}

        return WeeklyChannelResult(
            state=primary_payload.get("state"),
            position_hint=primary_payload.get("position_hint"),
            detail=detail,
            context={
                "primary_code": primary if detail else None,
                "week_end": primary_payload.get("week_end"),
                "chan_pos": primary_payload.get("chan_pos"),
                "note": primary_payload.get("note"),
                "wk_vol_ratio_20": primary_payload.get("wk_vol_ratio_20"),
                "slope_change_4w": primary_payload.get("slope_change_4w"),
            },
        )
