from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd

from .indicator_utils import consecutive_true


@dataclass
class MarketRegimeResult:
    score: float | None
    detail: Dict[str, Dict[str, Any]]
    regime: str | None
    position_hint: float | None
    context: Dict[str, Any]

    def to_payload(self) -> Dict[str, Any]:
        payload = {
            "score": self.score,
            "detail": self.detail,
            "regime": self.regime,
            "position_hint": self.position_hint,
        }
        payload.update(self.context)
        return payload


class MarketRegimeClassifier:
    """基于指数日线的市场环境分类器。"""

    def __init__(
        self,
        breakdown_window: int = 60,
        effective_breakdown_days: int = 3,
        effective_reclaim_days: int = 2,
    ) -> None:
        self.breakdown_window = max(int(breakdown_window), 1)
        self.effective_breakdown_days = max(int(effective_breakdown_days), 1)
        self.effective_reclaim_days = max(int(effective_reclaim_days), 1)

    def classify(
        self,
        df: pd.DataFrame,
        *,
        short: int = 20,
        mid: int = 60,
        long: int = 250,
    ) -> MarketRegimeResult:
        if df is None or df.empty:
            return MarketRegimeResult(
                score=None,
                detail={},
                regime=None,
                position_hint=None,
                context={},
            )

        work = df.copy()
        work["date"] = pd.to_datetime(work["date"], errors="coerce")
        for col in ["close", "ma20", "ma60", "ma250"]:
            if col in work.columns:
                work[col] = pd.to_numeric(work[col], errors="coerce")

        detail: Dict[str, Dict[str, Any]] = {}
        statuses: List[str] = []
        below_streaks: List[int] = []
        break_confirmed_flags: List[bool] = []
        reclaim_confirmed_flags: List[bool] = []

        for code, group in work.groupby("code"):
            grp = group.sort_values("date")
            close = pd.to_numeric(grp.get("close"), errors="coerce")
            grp["ma20"] = close.rolling(short, min_periods=short).mean()
            grp["ma60"] = close.rolling(mid, min_periods=mid).mean()
            grp["ma250"] = close.rolling(long, min_periods=long).mean()
            grp["rolling_low"] = close.rolling(self.breakdown_window, min_periods=1).min().shift(1)

            ma250_valid = grp["ma250"].notna()
            below_ma250 = (close < grp["ma250"]) & ma250_valid
            prev_below = below_ma250.shift(1).fillna(False)
            below_ma250_streak_series = consecutive_true(below_ma250)
            below_ma250_streak = (
                int(below_ma250_streak_series.iloc[-1])
                if not below_ma250_streak_series.empty
                else 0
            )

            above_ma250 = (close >= grp["ma250"]) & ma250_valid
            reclaim_streak_series = consecutive_true(above_ma250)
            reclaim_streak = (
                int(reclaim_streak_series.iloc[-1])
                if not reclaim_streak_series.empty
                else 0
            )

            break_confirmed = below_ma250_streak >= self.effective_breakdown_days
            reclaim_confirmed = (
                reclaim_streak >= self.effective_reclaim_days
                and bool(prev_below.iloc[-1])
            )

            yearline_state = "ABOVE"
            if break_confirmed:
                yearline_state = "BREAK_CONFIRMED"
            elif below_ma250_streak > 0:
                yearline_state = "BELOW_1_2D"
            elif reclaim_confirmed:
                yearline_state = "RECLAIM_CONFIRMED"

            latest = grp.iloc[-1]
            close_val = latest.get("close")
            ma20 = latest.get("ma20")
            ma60 = latest.get("ma60")
            ma250 = latest.get("ma250")
            rolling_low = latest.get("rolling_low")

            score = 0
            for ma in [ma20, ma60, ma250]:
                if ma is not None and close_val is not None and close_val > ma:
                    score += 1

            status = "RISK_ON"
            pullback_mode = None
            if close_val is None or pd.isna(close_val):
                status = "UNKNOWN"
            elif break_confirmed:
                status = "BEAR_CONFIRMED"
            elif rolling_low is not None and not pd.isna(rolling_low) and close_val < rolling_low:
                status = "BREAKDOWN"
            elif ma250 is not None and not pd.isna(ma250) and close_val < ma250:
                status = "RISK_OFF"
            elif ma60 is not None and not pd.isna(ma60) and close_val < ma60:
                status = "PULLBACK"
                daily_ret = close.pct_change().iloc[-1]
                pullback_mode = "FAST_DROP" if daily_ret <= -0.03 else "SIDEWAYS"
            elif ma20 is not None and not pd.isna(ma20) and close_val < ma20:
                status = "PULLBACK"
                pullback_mode = "SIDEWAYS"

            statuses.append(status)
            below_streaks.append(below_ma250_streak)
            break_confirmed_flags.append(break_confirmed)
            reclaim_confirmed_flags.append(reclaim_confirmed)
            detail[str(code)] = {
                "close": close_val,
                "ma20": ma20,
                "ma60": ma60,
                "ma250": ma250,
                "score": score,
                "status": status,
                "pullback_mode": pullback_mode,
                "rolling_low": rolling_low,
                "below_ma250_streak": below_ma250_streak,
                "break_confirmed": break_confirmed,
                "reclaim_confirmed": reclaim_confirmed,
                "reclaim_streak": reclaim_streak,
                "yearline_state": yearline_state,
            }

        if not detail:
            return MarketRegimeResult(
                score=None,
                detail={},
                regime=None,
                position_hint=None,
                context={},
            )

        avg_score = sum(v.get("score", 0) for v in detail.values()) / max(len(detail), 1)

        regime = "RISK_ON"
        if "BEAR_CONFIRMED" in statuses:
            regime = "BEAR_CONFIRMED"
        elif "BREAKDOWN" in statuses:
            regime = "BREAKDOWN"
        elif "RISK_OFF" in statuses:
            regime = "RISK_OFF"
        elif "PULLBACK" in statuses:
            regime = "PULLBACK"

        position_hint_map = {
            "RISK_ON": 0.8,
            "PULLBACK": 0.4,
            "RISK_OFF": 0.2,
            "BREAKDOWN": 0.0,
            "BEAR_CONFIRMED": 0.0,
        }
        position_hint = position_hint_map.get(regime)

        if regime == "PULLBACK":
            fast_drop = any(
                (v.get("pullback_mode") == "FAST_DROP") for v in detail.values()
            )
            if fast_drop and position_hint is not None:
                position_hint = min(position_hint, 0.3)

        below_ma250_max = max(below_streaks) if below_streaks else 0
        below_any = any(bs > 0 for bs in below_streaks)
        break_confirmed_any = any(break_confirmed_flags)
        reclaim_confirmed_any = any(reclaim_confirmed_flags)
        yearline_state = "ABOVE"
        if break_confirmed_any:
            yearline_state = "BREAK_CONFIRMED"
        elif below_any:
            yearline_state = "BELOW_1_2D"
        elif reclaim_confirmed_any:
            yearline_state = "RECLAIM_CONFIRMED"
        context: Dict[str, Any] = {
            "below_ma250_streak": below_ma250_max,
            "break_confirmed": break_confirmed_any,
            "reclaim_confirmed": reclaim_confirmed_any,
            "effective_breakdown_days": self.effective_breakdown_days,
            "effective_reclaim_days": self.effective_reclaim_days,
            "yearline_state": yearline_state,
        }
        if regime == "BEAR_CONFIRMED":
            context["regime_note"] = "年线有效跌破：连续3日收盘低于MA250，趋势破位"

        return MarketRegimeResult(
            score=avg_score,
            detail=detail,
            regime=regime,
            position_hint=position_hint,
            context=context,
        )
