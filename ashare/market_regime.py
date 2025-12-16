from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd


@dataclass
class MarketRegimeResult:
    score: float | None
    detail: Dict[str, Dict[str, Any]]
    regime: str | None
    position_hint: float | None

    def to_payload(self) -> Dict[str, Any]:
        return {
            "score": self.score,
            "detail": self.detail,
            "regime": self.regime,
            "position_hint": self.position_hint,
        }


class MarketRegimeClassifier:
    """基于指数日线的市场环境分类器。"""

    def __init__(self, breakdown_window: int = 60) -> None:
        self.breakdown_window = max(int(breakdown_window), 1)

    def classify(
        self,
        df: pd.DataFrame,
        *,
        short: int = 20,
        mid: int = 60,
        long: int = 250,
    ) -> MarketRegimeResult:
        if df is None or df.empty:
            return MarketRegimeResult(score=None, detail={}, regime=None, position_hint=None)

        work = df.copy()
        work["date"] = pd.to_datetime(work["date"], errors="coerce")
        for col in ["close", "ma20", "ma60", "ma250"]:
            if col in work.columns:
                work[col] = pd.to_numeric(work[col], errors="coerce")

        detail: Dict[str, Dict[str, Any]] = {}
        statuses: List[str] = []

        for code, group in work.groupby("code"):
            grp = group.sort_values("date")
            close = pd.to_numeric(grp.get("close"), errors="coerce")
            grp["ma20"] = close.rolling(short, min_periods=1).mean()
            grp["ma60"] = close.rolling(mid, min_periods=1).mean()
            grp["ma250"] = close.rolling(long, min_periods=1).mean()
            grp["rolling_low"] = close.rolling(self.breakdown_window, min_periods=1).min().shift(1)

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
            detail[str(code)] = {
                "close": close_val,
                "ma20": ma20,
                "ma60": ma60,
                "ma250": ma250,
                "score": score,
                "status": status,
                "pullback_mode": pullback_mode,
                "rolling_low": rolling_low,
            }

        if not detail:
            return MarketRegimeResult(score=None, detail={}, regime=None, position_hint=None)

        avg_score = sum(v.get("score", 0) for v in detail.values()) / max(len(detail), 1)

        regime = "RISK_ON"
        if "BREAKDOWN" in statuses:
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
        }
        position_hint = position_hint_map.get(regime)

        if regime == "PULLBACK":
            fast_drop = any(
                (v.get("pullback_mode") == "FAST_DROP") for v in detail.values()
            )
            if fast_drop and position_hint is not None:
                position_hint = min(position_hint, 0.3)

        return MarketRegimeResult(
            score=avg_score, detail=detail, regime=regime, position_hint=position_hint
        )
