"""周线形态识别与 Plan 生成系统。

基于周线 OHLCV 自动识别箱体/三角/楔形/旗形/双顶/头肩顶等形态，
输出统一的结构化计划字段，供 open_monitor 的周线 gating 使用。

实现原则：
- 仅依赖本地计算，遵循 ChartSchool 等常见规则的简化版本；
- 形态识别靠枢轴点+趋势线回归，避免主观手动画线；
- Plan 输出为机器可读 tokens + 简短中文，便于后续扩展。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd


def _sma(series: pd.Series, n: int) -> pd.Series:
    n = max(int(n), 1)
    return series.rolling(n, min_periods=n).mean()


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
    high = pd.to_numeric(high, errors="coerce")
    low = pd.to_numeric(low, errors="coerce")
    close = pd.to_numeric(close, errors="coerce")
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=n).mean()


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    close = pd.to_numeric(close, errors="coerce")
    volume = pd.to_numeric(volume, errors="coerce")
    direction = close.diff().apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
    obv = (direction * volume).fillna(0).cumsum()
    return obv


def _fit_regression(points: List[Tuple[int, float]]) -> Tuple[float | None, float | None]:
    if len(points) < 2:
        return None, None
    x = np.array([p[0] for p in points], dtype=float)
    y = np.array([p[1] for p in points], dtype=float)
    if np.any(np.isnan(y)):
        return None, None
    slope, intercept = np.polyfit(x, y, 1)
    return float(slope), float(intercept)


def _line_value(slope: float | None, intercept: float | None, x: int) -> float | None:
    if slope is None or intercept is None:
        return None
    return slope * x + intercept


def _clip_text(text: str | None, limit: int) -> str | None:
    if text is None:
        return None
    normalized = " ".join(str(text).split())
    return normalized[:limit]


@dataclass
class PatternCandidate:
    scene: str
    bias: str
    status: str
    score: float
    key_levels: Dict[str, float]
    structure_tags: List[str]
    confirm_tags: List[str]
    money_tags: List[str]
    description: str


class WeeklyPlanSystem:
    """周线形态识别系统。

    输入：weekly_payload.context.primary_weekly_bars（list[dict]）。
    输出：统一 schema 的计划字段，用于周线 gating。
    """

    def __init__(self) -> None:
        self.ma_fast = 30
        self.ma_slow = 60
        self.fractal_left = 2
        self.fractal_right = 2
        self.max_pivots = 8
        self.break_eps = 0.01
        self.confirm_vol_ratio_threshold = 1.05
        self.retest_tolerance = 0.01

    def _prepare_df(self, bars: List[Dict[str, Any]]) -> pd.DataFrame:
        df = pd.DataFrame(bars)
        if df.empty:
            return df
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df["week_end"] = pd.to_datetime(df.get("week_end"), errors="coerce")
        df = df.sort_values("week_end").reset_index(drop=True)
        df["ma_fast"] = _sma(df["close"], self.ma_fast)
        df["ma_slow"] = _sma(df["close"], self.ma_slow)
        df["atr14"] = _atr(df["high"], df["low"], df["close"], 14)
        df["vol_ma20"] = _sma(df["volume"], 20)
        df["wk_vol_ratio_20"] = df.apply(
            lambda row: row["volume"] / row["vol_ma20"] if row.get("vol_ma20") else np.nan,
            axis=1,
        )
        df["ret_4w"] = df["close"].pct_change(4)
        df["ret_8w"] = df["close"].pct_change(8)
        df["range_width_pct"] = (df["high"] - df["low"]) / df["close"]
        df["volatility_4w"] = df["close"].pct_change().rolling(4, min_periods=2).std()
        df["obv"] = _obv(df["close"], df["volume"])
        df["obv_slope_13"] = df["obv"].diff(13)
        return df

    def _extract_pivots(self, df: pd.DataFrame) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
        highs: List[Tuple[int, float]] = []
        lows: List[Tuple[int, float]] = []
        if df.empty:
            return highs, lows
        values_high = df["high"].to_numpy()
        values_low = df["low"].to_numpy()
        n = len(df)
        for i in range(n):
            left = max(i - self.fractal_left, 0)
            right = min(i + self.fractal_right, n - 1)
            window_high = values_high[left : right + 1]
            window_low = values_low[left : right + 1]
            if np.isnan(values_high[i]) or np.isnan(values_low[i]):
                continue
            if values_high[i] >= np.nanmax(window_high) and i not in {0, n - 1}:
                highs.append((i, float(values_high[i])))
            if values_low[i] <= np.nanmin(window_low) and i not in {0, n - 1}:
                lows.append((i, float(values_low[i])))
        highs = highs[-self.max_pivots :]
        lows = lows[-self.max_pivots :]
        return highs, lows

    def _convergence(self, upper_slope: float | None, lower_slope: float | None, width_start: float | None, width_end: float | None) -> float | None:
        if upper_slope is None or lower_slope is None:
            return None
        if width_start is None or width_start <= 0 or width_end is None or width_end <= 0:
            return None
        return width_end / width_start

    def _detect_flag(self, df: pd.DataFrame) -> PatternCandidate | None:
        if len(df) < 12:
            return None
        recent_close = df["close"].iloc[-1]
        base_idx = max(len(df) - 8, 0)
        base_close = df["close"].iloc[base_idx]
        if base_close == 0 or pd.isna(recent_close) or pd.isna(base_close):
            return None
        impulse = (recent_close - base_close) / base_close
        if abs(impulse) < 0.12:
            return None
        recent_vol = df["volatility_4w"].iloc[-1]
        prev_vol = df["volatility_4w"].iloc[-5] if len(df) >= 6 else np.nan
        consolidating = (pd.notna(recent_vol) and pd.notna(prev_vol) and recent_vol < prev_vol)
        width_mean = df["range_width_pct"].iloc[-4:].mean()
        width_prev = df["range_width_pct"].iloc[-8:-4].mean() if len(df) >= 8 else np.nan
        if pd.notna(width_mean) and pd.notna(width_prev) and width_mean < width_prev * 0.9:
            consolidating = consolidating or True
        if not consolidating:
            return None
        bias = "BULLISH" if impulse > 0 else "BEARISH"
        scene = "FLAG_PENNANT_BULL" if impulse > 0 else "FLAG_PENNANT_BEAR"
        status = "FORMING"
        score = 65.0 + min(abs(impulse) * 100, 15)
        tags = [scene, "CONSOLIDATION"]
        return PatternCandidate(
            scene=scene,
            bias=bias,
            status=status,
            score=score,
            key_levels={},
            structure_tags=tags,
            confirm_tags=[],
            money_tags=[],
            description="冲击后横盘整理，留意突破",  # 简短描述
        )

    def _detect_double(self, highs: List[Tuple[int, float]], lows: List[Tuple[int, float]]) -> PatternCandidate | None:
        if len(highs) >= 2:
            last_two = highs[-2:]
            if last_two[0][1] and last_two[1][1]:
                diff = abs(last_two[0][1] - last_two[1][1]) / last_two[0][1]
                if diff <= 0.02:
                    neckline = None
                    if lows:
                        neckline = float(np.mean([p[1] for p in lows[-2:]]))
                    return PatternCandidate(
                        scene="DOUBLE_TOP",
                        bias="BEARISH",
                        status="FORMING",
                        score=70.0,
                        key_levels={"neckline": neckline} if neckline else {},
                        structure_tags=["DOUBLE_TOP"],
                        confirm_tags=[],
                        money_tags=[],
                        description="双顶观察，关注颈线跌破",
                    )
        if len(lows) >= 2:
            last_two = lows[-2:]
            if last_two[0][1] and last_two[1][1]:
                diff = abs(last_two[0][1] - last_two[1][1]) / last_two[0][1]
                if diff <= 0.02:
                    neckline = None
                    if highs:
                        neckline = float(np.mean([p[1] for p in highs[-2:]]))
                    return PatternCandidate(
                        scene="DOUBLE_BOTTOM",
                        bias="BULLISH",
                        status="FORMING",
                        score=70.0,
                        key_levels={"neckline": neckline} if neckline else {},
                        structure_tags=["DOUBLE_BOTTOM"],
                        confirm_tags=[],
                        money_tags=[],
                        description="双底观察，关注颈线突破",
                    )
        return None

    def _detect_hs(self, highs: List[Tuple[int, float]], lows: List[Tuple[int, float]]) -> PatternCandidate | None:
        if len(highs) < 3 or len(lows) < 2:
            return None
        last_highs = highs[-3:]
        shoulder1, head, shoulder2 = last_highs[0][1], last_highs[1][1], last_highs[2][1]
        if head <= max(shoulder1, shoulder2):
            return None
        shoulders_close = abs(shoulder1 - shoulder2) / max(shoulder1, shoulder2) <= 0.05
        if not shoulders_close:
            return None
        neckline = float(np.mean([p[1] for p in lows[-2:]]))
        return PatternCandidate(
            scene="HS_TOP",
            bias="BEARISH",
            status="FORMING",
            score=78.0,
            key_levels={"neckline": neckline},
            structure_tags=["HS_TOP"],
            confirm_tags=[],
            money_tags=[],
            description="头肩顶雏形，关注颈线跌破",
        )

    def _detect_channel_triangle_wedge(
        self,
        df: pd.DataFrame,
        highs: List[Tuple[int, float]],
        lows: List[Tuple[int, float]],
    ) -> PatternCandidate:
        idx_last = len(df) - 1
        log_highs = [(i, float(np.log(p))) for i, p in highs if p and p > 0]
        log_lows = [(i, float(np.log(p))) for i, p in lows if p and p > 0]
        upper_slope, upper_intercept = _fit_regression(log_highs)
        lower_slope, lower_intercept = _fit_regression(log_lows)
        upper_last_log = _line_value(upper_slope, upper_intercept, idx_last)
        lower_last_log = _line_value(lower_slope, lower_intercept, idx_last)
        upper_last = float(np.exp(upper_last_log)) if upper_last_log is not None else None
        lower_last = float(np.exp(lower_last_log)) if lower_last_log is not None else None
        width_end = upper_last - lower_last if upper_last is not None and lower_last is not None else None
        width_start = None
        if log_highs and log_lows:
            min_idx = min(log_highs[0][0], log_lows[0][0])
            upper_start_log = _line_value(upper_slope, upper_intercept, min_idx)
            lower_start_log = _line_value(lower_slope, lower_intercept, min_idx)
            if upper_start_log is not None and lower_start_log is not None:
                upper_start = float(np.exp(upper_start_log))
                lower_start = float(np.exp(lower_start_log))
                width_start = upper_start - lower_start
        convergence = self._convergence(upper_slope, lower_slope, width_start, width_end)

        slope_diff = None
        if upper_slope is not None and lower_slope is not None:
            slope_diff = abs(upper_slope - lower_slope)

        scene = "CHANNEL"
        bias = "NEUTRAL"
        status = "FORMING"
        tags: List[str] = []
        score = 55.0

        flat_threshold_pct = 0.0005
        parallel_threshold_pct = 0.0005
        near_parallel = slope_diff is not None and slope_diff < parallel_threshold_pct
        upper_flat = upper_slope is not None and abs(upper_slope) < flat_threshold_pct
        lower_flat = lower_slope is not None and abs(lower_slope) < flat_threshold_pct
        converging = convergence is not None and convergence < 1.0

        if near_parallel:
            if upper_slope is not None and upper_slope > 0 and lower_slope is not None and lower_slope > 0:
                scene = "CHANNEL_UP"
                bias = "BULLISH"
                tags.append("PARALLEL")
                score = 60.0
            elif upper_slope is not None and upper_slope < 0 and lower_slope is not None and lower_slope < 0:
                scene = "CHANNEL_DOWN"
                bias = "BEARISH"
                tags.append("PARALLEL")
                score = 60.0
        if upper_flat and lower_flat:
            scene = "RECTANGLE"
            bias = "NEUTRAL"
            score = 65.0
            tags.append("RANGE")
        elif upper_flat and lower_slope is not None and lower_slope > 0 and converging:
            scene = "TRIANGLE_ASC"
            bias = "NEUTRAL"
            score = 70.0
            tags.append("ASC_TRIANGLE")
        elif lower_flat and upper_slope is not None and upper_slope < 0 and converging:
            scene = "TRIANGLE_DESC"
            bias = "NEUTRAL"
            score = 70.0
            tags.append("DESC_TRIANGLE")
        elif (
            upper_slope is not None
            and lower_slope is not None
            and upper_slope < 0
            and lower_slope > 0
            and converging
        ):
            scene = "TRIANGLE_SYMM"
            bias = "NEUTRAL"
            score = 72.0
            tags.append("SYMM_TRIANGLE")
        elif (
            upper_slope is not None
            and lower_slope is not None
            and ((upper_slope > 0 and lower_slope > 0) or (upper_slope < 0 and lower_slope < 0))
            and converging
        ):
            scene = "WEDGE_RISING" if upper_slope > 0 else "WEDGE_FALLING"
            bias = "BEARISH" if scene == "WEDGE_RISING" else "BULLISH"
            score = 75.0
            tags.append("WEDGE")

        key_levels = {}
        if upper_last is not None:
            key_levels["upper"] = upper_last
        if lower_last is not None:
            key_levels["lower"] = lower_last
        return PatternCandidate(
            scene=scene,
            bias=bias,
            status=status,
            score=score,
            key_levels=key_levels,
            structure_tags=[scene] + tags,
            confirm_tags=[],
            money_tags=[],
            description="形态识别完成，等待突破",
        )

    def _detect_pattern(
        self, df: pd.DataFrame, weekly_closed: bool, slope_change: float | None = None
    ) -> PatternCandidate:
        highs, lows = self._extract_pivots(df)
        base_candidate = self._detect_channel_triangle_wedge(df, highs, lows)
        candidates: List[PatternCandidate] = [base_candidate]
        flag_candidate = self._detect_flag(df)
        if flag_candidate:
            candidates.append(flag_candidate)
        double_candidate = self._detect_double(highs, lows)
        if double_candidate:
            candidates.append(double_candidate)
        hs_candidate = self._detect_hs(highs, lows)
        if hs_candidate:
            candidates.append(hs_candidate)
        candidates = sorted(candidates, key=lambda c: c.score, reverse=True)
        top = candidates[0]
        return self._apply_breakout_confirm(df, top, weekly_closed, slope_change)

    def _apply_breakout_confirm(
        self,
        df: pd.DataFrame,
        candidate: PatternCandidate,
        weekly_closed: bool,
        slope_change: float | None = None,
    ) -> PatternCandidate:
        last_idx = len(df) - 1
        last_close = float(df["close"].iloc[-1]) if not df.empty else None
        wk_vol_ratio = df["wk_vol_ratio_20"].iloc[-1] if not df.empty else np.nan
        confirm_tags = list(candidate.confirm_tags)
        money_tags = list(candidate.money_tags)
        structure_tags = list(candidate.structure_tags)
        status = candidate.status
        scene = candidate.scene
        key_levels = dict(candidate.key_levels)

        upper = key_levels.get("upper")
        lower = key_levels.get("lower")
        neckline = key_levels.get("neckline")

        breakout_up = False
        breakout_down = False
        if last_close is not None:
            if upper is not None and last_close > upper * (1 + self.break_eps):
                breakout_up = True
            if lower is not None and last_close < lower * (1 - self.break_eps):
                breakout_down = True
            if neckline is not None:
                if scene in {"DOUBLE_BOTTOM", "HS_BOTTOM"} and last_close > neckline * (1 + self.break_eps):
                    breakout_up = True
                if scene in {"DOUBLE_TOP", "HS_TOP"} and last_close < neckline * (1 - self.break_eps):
                    breakout_down = True

        bias = candidate.bias
        if breakout_up:
            status = "BREAKOUT_UP"
            bias = "BULLISH"
            structure_tags.append("BREAKOUT_UP")
        elif breakout_down:
            status = "BREAKOUT_DOWN"
            bias = "BEARISH"
            structure_tags.append("BREAKDOWN_DOWN")

        confirmed = False
        if pd.notna(wk_vol_ratio) and wk_vol_ratio >= 1.1:
            confirmed = True
            confirm_tags.append("VOL_CONFIRM")
        elif pd.notna(wk_vol_ratio) and wk_vol_ratio < 0.9:
            confirm_tags.append("VOL_WEAK")

        confirm_signals, money_signals = self._collect_confirm_signals(
            df, key_levels, wk_vol_ratio, slope_change
        )
        confirm_tags.extend(confirm_signals)
        money_tags.extend(money_signals)
        confirmed = confirmed or bool(confirm_signals)

        if status in {"BREAKOUT_UP", "BREAKOUT_DOWN"} and confirmed:
            status = "CONFIRMED"
            structure_tags.append("CONFIRMED")
        if not weekly_closed:
            confirm_tags.append("IF_CURRENT_WEEK_UNCLOSED")

        confirm_tags = list(dict.fromkeys(confirm_tags))
        money_tags = list(dict.fromkeys(money_tags))

        score = candidate.score
        if status == "CONFIRMED":
            score += 10
        elif status.startswith("BREAKOUT"):
            score += 5

        return PatternCandidate(
            scene=scene,
            bias=bias,
            status=status,
            score=score,
            key_levels=key_levels,
            structure_tags=structure_tags,
            confirm_tags=confirm_tags,
            money_tags=money_tags,
            description=candidate.description,
        )

    def _collect_confirm_signals(
        self,
        df: pd.DataFrame,
        key_levels: Dict[str, float],
        wk_vol_ratio: float | None,
        slope_change: float | None = None,
    ) -> Tuple[List[str], List[str]]:
        confirm_tags: List[str] = []
        money_tags: List[str] = []
        vol_threshold = self.confirm_vol_ratio_threshold
        if pd.notna(wk_vol_ratio) and wk_vol_ratio >= vol_threshold:
            confirm_tags.append("VOL_CONFIRM")

        obv_slope = float(df["obv_slope_13"].iloc[-1]) if not df.empty else None
        vol_not_weak = pd.isna(wk_vol_ratio) or wk_vol_ratio >= 0.9
        if obv_slope is not None and not pd.isna(obv_slope) and obv_slope > 0:
            money_tags.append("OBV_SLOPE_UP")
        elif slope_change is not None and not pd.isna(slope_change) and slope_change > 0 and vol_not_weak:
            money_tags.append("MONEY_FLOW_TURNING_UP")

        upper = key_levels.get("upper")
        if upper is not None and len(df) >= 2:
            recent_low = df["low"].iloc[-2:].min()
            last_close = df["close"].iloc[-1]
            if pd.notna(recent_low) and pd.notna(last_close):
                hold_threshold = upper * (1 - self.retest_tolerance)
                if last_close > upper * (1 + self.break_eps * 0.5) and recent_low >= hold_threshold:
                    confirm_tags.append("RETEST_HELD")

        return confirm_tags, money_tags

    def _risk(
        self,
        bias: str,
        status: str,
        confirmed: bool,
        *,
        chan_pos: float | None,
        width_pct: float | None,
        atr_pct: float | None,
        close_below_ma_slow: bool,
        close_below_lower: bool,
        close_below_neckline: bool,
    ) -> Tuple[float, str]:
        score = 50.0
        if bias == "BEARISH":
            score += 10.0
        elif bias == "BULLISH":
            score -= 5.0

        if status == "CONFIRMED" and bias == "BEARISH":
            score += 15.0
        elif status == "CONFIRMED" and bias == "BULLISH":
            score -= 10.0
        elif status == "BREAKOUT_DOWN":
            score += 12.0
        elif status == "BREAKOUT_UP":
            score -= 6.0

        if chan_pos is not None:
            edge_risk = abs(chan_pos - 0.5) * 40.0
            if chan_pos >= 0.8:
                edge_risk += 5.0
            if chan_pos <= 0.2:
                edge_risk += 10.0
            score += edge_risk

        vol_pct = None
        for val in [width_pct, atr_pct]:
            if val is not None and not pd.isna(val):
                vol_pct = max(vol_pct or 0.0, float(val))
        if vol_pct is not None:
            score += min(vol_pct * 100.0, 20.0)

        if close_below_ma_slow:
            score += 12.0
        if close_below_lower:
            score += 18.0
        if close_below_neckline:
            score += 18.0

        score = max(0.0, min(100.0, float(score)))

        force_high = close_below_lower or close_below_neckline
        if force_high:
            score = max(score, 80.0)

        if score >= 70.0 or force_high:
            level = "HIGH"
        elif score <= 40.0:
            level = "LOW"
        else:
            level = "MEDIUM"
        return score, level

    @staticmethod
    def _derive_weekly_phase(
        *,
        last_close: float | None,
        ma_fast: float | None,
        ma_slow: float | None,
        upper: float | None,
        lower: float | None,
        status: str,
        bias: str,
    ) -> str:
        if last_close is None or pd.isna(last_close):
            return "RANGE"
        breakdown_trigger = False
        if lower is not None and last_close < lower:
            breakdown_trigger = True
        if status == "BREAKOUT_DOWN":
            breakdown_trigger = True
        if status == "CONFIRMED" and bias == "BEARISH":
            breakdown_trigger = True
        if breakdown_trigger:
            return "BREAKDOWN_RISK"

        if ma_slow is not None and not pd.isna(ma_slow) and last_close < ma_slow:
            return "BEAR_TREND"

        if (
            ma_fast is not None
            and ma_slow is not None
            and not pd.isna(ma_fast)
            and not pd.isna(ma_slow)
            and last_close > ma_fast
            and ma_fast >= ma_slow
        ):
            return "BULL_TREND"

        return "RANGE"

    def _plan_texts(
        self,
        scene_code: str,
        bias: str,
        status: str,
        direction_confirmed: bool,
        key_levels: Dict[str, float],
        asof_week_closed: bool,
        gate_policy: str | None = None,
    ) -> Tuple[str, str, str | None, str, str, float | None, str | None, str, str | None]:
        plan_a_if: List[str] = []
        plan_b_if: List[str] = []
        plan_b_recover: List[str] = []
        plan_a_confirm = "VOL_CONFIRM_REQUIRED" if not direction_confirmed else "CONFIRMED"
        plan_a_then = "THEN_FOLLOW_BREAKOUT_UP" if bias == "BULLISH" else "THEN_DEFEND"
        plan_b_then = "THEN_DOWNGRADE_WAIT"
        exposure_cap = None

        gate_policy_norm = str(gate_policy or "").upper() or None

        if status in {"FORMING"}:
            plan_a_if.append("IF_WAIT_RANGE_BREAK")
        if status == "BREAKOUT_UP":
            plan_a_if.append("IF_BREAKOUT_UP")
        if status == "BREAKOUT_DOWN":
            plan_a_if.append("IF_BREAKDOWN")
        if gate_policy_norm == "WAIT" and "IF_WAIT_RANGE_BREAK" not in plan_a_if:
            plan_a_if.append("IF_WAIT_RANGE_BREAK")
        if not asof_week_closed:
            plan_a_if.append("IF_CURRENT_WEEK_UNCLOSED")
        if direction_confirmed:
            plan_a_if.append("IF_CONFIRMED")
        if "VOL_CONFIRM" in plan_a_if:
            plan_a_confirm = "VOL_CONFIRM_REQUIRED"

        if bias == "BEARISH":
            plan_b_if.append("IF_RISK_HIGH")
        if not direction_confirmed:
            plan_b_if.append("IF_CONFIRM_MISSING")

        if "ma_fast" in key_levels:
            plan_b_recover.append("RECOVER_RECLAIM_MA_FAST")
        if "upper" in key_levels:
            plan_b_recover.append("RECOVER_BACK_INSIDE")

        key_upper = key_levels.get("upper")
        key_lower = key_levels.get("lower")
        key_neckline = key_levels.get("neckline")
        plan_a = ""
        plan_b = ""
        if bias == "BULLISH":
            if status in {"BREAKOUT_UP", "CONFIRMED"}:
                plan_a = "突破上沿，跟随放量上攻"
                plan_b = "若跌回关键位，减仓等待回收"
            else:
                plan_a = "形态整理中，轻仓等待向上确认"
                plan_b = "跌破下沿或颈线则观望/止损"
        elif bias == "BEARISH":
            if status in {"BREAKOUT_DOWN", "CONFIRMED"}:
                plan_a = "跌破关键位，防守为主"
                plan_b = "若收回关键位且量能恢复再观察"
            else:
                plan_a = "偏空形态酝酿，谨慎试仓或等待"
                plan_b = "放量跌破下沿则离场"
        else:
            plan_a = "中性整理，等待突破方向"
            plan_b = "突破失败或假突破时观望"

        key_hint_parts = []
        if key_upper is not None:
            key_hint_parts.append(f"上沿{key_upper:.2f}")
        if key_lower is not None:
            key_hint_parts.append(f"下沿{key_lower:.2f}")
        if key_neckline is not None:
            key_hint_parts.append(f"颈线{key_neckline:.2f}")
        if key_hint_parts:
            plan_a = f"{plan_a}（{';'.join(key_hint_parts)}）"

        plan_a_if_str = _clip_text(";".join(plan_a_if), 255)
        plan_b_if_str = _clip_text(";".join(plan_b_if), 255)
        plan_b_recover_str = _clip_text(";".join(plan_b_recover), 128)
        return (
            _clip_text(plan_a, 255) or "",
            _clip_text(plan_b, 255) or "",
            plan_a_if_str,
            plan_a_then,
            plan_a_confirm,
            exposure_cap,
            plan_b_if_str,
            plan_b_then,
            plan_b_recover_str,
        )

    def build(self, weekly_payload: Dict[str, Any], index_trend: Dict[str, Any] | None = None) -> Dict[str, Any]:
        plan: Dict[str, Any] = {
            "weekly_scene_code": None,
            "weekly_bias": "NEUTRAL",
            "weekly_status": "FORMING",
            "weekly_structure_status": None,
            "weekly_pattern_status": None,
            "weekly_key_levels": {},
            "weekly_key_levels_str": None,
            "weekly_structure_tags": [],
            "weekly_confirm_tags": [],
            "weekly_money_tags": [],
            "weekly_money_proxy": {},
            "weekly_phase": None,
            "weekly_risk_score": None,
            "weekly_risk_level": "UNKNOWN",
            "weekly_confirm": False,
            "weekly_direction_confirmed": False,
            "weekly_gating_enabled": False,
            "weekly_plan_a": None,
            "weekly_plan_b": None,
            "weekly_plan_a_if": None,
            "weekly_plan_a_then": None,
            "weekly_plan_a_confirm": None,
            "weekly_plan_a_exposure_cap": None,
            "weekly_plan_b_if": None,
            "weekly_plan_b_then": None,
            "weekly_plan_b_recover_if": None,
            "weekly_plan_json": None,
        }

        if not isinstance(weekly_payload, dict):
            return plan
        weekly_bars = weekly_payload.get("primary_weekly_bars")
        if not isinstance(weekly_bars, list) or not weekly_bars:
            return plan

        df = self._prepare_df(weekly_bars)
        if df.empty:
            return plan

        asof_raw = weekly_payload.get("weekly_asof_trade_date")
        asof_ts = pd.to_datetime(asof_raw, errors="coerce") if asof_raw is not None else None
        asof_str = asof_ts.date().isoformat() if asof_ts is not None and pd.notna(asof_ts) else None
        asof_ts = pd.to_datetime(asof_str).normalize() if asof_str is not None else None
        if asof_ts is not None:
            df = df[df["week_end"] <= asof_ts].copy().reset_index(drop=True)
            if df.empty:
                return plan

        week_end = df["week_end"].iloc[-1]
        week_end_str = week_end.date().isoformat() if pd.notna(week_end) else None
        if asof_str is None:
            asof_str = week_end_str
        current_week_closed = bool(weekly_payload.get("weekly_current_week_closed", False))
        weekly_closed = weekly_payload.get("weekly_asof_week_closed")
        if weekly_closed is None:
            weekly_closed = bool(current_week_closed and asof_str == week_end_str)

        asof_week_closed = bool(weekly_closed)

        slope_delta = weekly_payload.get("slope_change_4w")
        if slope_delta is None and isinstance(weekly_payload.get("context", {}), dict):
            slope_delta = weekly_payload.get("context", {}).get("slope_change_4w")

        candidate = self._detect_pattern(df, weekly_closed, slope_delta)
        gate_policy = weekly_payload.get("weekly_gate_policy")
        if gate_policy is None and isinstance(index_trend, dict):
            gate_policy = index_trend.get("weekly_gate_policy")

        direction_confirmed = candidate.status == "CONFIRMED"
        structure_status = "CONFIRMED" if direction_confirmed else "FORMING"
        pattern_status = candidate.status
        key_levels = dict(candidate.key_levels)
        if not key_levels.get("ma_fast"):
            ma_fast_val = float(df["ma_fast"].iloc[-1]) if pd.notna(df["ma_fast"].iloc[-1]) else None
            if ma_fast_val:
                key_levels["ma_fast"] = ma_fast_val
        if not key_levels.get("ma_slow"):
            ma_slow_val = float(df["ma_slow"].iloc[-1]) if pd.notna(df["ma_slow"].iloc[-1]) else None
            if ma_slow_val:
                key_levels["ma_slow"] = ma_slow_val
        if not key_levels.get("atr14"):
            atr_val = float(df["atr14"].iloc[-1]) if pd.notna(df["atr14"].iloc[-1]) else None
            if atr_val:
                key_levels["atr14"] = atr_val

        last_close = float(df["close"].iloc[-1]) if pd.notna(df["close"].iloc[-1]) else None
        ma_fast_val = key_levels.get("ma_fast")
        ma_slow_val = key_levels.get("ma_slow")
        upper_level = key_levels.get("upper")
        lower_level = key_levels.get("lower")
        neckline_level = key_levels.get("neckline")

        weekly_phase = self._derive_weekly_phase(
            last_close=last_close,
            ma_fast=ma_fast_val,
            ma_slow=ma_slow_val,
            upper=upper_level,
            lower=lower_level,
            status=candidate.status,
            bias=candidate.bias,
        )

        range_width_pct = (
            float(df["range_width_pct"].iloc[-1])
            if pd.notna(df["range_width_pct"].iloc[-1])
            else None
        )
        atr_pct = None
        atr_val = key_levels.get("atr14")
        if atr_val is not None and last_close:
            atr_pct = float(atr_val) / float(last_close)

        chan_pos = None
        if (
            upper_level is not None
            and lower_level is not None
            and last_close is not None
            and upper_level != lower_level
        ):
            chan_pos = (float(last_close) - float(lower_level)) / (
                float(upper_level) - float(lower_level)
            )

        close_below_ma_slow = (
            last_close is not None
            and ma_slow_val is not None
            and last_close < ma_slow_val
        )
        close_below_lower = (
            last_close is not None and lower_level is not None and last_close < lower_level
        )
        close_below_neckline = (
            last_close is not None
            and neckline_level is not None
            and last_close < neckline_level
        )

        risk_score, risk_level = self._risk(
            candidate.bias,
            candidate.status,
            direction_confirmed,
            chan_pos=chan_pos,
            width_pct=range_width_pct,
            atr_pct=atr_pct,
            close_below_ma_slow=close_below_ma_slow,
            close_below_lower=close_below_lower,
            close_below_neckline=close_below_neckline,
        )

        key_parts = []
        for label in ["upper", "lower", "neckline", "ma_fast", "ma_slow"]:
            val = key_levels.get(label)
            if val is not None:
                key_parts.append(f"{label}={val:.2f}")
        key_levels_str = ";".join(key_parts)[:255] if key_parts else None

        (
            plan_a,
            plan_b,
            plan_a_if,
            plan_a_then,
            plan_a_confirm,
            exposure_cap,
            plan_b_if,
            plan_b_then,
            plan_b_recover,
        ) = self._plan_texts(
            f"{candidate.scene}_{candidate.status}",
            candidate.bias,
            candidate.status,
            direction_confirmed,
            key_levels,
            asof_week_closed,
            gate_policy,
        )

        if exposure_cap is None:
            base_cap_map = {
                "BULL_TREND": 0.7,
                "RANGE": 0.4,
                "BEAR_TREND": 0.2,
                "BREAKDOWN_RISK": 0.1,
            }
            exposure_cap = base_cap_map.get(weekly_phase, 0.4)

            modifier = 1.0
            confirm_tags = set(candidate.confirm_tags)
            if "VOL_CONFIRM" in confirm_tags:
                modifier += 0.1
            if "VOL_WEAK" in confirm_tags:
                modifier -= 0.1
            if chan_pos is not None:
                if chan_pos >= 0.85:
                    modifier -= 0.1
                elif chan_pos >= 0.75:
                    modifier -= 0.05
                if chan_pos <= 0.15:
                    modifier -= 0.1
                elif chan_pos <= 0.25:
                    modifier -= 0.05
            if range_width_pct is not None:
                if range_width_pct >= 0.08:
                    modifier -= 0.1
                elif range_width_pct >= 0.05:
                    modifier -= 0.05
            if atr_pct is not None and atr_pct >= 0.05:
                modifier -= 0.05

            exposure_cap = float(exposure_cap) * modifier
            if not asof_week_closed:
                exposure_cap *= 0.8
            exposure_cap = min(max(exposure_cap, 0.05), 0.95)

        money_proxy = {
            "vol_ratio_20": float(df["wk_vol_ratio_20"].iloc[-1])
            if pd.notna(df["wk_vol_ratio_20"].iloc[-1])
            else None,
            "obv_slope_13": float(df["obv_slope_13"].iloc[-1])
            if pd.notna(df["obv_slope_13"].iloc[-1])
            else None,
        }
        if slope_delta is not None:
            money_proxy["slope_change_4w"] = slope_delta

        plan.update(
            {
                "weekly_scene_code": f"{candidate.scene}_{candidate.status}",
                "weekly_bias": candidate.bias,
                "weekly_status": structure_status,
                "weekly_structure_status": structure_status,
                "weekly_pattern_status": pattern_status,
                "weekly_key_levels": key_levels,
                "weekly_key_levels_str": key_levels_str,
                "weekly_structure_tags": candidate.structure_tags,
                "weekly_confirm_tags": candidate.confirm_tags,
                "weekly_money_tags": candidate.money_tags,
                "weekly_money_proxy": money_proxy,
                "weekly_phase": weekly_phase,
                "weekly_risk_score": risk_score,
                "weekly_risk_level": risk_level,
                "weekly_confirm": direction_confirmed,
                "weekly_direction_confirmed": direction_confirmed,
                "weekly_gating_enabled": True,
                "weekly_plan_a": plan_a,
                "weekly_plan_b": plan_b,
                "weekly_plan_a_if": plan_a_if,
                "weekly_plan_a_then": plan_a_then,
                "weekly_plan_a_confirm": plan_a_confirm,
                "weekly_plan_a_exposure_cap": exposure_cap,
                "weekly_plan_b_if": plan_b_if,
                "weekly_plan_b_then": plan_b_then,
                "weekly_plan_b_recover_if": plan_b_recover,
                "weekly_asof_trade_date": asof_str,
                "weekly_week_closed": bool(weekly_closed),
                "weekly_current_week_closed": asof_week_closed,
            }
        )
        plan_payload = {k: v for k, v in plan.items() if k != "weekly_plan_json"}
        assert "weekly_plan_json" not in plan_payload
        plan["weekly_plan_json"] = _clip_text(
            pd.Series(plan_payload).to_json(force_ascii=False), 2000
        )
        return plan
