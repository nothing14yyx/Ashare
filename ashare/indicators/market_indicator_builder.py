from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any, Dict, List

import pandas as pd
from ashare.indicators.indicator_repo import IndicatorRepository
from ashare.indicators.indicator_utils import atr, consecutive_true, macd
from ashare.indicators.market_regime import MarketRegimeClassifier
from ashare.indicators.weekly_env_builder import WeeklyEnvironmentBuilder
from ashare.core.config import get_section


class MarketIndicatorBuilder:
    """负责日线/周线指标的构建。"""

    def __init__(
        self,
        *,
        env_builder: WeeklyEnvironmentBuilder,
        logger: logging.Logger,
    ) -> None:
        self.env_builder = env_builder
        self.logger = logger
        self.market_regime = MarketRegimeClassifier()
        self.regime_confirm_days = 2
        self.daily_regime_cfg = self._load_daily_regime_config()
        self.repo = IndicatorRepository(self.env_builder.db_writer, self.logger)

    @staticmethod
    def _load_daily_regime_config() -> dict[str, Any]:
        cfg = get_section("daily_env") or {}
        if not isinstance(cfg, dict):
            cfg = {}
        return {
            "majority_ratio": float(cfg.get("majority_ratio", 0.5)),
            "breakdown_vol_weak_threshold": float(
                cfg.get("breakdown_vol_weak_threshold", 0.9)
            ),
            "risk_off_vol_weak_threshold": float(
                cfg.get("risk_off_vol_weak_threshold", 0.9)
            ),
            "bb_width_high_threshold": float(cfg.get("bb_width_high_threshold", 0.12)),
            "bb_width_high_ratio": float(cfg.get("bb_width_high_ratio", 0.5)),
            "high_vol_cap": float(cfg.get("high_vol_cap", 0.5)),
        }

    @property
    def index_codes(self) -> list[str]:
        # 兼容性修复：从 env_builder.params 中获取 index_codes
        params = getattr(self.env_builder, "params", None)
        if params and hasattr(params, "index_codes"):
            return params.index_codes
        # 兜底
        return ["sh.000001", "sh.000300", "sh.000905", "sz.399001", "sz.399006"]

    @property
    def benchmark_code(self) -> str:
        params = getattr(self.env_builder, "params", None)
        if params and hasattr(params, "index_code"):
            return params.index_code
        if params and hasattr(params, "benchmark_code"):
            return params.benchmark_code
        return "sh.000001"

    def compute_weekly_indicator(
        self,
        asof_trade_date: str,
        *,
        checked_at: dt.datetime | None = None,
    ) -> list[dict[str, Any]]:
        env_context = self.env_builder.build_environment_context(
            asof_trade_date, checked_at=checked_at
        )
        weekly_scenario = env_context.get("weekly_scenario") if isinstance(env_context, dict) else {}
        if not isinstance(weekly_scenario, dict):
            weekly_scenario = {}

        weekly_asof = weekly_scenario.get("weekly_asof_trade_date") or asof_trade_date
        weekly_gate_policy = env_context.get("weekly_gate_policy")
        weekly_zone_id = env_context.get("weekly_zone_id")
        weekly_zone_score = env_context.get("weekly_zone_score")
        weekly_exp_return_bucket = env_context.get("weekly_exp_return_bucket")
        weekly_zone_reason = env_context.get("weekly_zone_reason")
        weekly_money_proxy = env_context.get("weekly_money_proxy")
        weekly_tags = env_context.get("weekly_tags")
        weekly_note = env_context.get("weekly_note")
        weekly_plan_json = weekly_scenario.get("weekly_plan_json")

        benchmark_code = self.benchmark_code
        return [
            {
                "weekly_asof_trade_date": weekly_asof,
                "benchmark_code": benchmark_code,
                "weekly_scene_code": weekly_scenario.get("weekly_scene_code"),
                "weekly_phase": weekly_scenario.get("weekly_phase"),
                "weekly_structure_status": weekly_scenario.get("weekly_structure_status"),
                "weekly_pattern_status": weekly_scenario.get("weekly_pattern_status"),
                "weekly_risk_score": weekly_scenario.get("weekly_risk_score"),
                "weekly_risk_level": weekly_scenario.get("weekly_risk_level"),
                "weekly_gate_policy": weekly_gate_policy,
                "weekly_plan_a_exposure_cap": weekly_scenario.get("weekly_plan_a_exposure_cap"),
                "weekly_key_levels_str": weekly_scenario.get("weekly_key_levels_str"),
                "weekly_zone_id": weekly_zone_id,
                "weekly_zone_score": weekly_zone_score,
                "weekly_exp_return_bucket": weekly_exp_return_bucket,
                "weekly_zone_reason": weekly_zone_reason,
                "weekly_money_proxy": weekly_money_proxy,
                "weekly_tags": weekly_tags,
                "weekly_note": weekly_note,
                "weekly_plan_json": weekly_plan_json,
            }
        ]

    def compute_and_persist_board_rotation(self, trade_date: str) -> None:
        """计算并持久化指定日期的板块轮动指标。"""
        # 1. 读取历史行情
        df = self.repo.fetch_board_history(trade_date, lookback_days=90)

        if df.empty:
            return

        # 2. 计算指标
        pivot_df = df.pivot_table(index="date", columns="board_name", values="close").ffill()
        if len(pivot_df) < 21:
            return

        ret_20d = pivot_df.pct_change(20).iloc[-1]
        ret_5d = pivot_df.pct_change(5).iloc[-1]
        
        metrics = pd.DataFrame({"ret_20d": ret_20d, "ret_5d": ret_5d}).dropna()
        if metrics.empty:
            return
            
        metrics["rank_trend"] = metrics["ret_20d"].rank(pct=True)
        metrics["rank_mom"] = metrics["ret_5d"].rank(pct=True)

        def _classify(row: pd.Series) -> str:
            strong_trend = row["rank_trend"] >= 0.5
            strong_mom = row["rank_mom"] >= 0.5
            if strong_trend and strong_mom:
                return "leading"
            if not strong_trend and strong_mom:
                return "improving"
            if strong_trend and not strong_mom:
                return "weakening"
            return "lagging"

        metrics["rotation_phase"] = metrics.apply(_classify, axis=1)

        # 3. 准备入库数据
        df_to_save = metrics.copy()
        df_to_save["board_name"] = df_to_save.index
        df_to_save["date"] = trade_date
        df_to_save["created_at"] = dt.datetime.now()
        df_to_save["board_code"] = None  # 暂不关联 code，如果需要可以从 spot 表补

        # 尝试补全 board_code
        spot_df = self.repo.fetch_board_spot()
        if not spot_df.empty:
            mapping = spot_df.set_index("board_name")["board_code"].to_dict()
            df_to_save["board_code"] = df_to_save["board_name"].map(mapping)

        # 4. 持久化
        self.repo.persist_board_rotation(trade_date, df_to_save)
        self.logger.debug("已更新 %s 板块轮动数据 (%s 条)", trade_date, len(df_to_save))

    def resolve_weekly_asof_dates(
        self, start_date: dt.date, end_date: dt.date
    ) -> list[dt.date]:
        primary_code = (self.index_codes or ["sh.000001"])[0]
        df = self.repo.fetch_index_trade_dates(primary_code, start_date, end_date)

        if df.empty or "trade_date" not in df.columns:
            return []

        dates = pd.to_datetime(df["trade_date"], errors="coerce").dropna()
        if dates.empty:
            return []

        weekly_end = dates.groupby(dates.dt.to_period("W-FRI")).max()
        return weekly_end.dt.date.dropna().tolist()

    def compute_daily_indicators(
        self, start_date: dt.date, end_date: dt.date
    ) -> list[dict[str, Any]]:
        index_codes = [c for c in self.index_codes if c]
        benchmark_code = self.benchmark_code
        if benchmark_code and benchmark_code not in index_codes:
            index_codes.append(benchmark_code)
        if not index_codes:
            return []

        lookback_start = start_date - dt.timedelta(days=400)
        df = self.repo.fetch_index_daily_kline(
            index_codes, lookback_start, end_date
        )

        if df.empty:
            return []

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        if df.empty:
            return []

        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        results: list[pd.DataFrame] = []
        for code, group in df.groupby("code", sort=False):
            grp = group.sort_values("date").copy()
            close = grp["close"]
            volume = grp["volume"]
            grp["ma20"] = close.rolling(20, min_periods=20).mean()
            grp["ma60"] = close.rolling(60, min_periods=60).mean()
            grp["ma250"] = close.rolling(250, min_periods=250).mean()
            grp["vol_ma20"] = volume.rolling(20, min_periods=20).mean()
            grp["vol_ratio_20"] = volume / grp["vol_ma20"].replace(0, pd.NA)
            std20 = close.rolling(20, min_periods=20).std()
            grp["bb_mid"] = grp["ma20"]
            grp["bb_upper"] = grp["bb_mid"] + 2 * std20
            grp["bb_lower"] = grp["bb_mid"] - 2 * std20
            bb_range = grp["bb_upper"] - grp["bb_lower"]
            grp["bb_width"] = bb_range / grp["bb_mid"]
            grp["bb_pos"] = (close - grp["bb_lower"]) / bb_range
            zero_range = bb_range == 0
            if zero_range.any():
                grp.loc[zero_range, "bb_pos"] = None
            zero_mid = grp["bb_mid"] == 0
            if zero_mid.any():
                grp.loc[zero_mid, "bb_width"] = None
            dif, dea, hist = macd(close)
            grp["macd_hist"] = hist
            preclose = close.shift(1)
            grp["atr14"] = atr(grp["high"], grp["low"], preclose)
            grp["dev_ma20_atr"] = (close - grp["ma20"]) / grp["atr14"]

            rolling_low = close.rolling(
                self.market_regime.breakdown_window, min_periods=1
            ).min().shift(1)
            grp["rolling_low"] = rolling_low
            ma250_valid = grp["ma250"].notna()
            below_ma250 = (close < grp["ma250"]) & ma250_valid
            prev_below = below_ma250.shift(1, fill_value=False)
            above_ma250 = (close >= grp["ma250"]) & ma250_valid

            grp["below_ma250_streak"] = consecutive_true(below_ma250)
            grp["reclaim_streak"] = consecutive_true(above_ma250)
            grp["break_confirmed"] = (
                grp["below_ma250_streak"] >= self.market_regime.effective_breakdown_days
            )
            grp["reclaim_confirmed"] = (
                (grp["reclaim_streak"] >= self.market_regime.effective_reclaim_days)
                & prev_below
            )

            daily_ret = close.pct_change()
            grp["pullback_mode"] = None
            grp.loc[
                daily_ret <= -0.03, "pullback_mode"
            ] = "FAST_DROP"

            status = pd.Series("RISK_ON", index=grp.index, dtype="object")
            status = status.mask(close.isna(), "UNKNOWN")
            status = status.mask(grp["ma60"].isna() | grp["ma250"].isna(), "UNKNOWN")

            risk_off_threshold = -0.5
            risk_off_condition = (
                (close < grp["ma60"])
                & (grp["macd_hist"] < 0)
                & (grp["dev_ma20_atr"] < risk_off_threshold)
            )
            grp["risk_off_flag"] = risk_off_condition
            vol_ratio = grp["vol_ratio_20"]
            breakdown_weak_mask = vol_ratio < self.daily_regime_cfg["breakdown_vol_weak_threshold"]
            breakdown_weak_mask = breakdown_weak_mask.fillna(False)
            risk_off_weak_mask = vol_ratio < self.daily_regime_cfg["risk_off_vol_weak_threshold"]
            risk_off_weak_mask = risk_off_weak_mask.fillna(False)

            status = status.mask(
                (status != "UNKNOWN") & grp["break_confirmed"], "BEAR_CONFIRMED"
            )
            status = status.mask(
                (status != "UNKNOWN")
                & (grp["rolling_low"].notna())
                & (close < grp["rolling_low"]),
                "BREAKDOWN",
            )
            status = status.mask(
                (status == "RISK_ON") & risk_off_condition,
                "RISK_OFF",
            )
            status = status.mask(
                (status == "BREAKDOWN") & breakdown_weak_mask,
                "BREAKDOWN_WEAK",
            )
            status = status.mask(
                (status == "RISK_OFF") & risk_off_weak_mask,
                "RISK_OFF_WEAK",
            )
            pullback_mask = (grp["ma60"].notna()) & (close < grp["ma60"])
            status = status.mask((status == "RISK_ON") & pullback_mask, "PULLBACK")
            pullback_mask = (grp["ma20"].notna()) & (close < grp["ma20"])
            status = status.mask((status == "RISK_ON") & pullback_mask, "PULLBACK")
            grp["status"] = status

            score = (
                (close > grp["ma20"]).astype(int)
                + (close > grp["ma60"]).astype(int)
                + (close > grp["ma250"]).astype(int)
            )
            grp["score_raw"] = score
            grp["code"] = str(code)
            results.append(grp)

        merged = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
        if merged.empty:
            return []

        merged["trade_date"] = merged["date"].dt.date
        merged["above_ma20"] = (merged["close"] > merged["ma20"]) & merged["ma20"].notna()
        merged["above_ma60"] = (merged["close"] > merged["ma60"]) & merged["ma60"].notna()
        day_summary = (
            merged.groupby("trade_date")[["status", "score_raw", "pullback_mode", "bb_width"]]
            .apply(self._resolve_daily_regime, cfg=self.daily_regime_cfg)
            .reset_index()
        )
        day_summary = day_summary.sort_values("trade_date").reset_index(drop=True)
        if "regime" in day_summary.columns:
            day_summary["regime_raw"] = day_summary["regime"]
            day_summary = self._apply_regime_hysteresis(
                day_summary, confirm_days=self.regime_confirm_days
            )
        if "position_hint" in day_summary.columns:
            day_summary["position_hint_raw"] = day_summary["position_hint"]
            pos = pd.to_numeric(day_summary["position_hint"], errors="coerce")
            day_summary["position_hint"] = pos.ewm(alpha=0.7, adjust=False).mean()

        breadth_summary = (
            merged.groupby("trade_date")
            .apply(self._resolve_breadth_metrics, include_groups=False)
            .reset_index()
        )
        merged = merged.merge(day_summary, on="trade_date", how="left")

        merged = merged[
            (merged["trade_date"] >= start_date) & (merged["trade_date"] <= end_date)
        ]
        day_summary = day_summary[
            (day_summary["trade_date"] >= start_date)
            & (day_summary["trade_date"] <= end_date)
        ]
        if merged.empty:
            return []

        benchmark_daily = merged.loc[merged["code"] == benchmark_code, :].copy()
        benchmark_daily = benchmark_daily[
            [
                "trade_date",
                "ma20",
                "ma60",
                "ma250",
                "macd_hist",
                "atr14",
                "dev_ma20_atr",
                "bb_mid",
                "bb_upper",
                "bb_lower",
                "bb_width",
                "bb_pos",
            ]
        ]

        daily_env = day_summary.merge(benchmark_daily, on="trade_date", how="left")
        daily_env = daily_env.merge(breadth_summary, on="trade_date", how="left")

        component_fields = [
            "code",
            "ma20",
            "ma60",
            "ma250",
            "macd_hist",
            "atr14",
            "dev_ma20_atr",
            "score_raw",
            "status",
        ]

        def _safe_value(value: Any) -> Any:
            try:
                if pd.isna(value):
                    return None
            except Exception:
                pass
            if isinstance(value, (dt.datetime, dt.date)):
                return value.isoformat()
            return value

        components_map: dict[dt.date, str] = {}
        for trade_date, group in merged.groupby("trade_date", sort=False):
            components = [
                {field: _safe_value(row.get(field)) for field in component_fields}
                for _, row in group.iterrows()
            ]
            components_map[trade_date] = json.dumps(
                components, ensure_ascii=False, separators=(",", ":")
            )

        daily_env["components_json"] = daily_env["trade_date"].map(components_map)

        invalid_ma_mask = daily_env["ma60"].isna() | daily_env["ma250"].isna()
        if invalid_ma_mask.any():
            daily_env.loc[invalid_ma_mask, "regime"] = "UNKNOWN"
            daily_env.loc[invalid_ma_mask, "position_hint"] = None
            daily_env.loc[invalid_ma_mask, "score"] = None

        zone_payloads = [self._resolve_daily_zone(row) for _, row in daily_env.iterrows()]
        zone_df = pd.DataFrame(zone_payloads)
        daily_env = pd.concat([daily_env.reset_index(drop=True), zone_df], axis=1)

        rows: list[dict[str, Any]] = []
        for _, row in daily_env.iterrows():
            rows.append(
                {
                    "asof_trade_date": row.get("trade_date"),
                    "benchmark_code": benchmark_code,
                    "regime": row.get("regime"),
                    "score": row.get("score"),
                    "position_hint": row.get("position_hint"),
                    "ma20": row.get("ma20"),
                    "ma60": row.get("ma60"),
                    "ma250": row.get("ma250"),
                    "macd_hist": row.get("macd_hist"),
                    "atr14": row.get("atr14"),
                    "dev_ma20_atr": row.get("dev_ma20_atr"),
                    "bb_mid": row.get("bb_mid"),
                    "bb_upper": row.get("bb_upper"),
                    "bb_lower": row.get("bb_lower"),
                    "bb_width": row.get("bb_width"),
                    "bb_pos": row.get("bb_pos"),
                    "cycle_phase": None,
                    "breadth_pct_above_ma20": row.get("breadth_pct_above_ma20"),
                    "breadth_pct_above_ma60": row.get("breadth_pct_above_ma60"),
                    "breadth_risk_off_ratio": row.get("breadth_risk_off_ratio"),
                    "dispersion_score": row.get("dispersion_score"),
                    "daily_zone_id": row.get("daily_zone_id"),
                    "daily_zone_score": row.get("daily_zone_score"),
                    "daily_cap_multiplier": row.get("daily_cap_multiplier"),
                    "daily_zone_reason": row.get("daily_zone_reason"),
                    "components_json": row.get("components_json"),
                }
            )
        return rows

    @staticmethod
    def _resolve_daily_regime(group: pd.DataFrame, *, cfg: dict[str, Any]) -> pd.Series:
        statuses = group["status"].dropna().astype(str).tolist()
        score = group["score_raw"].mean() if not group.empty else None
        pullback_fast = (
            group["pullback_mode"].fillna("").astype(str).eq("FAST_DROP").any()
        )

        def _normalize_status(val: str) -> str:
            if val == "BREAKDOWN_WEAK":
                return "RISK_OFF"
            if val == "RISK_OFF_WEAK":
                return "PULLBACK"
            return val

        normalized = [_normalize_status(v) for v in statuses]
        status_counts = pd.Series(normalized).value_counts() if normalized else pd.Series(dtype=float)
        total = float(status_counts.sum()) if not status_counts.empty else 0.0

        majority_ratio = float(cfg.get("majority_ratio", 0.5))
        majority_ratio = max(min(majority_ratio, 1.0), 0.0)

        regime = "RISK_ON"
        if normalized and all(val == "UNKNOWN" for val in normalized):
            regime = "UNKNOWN"
            score = None
        elif total > 0:
            for candidate in ["BEAR_CONFIRMED", "BREAKDOWN", "RISK_OFF", "PULLBACK"]:
                if status_counts.get(candidate, 0.0) / total >= majority_ratio:
                    regime = candidate
                    break
            else:
                if "PULLBACK" in normalized:
                    regime = "PULLBACK"

        position_hint_map = {
            "RISK_ON": 0.8,
            "PULLBACK": 0.4,
            "RISK_OFF": 0.1,
            "BREAKDOWN": 0.0,
            "BEAR_CONFIRMED": 0.0,
            "UNKNOWN": None,
        }
        position_hint = position_hint_map.get(regime)
        if regime == "PULLBACK" and pullback_fast and position_hint is not None:
            position_hint = min(position_hint, 0.3)
        high_vol = False
        if "bb_width" in group.columns:
            bb_threshold = float(cfg.get("bb_width_high_threshold", 0.12))
            bb_ratio = float(cfg.get("bb_width_high_ratio", 0.5))
            high_vol_mask = group["bb_width"] >= bb_threshold
            if high_vol_mask.any():
                ratio = float(high_vol_mask.sum()) / float(len(group))
                if ratio >= bb_ratio:
                    high_vol = True
        if high_vol and position_hint is not None:
            position_hint = min(position_hint, float(cfg.get("high_vol_cap", 0.5)))

        return pd.Series(
            {
                "score": score,
                "regime": regime,
                "position_hint": position_hint,
                "pullback_fast": pullback_fast,
                "high_vol": high_vol,
            }
        )

    @staticmethod
    def _apply_regime_hysteresis(
        day_summary: pd.DataFrame, *, confirm_days: int
    ) -> pd.DataFrame:
        if day_summary.empty or "regime" not in day_summary.columns:
            return day_summary

        confirm_days = max(int(confirm_days), 1)
        severe = {"BREAKDOWN", "BEAR_CONFIRMED"}
        position_hint_map = {
            "RISK_ON": 0.8,
            "PULLBACK": 0.4,
            "RISK_OFF": 0.1,
            "BREAKDOWN": 0.0,
            "BEAR_CONFIRMED": 0.0,
            "UNKNOWN": None,
        }

        current = None
        pending = None
        pending_count = 0

        smoothed = []
        for _, row in day_summary.iterrows():
            raw = str(row.get("regime") or "").upper()
            if raw == "UNKNOWN":
                smoothed.append(current or "UNKNOWN")
                continue

            if current is None:
                current = raw
                pending = None
                pending_count = 0
                smoothed.append(current)
                continue

            if raw == current:
                pending = None
                pending_count = 0
                smoothed.append(current)
                continue

            if raw in severe:
                current = raw
                pending = None
                pending_count = 0
                smoothed.append(current)
                continue

            if pending != raw:
                pending = raw
                pending_count = 1
            else:
                pending_count += 1

            if pending_count >= confirm_days:
                current = pending
                pending = None
                pending_count = 0

            smoothed.append(current)

        day_summary = day_summary.copy()
        day_summary["regime"] = smoothed

        def _recalc_hint(row: pd.Series) -> float | None:
            regime = str(row.get("regime") or "").upper()
            hint = position_hint_map.get(regime)
            if regime == "PULLBACK" and bool(row.get("pullback_fast")) and hint is not None:
                hint = min(hint, 0.3)
            return hint

        day_summary["position_hint"] = day_summary.apply(_recalc_hint, axis=1)
        return day_summary

    @staticmethod
    def _resolve_breadth_metrics(group: pd.DataFrame) -> pd.Series:
        valid_ma20 = group["ma20"].notna()
        valid_ma60 = group["ma60"].notna()

        breadth_ma20 = (
            float(group.loc[valid_ma20, "above_ma20"].sum()) / float(valid_ma20.sum())
            if valid_ma20.any()
            else None
        )
        breadth_ma60 = (
            float(group.loc[valid_ma60, "above_ma60"].sum()) / float(valid_ma60.sum())
            if valid_ma60.any()
            else None
        )

        risk_off_mask = (
            group["risk_off_flag"].fillna(False)
            & group["ma60"].notna()
            & group["macd_hist"].notna()
            & group["dev_ma20_atr"].notna()
        )
        denom = (
            group["ma60"].notna()
            & group["macd_hist"].notna()
            & group["dev_ma20_atr"].notna()
        )
        breadth_risk_off = (
            float(risk_off_mask.sum()) / float(denom.sum()) if denom.any() else None
        )

        dispersion = None
        if group["dev_ma20_atr"].notna().sum() >= 2:
            try:
                val = float(group["dev_ma20_atr"].std())
            except Exception:
                val = None
            if val is not None and not pd.isna(val):
                dispersion = val

        return pd.Series(
            {
                "breadth_pct_above_ma20": breadth_ma20,
                "breadth_pct_above_ma60": breadth_ma60,
                "breadth_risk_off_ratio": breadth_risk_off,
                "dispersion_score": dispersion,
            }
        )

    @staticmethod
    def _resolve_daily_zone(row: pd.Series) -> dict[str, Any]:
        def _as_float(val: Any) -> float | None:
            try:
                if pd.isna(val):
                    return None
            except Exception:
                pass
            try:
                return float(val)
            except Exception:
                return None

        regime = str(row.get("regime") or "").upper()
        bb_pos = _as_float(row.get("bb_pos"))
        bb_width = _as_float(row.get("bb_width"))

        zone_id = "DZ_NEUTRAL"
        zone_score = 50
        cap_multiplier = 1.0
        reason_parts: list[str] = []

        if regime == "UNKNOWN":
            zone_id = "DZ_UNKNOWN"
            zone_score = 50
            cap_multiplier = 1.0
            reason_parts.append("regime=UNKNOWN")
        elif regime in {"BREAKDOWN", "BEAR_CONFIRMED"}:
            zone_id = "DZ_BREAKDOWN"
            zone_score = 10
            cap_multiplier = 0.2
            reason_parts.append(f"regime={regime}")
        elif bb_pos is not None:
            if bb_pos <= 0.15:
                zone_id = "DZ_LOW_EDGE"
                zone_score = 40
                cap_multiplier = 0.6
                reason_parts.append(f"bb_pos={bb_pos:.2f}")
            elif bb_pos >= 0.85:
                if bb_width is not None and bb_width >= 0.12:
                    zone_id = "DZ_OVERHEAT"
                    zone_score = 30
                    cap_multiplier = 0.5
                    reason_parts.append(f"bb_pos={bb_pos:.2f}")
                    reason_parts.append(f"bb_width={bb_width:.2f}")
                else:
                    zone_id = "DZ_HIGH_EDGE"
                    zone_score = 60
                    cap_multiplier = 0.9
                    reason_parts.append(f"bb_pos={bb_pos:.2f}")
            else:
                reason_parts.append(f"bb_pos={bb_pos:.2f}")
        else:
            reason_parts.append("bb_pos=NA")

        if regime and regime != "UNKNOWN":
            reason_parts.append(f"regime={regime}")

        return {
            "daily_zone_id": zone_id,
            "daily_zone_score": zone_score,
            "daily_cap_multiplier": cap_multiplier,
            "daily_zone_reason": ";".join(reason_parts)[:255] if reason_parts else None,
        }
