from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any, Dict, List

import pandas as pd
from sqlalchemy import bindparam, text

from .indicator_utils import consecutive_true
from .ma5_ma20_trend_strategy import _atr, _macd
from .market_regime import MarketRegimeClassifier
from .weekly_env_builder import WeeklyEnvironmentBuilder


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

    @property
    def index_codes(self) -> list[str]:
        return self.env_builder.index_codes

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

        benchmark_code = self.env_builder.benchmark_code
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

    def resolve_weekly_asof_dates(
        self, start_date: dt.date, end_date: dt.date
    ) -> list[dt.date]:
        primary_code = (self.index_codes or ["sh.000001"])[0]
        stmt = text(
            """
            SELECT CAST(`date` AS CHAR) AS trade_date
            FROM history_index_daily_kline
            WHERE `code` = :code AND `date` BETWEEN :start_date AND :end_date
            ORDER BY `date`
            """
        )
        try:
            with self.env_builder.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "code": primary_code,
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取周线交易日失败：%s", exc)
            return []

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
        benchmark_code = self.env_builder.benchmark_code or "sh.000001"
        if benchmark_code and benchmark_code not in index_codes:
            index_codes.append(benchmark_code)
        if not index_codes:
            return []

        lookback_start = start_date - dt.timedelta(days=400)
        stmt = (
            text(
                """
                SELECT `code`,`date`,`open`,`high`,`low`,`close`,`volume`,`amount`
                FROM history_index_daily_kline
                WHERE `code` IN :codes AND `date` BETWEEN :start_date AND :end_date
                ORDER BY `code`, `date`
                """
            ).bindparams(bindparam("codes", expanding=True))
        )
        try:
            with self.env_builder.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "codes": index_codes,
                        "start_date": lookback_start.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取指数日线指标失败：%s", exc)
            return []

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
            grp["ma20"] = close.rolling(20, min_periods=20).mean()
            grp["ma60"] = close.rolling(60, min_periods=60).mean()
            grp["ma250"] = close.rolling(250, min_periods=250).mean()
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
            dif, dea, hist = _macd(close)
            grp["macd_hist"] = hist
            preclose = close.shift(1)
            grp["atr14"] = _atr(grp["high"], grp["low"], preclose)
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
            merged.groupby("trade_date")[["status", "score_raw", "pullback_mode"]]
            .apply(self._resolve_daily_regime)
            .reset_index()
        )
        day_summary = day_summary.sort_values("trade_date").reset_index(drop=True)
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
    def _resolve_daily_regime(group: pd.DataFrame) -> pd.Series:
        statuses = group["status"].dropna().astype(str).tolist()
        score = group["score_raw"].mean() if not group.empty else None
        pullback_fast = (
            group["pullback_mode"].fillna("").astype(str).eq("FAST_DROP").any()
        )

        regime = "RISK_ON"
        if statuses and all(val == "UNKNOWN" for val in statuses):
            regime = "UNKNOWN"
            score = None
        elif "BEAR_CONFIRMED" in statuses:
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
            "RISK_OFF": 0.1,
            "BREAKDOWN": 0.0,
            "BEAR_CONFIRMED": 0.0,
            "UNKNOWN": None,
        }
        position_hint = position_hint_map.get(regime)
        if regime == "PULLBACK" and pullback_fast and position_hint is not None:
            position_hint = min(position_hint, 0.3)

        return pd.Series(
            {
                "score": score,
                "regime": regime,
                "position_hint": position_hint,
            }
        )

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
