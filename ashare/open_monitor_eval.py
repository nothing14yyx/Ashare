"""open_monitor 评估与排名层。"""

from __future__ import annotations

import datetime as dt
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List

import pandas as pd

from .config import get_section
from .open_monitor_repo import calc_run_id, make_snapshot_hash
from .open_monitor_rules import DecisionContext, MarketEnvironment, RuleEngine
from .utils.convert import to_float as _to_float


def merge_gate_actions(*actions: str | None) -> str | None:
    severity = {
        "STOP": 3,
        "WAIT": 2,
        "ALLOW_SMALL": 1,
        "ALLOW": 0,
        "NOT_APPLIED": -1,
        None: -1,
    }
    normalized = []
    for action in actions:
        if action is None:
            normalized.append((severity[None], None))
            continue
        action_norm = str(action).strip().upper()
        if action_norm == "GO":
            action_norm = "ALLOW"
        score = severity.get(action_norm, 0)
        normalized.append((score, action_norm))
    if not normalized:
        return None
    normalized.sort(key=lambda x: x[0], reverse=True)
    top_action = normalized[0][1]
    if top_action == "GO":
        return "ALLOW"
    return top_action


@dataclass(frozen=True)
class RunupMetrics:
    runup_ref_price: float | None
    runup_ref_source: str | None
    runup_from_sigclose: float | None
    runup_from_sigclose_atr: float | None


def compute_runup_metrics(
    sig_close: float | None,
    *,
    asof_close: float | None,
    live_high: float | None,
    sig_atr14: float | None,
    eps: float = 1e-6,
) -> RunupMetrics:
    live_high_val = _to_float(live_high)
    asof_close_val = _to_float(asof_close)
    sig_close_val = _to_float(sig_close)
    sig_atr_val = _to_float(sig_atr14)

    ref_price = None
    ref_source = None
    if live_high_val is not None and asof_close_val is not None:
        ref_price = max(live_high_val, asof_close_val)
        ref_source = "max(asof_close, live_high)"
    elif live_high_val is not None:
        ref_price = live_high_val
        ref_source = "live_high"
    elif asof_close_val is not None:
        ref_price = asof_close_val
        ref_source = "asof_close"

    runup_from_sigclose = None
    runup_from_sigclose_atr = None
    if ref_price is not None and sig_close_val is not None:
        runup_from_sigclose = ref_price - sig_close_val
        if sig_atr_val is not None:
            runup_from_sigclose_atr = runup_from_sigclose / max(sig_atr_val, eps)

    return RunupMetrics(
        runup_ref_price=ref_price,
        runup_ref_source=ref_source,
        runup_from_sigclose=runup_from_sigclose,
        runup_from_sigclose_atr=runup_from_sigclose_atr,
    )


def evaluate_runup_breach(
    metrics: RunupMetrics,
    *,
    runup_atr_max: float | None,
    runup_atr_tol: float,
    dev_ma20_atr: float | None = None,
    dev_ma20_atr_min: float | None = None,
) -> tuple[bool, str | None]:
    if runup_atr_max is None or metrics.runup_from_sigclose_atr is None:
        return False, None

    if dev_ma20_atr_min is not None:
        if dev_ma20_atr is None or dev_ma20_atr < dev_ma20_atr_min:
            return False, None

    threshold = runup_atr_max + runup_atr_tol
    if metrics.runup_from_sigclose_atr > threshold:
        ref_price_display = (
            f"{metrics.runup_ref_price:.2f}" if metrics.runup_ref_price is not None else "N/A"
        )
        ref_source = metrics.runup_ref_source or "unknown"
        reason = (
            f"runup_ref_source={ref_source}, runup_ref_price={ref_price_display}, "
            f"runup_from_sigclose_atr={metrics.runup_from_sigclose_atr:.3f} "
            f"> runup_atr_max={runup_atr_max:.3f}+tol={runup_atr_tol:.3f}"
        )
        return True, reason
    return False, None


class OpenMonitorEvaluator:
    """负责监测评估与排名组装。"""

    def __init__(self, logger, params, rule_engine: RuleEngine, rule_config, rules) -> None:
        self.logger = logger
        self.params = params
        self.rule_engine = rule_engine
        self.rule_config = rule_config
        self.rules = rules

    @staticmethod
    def _calc_minutes_elapsed(now: dt.datetime) -> int:
        start_am = dt.time(9, 30)
        end_am = dt.time(11, 30)
        start_pm = dt.time(13, 0)
        end_pm = dt.time(15, 0)

        t = now.time()
        if t < start_am:
            return 0
        if t <= end_am:
            delta = dt.datetime.combine(now.date(), t) - dt.datetime.combine(
                now.date(), start_am
            )
            return int(delta.total_seconds() // 60)
        if t < start_pm:
            return 120
        if t <= end_pm:
            delta = dt.datetime.combine(now.date(), t) - dt.datetime.combine(
                now.date(), start_pm
            )
            return 120 + int(delta.total_seconds() // 60)
        return 240

    @staticmethod
    def _is_pullback_signal(signal_reason: str) -> bool:
        reason_text = str(signal_reason or "")
        lower = reason_text.lower()
        return ("回踩" in reason_text) and (("ma20" in lower) or ("ma 20" in lower))

    def _parse_market_environment(self, env_context: dict[str, Any] | None) -> MarketEnvironment:
        return MarketEnvironment.from_snapshot(env_context)

    def prepare_monitor_frame(
        self,
        signals: pd.DataFrame,
        quotes: pd.DataFrame,
        latest_snapshots: pd.DataFrame,
        latest_trade_date: str,
        checked_at_ts: dt.datetime,
        *,
        avg_volume_map: dict[str, float] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, float] | None, int, int]:
        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        if quotes is None or getattr(quotes, "empty", True):
            q = pd.DataFrame(
                columns=[
                    "code",
                    "live_open",
                    "live_high",
                    "live_low",
                    "live_latest",
                    "live_volume",
                    "live_amount",
                    "live_pct_change",
                    "live_gap_pct",
                    "live_intraday_vol_ratio",
                    "prev_close",
                ]
            )
        else:
            q = quotes.copy()
            if "code" not in q.columns:
                msg = "quotes 缺少 code 列"
                if strict_quotes:
                    raise RuntimeError(msg)
                self.logger.error("%s（strict_quotes=false，将跳过行情合并）", msg)
                q["code"] = ""
            q["code"] = q["code"].astype(str)
            required = [
                "live_open",
                "live_high",
                "live_low",
                "live_latest",
                "live_volume",
                "live_amount",
            ]
            missing = [c for c in required if c not in q.columns]
            if missing:
                msg = f"quotes 缺少统一行情列：{missing}"
                if strict_quotes:
                    raise RuntimeError(msg)
                self.logger.error("%s（strict_quotes=false，将补空列）", msg)
                for c in missing:
                    q[c] = None

        merged = signals.copy()
        merged["code"] = merged["code"].astype(str)
        merged = merged.merge(q, on="code", how="left", suffixes=("", "_q"))

        if not latest_snapshots.empty:
            snap = latest_snapshots.copy()
            snap["_has_latest_snapshot"] = True
            rename_map = {
                c: f"asof_{c}" for c in snap.columns if c not in {"code", "_has_latest_snapshot"}
            }
            snap = snap.rename(columns=rename_map)
            merged = merged.merge(snap, on="code", how="left")

        if "_has_latest_snapshot" not in merged.columns:
            merged["_has_latest_snapshot"] = False
        else:
            merged["_has_latest_snapshot"] = merged["_has_latest_snapshot"].eq(True)

        for col in [
            "asof_trade_date",
            "asof_close",
            "asof_ma5",
            "asof_ma20",
            "asof_ma60",
            "asof_ma250",
            "asof_vol_ratio",
            "asof_macd_hist",
            "asof_atr14",
            "asof_stop_ref",
        ]:
            if col not in merged.columns:
                merged[col] = None

        minutes_elapsed = self._calc_minutes_elapsed(checked_at_ts)
        total_minutes = 240
        merged["avg_volume_20"] = merged["code"].map(avg_volume_map) if avg_volume_map else None

        float_cols = [
            "sig_close",
            "sig_ma5",
            "sig_ma20",
            "sig_ma60",
            "sig_ma250",
            "sig_vol_ratio",
            "sig_macd_hist",
            "sig_kdj_k",
            "sig_kdj_d",
            "sig_atr14",
            "sig_stop_ref",
            "live_open",
            "live_latest",
            "live_high",
            "live_low",
            "live_pct_change",
            "live_volume",
            "live_amount",
            "prev_close",
        ]
        for col in float_cols:
            if col in merged.columns:
                merged[col] = merged.get(col).apply(_to_float)

        def _infer_volume_scale_factor(df: pd.DataFrame) -> float:
            if "live_volume" not in df.columns or "avg_volume_20" not in df.columns:
                return 1.0

            sample = df[["live_volume", "avg_volume_20"]].dropna()
            sample = sample[(sample["live_volume"] > 0) & (sample["avg_volume_20"] > 0)]
            if sample.empty:
                return 1.0

            ratio = (sample["live_volume"] / sample["avg_volume_20"]).median()
            ratio_mul = ((sample["live_volume"] * 100.0) / sample["avg_volume_20"]).median()
            ratio_div = ((sample["live_volume"] / 100.0) / sample["avg_volume_20"]).median()

            if ratio < 0.02 <= ratio_mul <= 200:
                return 100.0
            if ratio > 200 >= ratio_div >= 0.02:
                return 0.01
            return 1.0

        live_vol_scale = _infer_volume_scale_factor(merged)
        if live_vol_scale != 1.0 and "live_volume" in merged.columns:
            merged["live_volume"] = merged["live_volume"].apply(
                lambda x: None if x is None else x * live_vol_scale
            )

        return merged, avg_volume_map, minutes_elapsed, total_minutes

    def evaluate(
        self,
        signals: pd.DataFrame,
        quotes: pd.DataFrame,
        latest_snapshots: pd.DataFrame,
        latest_trade_date: str,
        env_context: dict[str, Any] | None = None,
        *,
        checked_at: dt.datetime | None = None,
        run_id: str | None = None,
        ready_signals_used: bool = False,
        avg_volume_map: dict[str, float] | None = None,
    ) -> pd.DataFrame:
        if signals.empty:
            return pd.DataFrame()

        checked_at_ts = checked_at or dt.datetime.now()
        run_id_val = run_id or calc_run_id(checked_at_ts, self.params.run_id_minutes)
        monitor_date = checked_at_ts.date().isoformat()
        run_id_norm = str(run_id_val or "").strip().upper()
        if not ready_signals_used:
            self.logger.error(
                "未启用 ready_signals_view（严格模式），已终止评估（monitor_date=%s, run_id=%s）。",
                monitor_date,
                run_id_val,
            )
            return pd.DataFrame()

        env = self._parse_market_environment(env_context)

        if env.gate_action is None:
            self.logger.error(
                "环境快照缺少 env_final_gate_action，已终止评估（monitor_date=%s, run_id=%s）。",
                monitor_date,
                run_id_val,
            )
            return pd.DataFrame()
        if env.position_cap_pct is None:
            self.logger.error(
                "环境快照缺少 env_final_cap_pct，已终止评估（monitor_date=%s, run_id=%s）。",
                monitor_date,
                run_id_val,
            )
            return pd.DataFrame()

        merged, avg_volume_map, minutes_elapsed, total_minutes = self.prepare_monitor_frame(
            signals,
            quotes,
            latest_snapshots,
            latest_trade_date,
            checked_at_ts,
            avg_volume_map=avg_volume_map,
        )

        def _resolve_ref_close(row: pd.Series) -> float | None:
            for key in ("prev_close", "sig_close"):
                val = _to_float(row.get(key))
                if val is not None and val > 0:
                    return val
            return None

        live_gap_list: List[float | None] = []
        live_pct_change_list: List[float | None] = []
        live_intraday_vol_list: List[float | None] = []
        dev_ma5_list: List[float | None] = []
        dev_ma20_list: List[float | None] = []
        dev_ma5_atr_list: List[float | None] = []
        dev_ma20_atr_list: List[float | None] = []
        runup_from_sigclose_list: List[float | None] = []
        runup_from_sigclose_atr_list: List[float | None] = []
        runup_ref_price_list: List[float | None] = []
        runup_ref_source_list: List[str | None] = []
        actions: List[str] = []
        action_reasons: List[str] = []
        states: List[str] = []
        status_reasons: List[str] = []
        signal_kinds: List[str] = []
        valid_days_list: List[int | None] = []
        entry_exposure_caps: List[float | None] = []
        trade_stop_refs: List[float | None] = []
        sig_stop_refs: List[float | None] = []
        effective_stop_refs: List[float | None] = []
        rule_hits_json_list: List[str | None] = []
        summary_lines: List[str | None] = []
        env_index_snapshot_hashes: List[str | None] = []
        env_final_gate_action_list: List[str | None] = []
        env_regimes: List[str | None] = []
        # feat: 补齐环境侧结构化字段（避免只剩 reason 文本）
        env_index_scores: List[float | None] = []
        env_position_hints: List[float | None] = []
        env_weekly_asof_trade_dates: List[str | None] = []
        env_weekly_risk_levels: List[str | None] = []
        env_weekly_scene_list: List[str | None] = []

        max_up = self.rule_config.max_gap_up_pct
        max_up_atr_mult = self.rule_config.max_gap_up_atr_mult
        max_down = self.rule_config.max_gap_down_pct
        min_vs_ma20 = self.rule_config.min_open_vs_ma20_pct
        pullback_min_vs_ma20 = self.rule_config.pullback_min_open_vs_ma20_pct
        limit_up_trigger = self.rule_config.limit_up_trigger_pct
        stop_atr_mult = self.rule_config.stop_atr_mult
        runup_atr_max = self.rule_config.runup_atr_max
        pullback_runup_atr_max = self.rule_config.pullback_runup_atr_max
        pullback_runup_dev_ma20_atr_min = self.rule_config.pullback_runup_dev_ma20_atr_min
        runup_atr_tol = self.rule_config.runup_atr_tol
        cross_valid_days = self.params.cross_valid_days
        pullback_valid_days = self.params.pullback_valid_days

        for _, row in merged.iterrows():
            sig_reason_text = str(row.get("sig_reason") or row.get("reason") or "")
            is_pullback = self._is_pullback_signal(sig_reason_text)
            valid_days = pullback_valid_days if is_pullback else cross_valid_days
            valid_days_list.append(valid_days)
            sig_stop_ref = _to_float(row.get("sig_stop_ref"))
            sig_stop_refs.append(sig_stop_ref)

            price_open = _to_float(row.get("live_open"))
            price_latest = _to_float(row.get("live_latest"))
            price_now = price_open if price_open is not None else price_latest
            ref_close = _resolve_ref_close(row)
            if price_now is None:
                if run_id_norm == "PREOPEN" and ref_close is not None:
                    price_now = ref_close
                elif run_id_norm == "POSTCLOSE":
                    price_now = _to_float(row.get("live_latest")) or ref_close or _to_float(
                        row.get("sig_close")
                    )

            live_gap = None
            live_pct = None
            if ref_close is not None and ref_close > 0:
                gap_price = price_open if price_open is not None else price_latest
                if gap_price is not None:
                    live_gap = (gap_price - ref_close) / ref_close
                if price_latest is not None:
                    live_pct = (price_latest / ref_close - 1.0) * 100.0
            live_gap_list.append(live_gap)
            live_pct_change_list.append(live_pct)

            avg_vol_20 = avg_volume_map.get(str(row.get("code"))) if avg_volume_map else None
            live_vol = _to_float(row.get("live_volume"))
            live_intraday = None
            if (
                avg_vol_20 is not None
                and avg_vol_20 > 0
                and live_vol is not None
                and live_vol > 0
                and minutes_elapsed > 0
            ):
                scaled = (live_vol / max(minutes_elapsed, 1)) * total_minutes
                live_intraday = scaled / avg_vol_20
            live_intraday_vol_list.append(live_intraday)

            sig_ma5 = _to_float(row.get("sig_ma5"))
            sig_ma20 = _to_float(row.get("sig_ma20"))
            sig_atr14 = _to_float(row.get("sig_atr14"))

            dev_ma5 = None
            dev_ma20 = None
            dev_ma5_atr = None
            dev_ma20_atr = None
            if price_now is not None:
                if sig_ma5:
                    dev_ma5 = price_now - sig_ma5
                if sig_ma20:
                    dev_ma20 = price_now - sig_ma20
                if sig_atr14 and sig_atr14 != 0:
                    if sig_ma5 is not None:
                        dev_ma5_atr = (price_now - sig_ma5) / sig_atr14
                    if sig_ma20 is not None:
                        dev_ma20_atr = (price_now - sig_ma20) / sig_atr14
            dev_ma5_list.append(dev_ma5)
            dev_ma20_list.append(dev_ma20)
            dev_ma5_atr_list.append(dev_ma5_atr)
            dev_ma20_atr_list.append(dev_ma20_atr)

            ref_asof_close = (
                _to_float(row.get("asof_close"))
                or ref_close
                or _to_float(row.get("sig_close"))
            )
            runup_metrics = compute_runup_metrics(
                _to_float(row.get("sig_close")),
                asof_close=ref_asof_close,
                live_high=_to_float(row.get("live_high")) or price_now,
                sig_atr14=sig_atr14,
            )
            runup_from_sigclose_list.append(runup_metrics.runup_from_sigclose)
            runup_from_sigclose_atr_list.append(runup_metrics.runup_from_sigclose_atr)
            runup_ref_price_list.append(runup_metrics.runup_ref_price)
            runup_ref_source_list.append(runup_metrics.runup_ref_source)

            threshold_gap_up = max_up
            if sig_atr14 and sig_atr14 > 0 and ref_close:
                atr_gap = max_up_atr_mult * sig_atr14 / ref_close
                threshold_gap_up = min(max_up, atr_gap)

            ma20_thresh = pullback_min_vs_ma20 if is_pullback else min_vs_ma20

            chip_score = _to_float(row.get("sig_chip_score"))

            breach = False
            breach_reason = None
            if price_now is not None:
                dev_ma20_atr_val = dev_ma20_atr
                runup_limit = pullback_runup_atr_max if is_pullback else runup_atr_max
                breach, breach_reason = evaluate_runup_breach(
                    runup_metrics,
                    runup_atr_max=runup_limit,
                    runup_atr_tol=runup_atr_tol,
                    dev_ma20_atr=dev_ma20_atr_val,
                    dev_ma20_atr_min=pullback_runup_dev_ma20_atr_min if is_pullback else None,
                )

            signal_age = None
            raw_signal_age = row.get("signal_age")
            if raw_signal_age is not None:
                try:
                    if not pd.isna(raw_signal_age):
                        signal_age = int(float(raw_signal_age))
                except Exception:
                    signal_age = None

            ctx = DecisionContext(
                entry_exposure_cap=env.position_cap_pct,
                env=env,
                chip_score=chip_score,
                price_now=price_now,
                live_gap=live_gap,
                live_pct=live_pct,
                threshold_gap_up=threshold_gap_up,
                max_gap_down=max_down,
                sig_ma20=sig_ma20,
                ma20_thresh=ma20_thresh,
                signal_age=signal_age,
                valid_days=valid_days,
                limit_up_trigger=limit_up_trigger,
                runup_breach=breach,
                runup_breach_reason=breach_reason,
            )
            self.rule_engine.apply(ctx, self.rules)

            result = ctx.export_result()

            action = result.action
            action_reason = result.action_reason

            trade_stop_ref = None
            if price_now is not None and sig_atr14 is not None:
                trade_stop_ref = price_now - stop_atr_mult * sig_atr14

            entry_cap = result.entry_exposure_cap
            if entry_cap is None:
                entry_cap = env.position_cap_pct
            sig_final_cap = _to_float(row.get("final_cap"))
            if sig_final_cap is not None:
                entry_cap = sig_final_cap if entry_cap is None else min(entry_cap, sig_final_cap)
            entry_exposure_caps.append(entry_cap)
            trade_stop_refs.append(trade_stop_ref)
            effective_stop_refs.append(trade_stop_ref)

            missing_live = (
                pd.isna(row.get("live_latest"))
                and pd.isna(row.get("live_open"))
                and pd.isna(row.get("live_high"))
                and pd.isna(row.get("live_low"))
            )
            missing_asof = pd.isna(row.get("asof_trade_date")) or pd.isna(row.get("asof_close"))

            state = result.state
            status_reason = result.status_reason
            rule_hits_json = result.rule_hits_json
            summary_line = result.summary_line

            if missing_live or missing_asof:
                reason = "MISSING_LIVE_QUOTE" if missing_live else "MISSING_LATEST_SNAPSHOT"
                state = "INVALID"
                status_reason = reason
                action = "SKIP"
                action_reason = reason
                summary_line = f"{summary_line} | DATA:{reason}"

                try:
                    hits = json.loads(rule_hits_json) if rule_hits_json else []
                    if not isinstance(hits, list):
                        hits = []
                except Exception:
                    hits = []
                hits.append({"rule": reason, "severity": 100, "reason": reason})
                rule_hits_json = json.dumps(hits, ensure_ascii=False)

            states.append(state)
            status_reasons.append(status_reason)
            actions.append(action)
            action_reasons.append(action_reason)
            signal_kinds.append("PULLBACK" if is_pullback else "CROSS")
            rule_hits_json_list.append(rule_hits_json)
            summary_lines.append(summary_line)
            env_index_snapshot_hashes.append(env.index_snapshot_hash)
            env_final_gate_action_list.append(result.env_gate_action or env.gate_action)
            env_regimes.append(env.regime)
            env_index_scores.append(env.score)
            env_position_hints.append(env.position_hint)
            env_weekly_asof_trade_dates.append(env.weekly_asof_trade_date)
            env_weekly_risk_levels.append(env.weekly_risk_level)
            env_weekly_scene_list.append(env.weekly_scene)

        merged["monitor_date"] = monitor_date
        if "live_trade_date" not in merged.columns:
            merged["live_trade_date"] = pd.NA
        merged["live_gap_pct"] = live_gap_list
        merged["live_pct_change"] = live_pct_change_list
        merged["live_intraday_vol_ratio"] = live_intraday_vol_list
        merged["action"] = actions
        merged["action_reason"] = action_reasons
        merged["state"] = states
        merged["status_reason"] = status_reasons
        merged["signal_kind"] = signal_kinds
        merged["signal_age"] = merged.get("signal_age")
        merged["valid_days"] = valid_days_list
        merged["trade_stop_ref"] = trade_stop_refs
        merged["effective_stop_ref"] = effective_stop_refs
        merged["entry_exposure_cap"] = entry_exposure_caps
        merged["checked_at"] = checked_at_ts
        merged["run_id"] = run_id_val
        merged["env_regime"] = env_regimes
        merged["env_index_score"] = env_index_scores
        merged["env_position_hint"] = env_position_hints
        merged["env_weekly_asof_trade_date"] = env_weekly_asof_trade_dates
        merged["env_weekly_risk_level"] = env_weekly_risk_levels
        merged["env_weekly_scene"] = env_weekly_scene_list
        merged["env_weekly_structure_status"] = None
        merged["env_weekly_pattern_status"] = None
        merged["env_weekly_gate_action"] = None
        merged["env_final_gate_action"] = env_final_gate_action_list
        merged["env_index_snapshot_hash"] = env_index_snapshot_hashes
        merged["runup_from_sigclose"] = runup_from_sigclose_list
        merged["runup_from_sigclose_atr"] = runup_from_sigclose_atr_list
        merged["runup_ref_price"] = runup_ref_price_list
        merged["runup_ref_source"] = runup_ref_source_list
        merged["dev_ma5"] = dev_ma5_list
        merged["dev_ma20"] = dev_ma20_list
        merged["dev_ma5_atr"] = dev_ma5_atr_list
        merged["dev_ma20_atr"] = dev_ma20_atr_list
        merged["rule_hits_json"] = rule_hits_json_list
        merged["summary_line"] = summary_lines

        def _coalesce_numeric_into(target: str, fallback: str) -> None:
            if target not in merged.columns:
                merged[target] = None
            if fallback not in merged.columns:
                return
            left = pd.to_numeric(merged[target], errors="coerce")
            right = pd.to_numeric(merged[fallback], errors="coerce")
            merged[target] = left.fillna(right)

        if "asof_trade_date" in merged.columns and "sig_date" in merged.columns:
            mask = merged["asof_trade_date"].notna()
            merged["asof_trade_date"] = merged["asof_trade_date"].where(mask, merged["sig_date"])

        _coalesce_numeric_into("asof_close", "sig_close")
        _coalesce_numeric_into("asof_ma5", "sig_ma5")
        _coalesce_numeric_into("asof_ma20", "sig_ma20")
        _coalesce_numeric_into("asof_ma60", "sig_ma60")
        _coalesce_numeric_into("asof_ma250", "sig_ma250")
        _coalesce_numeric_into("asof_vol_ratio", "sig_vol_ratio")
        _coalesce_numeric_into("asof_macd_hist", "sig_macd_hist")
        _coalesce_numeric_into("asof_atr14", "sig_atr14")
        _coalesce_numeric_into("asof_stop_ref", "sig_stop_ref")

        full_keep_cols = [
            "monitor_date",
            "sig_date",
            "signal_age",
            "valid_days",
            "code",
            "name",
            "asof_trade_date",
            "live_trade_date",
            "avg_volume_20",
            "live_open",
            "live_high",
            "live_low",
            "live_latest",
            "live_volume",
            "live_amount",
            "live_gap_pct",
            "live_pct_change",
            "live_intraday_vol_ratio",
            "sig_close",
            "sig_ma5",
            "sig_ma20",
            "sig_ma60",
            "sig_ma250",
            "sig_vol_ratio",
            "sig_macd_hist",
            "sig_atr14",
            "sig_stop_ref",
            "effective_stop_ref",
            "asof_close",
            "asof_ma5",
            "asof_ma20",
            "asof_ma60",
            "asof_ma250",
            "asof_vol_ratio",
            "asof_macd_hist",
            "asof_atr14",
            "asof_stop_ref",
            "sig_kdj_k",
            "sig_kdj_d",
            "trade_stop_ref",
            "dev_ma5",
            "dev_ma20",
            "dev_ma5_atr",
            "dev_ma20_atr",
            "runup_from_sigclose",
            "runup_from_sigclose_atr",
            "runup_ref_price",
            "runup_ref_source",
            "entry_exposure_cap",
            "env_index_score",
            "env_regime",
            "env_position_hint",
            "env_index_snapshot_hash",
            "env_final_gate_action",
            "env_weekly_asof_trade_date",
            "env_weekly_risk_level",
            "env_weekly_scene",
            "env_weekly_structure_status",
            "env_weekly_pattern_status",
            "env_weekly_gate_action",
            "industry",
            "board_name",
            "board_code",
            "board_status",
            "board_rank",
            "board_chg_pct",
            "signal_strength",
            "strength_delta",
            "strength_trend",
            "strength_note",
            "risk_tag",
            "risk_note",
            "rule_hits_json",
            "summary_line",
            "signal_kind",
            "sig_signal",
            "sig_reason",
            "state",
            "status_reason",
            "action",
            "action_reason",
            "checked_at",
            "run_id",
            "run_pk",
            "snapshot_hash",
        ]

        compact_keep_cols = [
            "monitor_date",
            "sig_date",
            "asof_trade_date",
            "live_trade_date",
            "signal_age",
            "valid_days",
            "code",
            "name",
            "live_open",
            "live_latest",
            "live_high",
            "live_low",
            "live_pct_change",
            "live_gap_pct",
            "live_intraday_vol_ratio",
            "sig_close",
            "sig_ma5",
            "sig_ma20",
            "sig_ma60",
            "sig_atr14",
            "sig_vol_ratio",
            "sig_macd_hist",
            "sig_stop_ref",
            "trade_stop_ref",
            "dev_ma5",
            "dev_ma20",
            "dev_ma5_atr",
            "dev_ma20_atr",
            "runup_from_sigclose",
            "runup_from_sigclose_atr",
            "runup_ref_price",
            "runup_ref_source",
            "effective_stop_ref",
            "env_regime",
            "env_position_hint",
            "env_index_score",
            "env_index_snapshot_hash",
            "env_final_gate_action",
            "env_weekly_asof_trade_date",
            "env_weekly_risk_level",
            "env_weekly_scene",
            "board_name",
            "board_code",
            "board_status",
            "board_rank",
            "board_chg_pct",
            "signal_strength",
            "strength_delta",
            "strength_trend",
            "strength_note",
            "risk_tag",
            "risk_note",
            "rule_hits_json",
            "summary_line",
            "signal_kind",
            "sig_signal",
            "sig_reason",
            "state",
            "status_reason",
            "action",
            "action_reason",
            "checked_at",
            "run_id",
            "run_pk",
            "snapshot_hash",
        ]

        output_mode = (self.params.output_mode or "COMPACT").upper()
        keep_cols = full_keep_cols if output_mode == "FULL" else compact_keep_cols

        for col in keep_cols:
            if col not in merged.columns:
                merged[col] = None

        merged["snapshot_hash"] = merged.apply(
            lambda row: make_snapshot_hash(row.to_dict()), axis=1
        )
        return merged[keep_cols].copy()

    @staticmethod
    def _clamp01(val: float | None) -> float:
        if val is None:
            return 0.0
        try:
            fv = float(val)
        except Exception:  # noqa: BLE001
            return 0.0
        if fv != fv:
            return 0.0
        return 0.0 if fv < 0.0 else 1.0 if fv > 1.0 else fv

    def _calc_market_weight(self, env_context: dict[str, Any] | None) -> tuple[float, str]:
        env = MarketEnvironment.from_snapshot(env_context)
        gate = str(env.gate_action or "").strip().upper() or "-"
        pos_hint = _to_float(env.position_hint)
        weekly_risk = str(env.weekly_risk_level or "").strip().upper() or "-"

        base_map = {"ALLOW": 1.0, "ALLOW_SMALL": 0.75, "WAIT": 0.45, "STOP": 0.0}
        base = base_map.get(gate, 0.85)
        pos_factor = 1.0 if pos_hint is None else self._clamp01(pos_hint)
        risk_factor = 1.0
        if weekly_risk == "HIGH":
            risk_factor = 0.65
        elif weekly_risk == "MEDIUM":
            risk_factor = 0.85
        elif weekly_risk == "LOW":
            risk_factor = 1.05

        market_weight = self._clamp01(base * pos_factor * risk_factor)
        note = f"gate={gate} pos_hint={pos_hint if pos_hint is not None else '-'} weekly_risk={weekly_risk}"
        return market_weight, note

    @staticmethod
    def _resolve_board_payload(
        boards_map: dict[str, Any] | None, board_code: Any, board_name: Any
    ) -> dict[str, Any] | None:
        if not isinstance(boards_map, dict) or not boards_map:
            return None
        for key in (board_code, board_name):
            key_norm = str(key or "").strip()
            if key_norm and key_norm in boards_map:
                payload = boards_map.get(key_norm)
                return payload if isinstance(payload, dict) else None
        return None

    def build_rank_frame(
        self, df: pd.DataFrame, env_context: dict[str, Any] | None
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        if df.empty:
            return df.copy(), {}

        ranked = df.copy()
        meta: dict[str, Any] = {}

        market_weight, market_note = self._calc_market_weight(env_context)
        ranked["market_weight"] = market_weight
        meta["market_weight"] = market_weight
        meta["market_note"] = market_note

        boards_map = env_context.get("boards") if isinstance(env_context, dict) else None
        if "board_status" not in ranked.columns:
            ranked["board_status"] = None
        if "board_rank" not in ranked.columns:
            ranked["board_rank"] = None
        if "board_chg_pct" not in ranked.columns:
            ranked["board_chg_pct"] = None

        def _fill_board(row: pd.Series) -> pd.Series:
            payload = self._resolve_board_payload(
                boards_map, row.get("board_code"), row.get("board_name")
            )
            if not payload:
                return row
            if not str(row.get("board_status") or "").strip():
                row["board_status"] = payload.get("status")
            if row.get("board_rank") in (None, "", 0) and payload.get("rank") is not None:
                row["board_rank"] = payload.get("rank")
            if row.get("board_chg_pct") in (None, "") and payload.get("chg_pct") is not None:
                row["board_chg_pct"] = payload.get("chg_pct")
            return row

        if boards_map:
            ranked = ranked.apply(_fill_board, axis=1)

        status_weight_map = {"strong": 1.0, "neutral": 0.7, "weak": 0.4}
        board_status = ranked.get("board_status")
        if board_status is None:
            ranked["board_weight"] = 0.6
        else:
            ranked["board_weight"] = (
                board_status.astype(str)
                .str.strip()
                .str.lower()
                .map(status_weight_map)
                .fillna(0.6)
            )

        meta["board_weight_map"] = status_weight_map

        quality_col = None
        quality_series = None
        for cand in ["signal_strength", "sig_chip_score", "sig_vol_ratio"]:
            if cand in ranked.columns:
                s = pd.to_numeric(ranked[cand], errors="coerce")
                if s.notna().sum() >= 3:
                    quality_col = cand
                    quality_series = s
                    break

        if quality_series is None:
            ranked["stock_quality_weight"] = 0.5
            meta["stock_quality_source"] = None
        else:
            pct = quality_series.rank(pct=True)
            median = float(pct.dropna().median()) if pct.notna().any() else 0.5
            ranked["stock_quality_weight"] = pct.fillna(median)
            meta["stock_quality_source"] = quality_col

        ranked["final_rank_score"] = (
            100.0
            * (
                0.45 * ranked["market_weight"]
                + 0.35 * ranked["board_weight"]
                + 0.20 * ranked["stock_quality_weight"]
            )
        )

        return ranked, meta

    @staticmethod
    def build_board_map_from_strength(board_strength: pd.DataFrame) -> dict[str, dict[str, Any]]:
        board_map: dict[str, dict[str, Any]] = {}
        if board_strength is None or getattr(board_strength, "empty", True):
            return board_map

        total = len(board_strength)
        for _, row in board_strength.iterrows():
            name = str(row.get("board_name") or "").strip()
            code = str(row.get("board_code") or "").strip()
            rank = row.get("rank")
            pct = row.get("chg_pct")
            status = "neutral"
            if total > 0 and rank not in (None, ""):
                try:
                    rank_i = int(rank)
                except Exception:  # noqa: BLE001
                    rank_i = None
                if rank_i is not None:
                    if rank_i <= max(1, int(total * 0.2)):
                        status = "strong"
                    elif rank_i >= max(1, int(total * 0.8)):
                        status = "weak"
            payload = {"rank": rank, "chg_pct": pct, "status": status}
            for key in [name, code]:
                key_norm = str(key).strip()
                if key_norm:
                    board_map[key_norm] = payload

        return board_map

    def export_csv(self, df: pd.DataFrame) -> None:
        if df.empty or (not self.params.export_csv):
            return

        app_sec = get_section("app") or {}
        base_dir = "output"
        if isinstance(app_sec, dict):
            base_dir = str(app_sec.get("output_dir", base_dir))

        outdir = Path(base_dir) / self.params.output_subdir
        outdir.mkdir(parents=True, exist_ok=True)

        monitor_date = str(df.iloc[0].get("monitor_date") or dt.date.today().isoformat())

        suffix = ""
        if self.params.incremental_export_timestamp:
            checked_at = str(df.iloc[0].get("checked_at") or "").strip()
            if checked_at and " " in checked_at:
                time_part = checked_at.split(" ", 1)[1].replace(":", "").replace(".", "")
                if time_part:
                    suffix = f"_{time_part}"

        path = outdir / f"open_monitor_{monitor_date}{suffix}.csv"

        export_df = df.copy()
        export_df = export_df.drop(columns=["snapshot_hash"], errors="ignore")
        gap_col = "live_gap_pct" if "live_gap_pct" in export_df.columns else "gap_pct"
        export_df[gap_col] = export_df[gap_col].apply(_to_float)
        action_rank = {"EXECUTE": 0, "WAIT": 1, "SKIP": 2, "UNKNOWN": 3}
        export_df["_action_rank"] = export_df["action"].map(action_rank).fillna(99)

        state_rank = {
            "OK": 0,
            "OVEREXTENDED": 1,
            "RUNUP_TOO_LARGE": 2,
            "INVALID": 3,
            "STALE": 4,
            "UNKNOWN": 9,
        }
        if "state" in export_df.columns:
            export_df["_state_rank"] = export_df["state"].map(state_rank).fillna(99)
        else:
            export_df["_state_rank"] = 99
        export_df = export_df.sort_values(
            by=["_action_rank", "_state_rank", gap_col],
            ascending=[True, True, True],
        )
        export_df = export_df.drop(
            columns=["_action_rank", "_state_rank"],
            errors="ignore",
        )
        if self.params.export_top_n > 0:
            export_df = export_df.head(self.params.export_top_n)

        export_df.to_csv(path, index=False, encoding="utf-8-sig")
        self.logger.info("开盘监测 CSV 已导出：%s", path)
