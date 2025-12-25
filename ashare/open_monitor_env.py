"""open_monitor 环境与快照服务。"""

from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any, Callable

import pandas as pd

from .open_monitor_repo import calc_run_id, make_snapshot_hash
from .utils.convert import to_float as _to_float


def derive_index_gate_action(regime: str | None, position_hint: float | None) -> str | None:
    regime_norm = str(regime or "").strip().upper() or None
    pos_hint_val = _to_float(position_hint)

    if regime_norm in {"BREAKDOWN", "BEAR_CONFIRMED"}:
        return "STOP"

    if regime_norm == "RISK_OFF":
        return "WAIT"

    if regime_norm == "PULLBACK":
        if pos_hint_val is not None and pos_hint_val <= 0.3:
            return "WAIT"
        return "ALLOW"

    if regime_norm == "RISK_ON":
        return "ALLOW"

    if pos_hint_val is not None:
        if pos_hint_val <= 0:
            return "STOP"
        if pos_hint_val < 0.3:
            return "WAIT"
        return "ALLOW"

    return None


class OpenMonitorEnvService:
    """负责环境快照构建与加载的服务层。"""

    def __init__(self, repo, logger, params, env_builder) -> None:
        self.repo = repo
        self.logger = logger
        self.params = params
        self.env_builder = env_builder

    def load_env_snapshot_context(
        self,
        monitor_date: str,
        run_id: str | None = None,
    ) -> dict[str, Any] | None:
        return self._load_env_snapshot_context(monitor_date, run_id)

    def _load_env_snapshot_context(
        self, monitor_date: str, run_id: str | None = None
    ) -> dict[str, Any] | None:
        df = self.repo.load_env_snapshot_row(monitor_date, run_id)
        if df is None or df.empty:
            return None

        row = df.iloc[0]
        weekly_scenario = {
            "weekly_asof_trade_date": row.get("env_weekly_asof_trade_date"),
            "weekly_risk_level": row.get("env_weekly_risk_level"),
            "weekly_scene_code": row.get("env_weekly_scene"),
            "weekly_plan_json": row.get("env_weekly_plan_json"),
            "weekly_plan_a": row.get("env_weekly_plan_a"),
            "weekly_plan_b": row.get("env_weekly_plan_b"),
            "weekly_plan_a_exposure_cap": row.get("env_weekly_plan_a_exposure_cap"),
            "weekly_bias": row.get("env_weekly_bias"),
            "weekly_status": row.get("env_weekly_status"),
            "weekly_structure_status": row.get("env_weekly_structure_status"),
            "weekly_pattern_status": row.get("env_weekly_pattern_status"),
            "weekly_gating_enabled": row.get("env_weekly_gating_enabled"),
            "weekly_tags": row.get("env_weekly_tags"),
            "weekly_money_proxy": row.get("env_weekly_money_proxy"),
            "weekly_note": row.get("env_weekly_note"),
        }

        def _is_empty_env_value(value: Any) -> bool:
            if value is None:
                return True
            if isinstance(value, str):
                return value.strip() == ""
            if isinstance(value, dict):
                return len(value) == 0
            if isinstance(value, (list, tuple, set)):
                return len(value) == 0

            size = getattr(value, "size", None)
            if isinstance(size, int):
                return size == 0

            try:
                return len(value) == 0  # type: ignore[arg-type]
            except Exception:
                return False

        raw_plan_json = row.get("env_weekly_plan_json")
        plan_dict: dict[str, Any] = {}
        if isinstance(raw_plan_json, str):
            plan_text = raw_plan_json.strip()
            if plan_text:
                try:
                    parsed = json.loads(plan_text)
                    if isinstance(parsed, dict):
                        plan_dict = parsed
                except Exception as exc:  # noqa: BLE001
                    self.logger.debug("解析 env_weekly_plan_json 失败：%s", exc)
        elif isinstance(raw_plan_json, dict):
            plan_dict = raw_plan_json

        def _backfill(target_key: str, source_key: str | None = None) -> None:
            if not isinstance(plan_dict, dict):
                return
            if not _is_empty_env_value(weekly_scenario.get(target_key)):
                return
            plan_key = source_key or target_key
            value = plan_dict.get(plan_key)
            if _is_empty_env_value(value):
                return
            weekly_scenario[target_key] = value

        _backfill("weekly_current_week_closed")
        _backfill("weekly_risk_score")
        _backfill("weekly_key_levels_str")
        _backfill("weekly_plan_a_if")
        _backfill("weekly_plan_a_then")
        _backfill("weekly_plan_a_confirm")
        _backfill("weekly_plan_b_if")
        _backfill("weekly_plan_b_then")
        _backfill("weekly_plan_b_recover_if")
        _backfill("weekly_structure_tags")
        _backfill("weekly_confirm_tags")
        _backfill("weekly_money_tags")
        _backfill("weekly_direction_confirmed")
        _backfill("weekly_key_levels")
        _backfill("weekly_structure_status")
        _backfill("weekly_pattern_status")
        _backfill("weekly_status", "weekly_structure_status")

        index_snapshot_hash = row.get("env_index_snapshot_hash")
        loaded_index_snapshot = self.repo.load_index_snapshot_by_hash(index_snapshot_hash)

        env_context: dict[str, Any] = {
            "weekly_scenario": weekly_scenario,
            "weekly_asof_trade_date": weekly_scenario.get("weekly_asof_trade_date"),
            "weekly_risk_level": weekly_scenario.get("weekly_risk_level"),
            "weekly_scene_code": weekly_scenario.get("weekly_scene_code"),
            "weekly_plan_json": weekly_scenario.get("weekly_plan_json"),
            "weekly_plan_a": weekly_scenario.get("weekly_plan_a"),
            "weekly_plan_b": weekly_scenario.get("weekly_plan_b"),
            "weekly_plan_a_exposure_cap": weekly_scenario.get("weekly_plan_a_exposure_cap"),
            "weekly_bias": weekly_scenario.get("weekly_bias"),
            "weekly_status": weekly_scenario.get("weekly_status"),
            "weekly_structure_status": weekly_scenario.get("weekly_structure_status")
            or weekly_scenario.get("weekly_status"),
            "weekly_pattern_status": weekly_scenario.get("weekly_pattern_status"),
            "weekly_direction_confirmed": weekly_scenario.get("weekly_direction_confirmed"),
            "weekly_money_tags": weekly_scenario.get("weekly_money_tags"),
            "weekly_gating_enabled": bool(weekly_scenario.get("weekly_gating_enabled", False)),
            "weekly_tags": weekly_scenario.get("weekly_tags"),
            "weekly_money_proxy": weekly_scenario.get("weekly_money_proxy"),
            "weekly_note": weekly_scenario.get("weekly_note"),
            "regime": row.get("env_regime"),
            "index_score": row.get("env_index_score"),
            "position_hint": row.get("env_position_hint"),
            "position_hint_raw": row.get("env_position_hint_raw"),
            "effective_position_hint": row.get("env_position_hint"),
            "weekly_gate_policy": (
                row.get("env_weekly_gate_policy") or row.get("env_weekly_gate_action")
            ),
            "run_id": row.get("run_id"),
            "env_index_snapshot_hash": index_snapshot_hash,
            "env_final_gate_action": row.get("env_final_gate_action"),
            "env_final_cap_pct": row.get("env_final_cap_pct"),
            "env_final_reason_json": row.get("env_final_reason_json"),
        }
        index_snapshot = loaded_index_snapshot or {
            "env_index_code": row.get("env_index_code"),
            "env_index_asof_trade_date": row.get("env_index_asof_trade_date"),
            "env_index_asof_close": row.get("env_index_asof_close"),
            "env_index_asof_ma20": row.get("env_index_asof_ma20"),
            "env_index_asof_ma60": row.get("env_index_asof_ma60"),
            "env_index_asof_macd_hist": row.get("env_index_asof_macd_hist"),
            "env_index_asof_atr14": row.get("env_index_asof_atr14"),
            "env_index_live_trade_date": row.get("env_index_live_trade_date"),
            "env_index_live_open": row.get("env_index_live_open"),
            "env_index_live_high": row.get("env_index_live_high"),
            "env_index_live_low": row.get("env_index_live_low"),
            "env_index_live_latest": row.get("env_index_live_latest"),
            "env_index_live_pct_change": row.get("env_index_live_pct_change"),
            "env_index_live_volume": row.get("env_index_live_volume"),
            "env_index_live_amount": row.get("env_index_live_amount"),
            "env_index_dev_ma20_atr": row.get("env_index_dev_ma20_atr"),
            "env_index_gate_action": row.get("env_index_gate_action"),
            "env_index_gate_reason": row.get("env_index_gate_reason"),
            "env_index_position_cap": row.get("env_index_position_cap"),
            "env_final_gate_action": row.get("env_final_gate_action"),
            "env_index_snapshot_hash": index_snapshot_hash,
        }
        env_context["index_intraday"] = index_snapshot
        if env_context.get("position_hint") is None:
            env_context["position_hint"] = index_snapshot.get("env_index_position_cap")
            env_context["effective_position_hint"] = env_context["position_hint"]

        # feat: 宽表需要 env_regime/env_index_score 等结构化字段；若库里缺失则从 gate_reason 或 index_trend 回填
        gate_reason = index_snapshot.get("env_index_gate_reason") or ""
        if env_context.get("regime") is None:
            m = re.search(r"regime=([A-Z_]+)", gate_reason)
            if m:
                env_context["regime"] = m.group(1)

        if env_context.get("position_hint") is None:
            m = re.search(r"pos_hint=([0-9.]+)", gate_reason)
            if m:
                try:
                    env_context["position_hint"] = float(m.group(1))
                except ValueError:
                    pass
            env_context["effective_position_hint"] = env_context.get("position_hint")

        if env_context.get("index_score") is None:
            trend_trade_date = (
                row.get("env_index_asof_trade_date")
                or index_snapshot.get("env_index_asof_trade_date")
                or index_snapshot.get("asof_trade_date")
                or row.get("sig_date")
                or row.get("monitor_date")
            )
            if trend_trade_date:
                try:
                    idx_trend = self.env_builder.load_index_trend(str(trend_trade_date))
                    if isinstance(idx_trend, dict):
                        env_context["index_score"] = idx_trend.get("env_index_score")
                        env_context["regime"] = env_context.get("regime") or idx_trend.get("env_regime")
                        if env_context.get("position_hint") is None:
                            env_context["position_hint"] = idx_trend.get("env_position_hint")
                        env_context["effective_position_hint"] = env_context.get("position_hint")
                except Exception as e:
                    logger.warning("load_index_trend fallback failed: %s", e)

        return env_context

    def resolve_env_weekly_gate_policy(
        self, env_context: dict[str, Any] | None
    ) -> str | None:
        return self.env_builder.resolve_env_weekly_gate_policy(env_context)

    def load_index_trend(self, latest_trade_date: str) -> dict[str, Any]:
        return self.env_builder.load_index_trend(latest_trade_date)

    def load_index_weekly_channel(self, latest_trade_date: str) -> dict[str, Any]:
        return self.env_builder.load_index_weekly_channel(latest_trade_date)

    def build_weekly_scenario(
        self, weekly_payload: dict[str, Any], index_trend: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return self.env_builder.build_weekly_scenario(weekly_payload, index_trend)

    def build_environment_context(
        self, latest_trade_date: str, *, checked_at: dt.datetime | None = None
    ) -> dict[str, Any]:
        return self.env_builder.build_environment_context(latest_trade_date, checked_at=checked_at)

    def build_and_persist_env_snapshot(
        self,
        latest_trade_date: str,
        *,
        monitor_date: str | None = None,
        run_id: str | None = None,
        checked_at: dt.datetime | None = None,
        fetch_index_live_quote: Callable[[], dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        if checked_at is None:
            checked_at = dt.datetime.now()
        if monitor_date is None:
            monitor_date = checked_at.date().isoformat()
        if run_id is None:
            run_id = calc_run_id(checked_at, self.params.run_id_minutes)

        env_context = self.build_environment_context(
            latest_trade_date, checked_at=checked_at
        )
        weekly_gate_policy = self.resolve_env_weekly_gate_policy(env_context)
        if isinstance(env_context, dict):
            env_context["weekly_gate_policy"] = weekly_gate_policy

        env_context, _, _ = self.attach_index_snapshot(
            latest_trade_date,
            monitor_date,
            run_id,
            checked_at,
            env_context,
            fetch_index_live_quote=fetch_index_live_quote,
        )

        self.env_builder._finalize_env_directives(
            env_context, weekly_gate_policy=weekly_gate_policy
        )

        self.repo.persist_env_snapshot(
            env_context,
            monitor_date,
            run_id,
            checked_at,
            weekly_gate_policy,
        )

        return env_context

    def _build_index_env_snapshot(
        self,
        asof_indicators: dict[str, Any],
        live_quote: dict[str, Any],
        env_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot: dict[str, Any] = {}
        if isinstance(asof_indicators, dict):
            snapshot.update(
                {
                    "env_index_code": asof_indicators.get("index_code"),
                    "env_index_asof_trade_date": asof_indicators.get("asof_trade_date"),
                    "env_index_asof_close": _to_float(asof_indicators.get("asof_close")),
                    "env_index_asof_ma20": _to_float(asof_indicators.get("asof_ma20")),
                    "env_index_asof_ma60": _to_float(asof_indicators.get("asof_ma60")),
                    "env_index_asof_macd_hist": _to_float(
                        asof_indicators.get("asof_macd_hist")
                    ),
                    "env_index_asof_atr14": _to_float(asof_indicators.get("asof_atr14")),
                }
            )
        if isinstance(live_quote, dict):
            snapshot.update(
                {
                    "env_index_live_trade_date": live_quote.get("live_trade_date")
                    or dt.date.today().isoformat(),
                    "env_index_live_open": _to_float(live_quote.get("live_open") or live_quote.get("open")),
                    "env_index_live_high": _to_float(live_quote.get("live_high") or live_quote.get("high")),
                    "env_index_live_low": _to_float(live_quote.get("live_low") or live_quote.get("low")),
                    "env_index_live_latest": _to_float(
                        live_quote.get("live_latest") or live_quote.get("latest")
                    ),
                    "env_index_live_pct_change": _to_float(
                        live_quote.get("live_pct_change") or live_quote.get("pct_change")
                    ),
                    "env_index_live_volume": _to_float(
                        live_quote.get("live_volume") or live_quote.get("volume")
                    ),
                    "env_index_live_amount": _to_float(
                        live_quote.get("live_amount") or live_quote.get("amount")
                    ),
                }
            )

        asof_close = _to_float(snapshot.get("env_index_asof_close"))
        asof_ma20 = _to_float(snapshot.get("env_index_asof_ma20"))
        asof_atr14 = _to_float(snapshot.get("env_index_asof_atr14"))
        live_latest = _to_float(snapshot.get("env_index_live_latest"))

        dev_ma20_atr = None
        if live_latest is not None and asof_ma20 is not None and asof_atr14:
            dev_ma20_atr = (live_latest - asof_ma20) / asof_atr14

        snapshot["env_index_dev_ma20_atr"] = dev_ma20_atr

        env_index_score = None
        regime = None
        position_hint = None
        if env_context and isinstance(env_context, dict):
            env_index_score = _to_float(env_context.get("env_index_score"))
            regime = env_context.get("env_regime")
            position_hint = env_context.get("env_position_hint")
        snapshot["env_index_score"] = env_index_score
        gate_action = derive_index_gate_action(regime, position_hint) or "ALLOW"
        gate_reason = f"regime={regime} pos_hint={position_hint}"
        snapshot["env_index_gate_action"] = gate_action
        snapshot["env_index_gate_reason"] = gate_reason
        snapshot["env_index_position_cap"] = _to_float(position_hint)

        return snapshot

    def attach_index_snapshot(
        self,
        latest_trade_date: str,
        monitor_date: str,
        run_id: str | None,
        checked_at: dt.datetime,
        env_context: dict[str, Any] | None,
        *,
        fetch_index_live_quote: Callable[[], dict[str, Any]] | None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any], str | None]:
        ctx = env_context or {}
        index_env_snapshot: dict[str, Any] = {}
        env_index_snapshot_hash: str | None = None

        if latest_trade_date:
            asof_indicators = self.repo.load_index_history(latest_trade_date)
            live_quote = fetch_index_live_quote() if fetch_index_live_quote else {}
            index_env_snapshot = self._build_index_env_snapshot(
                asof_indicators,
                live_quote,
                env_context=ctx,
            )

        if index_env_snapshot:
            index_snapshot_payload = {
                "monitor_date": monitor_date,
                "checked_at": checked_at,
                "index_code": index_env_snapshot.get("env_index_code"),
                "asof_trade_date": index_env_snapshot.get("env_index_asof_trade_date"),
                "live_trade_date": index_env_snapshot.get("env_index_live_trade_date"),
                "asof_close": _to_float(index_env_snapshot.get("env_index_asof_close")),
                "asof_ma20": _to_float(index_env_snapshot.get("env_index_asof_ma20")),
                "asof_ma60": _to_float(index_env_snapshot.get("env_index_asof_ma60")),
                "asof_macd_hist": _to_float(index_env_snapshot.get("env_index_asof_macd_hist")),
                "asof_atr14": _to_float(index_env_snapshot.get("env_index_asof_atr14")),
                "live_open": _to_float(index_env_snapshot.get("env_index_live_open")),
                "live_high": _to_float(index_env_snapshot.get("env_index_live_high")),
                "live_low": _to_float(index_env_snapshot.get("env_index_live_low")),
                "live_latest": _to_float(index_env_snapshot.get("env_index_live_latest")),
                "live_pct_change": _to_float(index_env_snapshot.get("env_index_live_pct_change")),
                "live_volume": _to_float(index_env_snapshot.get("env_index_live_volume")),
                "live_amount": _to_float(index_env_snapshot.get("env_index_live_amount")),
                "dev_ma20_atr": _to_float(index_env_snapshot.get("env_index_dev_ma20_atr")),
                "gate_action": index_env_snapshot.get("env_index_gate_action"),
                "gate_reason": index_env_snapshot.get("env_index_gate_reason"),
                "position_cap": _to_float(index_env_snapshot.get("env_index_position_cap")),
            }
            index_snapshot_payload["snapshot_hash"] = make_snapshot_hash(index_snapshot_payload)
            env_index_snapshot_hash = self.repo.persist_index_snapshot(
                index_snapshot_payload,
                table=self.params.env_index_snapshot_table,
            ) or index_snapshot_payload["snapshot_hash"]

            index_env_snapshot["env_index_snapshot_hash"] = env_index_snapshot_hash
            ctx["index_intraday"] = index_env_snapshot

        if index_env_snapshot:
            gate_action = index_env_snapshot.get("env_index_gate_action")
            gate_reason = index_env_snapshot.get("env_index_gate_reason") or "-"
            live_pct = _to_float(index_env_snapshot.get("env_index_live_pct_change"))
            dev_ma20_atr = _to_float(index_env_snapshot.get("env_index_dev_ma20_atr"))
            self.logger.info(
                "指数环境快照：%s asof=%s live=%s pct=%.2f%% dev_ma20_atr=%.2f gate=%s reason=%s",
                index_env_snapshot.get("env_index_code"),
                index_env_snapshot.get("env_index_asof_trade_date"),
                index_env_snapshot.get("env_index_live_trade_date"),
                live_pct * 100 if live_pct is not None else 0.0,
                dev_ma20_atr if dev_ma20_atr is not None else 0.0,
                gate_action,
                gate_reason,
            )

        return ctx or env_context, index_env_snapshot, env_index_snapshot_hash

    def log_weekly_scenario(self, env_context: dict[str, Any] | None) -> None:
        reason_ctx = {}
        if isinstance(env_context, dict):
            reason_json = env_context.get("env_final_reason_json")
            if isinstance(reason_json, str) and reason_json.strip():
                try:
                    reason_ctx = json.loads(reason_json)
                except Exception:
                    reason_ctx = {}

        weekly_scenario = (
            env_context.get("weekly_scenario", {}) if isinstance(env_context, dict) else {}
        )
        if not isinstance(weekly_scenario, dict):
            return

        confirm_tags = ",".join(weekly_scenario.get("weekly_confirm_tags", []) or [])

        asof_trade_date = weekly_scenario.get("weekly_asof_trade_date") or reason_ctx.get(
            "weekly_asof_trade_date"
        )
        current_week_closed = weekly_scenario.get("weekly_current_week_closed")
        if current_week_closed is None:
            current_week_closed = reason_ctx.get("weekly_current_week_closed")

        risk_level = weekly_scenario.get("weekly_risk_level") or reason_ctx.get("weekly_risk_level")
        risk_score = _to_float(weekly_scenario.get("weekly_risk_score"))
        if risk_score is None:
            risk_score = _to_float(reason_ctx.get("weekly_risk_score"))
        risk_score = risk_score or 0.0

        scene_code = weekly_scenario.get("weekly_scene_code") or reason_ctx.get(
            "weekly_scene_code"
        )

        bias = weekly_scenario.get("weekly_bias")
        if bias is None:
            tags_str = reason_ctx.get("weekly_tags")
            if isinstance(tags_str, str):
                if "BIAS_BULLISH" in tags_str:
                    bias = "BULLISH"
                elif "BIAS_BEARISH" in tags_str:
                    bias = "BEARISH"

        status = weekly_scenario.get("weekly_status")
        if status is None:
            status = (
                weekly_scenario.get("weekly_pattern_status")
                or reason_ctx.get("weekly_pattern_status")
                or reason_ctx.get("weekly_structure_status")
            )

        key_levels_str = weekly_scenario.get("weekly_key_levels_str") or reason_ctx.get(
            "weekly_key_levels_str"
        )
        key_levels_short = str(key_levels_str or "")[:120]

        if not confirm_tags:
            tags_str = reason_ctx.get("weekly_tags")
            if isinstance(tags_str, str) and tags_str.strip():
                confirm_tags = ",".join(
                    [t.strip() for t in tags_str.replace(";", ",").split(",") if t.strip()]
                )

        self.logger.info(
            "周线情景：asof=%s current_week_closed=%s risk=%s(%.1f) scene=%s bias=%s status=%s levels=%s 确认标签=%s",
            asof_trade_date,
            current_week_closed,
            risk_level,
            risk_score,
            scene_code,
            bias,
            status,
            key_levels_short,
            confirm_tags,
        )
        weekly_note = str(
            weekly_scenario.get("weekly_note") or reason_ctx.get("weekly_note") or ""
        ).strip()
        if weekly_note:
            self.logger.info("周线备注：%s", weekly_note[:200])
        plan_a = str(weekly_scenario.get("weekly_plan_a") or "").strip()[:200]
        plan_b = str(weekly_scenario.get("weekly_plan_b") or "").strip()[:200]
        if plan_a:
            self.logger.info("周线 PlanA：%s", plan_a)
        if plan_b:
            self.logger.info("周线 PlanB：%s", plan_b)
        plan_a_if = weekly_scenario.get("weekly_plan_a_if")
        plan_b_if = weekly_scenario.get("weekly_plan_b_if")
        plan_a_confirm = weekly_scenario.get("weekly_plan_a_confirm")
        plan_b_recover = weekly_scenario.get("weekly_plan_b_recover_if")
        plan_tokens = [
            plan_a_if,
            weekly_scenario.get("weekly_plan_a_then"),
            plan_a_confirm,
            plan_b_if,
            plan_b_recover,
        ]
        if any(str(token or "").strip() for token in plan_tokens):
            self.logger.info(
                "周线 Plan tokens: A_if=%s A_then=%s A_confirm=%s B_if=%s B_recover=%s",
                str(plan_a_if or "")[:120],
                str(weekly_scenario.get("weekly_plan_a_then") or "")[:64],
                str(plan_a_confirm or "")[:64],
                str(plan_b_if or "")[:120],
                str(plan_b_recover or "")[:120],
            )
