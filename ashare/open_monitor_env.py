"""open_monitor 环境与快照服务。"""

from __future__ import annotations

import datetime as dt
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


def _parse_weekly_key_levels(levels_str: str | None) -> dict[str, float]:
    if not levels_str:
        return {}
    levels: dict[str, float] = {}
    for token in str(levels_str).split(";"):
        token = token.strip()
        if not token or "=" not in token:
            continue
        key, value = token.split("=", 1)
        key = key.strip().lower()
        level = _to_float(value)
        if key and level is not None:
            levels[key] = level
    return levels


class OpenMonitorEnvService:
    """负责环境快照构建与加载的服务层。"""

    def __init__(self, repo, logger, params, env_builder, indicator_builder) -> None:
        self.repo = repo
        self.logger = logger
        self.params = params
        self.env_builder = env_builder
        self.indicator_builder = indicator_builder

    def load_open_monitor_env_context(
        self,
        monitor_date: str,
        run_pk: int | None = None,
    ) -> dict[str, Any] | None:
        return self._load_open_monitor_env_context(monitor_date, run_pk)

    def _load_open_monitor_env_context(
        self, monitor_date: str, run_pk: int | None = None
    ) -> dict[str, Any] | None:
        df = self.repo.load_open_monitor_env_row(monitor_date, run_pk)
        if df is None or df.empty:
            return None

        row = df.iloc[0]
        env_view_row = self.repo.load_open_monitor_env_view_row(monitor_date, run_pk)
        weekly_asof = row.get("env_weekly_asof_trade_date")
        daily_asof = row.get("env_daily_asof_trade_date")
        weekly_indicator = (
            self.repo.load_weekly_indicator(str(weekly_asof)) if weekly_asof else {}
        )
        benchmark_code = str(self.params.index_code or "sh.000001").strip() or "sh.000001"
        daily_env = (
            self.repo.load_daily_market_env(
                asof_trade_date=str(daily_asof),
                benchmark_code=benchmark_code,
            )
            if daily_asof
            else None
        )
        weekly_scenario = {
            "weekly_asof_trade_date": weekly_indicator.get("weekly_asof_trade_date")
            or weekly_asof,
            "weekly_risk_level": weekly_indicator.get("weekly_risk_level"),
            "weekly_scene_code": weekly_indicator.get("weekly_scene_code"),
            "weekly_phase": weekly_indicator.get("weekly_phase"),
            "weekly_gate_policy": weekly_indicator.get("weekly_gate_policy"),
            "weekly_gate_action": weekly_indicator.get("weekly_gate_policy"),
            "weekly_structure_status": weekly_indicator.get("weekly_structure_status"),
            "weekly_pattern_status": weekly_indicator.get("weekly_pattern_status"),
            "weekly_plan_a_exposure_cap": weekly_indicator.get("weekly_plan_a_exposure_cap"),
            "weekly_key_levels_str": weekly_indicator.get("weekly_key_levels_str"),
            "weekly_zone_id": weekly_indicator.get("weekly_zone_id"),
            "weekly_zone_score": weekly_indicator.get("weekly_zone_score"),
            "weekly_exp_return_bucket": weekly_indicator.get("weekly_exp_return_bucket"),
            "weekly_zone_reason": weekly_indicator.get("weekly_zone_reason"),
            "weekly_money_proxy": weekly_indicator.get("weekly_money_proxy"),
            "weekly_tags": weekly_indicator.get("weekly_tags"),
            "weekly_note": weekly_indicator.get("weekly_note"),
            "weekly_plan_json": weekly_indicator.get("weekly_plan_json"),
        }

        index_snapshot_hash = row.get("env_index_snapshot_hash")
        index_snapshot = {"env_index_snapshot_hash": index_snapshot_hash}
        if isinstance(env_view_row, dict):
            index_fields = [
                "env_index_code",
                "env_index_asof_trade_date",
                "env_index_live_trade_date",
                "env_index_asof_close",
                "env_index_asof_ma20",
                "env_index_asof_ma60",
                "env_index_asof_macd_hist",
                "env_index_asof_atr14",
                "env_index_live_open",
                "env_index_live_high",
                "env_index_live_low",
                "env_index_live_latest",
                "env_index_live_pct_change",
                "env_index_live_volume",
                "env_index_live_amount",
                "env_index_dev_ma20_atr",
                "env_index_gate_action",
                "env_index_gate_reason",
                "env_index_position_cap",
            ]
            index_snapshot.update(
                {key: env_view_row.get(key) for key in index_fields}
            )

        env_context: dict[str, Any] = {
            "weekly_scenario": weekly_scenario,
            "weekly_asof_trade_date": weekly_scenario.get("weekly_asof_trade_date"),
            "weekly_risk_level": weekly_scenario.get("weekly_risk_level"),
            "weekly_scene_code": weekly_scenario.get("weekly_scene_code"),
            "weekly_phase": weekly_scenario.get("weekly_phase"),
            "weekly_gate_policy": weekly_scenario.get("weekly_gate_policy"),
            "weekly_gate_action": weekly_scenario.get("weekly_gate_action"),
            "weekly_zone_id": weekly_scenario.get("weekly_zone_id"),
            "weekly_zone_score": weekly_scenario.get("weekly_zone_score"),
            "weekly_exp_return_bucket": weekly_scenario.get("weekly_exp_return_bucket"),
            "weekly_zone_reason": weekly_scenario.get("weekly_zone_reason"),
            "regime": (daily_env or {}).get("regime")
            or (env_view_row or {}).get("env_regime"),
            "index_score": (daily_env or {}).get("score")
            or (env_view_row or {}).get("env_index_score"),
            "position_hint": (daily_env or {}).get("position_hint")
            or (env_view_row or {}).get("env_position_hint"),
            "effective_position_hint": (daily_env or {}).get("position_hint")
            or (env_view_row or {}).get("env_position_hint"),
            "run_pk": row.get("run_pk"),
            "daily_asof_trade_date": (daily_env or {}).get("asof_trade_date")
            or daily_asof,
            "cycle_phase": (daily_env or {}).get("cycle_phase"),
            "daily_ma20": (daily_env or {}).get("ma20"),
            "daily_ma60": (daily_env or {}).get("ma60"),
            "daily_ma250": (daily_env or {}).get("ma250"),
            "daily_macd_hist": (daily_env or {}).get("macd_hist"),
            "daily_atr14": (daily_env or {}).get("atr14"),
            "daily_dev_ma20_atr": (daily_env or {}).get("dev_ma20_atr"),
            "daily_bb_mid": (daily_env or {}).get("bb_mid"),
            "daily_bb_upper": (daily_env or {}).get("bb_upper"),
            "daily_bb_lower": (daily_env or {}).get("bb_lower"),
            "daily_bb_width": (daily_env or {}).get("bb_width"),
            "daily_bb_pos": (daily_env or {}).get("bb_pos"),
            "daily_zone_id": (daily_env or {}).get("daily_zone_id"),
            "daily_zone_score": (daily_env or {}).get("daily_zone_score"),
            "daily_cap_multiplier": (daily_env or {}).get("daily_cap_multiplier"),
            "daily_zone_reason": (daily_env or {}).get("daily_zone_reason"),
            "breadth_pct_above_ma20": (daily_env or {}).get("breadth_pct_above_ma20"),
            "breadth_pct_above_ma60": (daily_env or {}).get("breadth_pct_above_ma60"),
            "breadth_risk_off_ratio": (daily_env or {}).get("breadth_risk_off_ratio"),
            "dispersion_score": (daily_env or {}).get("dispersion_score"),
            "env_index_snapshot_hash": index_snapshot_hash,
            "env_final_gate_action": row.get("env_final_gate_action"),
            "env_final_cap_pct": row.get("env_final_cap_pct"),
            "env_final_reason_json": row.get("env_final_reason_json"),
            "env_live_override_action": row.get("env_live_override_action"),
            "env_live_cap_multiplier": row.get("env_live_cap_multiplier"),
            "env_live_event_tags": row.get("env_live_event_tags"),
            "env_live_reason": row.get("env_live_reason"),
        }
        env_context["index_intraday"] = index_snapshot

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
        self,
        latest_trade_date: str,
        *,
        checked_at: dt.datetime | None = None,
        allow_auto_compute: bool = False,
    ) -> dict[str, Any]:
        expected_weekly_asof, _ = self.env_builder.resolve_latest_closed_week_end(
            latest_trade_date
        )
        weekly_indicator = self.repo.load_weekly_indicator(expected_weekly_asof) or {}
        if not weekly_indicator:
            if not allow_auto_compute:
                raise RuntimeError(
                    f"周线环境缺失（weekly_asof={expected_weekly_asof}），已禁止自动补算。"
                )
            weekly_rows = self.indicator_builder.compute_weekly_indicator(
                latest_trade_date, checked_at=checked_at
            )
            self.repo.upsert_weekly_indicator(weekly_rows)
            weekly_indicator = self.repo.load_weekly_indicator(expected_weekly_asof) or {}

        benchmark_code = str(self.params.index_code or "sh.000001").strip() or "sh.000001"
        daily_env = self.repo.load_daily_market_env(
            asof_trade_date=latest_trade_date, benchmark_code=benchmark_code
        )
        if not daily_env:
            if not allow_auto_compute:
                raise RuntimeError(
                    f"日线环境缺失（asof_trade_date={latest_trade_date}），已禁止自动补算。"
                )
            start_date = dt.date.fromisoformat(latest_trade_date)
            daily_rows = self.indicator_builder.compute_daily_indicators(
                start_date, start_date
            )
            daily_rows = self.repo.attach_cycle_phase_from_weekly(daily_rows)
            self.repo.upsert_daily_market_env(daily_rows)
            daily_env = self.repo.load_daily_market_env(
                asof_trade_date=latest_trade_date, benchmark_code=benchmark_code
            )

        weekly_scenario = {
            "weekly_asof_trade_date": weekly_indicator.get("weekly_asof_trade_date")
            or expected_weekly_asof,
            "weekly_scene_code": weekly_indicator.get("weekly_scene_code"),
            "weekly_phase": weekly_indicator.get("weekly_phase"),
            "weekly_structure_status": weekly_indicator.get("weekly_structure_status"),
            "weekly_pattern_status": weekly_indicator.get("weekly_pattern_status"),
            "weekly_risk_score": weekly_indicator.get("weekly_risk_score"),
            "weekly_risk_level": weekly_indicator.get("weekly_risk_level"),
            "weekly_gate_policy": weekly_indicator.get("weekly_gate_policy"),
            "weekly_gate_action": weekly_indicator.get("weekly_gate_policy"),
            "weekly_plan_a_exposure_cap": weekly_indicator.get("weekly_plan_a_exposure_cap"),
            "weekly_key_levels_str": weekly_indicator.get("weekly_key_levels_str"),
            "weekly_zone_id": weekly_indicator.get("weekly_zone_id"),
            "weekly_zone_score": weekly_indicator.get("weekly_zone_score"),
            "weekly_exp_return_bucket": weekly_indicator.get("weekly_exp_return_bucket"),
            "weekly_zone_reason": weekly_indicator.get("weekly_zone_reason"),
            "weekly_money_proxy": weekly_indicator.get("weekly_money_proxy"),
            "weekly_tags": weekly_indicator.get("weekly_tags"),
            "weekly_note": weekly_indicator.get("weekly_note"),
            "weekly_plan_json": weekly_indicator.get("weekly_plan_json"),
        }
        weekly_gate_policy = weekly_indicator.get("weekly_gate_policy")
        env_context = {
            "weekly_scenario": weekly_scenario,
            "weekly_asof_trade_date": weekly_scenario.get("weekly_asof_trade_date"),
            "weekly_risk_score": weekly_scenario.get("weekly_risk_score"),
            "weekly_risk_level": weekly_scenario.get("weekly_risk_level"),
            "weekly_scene_code": weekly_scenario.get("weekly_scene_code"),
            "weekly_phase": weekly_scenario.get("weekly_phase"),
            "weekly_structure_status": weekly_scenario.get("weekly_structure_status"),
            "weekly_pattern_status": weekly_scenario.get("weekly_pattern_status"),
            "weekly_plan_a_exposure_cap": weekly_scenario.get("weekly_plan_a_exposure_cap"),
            "weekly_key_levels_str": weekly_scenario.get("weekly_key_levels_str"),
            "weekly_money_proxy": weekly_scenario.get("weekly_money_proxy"),
            "weekly_tags": weekly_scenario.get("weekly_tags"),
            "weekly_note": weekly_scenario.get("weekly_note"),
            "weekly_gate_policy": weekly_gate_policy,
            "weekly_gate_action": weekly_gate_policy,
            "weekly_zone_id": weekly_scenario.get("weekly_zone_id"),
            "weekly_zone_score": weekly_scenario.get("weekly_zone_score"),
            "weekly_exp_return_bucket": weekly_scenario.get("weekly_exp_return_bucket"),
            "weekly_zone_reason": weekly_scenario.get("weekly_zone_reason"),
            "daily_asof_trade_date": daily_env.get("asof_trade_date")
            if daily_env
            else latest_trade_date,
            "index_score": _to_float(daily_env.get("score"))
            if daily_env
            else None,
            "regime": daily_env.get("regime") if daily_env else None,
            "position_hint": _to_float(daily_env.get("position_hint"))
            if daily_env
            else None,
            "effective_position_hint": _to_float(daily_env.get("position_hint"))
            if daily_env
            else None,
            "cycle_phase": daily_env.get("cycle_phase") if daily_env else None,
            "daily_ma20": _to_float(daily_env.get("ma20")) if daily_env else None,
            "daily_ma60": _to_float(daily_env.get("ma60")) if daily_env else None,
            "daily_ma250": _to_float(daily_env.get("ma250")) if daily_env else None,
            "daily_macd_hist": _to_float(daily_env.get("macd_hist")) if daily_env else None,
            "daily_atr14": _to_float(daily_env.get("atr14")) if daily_env else None,
            "daily_dev_ma20_atr": _to_float(daily_env.get("dev_ma20_atr"))
            if daily_env
            else None,
            "daily_bb_mid": _to_float(daily_env.get("bb_mid")) if daily_env else None,
            "daily_bb_upper": _to_float(daily_env.get("bb_upper")) if daily_env else None,
            "daily_bb_lower": _to_float(daily_env.get("bb_lower")) if daily_env else None,
            "daily_bb_width": _to_float(daily_env.get("bb_width")) if daily_env else None,
            "daily_bb_pos": _to_float(daily_env.get("bb_pos")) if daily_env else None,
            "daily_zone_id": daily_env.get("daily_zone_id") if daily_env else None,
            "daily_zone_score": daily_env.get("daily_zone_score") if daily_env else None,
            "daily_cap_multiplier": _to_float(daily_env.get("daily_cap_multiplier"))
            if daily_env
            else None,
            "daily_zone_reason": daily_env.get("daily_zone_reason") if daily_env else None,
            "breadth_pct_above_ma20": _to_float(daily_env.get("breadth_pct_above_ma20"))
            if daily_env
            else None,
            "breadth_pct_above_ma60": _to_float(daily_env.get("breadth_pct_above_ma60"))
            if daily_env
            else None,
            "breadth_risk_off_ratio": _to_float(daily_env.get("breadth_risk_off_ratio"))
            if daily_env
            else None,
            "dispersion_score": _to_float(daily_env.get("dispersion_score"))
            if daily_env
            else None,
        }
        if not weekly_gate_policy:
            weekly_gate_policy = self.env_builder.resolve_env_weekly_gate_policy(env_context)
            env_context["weekly_gate_policy"] = weekly_gate_policy
            env_context["weekly_gate_action"] = weekly_gate_policy
        self.env_builder._finalize_env_directives(
            env_context, weekly_gate_policy=weekly_gate_policy
        )

        # 补充板块强弱映射：用于展示/排序的 board_status（不影响 gate/action/入库）
        board_map: dict[str, dict[str, Any]] = {}
        try:
            from .open_monitor_eval import OpenMonitorEvaluator

            board_strength = self.env_builder.load_board_spot_strength(
                latest_trade_date, checked_at
            )
            board_map = OpenMonitorEvaluator.build_board_map_from_strength(board_strength)
        except Exception:  # noqa: BLE001
            board_map = {}

        env_context["boards"] = board_map
        return env_context

    def build_and_persist_open_monitor_env(
        self,
        latest_trade_date: str,
        *,
        monitor_date: str | None = None,
        run_id: str | None = None,
        run_pk: int | None = None,
        checked_at: dt.datetime | None = None,
        allow_auto_compute: bool = False,
        fetch_index_live_quote: Callable[[], dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        if checked_at is None:
            checked_at = dt.datetime.now()
        if monitor_date is None:
            monitor_date = checked_at.date().isoformat()
        if run_id is None:
            run_id = calc_run_id(checked_at, self.params.run_id_minutes)
        if run_pk is None:
            run_pk = self.repo.ensure_run_context(
                monitor_date,
                run_id,
                checked_at=checked_at,
                triggered_at=checked_at,
                params_json=None,
            )

        env_context = self.build_environment_context(
            latest_trade_date,
            checked_at=checked_at,
            allow_auto_compute=allow_auto_compute,
        )

        env_context, _, _ = self.attach_index_snapshot(
            latest_trade_date,
            monitor_date,
            run_id,
            checked_at,
            env_context,
            fetch_index_live_quote=fetch_index_live_quote,
        )

        weekly_gate_policy = env_context.get("weekly_gate_policy")
        self.env_builder._finalize_env_directives(
            env_context, weekly_gate_policy=weekly_gate_policy
        )

        if run_pk is None:
            self.logger.error("环境快照缺少 run_pk，已跳过写入。")
            return env_context

        self.repo.persist_open_monitor_env(env_context, monitor_date, run_pk)

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
                    "env_index_live_trade_date": live_quote.get("live_trade_date"),
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

        index_score = None
        regime = None
        position_hint = None
        if env_context and isinstance(env_context, dict):
            index_score = _to_float(env_context.get("index_score"))
            regime = env_context.get("regime")
            position_hint = env_context.get("position_hint")
        snapshot["env_index_score"] = index_score
        gate_action = derive_index_gate_action(regime, position_hint)
        gate_reason = f"regime={regime} pos_hint={position_hint}"
        snapshot["env_index_gate_action"] = gate_action
        snapshot["env_index_gate_reason"] = gate_reason
        snapshot["env_index_position_cap"] = _to_float(position_hint)

        return snapshot

    @staticmethod
    def _merge_live_actions(actions: list[str]) -> str:
        severity = {"EXIT": 3, "PAUSE": 2, "REDUCE": 1, "NONE": 0}
        if not actions:
            return "NONE"
        normalized = [str(action).strip().upper() for action in actions if action]
        if not normalized:
            return "NONE"
        normalized.sort(key=lambda x: severity.get(x, 0), reverse=True)
        return normalized[0]

    def evaluate_live_override(
        self, env_context: dict[str, Any], index_snapshot: dict[str, Any]
    ) -> dict[str, Any]:
        live_pct = _to_float(index_snapshot.get("env_index_live_pct_change"))
        live_latest = _to_float(index_snapshot.get("env_index_live_latest"))
        live_high = _to_float(index_snapshot.get("env_index_live_high"))
        live_low = _to_float(index_snapshot.get("env_index_live_low"))
        live_dev_ma20_atr = _to_float(index_snapshot.get("env_index_dev_ma20_atr"))
        daily_ma20 = _to_float(env_context.get("daily_ma20"))
        daily_bb_lower = _to_float(env_context.get("daily_bb_lower"))
        daily_bb_upper = _to_float(env_context.get("daily_bb_upper"))
        daily_bb_width = _to_float(env_context.get("daily_bb_width"))
        weekly_key_levels_str = env_context.get("weekly_key_levels_str")
        if not weekly_key_levels_str:
            weekly_scenario = env_context.get("weekly_scenario") or {}
            if isinstance(weekly_scenario, dict):
                weekly_key_levels_str = weekly_scenario.get("weekly_key_levels_str")
        key_levels = _parse_weekly_key_levels(weekly_key_levels_str)

        actions: list[str] = []
        cap_candidates: list[float] = []
        events: list[str] = []
        unlock_gate = None

        if live_pct is not None:
            if live_pct <= -3.0:
                actions.append("PAUSE")
                cap_candidates.append(0.0)
                events.append("INTRADAY_CRASH")
            elif live_pct <= -2.0:
                actions.append("REDUCE")
                cap_candidates.append(0.25)
                events.append("INTRADAY_DROP")

        if live_latest is not None and daily_bb_lower is not None:
            if live_latest < daily_bb_lower:
                actions.append("PAUSE")
                cap_candidates.append(0.0)
                events.append("BB_BREAK_LOWER")

        if (
            live_latest is not None
            and daily_ma20 is not None
            and live_dev_ma20_atr is not None
            and live_latest < daily_ma20
            and live_dev_ma20_atr <= -1.2
        ):
            actions.append("REDUCE")
            cap_candidates.append(0.5)
            events.append("BREAK_MA20")

        if (
            live_latest is not None
            and daily_bb_upper is not None
            and daily_bb_width is not None
            and live_latest > daily_bb_upper
            and daily_bb_width >= 0.12
        ):
            actions.append("REDUCE")
            cap_candidates.append(0.5)
            events.append("BB_OVERHEAT")

        upper_level = key_levels.get("upper")
        if (
            upper_level is not None
            and live_high is not None
            and live_latest is not None
            and live_low is not None
        ):
            if (
                live_high >= upper_level * 1.002
                and live_latest >= upper_level * 1.001
                and live_low >= upper_level * 0.997
            ):
                unlock_gate = "ALLOW_SMALL"
                events.append("LIVE_BREAKOUT_UPPER")
                events.append("LIVE_BREAKOUT_RETEST_HELD")

        action = self._merge_live_actions(actions)
        cap_multiplier = min(cap_candidates) if cap_candidates else 1.0
        reason = ";".join(events)[:255] if events else None
        return {
            "env_live_override_action": action,
            "env_live_cap_multiplier": cap_multiplier,
            "env_live_event_tags": ";".join(events)[:255] if events else None,
            "env_live_reason": reason,
            "env_live_unlock_gate": unlock_gate,
        }

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

        # run_id 用作 join key：保持非空（为空时用 checked_at 的 HH:MM 兜底）
        rid = str(run_id or "").strip()
        if not rid:
            rid = checked_at.strftime("%H:%M")
            self.logger.warning("env index snapshot: empty run_id -> fallback to %s", rid)
        run_id = rid
        index_env_snapshot: dict[str, Any] = {}
        env_index_snapshot_hash: str | None = None

        if latest_trade_date:
            idx = ctx.get("index") if isinstance(ctx, dict) else {}
            if not isinstance(idx, dict):
                idx = {}
            index_score = _to_float(ctx.get("index_score"))
            if index_score is None:
                index_score = _to_float(idx.get("score"))

            regime = ctx.get("regime") or idx.get("regime")

            position_hint = _to_float(ctx.get("position_hint"))
            if position_hint is None:
                position_hint = _to_float(idx.get("position_hint"))
            if regime is not None:
                regime = str(regime).strip() or None

            ctx["index_score"] = index_score
            ctx["regime"] = regime
            ctx["position_hint"] = position_hint

            if regime is None or position_hint is None:
                self.logger.warning(
                    "指数环境缺失（regime/position_hint），已跳过指数快照构建。 index_score=%s regime=%s position_hint=%s",
                    index_score,
                    regime,
                    position_hint,
                )
                return ctx or env_context, {}, None

            asof_indicators = self.repo.load_index_history(latest_trade_date)
            live_quote = fetch_index_live_quote() if fetch_index_live_quote else {}
            if isinstance(live_quote, dict):
                live_trade_date = live_quote.get("live_trade_date")
                if pd.isna(live_trade_date) or (
                    isinstance(live_trade_date, str) and not live_trade_date.strip()
                ):
                    live_quote["live_trade_date"] = monitor_date
            index_env_snapshot = self._build_index_env_snapshot(
                asof_indicators,
                live_quote,
                env_context=ctx,
            )

        if index_env_snapshot:
            index_snapshot_payload = {
                "monitor_date": monitor_date,
                "checked_at": checked_at,
                "run_id": run_id,
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
            env_index_snapshot_hash = index_snapshot_payload["snapshot_hash"]
            index_env_snapshot["env_index_snapshot_hash"] = env_index_snapshot_hash
            ctx["index_intraday"] = index_env_snapshot
            live_override = self.evaluate_live_override(ctx, index_env_snapshot)
            ctx.update(live_override)
            if index_env_snapshot.get("env_index_code"):
                quote_payload = {
                    "monitor_date": monitor_date,
                    "run_id": run_id,
                    "code": index_env_snapshot.get("env_index_code"),
                    "live_trade_date": index_env_snapshot.get("env_index_live_trade_date"),
                    "live_open": _to_float(index_env_snapshot.get("env_index_live_open")),
                    "live_high": _to_float(index_env_snapshot.get("env_index_live_high")),
                    "live_low": _to_float(index_env_snapshot.get("env_index_live_low")),
                    "live_latest": _to_float(index_env_snapshot.get("env_index_live_latest")),
                    "live_volume": _to_float(index_env_snapshot.get("env_index_live_volume")),
                    "live_amount": _to_float(index_env_snapshot.get("env_index_live_amount")),
                }
                quote_df = pd.DataFrame([quote_payload])
                self.repo.persist_quote_snapshots(quote_df)

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
                live_pct if live_pct is not None else 0.0,
                dev_ma20_atr if dev_ma20_atr is not None else 0.0,
                gate_action,
                gate_reason,
            )

        return ctx or env_context, index_env_snapshot, env_index_snapshot_hash

    def log_weekly_scenario(self, env_context: dict[str, Any] | None) -> None:
        weekly_scenario = (
            env_context.get("weekly_scenario", {}) if isinstance(env_context, dict) else {}
        )
        if not isinstance(weekly_scenario, dict):
            return

        confirm_tags = ",".join(weekly_scenario.get("weekly_confirm_tags", []) or [])

        asof_trade_date = weekly_scenario.get("weekly_asof_trade_date")
        current_week_closed = weekly_scenario.get("weekly_current_week_closed")

        risk_level = weekly_scenario.get("weekly_risk_level")
        risk_score = _to_float(weekly_scenario.get("weekly_risk_score"))
        risk_score = risk_score or 0.0

        scene_code = weekly_scenario.get("weekly_scene_code")

        bias = weekly_scenario.get("weekly_bias")

        status = weekly_scenario.get("weekly_status")
        if status is None:
            status = (
                weekly_scenario.get("weekly_pattern_status")
                or weekly_scenario.get("weekly_structure_status")
            )

        key_levels_str = weekly_scenario.get("weekly_key_levels_str")
        key_levels_short = str(key_levels_str or "")[:120]

        if not confirm_tags:
            tags_str = weekly_scenario.get("weekly_tags")
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
        weekly_note = str(weekly_scenario.get("weekly_note") or "").strip()
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
