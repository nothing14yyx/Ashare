"""周线环境构建器。"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Dict

import pandas as pd
from sqlalchemy import bindparam, text

from .baostock_core import BaostockDataFetcher
from .baostock_session import BaostockSession
from .market_regime import MarketRegimeClassifier
from .utils.convert import to_float
from .weekly_channel_regime import WeeklyChannelClassifier
from .weekly_pattern_system import WeeklyPlanSystem


class WeeklyEnvironmentBuilder:
    """负责构建周线环境上下文的组件。"""

    def __init__(
        self,
        *,
        db_writer,
        logger: logging.Logger,
        index_codes: list[str],
        board_env_enabled: bool,
        board_spot_enabled: bool,
        env_index_score_threshold: float,
        weekly_soft_gate_strength_threshold: float,
    ) -> None:
        self.db_writer = db_writer
        self.logger = logger
        self.index_codes = index_codes
        self.board_env_enabled = board_env_enabled
        self.board_spot_enabled = board_spot_enabled
        self.env_index_score_threshold = env_index_score_threshold
        self.weekly_soft_gate_strength_threshold = weekly_soft_gate_strength_threshold

        self.market_regime = MarketRegimeClassifier()
        self.weekly_channel = WeeklyChannelClassifier(primary_code="sh.000001")
        self.weekly_plan_system = WeeklyPlanSystem()

        self._calendar_cache: set[str] = set()
        self._calendar_range: tuple[dt.date, dt.date] | None = None
        self._baostock_client: BaostockDataFetcher | None = None

    def _get_baostock_client(self) -> BaostockDataFetcher:
        if self._baostock_client is None:
            self._baostock_client = BaostockDataFetcher(BaostockSession())
        return self._baostock_client

    def load_trading_calendar(self, start: dt.date, end: dt.date) -> bool:
        """加载并缓存交易日历，避免节假日误判。"""

        if (
            self._calendar_range
            and start >= self._calendar_range[0]
            and end <= self._calendar_range[1]
        ):
            return True

        current_start = start
        current_end = end
        if self._calendar_range:
            current_start = min(self._calendar_range[0], start)
            current_end = max(self._calendar_range[1], end)

        try:
            client = self._get_baostock_client()
            calendar_df = client.get_trade_calendar(
                current_start.isoformat(), current_end.isoformat()
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("加载交易日历失败，将回退工作日判断：%s", exc)
            return False

        if calendar_df.empty or "calendar_date" not in calendar_df.columns:
            return False

        dates = (
            pd.to_datetime(calendar_df["calendar_date"], errors="coerce")
            .dt.date.dropna()
            .tolist()
        )
        self._calendar_cache.update({d.isoformat() for d in dates})
        self._calendar_range = (current_start, current_end)
        return True

    def resolve_latest_closed_week_end(self, latest_trade_date: str) -> tuple[str, bool]:
        """确定最近一个已收盘的周末交易日（周线确认）。"""

        def _parse_date(val: str) -> dt.date | None:
            try:
                return dt.datetime.strptime(val, "%Y-%m-%d").date()
            except Exception:  # noqa: BLE001
                return None

        trade_date = _parse_date(latest_trade_date)
        if trade_date is None:
            return latest_trade_date, True

        week_start = trade_date - dt.timedelta(days=trade_date.weekday())
        week_end = week_start + dt.timedelta(days=6)
        calendar_loaded = self.load_trading_calendar(
            week_start - dt.timedelta(days=21), week_end
        )

        def _in_cache(date_val: dt.date) -> bool:
            return date_val.isoformat() in self._calendar_cache

        if calendar_loaded:
            last_trade_day_in_week: dt.date | None = None
            for i in range(7):
                candidate = week_end - dt.timedelta(days=i)
                if _in_cache(candidate):
                    last_trade_day_in_week = candidate
                    break

            if last_trade_day_in_week:
                if trade_date == last_trade_day_in_week:
                    return trade_date.isoformat(), True

                prev_week_last: dt.date | None = None
                prev_candidate = week_start - dt.timedelta(days=1)
                for _ in range(30):
                    if _in_cache(prev_candidate):
                        prev_week_last = prev_candidate
                        break
                    prev_candidate -= dt.timedelta(days=1)

                if prev_week_last:
                    return prev_week_last.isoformat(), False

        fallback_friday = week_start + dt.timedelta(days=4)
        if trade_date >= fallback_friday:
            return fallback_friday.isoformat(), trade_date == fallback_friday

        prev_friday = fallback_friday - dt.timedelta(days=7)
        return prev_friday.isoformat(), False

    def load_index_trend(self, latest_trade_date: str) -> Dict[str, Any]:
        """加载指数趋势情景。"""

        if not self.index_codes or not self._table_exists("history_index_daily_kline"):
            return {"score": None, "detail": {}, "regime": None, "position_hint": None}

        end_date = dt.datetime.strptime(latest_trade_date, "%Y-%m-%d").date()
        start_date = (end_date - dt.timedelta(days=200)).isoformat()

        stmt = text(
            """
            SELECT `code`, `date`, `open`, `high`, `low`, `close`, `volume`, `amount`
            FROM history_index_daily_kline
            WHERE `code` IN :codes AND `date` >= :start_date AND `date` <= :end_date
            ORDER BY `code`, `date`
            """
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "codes": self.index_codes,
                        "start_date": start_date,
                        "end_date": latest_trade_date,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取指数日线失败：%s", exc)
            return {"score": None, "detail": {}, "regime": None, "position_hint": None}

        if df.empty:
            return {"score": None, "detail": {}, "regime": None, "position_hint": None}

        regime_result = self.market_regime.classify(df)
        payload = regime_result.to_payload()
        return payload

    def _table_exists(self, table: str) -> bool:
        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1"))
            return True
        except Exception:
            return False

    def load_board_spot_strength(self) -> pd.DataFrame:
        """读取板块强弱（实时）。"""

        if not self.board_env_enabled or not self.board_spot_enabled:
            return pd.DataFrame()

        try:
            import akshare as ak  # type: ignore
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("AkShare 不可用，无法获取板块强弱：%s", exc)
            return pd.DataFrame()

        try:
            board_df = ak.stock_board_industry_spot_em()
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("获取板块强弱失败：%s", exc)
            return pd.DataFrame()

        if board_df is None or getattr(board_df, "empty", True):
            return pd.DataFrame()

        rename_map = {"板块名称": "board_name", "板块代码": "board_code", "涨跌幅": "chg_pct"}
        for key in list(rename_map.keys()):
            if key not in board_df.columns:
                rename_map.pop(key, None)

        board_df = board_df.rename(columns=rename_map)
        if "chg_pct" in board_df.columns:
            board_df = board_df.sort_values(by="chg_pct", ascending=False)
            board_df["rank"] = range(1, len(board_df) + 1)
        return board_df

    def load_index_weekly_channel(self, latest_trade_date: str) -> dict[str, Any]:
        """加载指数周线通道情景（从指数日线聚合为周线计算）。"""

        if not self.index_codes or not self._table_exists("history_index_daily_kline"):
            return {"state": None, "position_hint": None, "detail": {}, "primary_code": None}

        week_end_asof, current_week_closed = self.resolve_latest_closed_week_end(
            latest_trade_date
        )

        start_date = None
        try:
            end_dt = dt.datetime.strptime(week_end_asof, "%Y-%m-%d").date()
            start_date = (end_dt - dt.timedelta(days=900)).isoformat()
        except Exception:  # noqa: BLE001
            start_date = None

        stmt = text(
            f"""
            SELECT `code`, `date`, `open`, `high`, `low`, `close`, `volume`, `amount`
            FROM history_index_daily_kline
            WHERE `code` IN :codes AND `date` <= :d
            {'AND `date` >= :start_date' if start_date is not None else ''}
            ORDER BY `code`, `date`
            """
        ).bindparams(bindparam("codes", expanding=True))
        try:
            with self.db_writer.engine.begin() as conn:
                params = {"codes": self.index_codes, "d": week_end_asof}
                if start_date is not None:
                    params["start_date"] = start_date
                df = pd.read_sql_query(stmt, conn, params=params)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取指数日线用于周线通道失败：%s", exc)
            return {"state": None, "position_hint": None, "detail": {}, "primary_code": None}

        if df.empty:
            return {"state": None, "position_hint": None, "detail": {}, "primary_code": None}

        result = self.weekly_channel.classify(df)
        payload = result.to_payload()
        payload["weekly_asof_trade_date"] = week_end_asof
        payload["weekly_current_week_closed"] = current_week_closed
        payload["weekly_asof_week_closed"] = True

        weekly_bars_by_code = {}
        if isinstance(result.context, dict):
            raw_weekly_bars = result.context.get("weekly_bars_by_code")
            if isinstance(raw_weekly_bars, dict):
                weekly_bars_by_code = {
                    str(code): bars
                    for code, bars in raw_weekly_bars.items()
                    if isinstance(bars, list)
                }

        merged_weekly_windows: list[dict[str, Any]] = []
        for code, bars in weekly_bars_by_code.items():
            for item in bars:
                if isinstance(item, dict):
                    merged_weekly_windows.append({"code": code, **item})

        payload["weekly_windows_by_code"] = weekly_bars_by_code
        payload["weekly_windows_merged"] = merged_weekly_windows
        return payload

    @staticmethod
    def _clip(text_val: str | None, limit: int = 255) -> str | None:
        if text_val is None:
            return None
        normalized = " ".join(str(text_val).split())
        return normalized[:limit]

    def build_weekly_scenario(
        self, weekly_payload: dict[str, Any], index_trend: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        scenario: dict[str, Any] = {
            "weekly_asof_trade_date": None,
            "weekly_week_closed": False,
            "weekly_current_week_closed": False,
            "weekly_gating_enabled": False,
            "weekly_structure_tags": [],
            "weekly_confirm_tags": [],
            "weekly_risk_score": None,
            "weekly_risk_level": "UNKNOWN",
            "weekly_confirm": None,
            "weekly_key_levels": {},
            "weekly_money_proxy": {},
            "weekly_plan_a": None,
            "weekly_plan_b": None,
            "weekly_scene_code": None,
            "weekly_bias": "NEUTRAL",
            "weekly_status": "FORMING",
            "weekly_key_levels_str": None,
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
            scenario["weekly_plan_a"] = "周线数据缺失，轻仓观望"
            scenario["weekly_plan_b"] = "周线数据缺失，轻仓观望"
            return scenario

        plan = self.weekly_plan_system.build(weekly_payload, index_trend or {})

        scenario.update(plan)
        scenario["weekly_asof_trade_date"] = plan.get("weekly_asof_trade_date")
        scenario["weekly_week_closed"] = plan.get("weekly_week_closed", False)
        scenario["weekly_current_week_closed"] = plan.get("weekly_current_week_closed", False)
        scenario["weekly_gating_enabled"] = bool(plan.get("weekly_gating_enabled", False))
        scenario["weekly_risk_score"] = to_float(plan.get("weekly_risk_score"))
        scenario["weekly_risk_level"] = plan.get("weekly_risk_level") or "UNKNOWN"
        scenario["weekly_confirm"] = plan.get("weekly_confirm")
        scenario["weekly_key_levels"] = plan.get("weekly_key_levels", {})
        scenario["weekly_key_levels_str"] = self._clip(plan.get("weekly_key_levels_str"), 255)
        scenario["weekly_plan_a"] = self._clip(plan.get("weekly_plan_a"), 255)
        scenario["weekly_plan_b"] = self._clip(plan.get("weekly_plan_b"), 255)
        scenario["weekly_plan_a_if"] = self._clip(plan.get("weekly_plan_a_if"), 255)
        scenario["weekly_plan_a_then"] = self._clip(plan.get("weekly_plan_a_then"), 64)
        scenario["weekly_plan_a_confirm"] = self._clip(plan.get("weekly_plan_a_confirm"), 128)
        scenario["weekly_plan_a_exposure_cap"] = to_float(plan.get("weekly_plan_a_exposure_cap"))
        scenario["weekly_plan_b_if"] = self._clip(plan.get("weekly_plan_b_if"), 255)
        scenario["weekly_plan_b_then"] = self._clip(plan.get("weekly_plan_b_then"), 64)
        scenario["weekly_plan_b_recover_if"] = self._clip(plan.get("weekly_plan_b_recover_if"), 128)
        scenario["weekly_plan_json"] = self._clip(plan.get("weekly_plan_json"), 2000)

        tags: list[str] = []
        for key in ["weekly_structure_tags", "weekly_confirm_tags"]:
            vals = plan.get(key)
            if isinstance(vals, list):
                tags.extend([str(v) for v in vals if str(v)])
        if plan.get("weekly_bias"):
            tags.append(f"BIAS_{plan['weekly_bias']}")
        if plan.get("weekly_status"):
            tags.append(f"STATUS_{plan['weekly_status']}")
        scenario["weekly_structure_tags"] = plan.get("weekly_structure_tags", [])
        scenario["weekly_confirm_tags"] = plan.get("weekly_confirm_tags", [])
        scenario["weekly_tags"] = ";".join(tags)[:255] if tags else None

        return scenario

    def build_environment_context(self, latest_trade_date: str) -> dict[str, Any]:
        index_trend = self.load_index_trend(latest_trade_date)
        weekly_channel = self.load_index_weekly_channel(latest_trade_date)
        weekly_scenario = self.build_weekly_scenario(weekly_channel, index_trend)
        board_strength = self.load_board_spot_strength()
        board_map: dict[str, Any] = {}
        if not board_strength.empty and "board_name" in board_strength.columns:
            total = len(board_strength)
            for _, row in board_strength.iterrows():
                name = str(row.get("board_name") or "").strip()
                code = str(row.get("board_code") or "").strip()
                rank = row.get("rank")
                pct = row.get("chg_pct")
                status = "neutral"
                if total > 0 and rank:
                    if rank <= max(1, int(total * 0.2)):
                        status = "strong"
                    elif rank >= max(1, int(total * 0.8)):
                        status = "weak"
                payload = {"rank": rank, "chg_pct": pct, "status": status}
                for key in [name, code]:
                    key_norm = str(key).strip()
                    if key_norm:
                        board_map[key_norm] = payload

        env_context = {
            "index": index_trend,
            "weekly": weekly_channel,
            "weekly_windows": weekly_channel.get("weekly_windows_merged"),
            "weekly_windows_by_code": weekly_channel.get("weekly_windows_by_code"),
            "boards": board_map,
            "regime": index_trend.get("regime"),
            "position_hint": index_trend.get("position_hint"),
            "weekly_state": weekly_channel.get("state") if isinstance(weekly_channel, dict) else None,
            "weekly_position_hint": weekly_channel.get("position_hint") if isinstance(weekly_channel, dict) else None,
            "weekly_note": weekly_channel.get("note") if isinstance(weekly_channel, dict) else None,
            "weekly_scenario": weekly_scenario,
            "weekly_asof_trade_date": weekly_scenario.get("weekly_asof_trade_date"),
            "weekly_week_closed": weekly_scenario.get("weekly_week_closed"),
            "weekly_current_week_closed": weekly_scenario.get("weekly_current_week_closed"),
            "weekly_risk_score": weekly_scenario.get("weekly_risk_score"),
            "weekly_risk_level": weekly_scenario.get("weekly_risk_level"),
            "weekly_confirm": weekly_scenario.get("weekly_confirm"),
            "weekly_gating_enabled": weekly_scenario.get("weekly_gating_enabled", False),
            "weekly_plan_a": weekly_scenario.get("weekly_plan_a"),
            "weekly_plan_b": weekly_scenario.get("weekly_plan_b"),
            "weekly_scene_code": weekly_scenario.get("weekly_scene_code"),
            "weekly_key_levels_str": weekly_scenario.get("weekly_key_levels_str"),
            "weekly_plan_a_if": weekly_scenario.get("weekly_plan_a_if"),
            "weekly_plan_a_then": weekly_scenario.get("weekly_plan_a_then"),
            "weekly_plan_a_confirm": weekly_scenario.get("weekly_plan_a_confirm"),
            "weekly_plan_a_exposure_cap": weekly_scenario.get("weekly_plan_a_exposure_cap"),
            "weekly_plan_b_if": weekly_scenario.get("weekly_plan_b_if"),
            "weekly_plan_b_then": weekly_scenario.get("weekly_plan_b_then"),
            "weekly_plan_b_recover_if": weekly_scenario.get("weekly_plan_b_recover_if"),
            "weekly_plan_json": weekly_scenario.get("weekly_plan_json"),
            "weekly_bias": weekly_scenario.get("weekly_bias"),
            "weekly_status": weekly_scenario.get("weekly_status"),
        }

        money_proxy = weekly_scenario.get("weekly_money_proxy") if isinstance(weekly_scenario, dict) else {}
        proxy_parts: list[str] = []
        if isinstance(money_proxy, dict):
            vol_ratio = money_proxy.get("vol_ratio_20")
            slope_delta = money_proxy.get("slope_change_4w")
            obv_slope = money_proxy.get("obv_slope_13")
            if vol_ratio is not None:
                proxy_parts.append(f"vol_ratio_20={vol_ratio:.2f}")
            if slope_delta is not None:
                proxy_parts.append(f"slope_chg_4w={slope_delta:.4f}")
            if obv_slope is not None:
                proxy_parts.append(f"obv_slope_13={obv_slope:.2f}")
        env_context["weekly_money_proxy"] = ";".join(proxy_parts)[:255] if proxy_parts else None

        scenario_tags: list[str] = []
        if isinstance(weekly_scenario, dict):
            for key in ["weekly_structure_tags", "weekly_confirm_tags"]:
                tags = weekly_scenario.get(key)
                if isinstance(tags, list):
                    scenario_tags.extend([str(t) for t in tags if str(t)])
            if weekly_scenario.get("weekly_bias"):
                scenario_tags.append(f"BIAS_{weekly_scenario['weekly_bias']}")
            if weekly_scenario.get("weekly_status"):
                scenario_tags.append(f"STATUS_{weekly_scenario['weekly_status']}")
            if weekly_scenario.get("weekly_tags") and not scenario_tags:
                scenario_tags.extend(str(weekly_scenario.get("weekly_tags")).split(";"))
        env_context["weekly_tags"] = ";".join(scenario_tags)[:255] if scenario_tags else None

        for key in [
            "below_ma250_streak",
            "break_confirmed",
            "reclaim_confirmed",
            "effective_breakdown_days",
            "effective_reclaim_days",
            "yearline_state",
            "regime_note",
        ]:
            if isinstance(index_trend, dict) and key in index_trend:
                env_context[key] = index_trend[key]

        return env_context

    @staticmethod
    def resolve_env_weekly_gate_policy(env_context: dict[str, Any] | None) -> str | None:
        if not env_context:
            return None

        weekly_scenario = (
            env_context.get("weekly_scenario") if isinstance(env_context, dict) else {}
        )
        if not isinstance(weekly_scenario, dict):
            weekly_scenario = {}

        existing_policy = None
        if isinstance(env_context, dict):
            existing_policy = env_context.get("weekly_gate_policy")
            if existing_policy:
                return str(existing_policy)

        def _get_env(key: str) -> Any:  # noqa: ANN401
            if isinstance(env_context, dict):
                value = env_context.get(key, None)
                if value not in (None, "", [], {}):
                    return value
            return weekly_scenario.get(key)

        gating_enabled = bool(_get_env("weekly_gating_enabled"))
        risk_level = str(_get_env("weekly_risk_level") or "").upper()
        status = str(_get_env("weekly_status") or "").upper()

        if not gating_enabled:
            return "ALLOW"

        if risk_level == "HIGH":
            return "WAIT"

        if risk_level == "MEDIUM" and status == "FORMING":
            return "WAIT"

        if risk_level in {"LOW", "MEDIUM"}:
            return "ALLOW"

        return None

    @property
    def calendar_cache(self) -> set[str]:
        return self._calendar_cache

    @property
    def calendar_range(self) -> tuple[dt.date, dt.date] | None:
        return self._calendar_range
