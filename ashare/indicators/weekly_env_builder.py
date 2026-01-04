"""周线环境构建器。"""

from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any, Dict

import pandas as pd
from sqlalchemy import bindparam, text

from ashare.data.baostock_core import BaostockDataFetcher
from ashare.data.baostock_session import BaostockSession
from ashare.indicators.market_regime import MarketRegimeClassifier
from ashare.core.schema_manager import WEEKLY_MARKET_BENCHMARK_CODE
from ashare.core.config import get_section
from ashare.utils.convert import to_float
from ashare.indicators.weekly_channel_regime import WeeklyChannelClassifier
from ashare.indicators.weekly_pattern_system import WeeklyPlanSystem


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
        self.benchmark_code = WEEKLY_MARKET_BENCHMARK_CODE
        channel_cfg = get_section("weekly_env") or {}
        if not isinstance(channel_cfg, dict):
            channel_cfg = {}

        def _get_float(key: str, default: float) -> float:
            raw = channel_cfg.get(key, default)
            try:
                return float(raw)
            except Exception:
                return float(default)

        def _get_int(key: str, default: int) -> int:
            raw = channel_cfg.get(key, default)
            try:
                return int(raw)
            except Exception:
                return int(default)

        def _get_str(key: str, default: str) -> str:
            raw = channel_cfg.get(key, default)
            return str(raw).strip() or str(default)

        def _get_bool(key: str, default: bool) -> bool:
            raw = channel_cfg.get(key, default)
            if isinstance(raw, bool):
                return raw
            if isinstance(raw, str):
                return raw.strip().lower() in {"1", "true", "yes", "y", "on"}
            return bool(raw)

        self.weekly_channel = WeeklyChannelClassifier(
            primary_code=self.benchmark_code,
            lrc_length=_get_int("lrc_length", 52),
            lrc_dev=_get_float("lrc_dev", 2.0),
            ma_fast=_get_int("ma_fast", 30),
            ma_slow=_get_int("ma_slow", 60),
            breakout_vol_ratio_threshold=_get_float("breakout_vol_ratio_threshold", 1.2),
            breakdown_vol_weak_threshold=_get_float("breakdown_vol_weak_threshold", 0.9),
            slope_change_ratio_threshold=_get_float("slope_change_ratio_threshold", 0.002),
            bandwidth_lookback=_get_int("bandwidth_lookback", 104),
            bandwidth_high_quantile=_get_float("bandwidth_high_quantile", 0.9),
            bandwidth_cap=_get_float("bandwidth_cap", 0.4),
        )
        self.conflict_requires_vol_confirm = _get_bool(
            "conflict_requires_vol_confirm", True
        )
        self.conflict_gate = _get_str("conflict_gate", "ALLOW_SMALL").upper() or "ALLOW_SMALL"
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
            return latest_trade_date, False

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
                prev_week_last: dt.date | None = None
                prev_candidate = week_start - dt.timedelta(days=1)
                for _ in range(30):
                    if _in_cache(prev_candidate):
                        prev_week_last = prev_candidate
                        break
                    prev_candidate -= dt.timedelta(days=1)

                week_end_asof = (
                    trade_date
                    if trade_date == last_trade_day_in_week
                    else prev_week_last or last_trade_day_in_week
                )
                return week_end_asof.isoformat(), trade_date == week_end_asof

        fallback_friday = week_start + dt.timedelta(days=4)
        if trade_date >= fallback_friday:
            week_end_asof = fallback_friday
            return week_end_asof.isoformat(), trade_date == week_end_asof

        prev_friday = fallback_friday - dt.timedelta(days=7)
        return prev_friday.isoformat(), trade_date == prev_friday

    def load_index_trend(self, latest_trade_date: str) -> Dict[str, Any]:
        """加载指数趋势情景。"""

        if not self.index_codes or not self._table_exists("history_index_daily_kline"):
            return {"score": None, "detail": {}, "regime": None, "position_hint": None}

        end_date = dt.datetime.strptime(latest_trade_date, "%Y-%m-%d").date()
        start_date = (end_date - dt.timedelta(days=600)).isoformat()

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

    def load_board_spot_strength_from_db(
        self, latest_trade_date: str | None, checked_at: dt.datetime | None
    ) -> pd.DataFrame:
        """优先从数据库读取板块强弱快照。"""

        if not self.board_env_enabled or not self.board_spot_enabled:
            return pd.DataFrame()

        table = "board_industry_spot"
        if not self._table_exists(table):
            return pd.DataFrame()

        target_ts = checked_at or dt.datetime.now()
        latest_ts = None
        stmt_latest = text(
            f"""
            SELECT MAX(`ts`) AS ts
            FROM `{table}`
            WHERE `ts` <= :ts
            """
        )
        try:
            with self.db_writer.engine.begin() as conn:
                latest_df = pd.read_sql_query(stmt_latest, conn, params={"ts": target_ts})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取板块快照时间失败：%s", exc)
            latest_df = pd.DataFrame()

        if not latest_df.empty:
            latest_ts = latest_df.iloc[0].get("ts")

        if latest_ts is None and latest_trade_date:
            try:
                trade_date = dt.datetime.strptime(latest_trade_date, "%Y-%m-%d").date()
            except Exception:
                trade_date = None
            if trade_date is not None:
                stmt_trade_date = text(
                    f"""
                    SELECT MAX(`ts`) AS ts
                    FROM `{table}`
                    WHERE DATE(`ts`) = :trade_date
                    """
                )
                try:
                    with self.db_writer.engine.begin() as conn:
                        trade_df = pd.read_sql_query(
                            stmt_trade_date, conn, params={"trade_date": trade_date}
                        )
                    if not trade_df.empty:
                        latest_ts = trade_df.iloc[0].get("ts")
                except Exception as exc:  # noqa: BLE001
                    self.logger.debug("按交易日读取板块快照失败：%s", exc)

        if latest_ts is None:
            return pd.DataFrame()

        stmt_fetch = text(
            f"""
            SELECT * FROM `{table}`
            WHERE `ts` = :ts
            """
        )
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt_fetch, conn, params={"ts": latest_ts})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取板块快照明细失败：%s", exc)
            return pd.DataFrame()

        if df.empty:
            return pd.DataFrame()

        rename_map = {}
        for col in df.columns:
            if "代码" in col and "board_code" not in rename_map:
                rename_map[col] = "board_code"
            if "名称" in col and "board_name" not in rename_map and "板块" in col:
                rename_map[col] = "board_name"
            if col in {"涨跌幅", "涨跌幅(%)", "chg_pct", "change_rate", "pct_chg"}:
                rename_map[col] = "chg_pct"
            if "排名" in col and "rank" not in rename_map:
                rename_map[col] = "rank"

        df = df.rename(columns=rename_map)
        if "board_code" in df.columns:
            df["board_code"] = df["board_code"].astype(str)
        if "chg_pct" in df.columns:
            if df["chg_pct"].dtype == object:
                df["chg_pct"] = (
                    df["chg_pct"]
                    .astype(str)
                    .str.replace("%", "", regex=False)
                    .str.replace("％", "", regex=False)
                    .str.strip()
                )
            df["chg_pct"] = pd.to_numeric(df["chg_pct"], errors="coerce")
        if "rank" in df.columns:
            df["rank"] = pd.to_numeric(df["rank"], errors="coerce")

        if "rank" not in df.columns or df["rank"].isna().all():
            if "chg_pct" in df.columns:
                df = df.sort_values(by="chg_pct", ascending=False)
            df["rank"] = range(1, len(df) + 1)
        return df

    def load_board_spot_strength(
        self, latest_trade_date: str | None = None, checked_at: dt.datetime | None = None
    ) -> pd.DataFrame:
        """读取板块强弱，优先数据库失败再实时。"""

        db_df = self.load_board_spot_strength_from_db(latest_trade_date, checked_at)
        if not db_df.empty:
            return db_df

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
            "weekly_money_tags": [],
            "weekly_risk_score": None,
            "weekly_risk_level": "UNKNOWN",
            "weekly_confirm": False,
            "weekly_direction_confirmed": False,
            "weekly_key_levels": {},
            "weekly_money_proxy": {},
            "weekly_phase": None,
            "weekly_plan_a": None,
            "weekly_plan_b": None,
            "weekly_scene_code": None,
            "weekly_bias": "NEUTRAL",
            "weekly_status": "FORMING",
            "weekly_structure_status": "FORMING",
            "weekly_pattern_status": None,
            "weekly_key_levels_str": None,
            "weekly_plan_a_if": None,
            "weekly_plan_a_then": None,
            "weekly_plan_a_confirm": None,
            "weekly_plan_a_exposure_cap": None,
            "weekly_plan_b_if": None,
            "weekly_plan_b_then": None,
            "weekly_plan_b_recover_if": None,
            "weekly_plan_json": None,
            "weekly_note": None,
        }

        if not isinstance(weekly_payload, dict):
            scenario["weekly_plan_a"] = "周线数据缺失，轻仓观望"
            scenario["weekly_plan_b"] = "周线数据缺失，轻仓观望"
            return scenario

        plan = self.weekly_plan_system.build(weekly_payload, index_trend or {})

        scenario.update(plan)
        scenario["weekly_asof_trade_date"] = plan.get("weekly_asof_trade_date")
        scenario["weekly_week_closed"] = plan.get("weekly_week_closed", False)
        scenario["weekly_current_week_closed"] = bool(
            plan.get("weekly_current_week_closed", False)
        )
        scenario["weekly_gating_enabled"] = bool(plan.get("weekly_gating_enabled", False))
        scenario["weekly_risk_score"] = to_float(plan.get("weekly_risk_score"))
        scenario["weekly_risk_level"] = plan.get("weekly_risk_level") or "UNKNOWN"
        scenario["weekly_confirm"] = plan.get("weekly_confirm")
        scenario["weekly_direction_confirmed"] = bool(
            plan.get("weekly_direction_confirmed", False)
        )
        scenario["weekly_phase"] = plan.get("weekly_phase")
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
        scenario["weekly_structure_status"] = (
            plan.get("weekly_structure_status") or plan.get("weekly_status")
        )
        scenario["weekly_pattern_status"] = plan.get("weekly_pattern_status")
        if not scenario.get("weekly_week_closed", True):
            scenario["weekly_note"] = "本周未收盘，等待区间破位/突破（周收盘有效）"

        tags: list[str] = []
        for key in ["weekly_structure_tags", "weekly_confirm_tags"]:
            vals = plan.get(key)
            if isinstance(vals, list):
                tags.extend([str(v) for v in vals if str(v)])
        if plan.get("weekly_bias"):
            tags.append(f"BIAS_{plan['weekly_bias']}")
        if plan.get("weekly_structure_status"):
            tags.append(f"STATUS_{plan['weekly_structure_status']}")
        scenario["weekly_structure_tags"] = plan.get("weekly_structure_tags", [])
        scenario["weekly_confirm_tags"] = plan.get("weekly_confirm_tags", [])
        scenario["weekly_tags"] = ";".join(tags)[:255] if tags else None

        return scenario

    def load_board_rotation_metrics(self, latest_trade_date: str) -> pd.DataFrame:
        """从数据库读取或计算板块轮动指标。"""
        if not self.board_env_enabled:
            return pd.DataFrame()

        # 优先直接读取已生成的指标表
        if self._table_exists("strategy_board_rotation"):
            try:
                stmt = text("SELECT * FROM strategy_board_rotation WHERE `date` = :d")
                with self.db_writer.engine.begin() as conn:
                    df = pd.read_sql_query(stmt, conn, params={"d": latest_trade_date})
                if not df.empty:
                    # 适配返回格式：Index=board_name
                    if "board_name" in df.columns:
                        df = df.set_index("board_name")
                    return df
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("读取 strategy_board_rotation 失败：%s", exc)

        if not self._table_exists("board_industry_hist_daily"):
            return pd.DataFrame()

        # 读取最近约 60 个交易日的数据以确保有足够窗口计算 20d 收益
        stmt = text(
            """
            SELECT `date`, `board_name`, `收盘` AS `close`
            FROM board_industry_hist_daily
            WHERE `date` >= DATE_SUB(:d, INTERVAL 90 DAY)
              AND `date` <= :d
            ORDER BY `date` ASC
            """
        )
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": latest_trade_date})
            if df.empty:
                return pd.DataFrame()

            # 数据透视：行=日期，列=板块
            pivot_df = df.pivot_table(index="date", columns="board_name", values="close").ffill()
            if len(pivot_df) < 21:
                return pd.DataFrame()

            # 计算收益率
            ret_20d = pivot_df.pct_change(20).iloc[-1]
            ret_5d = pivot_df.pct_change(5).iloc[-1]

            metrics = pd.DataFrame({"ret_20d": ret_20d, "ret_5d": ret_5d}).dropna()
            # 排名百分比 (0~1)
            metrics["rank_trend"] = metrics["ret_20d"].rank(pct=True)
            metrics["rank_mom"] = metrics["ret_5d"].rank(pct=True)

            def _classify(row):
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
            return metrics
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("计算板块轮动指标失败：%s", exc)
            return pd.DataFrame()


    def build_environment_context(
        self, latest_trade_date: str, *, checked_at: dt.datetime | None = None
    ) -> dict[str, Any]:
        index_trend = self.load_index_trend(latest_trade_date)
        weekly_channel = self.load_index_weekly_channel(latest_trade_date)
        weekly_scenario = self.build_weekly_scenario(weekly_channel, index_trend)
        
        # 1. 获取基础行情快照
        board_strength = self.load_board_spot_strength(latest_trade_date, checked_at)
        # 2. 获取轮动指标
        rotation_metrics = self.load_board_rotation_metrics(latest_trade_date)
        
        board_map: dict[str, Any] = {}
        if not board_strength.empty and "board_name" in board_strength.columns:
            total = len(board_strength)
            for _, row in board_strength.iterrows():
                name = str(row.get("board_name") or "").strip()
                code = str(row.get("board_code") or "").strip()
                rank = row.get("rank")
                pct = row.get("chg_pct")
                
                # 初始状态
                status = "neutral"
                if total > 0 and rank:
                    if rank <= max(1, int(total * 0.2)):
                        status = "strong"
                    elif rank >= max(1, int(total * 0.8)):
                        status = "weak"
                
                # 整合轮动数据
                phase = None
                trend_score = None
                mom_score = None
                if not rotation_metrics.empty and name in rotation_metrics.index:
                    m = rotation_metrics.loc[name]
                    phase = m["rotation_phase"]
                    trend_score = m["rank_trend"]
                    mom_score = m["rank_mom"]
                    
                    # 如果是领涨象限，强制提升为 strong
                    if phase == "leading":
                        status = "strong"
                    # 如果是滞后象限，强制降级为 weak
                    elif phase == "lagging":
                        status = "weak"
                    # 如果是转弱象限且动量评分极低，降级为 weak
                    elif phase == "weakening" and mom_score < 0.2:
                        status = "weak"

                payload = {
                    "rank": rank, 
                    "chg_pct": pct, 
                    "status": status,
                    "rotation_phase": phase,
                    "trend_score": trend_score,
                    "mom_score": mom_score
                }
                for key in [name, code]:
                    key_norm = str(key).strip()
                    if key_norm:
                        board_map[key_norm] = payload

        position_hint_raw = None
        weekly_cap = None
        if isinstance(index_trend, dict):
            position_hint_raw = to_float(index_trend.get("position_hint"))
        if isinstance(weekly_scenario, dict):
            weekly_cap = to_float(weekly_scenario.get("weekly_plan_a_exposure_cap"))
        gating_enabled = bool(weekly_scenario.get("weekly_gating_enabled", False))

        effective_position_hint = position_hint_raw
        if gating_enabled and weekly_cap is not None:
            effective_position_hint = (
                weekly_cap
                if position_hint_raw is None
                else min(position_hint_raw, weekly_cap)
            )

        weekly_note = None
        if isinstance(weekly_scenario, dict):
            weekly_note = weekly_scenario.get("weekly_note")
        if weekly_note is None and isinstance(weekly_channel, dict):
            weekly_note = weekly_channel.get("note")

        env_context = {
            "index": index_trend,
            "weekly": weekly_channel,
            "weekly_windows": weekly_channel.get("weekly_windows_merged"),
            "weekly_windows_by_code": weekly_channel.get("weekly_windows_by_code"),
            "boards": board_map,
            "regime": index_trend.get("regime"),
            "index_score": to_float(index_trend.get("score")),
            "position_hint": effective_position_hint,
            "position_hint_raw": position_hint_raw,
            "effective_position_hint": effective_position_hint,
            "weekly_state": weekly_channel.get("state") if isinstance(weekly_channel, dict) else None,
            "weekly_position_hint": weekly_channel.get("position_hint") if isinstance(weekly_channel, dict) else None,
            "weekly_note": weekly_note,
            "weekly_scenario": weekly_scenario,
            "weekly_asof_trade_date": weekly_scenario.get("weekly_asof_trade_date"),
            "weekly_week_closed": weekly_scenario.get("weekly_week_closed"),
            "weekly_current_week_closed": weekly_scenario.get("weekly_current_week_closed"),
            "weekly_risk_score": weekly_scenario.get("weekly_risk_score"),
            "weekly_risk_level": weekly_scenario.get("weekly_risk_level"),
            "weekly_confirm": weekly_scenario.get("weekly_confirm"),
            "weekly_gating_enabled": gating_enabled,
            "weekly_plan_a": weekly_scenario.get("weekly_plan_a"),
            "weekly_plan_b": weekly_scenario.get("weekly_plan_b"),
            "weekly_scene_code": weekly_scenario.get("weekly_scene_code"),
            "weekly_phase": weekly_scenario.get("weekly_phase"),
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
            "weekly_status": weekly_scenario.get("weekly_structure_status")
            or weekly_scenario.get("weekly_status"),
            "weekly_structure_status": weekly_scenario.get("weekly_structure_status"),
            "weekly_pattern_status": weekly_scenario.get("weekly_pattern_status"),
            "weekly_direction_confirmed": weekly_scenario.get(
                "weekly_direction_confirmed"
            ),
            "weekly_money_tags": weekly_scenario.get("weekly_money_tags"),
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
            if weekly_scenario.get("weekly_structure_status"):
                scenario_tags.append(f"STATUS_{weekly_scenario['weekly_structure_status']}")
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

        weekly_gate_policy = self.resolve_env_weekly_gate_policy(env_context)
        env_context["weekly_gate_policy"] = weekly_gate_policy
        weekly_zone = self._resolve_weekly_zone(
            weekly_scenario,
            weekly_gate_policy,
            env_context.get("weekly_tags"),
        )
        weekly_scenario.update(weekly_zone)
        env_context.update(weekly_zone)
        self._finalize_env_directives(env_context, weekly_gate_policy=weekly_gate_policy)

        return env_context

    def resolve_env_weekly_gate_policy(self, env_context: dict[str, Any] | None) -> str | None:
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
        structure_status = str(_get_env("weekly_structure_status") or status).upper()
        weekly_phase = str(_get_env("weekly_phase") or "").upper()
        weekly_tags = str(_get_env("weekly_tags") or "")
        weekly_risk_score = to_float(_get_env("weekly_risk_score"))
        weekly_state = str(_get_env("weekly_state") or "").upper()
        weekly_scene_code = str(_get_env("weekly_scene_code") or "").upper()
        daily_regime = str(_get_env("regime") or "").upper()

        if not gating_enabled:
            return "ALLOW"

        baseline_gate = "ALLOW"
        if weekly_phase == "BREAKDOWN_RISK":
            baseline_gate = "WAIT"
        elif risk_level == "HIGH":
            if weekly_risk_score is not None and weekly_risk_score >= 85:
                baseline_gate = "WAIT"
            elif (
                weekly_risk_score is not None
                and 70 <= weekly_risk_score < 85
                and weekly_phase == "BULL_TREND"
                and structure_status == "FORMING"
            ):
                baseline_gate = "ALLOW_SMALL"
            else:
                baseline_gate = "WAIT"
        elif risk_level == "MEDIUM" and structure_status == "FORMING":
            # feat: MEDIUM 风险周线不一票 WAIT，改为 ALLOW_SMALL 以降仓放行
            baseline_gate = "ALLOW_SMALL"

        def _tighten(current: str, target: str) -> str:
            severity = {"ALLOW": 0, "ALLOW_SMALL": 1, "WAIT": 2, "STOP": 3}
            if severity.get(target, 0) > severity.get(current, 0):
                return target
            return current

        if weekly_phase == "BEAR_TREND" and risk_level in {"MEDIUM", "HIGH"}:
            baseline_gate = _tighten(baseline_gate, "ALLOW_SMALL")

        if "VOL_WEAK" in weekly_tags and risk_level != "LOW":
            baseline_gate = _tighten(baseline_gate, "ALLOW_SMALL")

        if risk_level == "HIGH" and baseline_gate == "WAIT":
            baseline_gate = "ALLOW_SMALL"

        def _channel_bias(state: str) -> str:
            if state in {"CHANNEL_BREAKOUT_UP"}:
                return "BULL"
            if state in {"BREAKOUT_UP_WEAK"}:
                return "BULL_WEAK"
            if state in {"CHANNEL_BREAKDOWN"}:
                return "BEAR"
            if state in {"BREAKDOWN_WEAK"}:
                return "BEAR_WEAK"
            return "NEUTRAL"

        def _pattern_bias(phase: str) -> str:
            phase_norm = str(phase or "").upper()
            if phase_norm in {"BULL_TREND"}:
                return "BULL"
            if phase_norm in {"BEAR_TREND", "BREAKDOWN_RISK"}:
                return "BEAR"
            return "NEUTRAL"

        channel_bias = _channel_bias(weekly_state)
        pattern_bias = _pattern_bias(weekly_phase)
        vol_confirmed = "VOL_CONFIRM" in weekly_tags

        # 冲突裁决：通道/形态方向不一致时，强制要求量能确认，否则降级 gate
        conflict_gate = self.conflict_gate or "ALLOW_SMALL"
        if (
            channel_bias.startswith("BULL")
            and pattern_bias == "BEAR"
            and (not vol_confirmed or not self.conflict_requires_vol_confirm)
        ):
            baseline_gate = _tighten(baseline_gate, conflict_gate)
        if (
            channel_bias.startswith("BEAR")
            and pattern_bias == "BULL"
            and (not vol_confirmed or not self.conflict_requires_vol_confirm)
        ):
            baseline_gate = _tighten(baseline_gate, conflict_gate)
        if channel_bias == "BULL_WEAK" and (not vol_confirmed or not self.conflict_requires_vol_confirm):
            baseline_gate = _tighten(baseline_gate, conflict_gate)
        if channel_bias == "BEAR_WEAK" and pattern_bias != "BEAR":
            baseline_gate = _tighten(baseline_gate, conflict_gate)

        if weekly_state in {"NEAR_LOWER_RAIL", "LOWER_RAIL_NEAR_MA30_ZONE"} and daily_regime in {
            "RISK_OFF",
            "BREAKDOWN",
            "BEAR_CONFIRMED",
        }:
            baseline_gate = _tighten(baseline_gate, "WAIT")

        return baseline_gate

    @staticmethod
    def _merge_gate_actions(*actions: str | None) -> str | None:
        severity = {"STOP": 3, "WAIT": 2, "ALLOW_SMALL": 1, "ALLOW": 0, None: -1}
        normalized = []
        for action in actions:
            if action is None:
                normalized.append((severity[None], None))
                continue
            action_norm = str(action).strip().upper()
            if action_norm == "GO":
                action_norm = "ALLOW"
            normalized.append((severity.get(action_norm, 0), action_norm))
        if not normalized:
            return None
        normalized.sort(key=lambda x: x[0], reverse=True)
        return normalized[0][1]

    @staticmethod
    def _derive_gate_action(regime: str | None, position_hint: float | None) -> str | None:
        regime_norm = str(regime or "").strip().upper() or None
        pos_hint_val = to_float(position_hint)
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

    @staticmethod
    def _resolve_risk_emotion_policy(env_context: dict[str, Any]) -> dict[str, Any]:
        risk_level = str(env_context.get("weekly_risk_level") or "").upper()
        regime = str(env_context.get("regime") or "").upper()

        # Risk (weekly) x Emotion (daily regime) matrix.
        matrix = {
            "HIGH": {
                "RISK_ON": ("ALLOW_SMALL", 0.15, "HIGH_RISK_RISK_ON"),
                "PULLBACK": ("WAIT", 0.15, "HIGH_RISK_PULLBACK"),
                "RISK_OFF": ("STOP", 0.0, "HIGH_RISK_RISK_OFF"),
                "BREAKDOWN": ("STOP", 0.0, "HIGH_RISK_BREAKDOWN"),
                "BEAR_CONFIRMED": ("STOP", 0.0, "HIGH_RISK_BEAR"),
            },
            "MEDIUM": {
                "RISK_ON": ("ALLOW_SMALL", 0.3, "MEDIUM_RISK_RISK_ON"),
                "PULLBACK": ("ALLOW_SMALL", 0.25, "MEDIUM_RISK_PULLBACK"),
                "RISK_OFF": ("WAIT", 0.15, "MEDIUM_RISK_RISK_OFF"),
                "BREAKDOWN": ("WAIT", 0.15, "MEDIUM_RISK_BREAKDOWN"),
            },
            "LOW": {
                "RISK_ON": ("ALLOW", 1.0, "LOW_RISK_RISK_ON"),
                "PULLBACK": ("ALLOW_SMALL", 0.5, "LOW_RISK_PULLBACK"),
                "RISK_OFF": ("WAIT", 0.25, "LOW_RISK_RISK_OFF"),
            },
        }

        if not risk_level or not regime:
            return {}

        row = matrix.get(risk_level, {})
        if regime not in row:
            return {}

        gate_action, cap_limit, tag = row[regime]
        return {"gate_action": gate_action, "cap_limit": cap_limit, "matrix_tag": tag}

    @staticmethod
    def _resolve_weekly_zone(
        weekly_scenario: dict[str, Any] | None,
        weekly_gate_policy: str | None,
        weekly_tags: str | None,
    ) -> dict[str, Any]:
        scenario = weekly_scenario or {}
        risk_level = str(scenario.get("weekly_risk_level") or "").upper()
        gate_policy = str(weekly_gate_policy or scenario.get("weekly_gate_policy") or "").upper()
        plan_cap = to_float(scenario.get("weekly_plan_a_exposure_cap"))
        direction_confirmed = bool(scenario.get("weekly_direction_confirmed", False))
        tags = str(weekly_tags or scenario.get("weekly_tags") or "")

        zone_id = "WZ2_NEUTRAL"
        reason_parts: list[str] = []

        if gate_policy in {"STOP", "WAIT"} or risk_level == "HIGH":
            zone_id = "WZ0_RISK_OFF"
        elif "OVERHEAT" in tags or "EUPHORIA" in tags:
            zone_id = "WZ4_EUPHORIA"
        elif risk_level == "MEDIUM" and plan_cap is not None and plan_cap <= 0.25:
            zone_id = "WZ1_DEFENSIVE"
        elif risk_level == "LOW" and direction_confirmed:
            zone_id = "WZ3_ATTACK"

        zone_score_map = {
            "WZ0_RISK_OFF": 10,
            "WZ1_DEFENSIVE": 30,
            "WZ2_NEUTRAL": 50,
            "WZ3_ATTACK": 70,
            "WZ4_EUPHORIA": 85,
        }
        exp_return_map = {
            "WZ0_RISK_OFF": "LOW",
            "WZ1_DEFENSIVE": "LOW",
            "WZ2_NEUTRAL": "MID",
            "WZ3_ATTACK": "HIGH",
            "WZ4_EUPHORIA": "HIGH",
        }

        if gate_policy:
            reason_parts.append(f"gate={gate_policy}")
        if risk_level:
            reason_parts.append(f"risk={risk_level}")
        if plan_cap is not None:
            reason_parts.append(f"cap={plan_cap:.2f}")
        if direction_confirmed:
            reason_parts.append("direction_confirmed")
        if tags:
            reason_parts.append(f"tags={tags}")

        return {
            "weekly_zone_id": zone_id,
            "weekly_zone_score": zone_score_map.get(zone_id, 50),
            "weekly_exp_return_bucket": exp_return_map.get(zone_id, "MID"),
            "weekly_zone_reason": ";".join(reason_parts)[:255] if reason_parts else None,
        }

    def _finalize_env_directives(
        self,
        env_context: dict[str, Any] | None,
        *,
        weekly_gate_policy: str | None = None,
    ) -> None:
        if not isinstance(env_context, dict):
            return

        risk_emotion = self._resolve_risk_emotion_policy(env_context)

        gate_candidates: list[str | None] = []
        reason_parts: dict[str, Any] = {}
        gate_norm = None
        if weekly_gate_policy:
            gate_norm = str(weekly_gate_policy).strip().upper()
            reason_parts["weekly_gate_policy"] = gate_norm
        weekly_risk_level = str(env_context.get("weekly_risk_level") or "").strip().upper()
        if gate_norm == "WAIT" and weekly_risk_level == "HIGH":
            gate_norm = "ALLOW_SMALL"
            reason_parts["weekly_gate_policy_override"] = "ALLOW_SMALL"

        index_gate_norm = None
        index_snapshot = env_context.get("index_intraday")
        if isinstance(index_snapshot, dict):
            index_gate = index_snapshot.get("env_index_gate_action")
            if index_gate is not None:
                index_gate_norm = str(index_gate).strip().upper()
                reason_parts["index_gate_action"] = index_gate_norm

        daily_zone_id = env_context.get("daily_zone_id")
        daily_gate_hint = None
        if str(daily_zone_id or "").upper() == "DZ_BREAKDOWN":
            daily_gate_hint = "WAIT"
        if daily_gate_hint:
            gate_candidates.append(daily_gate_hint)
            reason_parts["daily_gate_hint"] = daily_gate_hint

        live_override_action = str(env_context.get("env_live_override_action") or "").upper()
        live_gate_hint = None
        if live_override_action == "PAUSE":
            live_gate_hint = "WAIT"
        elif live_override_action == "EXIT":
            live_gate_hint = "STOP"

        regime_gate = self._derive_gate_action(
            env_context.get("regime"), env_context.get("position_hint_raw") or env_context.get("position_hint")
        )
        if regime_gate:
            reason_parts["regime_gate_action"] = regime_gate

        if risk_emotion:
            matrix_gate = risk_emotion.get("gate_action")
            if matrix_gate:
                gate_candidates.append(matrix_gate)
                reason_parts["risk_emotion_gate_action"] = matrix_gate
            matrix_cap = risk_emotion.get("cap_limit")
            if matrix_cap is not None:
                reason_parts["risk_emotion_cap_limit"] = f"{matrix_cap:.2f}"
            matrix_tag = risk_emotion.get("matrix_tag")
            if matrix_tag:
                reason_parts["risk_emotion_matrix"] = matrix_tag

        effective_weekly_gate = gate_norm
        live_unlock_gate = str(env_context.get("env_live_unlock_gate") or "").strip().upper()
        if (
            gate_norm == "WAIT"
            and live_unlock_gate == "ALLOW_SMALL"
            and live_override_action not in {"PAUSE", "EXIT"}
            and index_gate_norm not in {"WAIT", "STOP"}
            and regime_gate not in {"WAIT", "STOP"}
        ):
            effective_weekly_gate = "ALLOW_SMALL"
            reason_parts["weekly_gate_effective"] = effective_weekly_gate
            reason_parts["env_live_unlock_gate"] = live_unlock_gate

        if effective_weekly_gate:
            gate_candidates.append(effective_weekly_gate)
        if index_gate_norm == "STOP":
            gate_candidates.append(index_gate_norm)
        if live_gate_hint:
            gate_candidates.append(live_gate_hint)
            reason_parts["live_gate_hint"] = live_gate_hint
        if regime_gate:
            gate_candidates.append(regime_gate)

        for key in [
            "weekly_asof_trade_date",
            "weekly_risk_score",
            "weekly_scene_code",
            "weekly_structure_status",
            "weekly_pattern_status",
            "daily_asof_trade_date",
            "weekly_zone_id",
            "daily_zone_id",
            "env_live_override_action",
            "env_live_cap_multiplier",
            "env_live_event_tags",
            "env_live_reason",
            "env_live_unlock_gate",
        ]:
            value = env_context.get(key)
            if value not in (None, "", [], {}, ()):  # noqa: PLC1901
                reason_parts[key] = value

        final_gate = self._merge_gate_actions(*gate_candidates) or "ALLOW"

        weekly_cap = to_float(env_context.get("weekly_plan_a_exposure_cap"))
        daily_pos_hint = to_float(env_context.get("position_hint"))
        daily_cap_multiplier = to_float(env_context.get("daily_cap_multiplier")) or 1.0
        live_cap_multiplier = to_float(env_context.get("env_live_cap_multiplier")) or 1.0
        breadth_pct = to_float(env_context.get("breadth_pct_above_ma20"))
        breadth_factor = (
            0.6 + 0.4 * breadth_pct if breadth_pct is not None else 1.0
        )
        if breadth_pct is not None:
            if breadth_pct >= 0.95:
                breadth_factor = min(breadth_factor, 0.85)
                reason_parts["breadth_saturation"] = "RISK_ON_PEAK"
            elif breadth_pct <= 0.05:
                breadth_factor = min(breadth_factor, 0.75)
                reason_parts["breadth_saturation"] = "RISK_OFF_WASHOUT"

        cap_candidates = [weekly_cap, daily_pos_hint]
        filtered_caps = [c for c in cap_candidates if c is not None]
        base_cap = min(filtered_caps) if filtered_caps else 1.0
        final_cap = base_cap * daily_cap_multiplier * breadth_factor * live_cap_multiplier
        final_cap = min(max(final_cap, 0.0), 1.0)
        small_cap_limit = 0.25
        if risk_emotion and risk_emotion.get("cap_limit") is not None:
            cap_limit = float(risk_emotion["cap_limit"])
            if final_cap > cap_limit:
                final_cap = cap_limit
                reason_parts["gate_cap_limit"] = f"RISK_EMOTION_{cap_limit:.2f}"
        weekly_scene = str(
            env_context.get("weekly_scene_code") or env_context.get("weekly_scene") or ""
        ).strip().upper()
        breadth_saturation = reason_parts.get("breadth_saturation")
        tier_cap = None
        if weekly_risk_level == "HIGH":
            if breadth_saturation == "RISK_ON_PEAK":
                tier_cap = 0.05
            elif weekly_scene.startswith("WEDGE"):
                tier_cap = 0.08
        if tier_cap is not None and final_cap > tier_cap:
            final_cap = tier_cap
            reason_parts["risk_tier_cap"] = f"{weekly_risk_level}_{tier_cap:.2f}"
        if effective_weekly_gate == "ALLOW_SMALL" and gate_norm == "WAIT":
            small_cap_limit = 0.15
            reason_parts["unlock_cap_limit"] = f"{small_cap_limit:.2f}"
        if final_gate in {"STOP", "WAIT"}:
            final_cap = 0.0
            reason_parts["gate_cap_limit"] = f"{final_gate}_0"
        elif final_gate == "ALLOW_SMALL":
            if final_cap > small_cap_limit:
                final_cap = small_cap_limit
                reason_parts["gate_cap_limit"] = f"ALLOW_SMALL_{small_cap_limit:.2f}"
        env_context["env_final_gate_action"] = final_gate
        env_context["env_final_cap_pct"] = final_cap
        env_context["env_final_reason_json"] = self._clip(
            json.dumps(reason_parts, ensure_ascii=False, default=self._json_default),
            2000,
        )

    @property
    def calendar_cache(self) -> set[str]:
        return self._calendar_cache

    @property
    def calendar_range(self) -> tuple[dt.date, dt.date] | None:
        return self._calendar_range

    @staticmethod
    def _json_default(obj: Any) -> str:
        if isinstance(obj, (dt.date, dt.datetime)):
            return obj.isoformat()
        return str(obj)
