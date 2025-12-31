"""open_monitor 的数据访问层。"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
from typing import Any, Dict, List, Tuple

import pandas as pd
from sqlalchemy import bindparam, text

from .baostock_core import BaostockDataFetcher
from .baostock_session import BaostockSession
from .config import get_section
from .db import DatabaseConfig, MySQLWriter
from .env_snapshot_utils import load_trading_calendar
from .ma5_ma20_trend_strategy import _atr, _macd
from .utils.convert import to_float as _to_float


SNAPSHOT_HASH_EXCLUDE = {
    "checked_at",
    "run_id",
    "run_pk",
}

READY_SIGNALS_REQUIRED_COLS = (
    "sig_date",
    "code",
    "strategy_code",
    "expires_on",
    "close",
    "ma20",
    "atr14",
)


def _normalize_snapshot_value(value: Any) -> Any:  # noqa: ANN401
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 6)
    if isinstance(value, int):
        return value
    try:
        if hasattr(value, "item"):
            base = value.item()
            if isinstance(base, (float, int)):
                return _normalize_snapshot_value(base)
            return base
    except Exception:
        pass
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, dt.date):
        return value.isoformat()
    return value


def make_snapshot_hash(row: Dict[str, Any]) -> str:
    payload = {}
    for key, value in row.items():
        if key in SNAPSHOT_HASH_EXCLUDE:
            continue
        payload[key] = _normalize_snapshot_value(value)
    serialized = json.dumps(
        payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.md5(serialized.encode("utf-8")).hexdigest()


def calc_run_id(ts: dt.datetime, run_id_minutes: int | None) -> str:
    window_minutes = max(int(run_id_minutes or 5), 1)

    auction_start = dt.time(9, 15)
    lunch_break_start = dt.time(11, 30)
    lunch_break_end = dt.time(13, 0)
    market_close = dt.time(15, 0)

    minute_of_day = ts.hour * 60 + ts.minute
    slot_minute = (minute_of_day // window_minutes) * window_minutes
    slot_time = dt.datetime.combine(ts.date(), dt.time(slot_minute // 60, slot_minute % 60))
    slot_text = slot_time.strftime("%Y-%m-%d %H:%M")

    t = ts.time()
    if t < auction_start:
        return f"PREOPEN {slot_text}"
    if lunch_break_start <= t < lunch_break_end:
        return f"BREAK {slot_text}"
    if t >= market_close:
        return f"POSTCLOSE {slot_text}"

    return slot_text


class OpenMonitorRepository:
    """开盘监测数据访问层，集中管理 SQL 与表结构。"""

    def __init__(self, engine, logger, params) -> None:
        self.engine = engine
        self.logger = logger
        self.params = params
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())
        self.db_name = self.db_writer.config.db_name

        self._ready_signals_used: bool = False
        self._recent_buy_signals_cache_key = None
        self._recent_buy_signals_cache = None

    @property
    def ready_signals_used(self) -> bool:
        return self._ready_signals_used

    def _table_exists(self, table: str) -> bool:
        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(text("SHOW TABLES LIKE :t"), conn, params={"t": table})
            return not df.empty
        except Exception:
            return False

    def _column_exists(self, table: str, column: str) -> bool:
        try:
            stmt = text(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table AND column_name = :column
                """
            )
            with self.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "schema": self.db_name,
                        "table": table,
                        "column": column,
                    },
                )
            return not df.empty and bool(df.iloc[0].get("cnt", 0))
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("检查列 %s.%s 是否存在失败：%s", table, column, exc)
            return False

    def _get_table_columns(self, table: str) -> List[str]:
        stmt = text(
            """
            SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t
            """
        )
        try:
            with self.engine.begin() as conn:
                return pd.read_sql(stmt, conn, params={"t": table})["COLUMN_NAME"].tolist()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("无法获取 %s 列信息：%s", table, exc)
            return []

    def _daily_table(self) -> str:
        """获取日线数据表名（用于补充计算“信号日涨幅”等信息）。"""

        strat = get_section("strategy_ma5_ma20_trend") or {}
        if isinstance(strat, dict):
            name = str(strat.get("daily_table") or "").strip()
            if name:
                return name
        return "history_daily_kline"

    def _is_trading_day(self, date_str: str, latest_trade_date: str | None = None) -> bool:
        """粗略判断是否为交易日（优先用日线表，其次用工作日）。"""

        try:
            d = dt.datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
        except Exception:  # noqa: BLE001
            return False

        if d.weekday() >= 5:
            return False

        target_str = d.isoformat()
        start = d - dt.timedelta(days=400)
        calendar = load_trading_calendar(start, d)
        if calendar:
            return target_str in calendar

        daily = self._daily_table()
        if not self._table_exists(daily):
            return True

        stmt = text(f"SELECT 1 FROM `{daily}` WHERE `date` = :d LIMIT 1")
        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": str(d)})
            if not df.empty:
                return True
            if latest_trade_date and target_str > str(latest_trade_date)[:10]:
                return True
            return False
        except Exception:  # noqa: BLE001
            if latest_trade_date and target_str > str(latest_trade_date)[:10]:
                return True
            return False

    def _load_trade_age_map(
        self, latest_trade_date: str, min_date: str, monitor_date: str | None
    ) -> Dict[str, int]:
        """返回 {date_str: trading_day_age}，0 表示监控基准日。"""

        base_date = latest_trade_date
        monitor_str = str(monitor_date or "").strip()
        if (
            monitor_str
            and monitor_str > latest_trade_date
            and self._is_trading_day(monitor_str, latest_trade_date)
        ):
            base_date = monitor_str

        daily = self._daily_table()
        if not self._table_exists(daily):
            return {monitor_str: 0} if monitor_str else {}

        stmt = text(
            f"""
            SELECT DISTINCT CAST(`date` AS CHAR) AS d
            FROM `{daily}`
            WHERE `date` <= :base_date AND `date` >= :min_date
            ORDER BY d DESC
            """
        )
        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt, conn, params={"base_date": base_date, "min_date": min_date}
                )
        except Exception:
            df = None

        dates = df["d"].dropna().astype(str).str[:10].tolist() if df is not None else []
        if (
            monitor_str
            and monitor_str not in dates
            and monitor_str > latest_trade_date
            and self._is_trading_day(monitor_str, latest_trade_date)
        ):
            dates.insert(0, monitor_str)

        if not dates:
            return {}

        return {d: i for i, d in enumerate(dates)}

    def _resolve_latest_trade_date(
        self,
        *,
        signals: pd.DataFrame | None = None,
        ready_view: str | None = None,
    ) -> str | None:
        """统一推导 latest_trade_date，避免对基础数据表的隐性依赖。"""

        if isinstance(signals, pd.DataFrame) and not signals.empty:
            for col in ("asof_trade_date", "sig_date"):
                if col in signals.columns:
                    s = pd.to_datetime(signals[col], errors="coerce").dropna()
                    if not s.empty:
                        return s.max().date().isoformat()

        daily = self._daily_table()
        if daily and self._table_exists(daily):
            try:
                with self.engine.begin() as conn:
                    df = pd.read_sql_query(
                        text(f"SELECT MAX(`date`) AS latest_trade_date FROM `{daily}`"),
                        conn,
                    )
                if df is not None and not df.empty:
                    v = df.iloc[0].get("latest_trade_date")
                    ts = pd.to_datetime(v, errors="coerce")
                    if pd.notna(ts):
                        return ts.date().isoformat()
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("推导 latest_trade_date 失败（daily_table）：%s", exc)

        view = ready_view or str(self.params.ready_signals_view or "").strip()
        if view and self._table_exists(view):
            try:
                with self.engine.begin() as conn:
                    df = pd.read_sql_query(
                        text(f"SELECT MAX(`sig_date`) AS latest_sig_date FROM `{view}`"),
                        conn,
                    )
                if df is not None and not df.empty:
                    v = df.iloc[0].get("latest_sig_date")
                    ts = pd.to_datetime(v, errors="coerce")
                    if pd.notna(ts):
                        return ts.date().isoformat()
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("推导 latest_trade_date 失败（ready_view）：%s", exc)

        return None

    def resolve_monitor_trade_date(self, checked_at: dt.datetime) -> str:
        """解析业务交易日：非交易日则回落到最近交易日。"""

        try:
            candidate_date = checked_at.date()

            try:
                calendar = load_trading_calendar(
                    start=candidate_date - dt.timedelta(days=370),
                    end=candidate_date,
                )
            except Exception:  # noqa: BLE001
                calendar = set()

            if calendar:
                calendar_dates = [
                    dt.datetime.strptime(d, "%Y-%m-%d").date()
                    for d in calendar
                    if isinstance(d, str)
                ]
                calendar_dates = [d for d in calendar_dates if d <= candidate_date]
                if calendar_dates:
                    return max(calendar_dates).isoformat()

            view = str(getattr(self.params, "ready_signals_view", "") or "").strip() or None
            latest_trade_date = self._resolve_latest_trade_date(ready_view=view)
            if latest_trade_date and self._is_trading_day(candidate_date.isoformat(), latest_trade_date):
                return candidate_date.isoformat()
            if latest_trade_date:
                return latest_trade_date

            target = candidate_date
            for _ in range(14):
                if target.weekday() < 5:
                    return target.isoformat()
                target -= dt.timedelta(days=1)
            return candidate_date.isoformat()
        except Exception:  # noqa: BLE001
            return checked_at.date().isoformat()

    def _load_signal_day_pct_change(
        self, signal_date: str, codes: List[str]
    ) -> Dict[str, float]:
        if not signal_date or not codes:
            return {}

        daily = self._daily_table()
        if not self._table_exists(daily):
            return {}

        stmt = text(
            f"""
            SELECT `code`, `close`, `prev_close`
            FROM (
              SELECT
                `code`, `date`, `close`,
                LAG(`close`) OVER (PARTITION BY `code` ORDER BY `date`) AS `prev_close`
              FROM `{daily}`
              WHERE `code` IN :codes AND `date` <= :d
            ) t
            WHERE `date` = :d
            """
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": signal_date, "codes": codes})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取 %s 信号日涨幅失败（将跳过）：%s", daily, exc)
            return {}

        if df is None or df.empty:
            return {}

        out: Dict[str, float] = {}
        for _, row in df.iterrows():
            code = str(row.get("code") or "").strip()
            close = _to_float(row.get("close"))
            prev_close = _to_float(row.get("prev_close"))
            if not code or close is None or prev_close is None or prev_close <= 0:
                continue
            out[code] = (close - prev_close) / prev_close

        return out

    def load_recent_buy_signals(self) -> Tuple[str | None, List[str], pd.DataFrame]:
        """从 ready_signals_view 读取最近 BUY 信号（严格模式，无旧逻辑回退）。"""

        view = str(self.params.ready_signals_view or "").strip()
        strict_ready = bool(getattr(self.params, "strict_ready_signals_required", True))
        if not view:
            msg = "未配置 ready_signals_view"
            if strict_ready:
                raise RuntimeError(msg)
            self.logger.error("%s，已跳过开盘监测。", msg)
            return None, [], pd.DataFrame()
        if not self._table_exists(view):
            msg = f"ready_signals_view={view} 不存在"
            if strict_ready:
                raise RuntimeError(msg)
            self.logger.error(
                "%s，已跳过开盘监测；请先确保 SchemaManager.ensure_all 已执行或检查配置。",
                msg,
            )
            return None, [], pd.DataFrame()

        required_view_cols = set(READY_SIGNALS_REQUIRED_COLS)
        view_cols = set(self._get_table_columns(view))
        missing_view_cols = sorted(required_view_cols - view_cols)
        if missing_view_cols:
            msg = f"ready_signals_view `{view}` 缺少关键列：{missing_view_cols}"
            if strict_ready:
                raise RuntimeError(msg)
            self.logger.error("%s，已跳过开盘监测。", msg)
            return None, [], pd.DataFrame()

        self._ready_signals_used = True

        monitor_date = str(getattr(self.params, "monitor_date", "") or "").strip()
        if not monitor_date:
            monitor_date = dt.date.today().isoformat()
        lookback = max(int(self.params.signal_lookback_days or 0), 1)

        cache_key = (monitor_date, view, lookback, str(self.params.strategy_code))
        if cache_key == self._recent_buy_signals_cache_key and self._recent_buy_signals_cache:
            return self._recent_buy_signals_cache

        latest_trade_date = self._resolve_latest_trade_date(ready_view=view)
        if latest_trade_date is None:
            self.logger.error("无法推导 latest_trade_date（daily_table/view 均不可用），已跳过开盘监测。")
            return None, [], pd.DataFrame()

        base_table = view
        signal_dates: List[str] = []
        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(
                    text(
                        f"""
                        SELECT DISTINCT `sig_date`
                        FROM `{base_table}`
                        WHERE `sig_date` <= :latest_trade_date AND `strategy_code` = :strategy_code
                          AND `expires_on` >= :monitor_date
                        ORDER BY `sig_date` DESC
                        LIMIT :lookback
                        """
                    ),
                    conn,
                    params={
                        "latest_trade_date": latest_trade_date,
                        "strategy_code": self.params.strategy_code,
                        "monitor_date": monitor_date,
                        "lookback": lookback,
                    },
                )
            if df is not None and not df.empty:
                signal_dates = (
                    pd.to_datetime(df["sig_date"], errors="coerce")
                    .dropna()
                    .dt.strftime("%Y-%m-%d")
                    .tolist()
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.error("读取 %s BUY 信号日期失败：%s", view, exc)
            return None, [], pd.DataFrame()

        if not signal_dates:
            self.logger.error("未读取到任何 BUY 信号日期（latest_trade_date=%s）。", latest_trade_date)
            return latest_trade_date, [], pd.DataFrame()

        stmt = text(
            f"""
            SELECT *
            FROM `{view}`
            WHERE `sig_date` IN :dates
              AND `strategy_code` = :strategy_code
              AND `expires_on` >= :monitor_date
            ORDER BY `sig_date` DESC, `code`
            """
        ).bindparams(bindparam("dates", expanding=True))

        try:
            with self.engine.begin() as conn:
                events_df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "dates": signal_dates,
                        "strategy_code": self.params.strategy_code,
                        "monitor_date": monitor_date,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.error("读取 %s BUY 信号失败：%s", view, exc)
            return None, [], pd.DataFrame()

        if events_df is None or events_df.empty:
            return latest_trade_date, [], pd.DataFrame()

        events_df = events_df.copy()
        events_df["code"] = events_df["code"].astype(str)
        events_df["sig_date"] = pd.to_datetime(events_df["sig_date"], errors="coerce")

        base_cols = [
            "sig_date",
            "code",
            "strategy_code",
            "expires_on",
            "close",
            "ma5",
            "ma20",
            "ma60",
            "ma250",
            "vol_ratio",
            "macd_hist",
            "kdj_k",
            "kdj_d",
            "atr14",
            "stop_ref",
            "signal",
            "final_action",
            "final_reason",
            "final_cap",
            "macd_event",
            "chip_score",
            "gdhs_delta_pct",
            "gdhs_announce_date",
            "chip_reason",
            "chip_penalty",
            "chip_note",
            "age_days",
            "deadzone_hit",
            "stale_hit",
            "fear_score",
            "wave_type",
            "extra_json",
            "reason",
            "yearline_state",
            "risk_tag",
            "risk_note",
            "industry",
            "board_name",
            "board_code",
            "industry_classification",
        ]
        for col in base_cols:
            if col not in events_df.columns:
                events_df[col] = None

        events_df["sig_date"] = events_df["sig_date"].dt.strftime("%Y-%m-%d")

        if self.params.unique_code_latest_date_only:
            before = len(events_df)
            events_df["_date_dt"] = pd.to_datetime(events_df["sig_date"], errors="coerce")
            events_df = events_df.sort_values(by=["code", "_date_dt"], ascending=[True, False])
            events_df = events_df.drop_duplicates(subset=["code"], keep="first")
            events_df = events_df.drop(columns=["_date_dt"], errors="ignore")
            dropped = before - len(events_df)
            if dropped > 0:
                self.logger.info(
                    "同一 code 多次触发 BUY：已按最新信号日去重 %s 条（保留 %s 条）。",
                    dropped,
                    len(events_df),
                )
            signal_dates = sorted(
                events_df["sig_date"].dropna().unique().tolist(), reverse=True
            )

        min_date = events_df["sig_date"].min()
        trade_age_map = self._load_trade_age_map(latest_trade_date, str(min_date), monitor_date)
        events_df["signal_age"] = events_df["sig_date"].map(trade_age_map)

        # 过滤后重算信号日列表（用于日志、行情/指标补全等后续路径）
        signal_dates = sorted(events_df["sig_date"].dropna().unique().tolist())

        try:
            for d in signal_dates:
                codes = (
                    events_df.loc[events_df["sig_date"] == d, "code"]
                    .dropna()
                    .unique()
                    .tolist()
                )
                pct_map = self._load_signal_day_pct_change(d, codes)
                mask = events_df["sig_date"] == d
                events_df.loc[mask, "_signal_day_pct_change"] = events_df.loc[
                    mask, "code"
                ].map(pct_map)
        except Exception:
            events_df["_signal_day_pct_change"] = None

        signal_prefix_map = {
            "close": "sig_close",
            "ma5": "sig_ma5",
            "ma20": "sig_ma20",
            "ma60": "sig_ma60",
            "ma250": "sig_ma250",
            "vol_ratio": "sig_vol_ratio",
            "macd_hist": "sig_macd_hist",
            "kdj_k": "sig_kdj_k",
            "kdj_d": "sig_kdj_d",
            "atr14": "sig_atr14",
            "stop_ref": "sig_stop_ref",
            "signal": "sig_signal",
            "final_action": "sig_final_action",
            "final_reason": "sig_final_reason",
            "final_cap": "sig_final_cap",
            "macd_event": "sig_macd_event",
            "chip_score": "sig_chip_score",
            "gdhs_delta_pct": "sig_gdhs_delta_pct",
            "gdhs_announce_date": "sig_gdhs_announce_date",
            "chip_reason": "sig_chip_reason",
            "chip_penalty": "sig_chip_penalty",
            "chip_note": "sig_chip_note",
            "age_days": "sig_chip_age_days",
            "deadzone_hit": "sig_chip_deadzone",
            "stale_hit": "sig_chip_stale",
            "fear_score": "sig_fear_score",
            "wave_type": "sig_wave_type",
            "reason": "sig_reason",
        }
        df = events_df.rename(
            columns={k: v for k, v in signal_prefix_map.items() if k in events_df.columns}
        )
        for src, target in signal_prefix_map.items():
            if target not in df.columns:
                df[target] = None

        self._recent_buy_signals_cache_key = cache_key
        self._recent_buy_signals_cache = (latest_trade_date, list(signal_dates), df.copy())

        return latest_trade_date, signal_dates, df

    def load_latest_snapshots(self, latest_trade_date: str, codes: List[str]) -> pd.DataFrame:
        if not latest_trade_date or not codes:
            return pd.DataFrame()

        daily_table = self._daily_table()
        if not daily_table or not self._table_exists(daily_table):
            return pd.DataFrame()

        codes = [str(c) for c in codes if str(c).strip()]
        if not codes:
            return pd.DataFrame()

        stmt = (
            text(
                f"""
                SELECT CAST(`date` AS CHAR) AS trade_date, `code`, `close`
                FROM `{daily_table}`
                WHERE `date` = :d AND `code` IN :codes
                """
            ).bindparams(bindparam("codes", expanding=True))
        )
        try:
            with self.engine.begin() as conn:
                snap_df = pd.read_sql_query(
                    stmt, conn, params={"d": latest_trade_date, "codes": codes}
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取日线快照失败，将跳过最新快照：%s", exc)
            return pd.DataFrame()

        if snap_df is None or snap_df.empty:
            return pd.DataFrame()

        snap_df["code"] = snap_df["code"].astype(str)
        merged = snap_df.copy()
        stop_df = pd.DataFrame()
        if not stop_df.empty:
            stop_df = stop_df.rename(columns={"sig_date": "trade_date"})
            stop_df["trade_date"] = pd.to_datetime(stop_df["trade_date"]).dt.strftime("%Y-%m-%d")
            merged["trade_date"] = pd.to_datetime(merged["trade_date"]).dt.strftime("%Y-%m-%d")
            merged = merged.merge(stop_df, on=["trade_date", "code"], how="left")
        merged["trade_date"] = pd.to_datetime(merged["trade_date"]).dt.strftime("%Y-%m-%d")
        return merged

    def load_index_history(self, latest_trade_date: str) -> dict[str, Any]:
        code = str(self.params.index_code or "").strip()
        if not code or not latest_trade_date:
            return {}

        table = "history_index_daily_kline"
        lookback = max(int(self.params.index_hist_lookback_days or 0), 1)
        df = pd.DataFrame()
        start_date = None
        try:
            end_date = pd.to_datetime(latest_trade_date).date()
            start_date = (end_date - dt.timedelta(days=lookback * 3)).isoformat()
        except Exception:
            end_date = None

        if self._table_exists(table) and end_date is not None and start_date is not None:
            stmt = text(
                f"""
                SELECT `date`,`open`,`high`,`low`,`close`,`volume`,`amount`
                FROM `{table}`
                WHERE `code` = :code AND `date` BETWEEN :start_date AND :end_date
                ORDER BY `date`
                """
            )
            try:
                with self.engine.begin() as conn:
                    df = pd.read_sql_query(
                        stmt,
                        conn,
                        params={
                            "code": code,
                            "start_date": start_date,
                            "end_date": latest_trade_date,
                        },
                    )
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("读取指数日线失败，将尝试 Baostock：%s", exc)

        if df.empty and end_date is not None and start_date is not None:
            try:
                client = BaostockDataFetcher(BaostockSession())
                df = client.get_kline(code, start_date, latest_trade_date)
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("Baostock 指数日线兜底失败：%s", exc)

        if df.empty:
            return {"index_code": code}

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.sort_values("date")
        if len(df) > lookback:
            df = df.tail(lookback)

        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "preclose" not in df.columns:
            df["preclose"] = df["close"].shift(1)

        dif, dea, hist = _macd(df["close"])
        df["macd_hist"] = hist
        df["atr14"] = _atr(df["high"], df["low"], df["preclose"])
        df["ma20"] = df["close"].rolling(20, min_periods=1).mean()
        df["ma60"] = df["close"].rolling(60, min_periods=1).mean()

        asof_row = df.iloc[-1]
        asof_trade_date = asof_row.get("date")
        asof_trade_date_str = (
            asof_trade_date.date().isoformat() if not pd.isna(asof_trade_date) else None
        )

        return {
            "index_code": code,
            "asof_trade_date": asof_trade_date_str,
            "asof_close": _to_float(asof_row.get("close")),
            "asof_ma20": _to_float(asof_row.get("ma20")),
            "asof_ma60": _to_float(asof_row.get("ma60")),
            "asof_macd_hist": _to_float(asof_row.get("macd_hist")),
            "asof_atr14": _to_float(asof_row.get("atr14")),
            "history": df,
        }

    def load_avg_volume(
        self,
        latest_trade_date: str,
        codes: List[str],
        window: int = 20,
        *,
        asof_trade_date_map: Dict[str, str] | None = None,
    ) -> Dict[str, float]:
        if not codes:
            return {}

        table = self._daily_table()
        if not self._table_exists(table):
            return {}

        parsed_asof_map: Dict[str, dt.date] = {}
        if isinstance(asof_trade_date_map, dict):
            for code, raw_date in asof_trade_date_map.items():
                try:
                    parsed = pd.to_datetime(raw_date).date()
                except Exception:
                    parsed = None
                if parsed is not None:
                    parsed_asof_map[str(code)] = parsed

        default_end_date = None
        if latest_trade_date:
            try:
                default_end_date = pd.to_datetime(latest_trade_date).date()
            except Exception:
                default_end_date = None

        end_dates = [default_end_date] if default_end_date else []
        end_dates.extend(parsed_asof_map.values())
        if not end_dates:
            return {}

        max_end_date = max(end_dates)
        min_start_date = min(
            [d - dt.timedelta(days=max(window * 4, 60)) for d in end_dates]
        )

        stmt = text(
            f"""
            SELECT `date`,`code`,`volume`
            FROM `{table}`
            WHERE `code` IN :codes AND `date` BETWEEN :start_date AND :end_date
            """
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "codes": codes,
                        "start_date": min_start_date.isoformat(),
                        "end_date": max_end_date.isoformat(),
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取历史成交量失败，将跳过盘中量比：%s", exc)
            return {}

        if df.empty or "volume" not in df.columns:
            return {}

        df["code"] = df["code"].astype(str)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["volume"] = df["volume"].apply(_to_float)
        avg_map: Dict[str, float] = {}
        for code, grp in df.groupby("code", sort=False):
            end_date = parsed_asof_map.get(code, default_end_date)
            if end_date is None:
                continue
            recent = grp[grp["date"] <= end_date]
            if recent.empty:
                continue
            top = recent.sort_values("date", ascending=False).head(window)
            volumes = top["volume"].dropna()
            if not volumes.empty:
                avg_map[code] = float(volumes.mean())
        return avg_map

    def load_previous_strength(
        self, codes: List[str], as_of: dt.datetime | None = None
    ) -> Dict[str, float]:
        table = self.params.output_table
        run_table = self.params.run_table
        if (
            not codes
            or not run_table
            or not self._table_exists(table)
            or not self._table_exists(run_table)
        ):
            return {}

        if not self._column_exists(table, "signal_strength") or not self._column_exists(
            run_table, "checked_at"
        ):
            return {}

        stmt = text(
            f"""
            SELECT t1.`code`, t1.`signal_strength`
            FROM `{table}` t1
            JOIN `{run_table}` r1
              ON t1.`run_pk` = r1.`run_pk`
            JOIN (
                SELECT e.`code`, MAX(r.`checked_at`) AS latest_checked
                FROM `{table}` e
                JOIN `{run_table}` r
                  ON e.`run_pk` = r.`run_pk`
                WHERE e.`code` IN :codes
                  AND e.`signal_strength` IS NOT NULL
                  {"AND r.`checked_at` < :as_of" if as_of else ""}
                GROUP BY e.`code`
            ) t2
              ON t1.`code` = t2.`code` AND r1.`checked_at` = t2.`latest_checked`
            WHERE t1.`code` IN :codes
              AND t1.`signal_strength` IS NOT NULL
              {"AND r1.`checked_at` < :as_of" if as_of else ""}
            """
        ).bindparams(bindparam("codes", expanding=True))

        params: dict[str, Any] = {"codes": codes}
        if as_of:
            params["as_of"] = as_of

        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params=params)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取历史信号强度失败，将跳过强度对比：%s", exc)
            return {}

        if df.empty or "code" not in df.columns:
            return {}

        df["code"] = df["code"].astype(str)
        strength_map: Dict[str, float] = {}
        for _, row in df.iterrows():
            score = _to_float(row.get("signal_strength"))
            if score is None:
                continue
            strength_map[str(row.get("code"))] = score
        return strength_map

    def load_stock_industry_dim(self) -> pd.DataFrame:
        candidates = ["dim_stock_industry", "a_share_stock_industry"]
        for table in candidates:
            if not self._table_exists(table):
                continue
            try:
                with self.engine.begin() as conn:
                    df = pd.read_sql_query(text(f"SELECT * FROM `{table}`"), conn)
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("读取 %s 失败：%s", table, exc)
                continue
            if not df.empty:
                df["code"] = df["code"].astype(str)
                return df
        return pd.DataFrame()

    def load_board_constituent_dim(self) -> pd.DataFrame:
        table = "dim_stock_board_industry"
        if not self._table_exists(table):
            return pd.DataFrame()
        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(text(f"SELECT * FROM `{table}`"), conn)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取 %s 失败：%s", table, exc)
            return pd.DataFrame()
        if not df.empty and "code" in df.columns:
            df["code"] = df["code"].astype(str)
        return df


    def load_latest_run_context_by_stage(
        self,
        monitor_date: str,
        *,
        stage: str | None = None,
    ) -> dict[str, Any] | None:
        table = getattr(self.params, "run_table", None)
        if not (table and monitor_date and self._table_exists(table)):
            return None

        stage_norm = (str(stage).strip().upper() if stage else "") or None
        where_clauses = ["`monitor_date` = :d"]
        params: dict[str, Any] = {"d": monitor_date}

        if stage_norm in {"PREOPEN", "BREAK", "POSTCLOSE"}:
            where_clauses.append("`run_id` LIKE :p")
            params["p"] = f"{stage_norm} %"
        elif stage_norm == "INTRADAY":
            where_clauses.append("`run_id` NOT LIKE 'PREOPEN %'")
            where_clauses.append("`run_id` NOT LIKE 'BREAK %'")
            where_clauses.append("`run_id` NOT LIKE 'POSTCLOSE %'")

        stmt = text(
            f"""
            SELECT `run_pk`,`run_id`,`params_json`
            FROM `{table}`
            WHERE {" AND ".join(where_clauses)}
            ORDER BY `run_pk` DESC
            LIMIT 1
            """
        )
        try:
            with self.engine.begin() as conn:
                row = conn.execute(stmt, params).fetchone()
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取最新 run_context 失败：%s", exc)
            return None

        if not row:
            return None

        run_pk_val = row[0]
        run_id_val = row[1]
        params_json_raw = row[2]

        parsed: dict[str, Any] = {}
        if isinstance(params_json_raw, str) and params_json_raw.strip():
            try:
                parsed = json.loads(params_json_raw)
            except Exception:
                parsed = {}
        if not isinstance(parsed, dict):
            parsed = {}

        return {
            "run_pk": int(run_pk_val) if run_pk_val is not None else None,
            "run_id": str(run_id_val) if run_id_val is not None else None,
            "params_json": params_json_raw,
            "dedup_sig": parsed.get("dedup_sig"),
            "dedup_stage": parsed.get("dedup_stage"),
        }

    def ensure_run_context(
        self,
        monitor_date: str,
        run_id: str,
        *,
        checked_at: dt.datetime | None,
        triggered_at: dt.datetime | None = None,
        params_json: str | dict[str, Any] | None = None,
    ) -> int | None:
        table = getattr(self.params, "run_table", None)
        if not (table and monitor_date and run_id and self._table_exists(table)):
            return None

        parsed_monitor = pd.to_datetime(monitor_date, errors="coerce")
        monitor_date_val = parsed_monitor.date() if not pd.isna(parsed_monitor) else monitor_date
        params_payload = params_json
        if isinstance(params_json, dict):
            params_payload = json.dumps(params_json, ensure_ascii=False, separators=(",", ":"))
        payload = {
            "monitor_date": monitor_date_val,
            "run_id": run_id,
            "triggered_at": triggered_at,
            "checked_at": checked_at,
            "params_json": params_payload,
        }
        columns = list(payload.keys())
        update_clause = """
            `triggered_at` = COALESCE(`triggered_at`, VALUES(`triggered_at`)),
            `checked_at` = VALUES(`checked_at`),
            `params_json` = COALESCE(VALUES(`params_json`), `params_json`)
        """
        stmt = text(
            f"""
            INSERT INTO `{table}` ({", ".join(f"`{c}`" for c in columns)})
            VALUES ({", ".join(f":{c}" for c in columns)})
            ON DUPLICATE KEY UPDATE {update_clause}
            """
        )
        try:
            with self.engine.begin() as conn:
                conn.execute(stmt, payload)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("写入开盘监测运行记录失败：%s", exc)
            return None

        try:
            with self.engine.begin() as conn:
                row = conn.execute(
                    text(
                        f"""
                        SELECT `run_pk`
                        FROM `{table}`
                        WHERE `monitor_date` = :d AND `run_id` = :r
                        LIMIT 1
                        """
                    ),
                    {"d": monitor_date, "r": run_id},
                ).fetchone()
            if row:
                return int(row[0])
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取 run_pk 失败：%s", exc)
        return None

    def load_open_monitor_env_row(
        self, monitor_date: str, run_pk: int | None
    ) -> pd.DataFrame | None:
        table = self.params.open_monitor_env_table
        if not (table and monitor_date and self._table_exists(table)):
            return None
        if run_pk is None:
            self.logger.error("读取环境快照时缺少 run_pk（monitor_date=%s）。", monitor_date)
            return None

        stmt = text(
            f"""
            SELECT * FROM `{table}`
            WHERE `run_pk` = :b AND `monitor_date` = :d
            LIMIT 1
            """
        )

        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": monitor_date, "b": run_pk})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取环境快照失败：%s", exc)
            return None

        if df.empty:
            self.logger.error("未找到环境快照（monitor_date=%s, run_pk=%s）。", monitor_date, run_pk)
            return None

        return df

    def load_open_monitor_env_view_row(
        self, monitor_date: str, run_pk: int | None
    ) -> dict[str, Any] | None:
        view = getattr(self.params, "open_monitor_env_view", None)
        if not (view and monitor_date and run_pk):
            return None
        if not self._table_exists(view):
            return None

        stmt = text(
            f"""
            SELECT * FROM `{view}`
            WHERE `run_pk` = :b AND `monitor_date` = :d
            LIMIT 1
            """
        )
        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": monitor_date, "b": run_pk})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取环境视图失败：%s", exc)
            return None

        if df.empty:
            return None

        return df.iloc[0].to_dict()

    def get_latest_weekly_indicator_date(self) -> dt.date | None:
        table = self.params.weekly_indicator_table
        if not (table and self._table_exists(table)):
            return None
        benchmark_code = self.params.weekly_benchmark_code
        stmt = text(
            f"""
            SELECT MAX(`weekly_asof_trade_date`) AS latest_date
            FROM `{table}`
            WHERE `benchmark_code` = :code
            """
        )
        try:
            with self.engine.begin() as conn:
                row = conn.execute(stmt, {"code": benchmark_code}).mappings().first()
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取周线指标最新日期失败：%s", exc)
            return None
        if not row:
            return None
        latest = row.get("latest_date")
        if isinstance(latest, dt.datetime):
            return latest.date()
        if isinstance(latest, dt.date):
            return latest
        if latest:
            try:
                return pd.to_datetime(latest).date()
            except Exception:
                return None
        return None

    def get_latest_daily_market_env_date(
        self, *, benchmark_code: str = "sh.000001"
    ) -> str | None:
        table = self.params.daily_indicator_table
        if not (table and self._table_exists(table)):
            return None
        stmt = text(
            f"""
            SELECT MAX(`asof_trade_date`) AS latest_date
            FROM `{table}`
            WHERE `benchmark_code` = :code
            """
        )
        try:
            with self.engine.begin() as conn:
                row = conn.execute(stmt, {"code": benchmark_code}).mappings().first()
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取日线环境最新日期失败：%s", exc)
            return None
        if not row:
            return None
        latest = row.get("latest_date")
        if isinstance(latest, (dt.datetime, dt.date)):
            if isinstance(latest, dt.datetime):
                return latest.date().isoformat()
            return latest.isoformat()
        if latest:
            try:
                return pd.to_datetime(latest).date().isoformat()
            except Exception:
                return None
        return None

    def load_weekly_indicator(self, asof_trade_date: str) -> dict[str, Any]:
        table = self.params.weekly_indicator_table
        if not (table and self._table_exists(table)):
            return {}
        benchmark_code = self.params.weekly_benchmark_code
        stmt = text(
            f"""
            SELECT *
            FROM `{table}`
            WHERE `weekly_asof_trade_date` = :d AND `benchmark_code` = :code
            LIMIT 1
            """
        )
        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={"d": asof_trade_date, "code": benchmark_code},
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取周线指标失败：%s", exc)
            return {}
        if df.empty:
            return {}
        return df.iloc[0].to_dict()

    def load_daily_market_env(
        self, *, asof_trade_date: str, benchmark_code: str = "sh.000001"
    ) -> dict[str, Any] | None:
        table = self.params.daily_indicator_table
        if not (table and self._table_exists(table)):
            return None
        stmt = text(
            f"""
            SELECT *
            FROM `{table}`
            WHERE `asof_trade_date` = :d AND `benchmark_code` = :code
            LIMIT 1
            """
        )
        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={"d": asof_trade_date, "code": benchmark_code},
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取日线环境失败：%s", exc)
            return None
        if df.empty:
            return None
        return df.iloc[0].to_dict()

    def upsert_weekly_indicator(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        table = self.params.weekly_indicator_table
        if not (table and self._table_exists(table)):
            return 0
        df = pd.DataFrame(rows)
        if df.empty:
            return 0
        columns = df.columns.tolist()
        update_cols = [
            c
            for c in columns
            if c not in {"weekly_asof_trade_date", "benchmark_code"}
        ]
        stmt = text(
            f"""
            INSERT INTO `{table}` ({", ".join(f"`{c}`" for c in columns)})
            VALUES ({", ".join(f":{c}" for c in columns)})
            ON DUPLICATE KEY UPDATE {", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)}
            """
        )
        df = df.where(pd.notna(df), None)
        payloads = df.to_dict(orient="records")
        try:
            with self.engine.begin() as conn:
                conn.execute(stmt, payloads)
            return len(payloads)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("写入周线指标失败：%s", exc)
            return 0

    def upsert_daily_market_env(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        table = self.params.daily_indicator_table
        if not (table and self._table_exists(table)):
            return 0
        df = pd.DataFrame(rows)
        if df.empty:
            return 0
        columns = df.columns.tolist()
        update_cols = [
            c for c in columns if c not in {"asof_trade_date", "benchmark_code"}
        ]
        stmt = text(
            f"""
            INSERT INTO `{table}` ({", ".join(f"`{c}`" for c in columns)})
            VALUES ({", ".join(f":{c}" for c in columns)})
            ON DUPLICATE KEY UPDATE {", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)}
            """
        )
        payloads = df.to_dict(orient="records")
        try:
            with self.engine.begin() as conn:
                conn.execute(stmt, payloads)
            return len(payloads)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("写入日线环境失败：%s", exc)
            return 0

    def attach_cycle_phase_from_weekly(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not rows:
            return rows
        table = self.params.weekly_indicator_table
        if not table:
            return rows

        df_daily = pd.DataFrame(rows)
        if df_daily.empty or "asof_trade_date" not in df_daily.columns:
            return rows

        df_daily = df_daily.copy()
        df_daily["__order"] = range(len(df_daily))
        df_daily["__asof_trade_date"] = pd.to_datetime(
            df_daily["asof_trade_date"], errors="coerce"
        )
        parsed_dates = df_daily["__asof_trade_date"].dropna()
        if parsed_dates.empty:
            df_daily["cycle_phase"] = None
            df_daily["cycle_weekly_asof_trade_date"] = None
            df_daily["cycle_weekly_scene_code"] = None
            df_daily = df_daily.sort_values("__order")
            df_daily = df_daily.drop(columns=["__order", "__asof_trade_date"])
            return df_daily.to_dict(orient="records")

        min_date = parsed_dates.min()
        max_date = parsed_dates.max()
        min_date_floor = (min_date - pd.Timedelta(days=14)).date()
        max_date_val = max_date.date()

        benchmark_code = self.params.weekly_benchmark_code
        stmt = text(
            f"""
            SELECT weekly_asof_trade_date, weekly_scene_code, weekly_phase
            FROM `{table}`
            WHERE benchmark_code = :benchmark_code
              AND weekly_asof_trade_date <= :max_date
              AND weekly_asof_trade_date >= :min_date
            """
        )
        try:
            with self.engine.begin() as conn:
                df_weekly = pd.read_sql_query(
                    stmt,
                    conn,
                    params={
                        "max_date": max_date_val,
                        "min_date": min_date_floor,
                        "benchmark_code": benchmark_code,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取周线场景失败：%s", exc)
            df_daily = df_daily.sort_values("__order")
            df_daily = df_daily.drop(columns=["__order", "__asof_trade_date"])
            return df_daily.to_dict(orient="records")

        if df_weekly.empty:
            df_daily["cycle_phase"] = None
            df_daily["cycle_weekly_asof_trade_date"] = None
            df_daily["cycle_weekly_scene_code"] = None
            df_daily = df_daily.sort_values("__order")
            df_daily = df_daily.drop(columns=["__order", "__asof_trade_date"])
            return df_daily.to_dict(orient="records")

        df_weekly = df_weekly.copy()
        df_weekly["weekly_asof_trade_date"] = pd.to_datetime(
            df_weekly["weekly_asof_trade_date"], errors="coerce"
        )
        df_weekly = df_weekly.dropna(subset=["weekly_asof_trade_date"])
        if df_weekly.empty:
            df_daily["cycle_phase"] = None
            df_daily["cycle_weekly_asof_trade_date"] = None
            df_daily["cycle_weekly_scene_code"] = None
            df_daily = df_daily.sort_values("__order")
            df_daily = df_daily.drop(columns=["__order", "__asof_trade_date"])
            return df_daily.to_dict(orient="records")

        df_daily_sorted = df_daily.sort_values("__asof_trade_date")
        df_weekly_sorted = df_weekly.sort_values("weekly_asof_trade_date")

        df_daily_valid = df_daily_sorted.dropna(subset=["__asof_trade_date"])
        df_daily_invalid = df_daily_sorted[df_daily_sorted["__asof_trade_date"].isna()]

        merged = pd.merge_asof(
            df_daily_valid,
            df_weekly_sorted,
            left_on="__asof_trade_date",
            right_on="weekly_asof_trade_date",
            direction="backward",
        )
        merged["cycle_weekly_asof_trade_date"] = (
            merged["weekly_asof_trade_date"].dt.date
        )
        merged["cycle_weekly_scene_code"] = merged["weekly_scene_code"]
        merged["cycle_phase"] = merged["weekly_phase"]
        merged["cycle_phase"] = merged["cycle_phase"].where(
            pd.notna(merged["cycle_phase"]), None
        )
        merged["cycle_weekly_asof_trade_date"] = merged[
            "cycle_weekly_asof_trade_date"
        ].where(pd.notna(merged["cycle_weekly_asof_trade_date"]), None)
        merged["cycle_weekly_scene_code"] = merged[
            "cycle_weekly_scene_code"
        ].where(pd.notna(merged["cycle_weekly_scene_code"]), None)

        if not df_daily_invalid.empty:
            df_daily_invalid = df_daily_invalid.copy()
            df_daily_invalid["cycle_phase"] = None
            df_daily_invalid["cycle_weekly_asof_trade_date"] = None
            df_daily_invalid["cycle_weekly_scene_code"] = None
            merged = pd.concat([merged, df_daily_invalid], ignore_index=True)

        merged = merged.sort_values("__order")
        merged = merged.drop(
            columns=[
                "__order",
                "__asof_trade_date",
                "weekly_asof_trade_date",
                "weekly_scene_code",
                "weekly_phase",
            ],
            errors="ignore",
        )
        return merged.to_dict(orient="records")

    def persist_open_monitor_env(
        self,
        env_context: dict[str, Any] | None,
        monitor_date: str,
        run_pk: int,
    ) -> None:
        if not env_context:
            return

        table = self.params.open_monitor_env_table
        if not table:
            return

        monitor_dt = pd.to_datetime(monitor_date, errors="coerce")
        monitor_date_val = monitor_dt.date() if not pd.isna(monitor_dt) else monitor_date

        payload: dict[str, Any] = {}

        payload["monitor_date"] = monitor_date_val
        payload["run_pk"] = run_pk
        env_weekly_asof = env_context.get("weekly_asof_trade_date")
        if env_weekly_asof is not None:
            parsed_weekly = pd.to_datetime(env_weekly_asof, errors="coerce")
            env_weekly_asof = parsed_weekly.date() if not pd.isna(parsed_weekly) else env_weekly_asof
        payload["env_weekly_asof_trade_date"] = env_weekly_asof
        env_daily_asof = env_context.get("daily_asof_trade_date")
        if env_daily_asof is not None:
            parsed_daily = pd.to_datetime(env_daily_asof, errors="coerce")
            env_daily_asof = parsed_daily.date() if not pd.isna(parsed_daily) else env_daily_asof
        payload["env_daily_asof_trade_date"] = env_daily_asof
        index_snapshot = {}
        if isinstance(env_context, dict):
            raw_index_snapshot = env_context.get("index_intraday")
            if isinstance(raw_index_snapshot, dict):
                index_snapshot = raw_index_snapshot
        env_index_hash = index_snapshot.get("env_index_snapshot_hash")
        payload["env_index_snapshot_hash"] = env_index_hash
        payload["env_final_gate_action"] = env_context.get("env_final_gate_action")
        if payload["env_final_gate_action"] is None:
            self.logger.error(
                "环境快照缺少 env_final_gate_action，已跳过写入（monitor_date=%s, run_pk=%s）。",
                monitor_date,
                run_pk,
            )
            return
        payload["env_live_override_action"] = env_context.get("env_live_override_action")
        payload["env_live_cap_multiplier"] = _to_float(env_context.get("env_live_cap_multiplier"))
        payload["env_live_event_tags"] = env_context.get("env_live_event_tags")
        payload["env_live_reason"] = env_context.get("env_live_reason")
        cap_candidates = [
            env_context.get("env_final_cap_pct") if isinstance(env_context, dict) else None,
            env_context.get("effective_position_hint") if isinstance(env_context, dict) else None,
            env_context.get("position_hint") if isinstance(env_context, dict) else None,
            index_snapshot.get("env_index_position_cap") if isinstance(index_snapshot, dict) else None,
        ]
        payload["env_final_cap_pct"] = next(
            (_to_float(c) for c in cap_candidates if _to_float(c) is not None),
            None,
        )
        payload["env_final_reason_json"] = env_context.get("env_final_reason_json")

        if not self._table_exists(table):
            self.logger.error("环境快照表 %s 不存在，已跳过写入。", table)
            return
        columns = list(payload.keys())
        update_cols = [c for c in columns if c not in {"monitor_date", "run_pk"}]
        col_clause = ", ".join(f"`{c}`" for c in columns)
        value_clause = ", ".join(f":{c}" for c in columns)
        update_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)
        stmt = text(
            f"""
            INSERT INTO `{table}` ({col_clause})
            VALUES ({value_clause})
            ON DUPLICATE KEY UPDATE {update_clause}
            """
        )

        try:
            with self.engine.begin() as conn:
                conn.execute(stmt, payload)
            self.logger.info(
                "环境快照已写入表 %s（monitor_date=%s, run_pk=%s）",
                table,
                monitor_date,
                run_pk,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("写入环境快照失败：%s", exc)

    def _delete_existing_run_rows(
        self, table: str, monitor_date: str, run_pk: int, codes: List[str]
    ) -> int:
        if not (
            table
            and monitor_date
            and run_pk
            and codes
            and self._table_exists(table)
        ):
            return 0

        stmt = text(
            "DELETE FROM `{table}` WHERE `monitor_date` = :d AND `run_pk` = :b AND `code` IN :codes".format(
                table=table
            )
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    stmt, {"d": monitor_date, "b": run_pk, "codes": codes}
                )
                return int(getattr(result, "rowcount", 0) or 0)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("覆盖写入时清理旧快照失败：%s", exc)
            return 0

    def persist_quote_snapshots(self, df: pd.DataFrame) -> None:
        if df.empty or not self.params.write_to_db:
            return
        table = self.params.quote_table
        if not table:
            return

        keep_cols = [
            "monitor_date",
            "run_pk",
            "code",
            "live_trade_date",
            "live_open",
            "live_high",
            "live_low",
            "live_latest",
            "live_volume",
            "live_amount",
        ]
        for col in keep_cols:
            if col not in df.columns:
                df[col] = None
        quotes = df[keep_cols].copy()
        quotes["monitor_date"] = pd.to_datetime(quotes["monitor_date"]).dt.date
        quotes["live_trade_date"] = pd.to_datetime(quotes["live_trade_date"]).dt.date
        quotes["run_pk"] = pd.to_numeric(quotes["run_pk"], errors="coerce")
        quotes["code"] = quotes["code"].astype(str)
        quotes = quotes.dropna(subset=["run_pk"]).copy()
        if quotes.empty:
            return

        monitor_dates = quotes["monitor_date"].dropna().astype(str).unique().tolist()
        if self._table_exists(table) and monitor_dates:
            for monitor in monitor_dates:
                run_pks = (
                    quotes.loc[quotes["monitor_date"].astype(str) == monitor, "run_pk"]
                    .dropna()
                    .unique()
                    .tolist()
                )
                for run_pk in run_pks:
                    run_pk_codes = quotes.loc[
                        (quotes["monitor_date"].astype(str) == monitor)
                        & (quotes["run_pk"] == run_pk),
                        "code",
                    ].dropna().astype(str).unique().tolist()
                    if run_pk_codes:
                        self._delete_existing_run_rows(
                            table,
                            monitor,
                            int(run_pk),
                            run_pk_codes,
                        )

        try:
            self.db_writer.write_dataframe(quotes, table, if_exists="append")
            self.logger.info("开盘监测实时行情快照已写入表 %s：%s 条", table, len(quotes))
        except Exception as exc:  # noqa: BLE001
            self.logger.error("写入开盘监测行情快照失败：%s", exc)

    def persist_results(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        df = df.copy()
        table = self.params.output_table
        if not self.params.write_to_db:
            return

        codes = df["code"].dropna().astype(str).unique().tolist()
        table_exists = self._table_exists(table)
        table_columns = set(self._get_table_columns(table))

        for col in ["signal_strength", "strength_delta"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "run_pk" not in df.columns:
            df["run_pk"] = None
        df["run_pk"] = pd.to_numeric(df["run_pk"], errors="coerce")
        if "strategy_code" not in df.columns:
            df["strategy_code"] = str(getattr(self.params, "strategy_code", "") or "").strip() or None
        else:
            fallback_strategy = str(getattr(self.params, "strategy_code", "") or "").strip() or None
            df["strategy_code"] = df["strategy_code"].fillna(fallback_strategy)
        if "run_pk" in df.columns:
            missing_run_pk = df["run_pk"].isna()
            if missing_run_pk.any():
                self.logger.warning(
                    "检测到缺失 run_pk 的开盘监测结果 %s 条，已跳过写入。",
                    int(missing_run_pk.sum()),
                )
                df = df.loc[~missing_run_pk].copy()
                if df.empty:
                    return

        for col in ["monitor_date", "sig_date", "asof_trade_date", "live_trade_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        monitor_date_val = (
            df["monitor_date"].iloc[0] if "monitor_date" in df.columns and not df.empty else None
        )
        monitor_date = monitor_date_val.isoformat() if monitor_date_val is not None else ""

        for col in [
            "risk_tag",
            "risk_note",
            "sig_reason",
            "action_reason",
            "status_reason",
        ]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.slice(0, 250)

        if "summary_line" in df.columns:
            df["summary_line"] = df["summary_line"].fillna("").astype(str).str.slice(0, 512)

        if "rule_hits_json" in df.columns:
            df["rule_hits_json"] = df["rule_hits_json"].fillna("").astype(str).str.slice(0, 4000)

        df["snapshot_hash"] = df.apply(lambda row: make_snapshot_hash(row.to_dict()), axis=1)
        df = df.drop_duplicates(subset=["run_pk", "strategy_code", "sig_date", "code"])

        self.persist_quote_snapshots(df)

        if table_columns:
            keep_cols = [c for c in df.columns if c in table_columns]
            df = df[keep_cols]

        if (not self.params.incremental_write) and monitor_date and codes and table_exists:
            delete_stmt = text(
                "DELETE FROM `{table}` WHERE `monitor_date` = :d AND `code` IN :codes".format(
                    table=table
                )
            ).bindparams(bindparam("codes", expanding=True))
            try:
                with self.engine.begin() as conn:
                    conn.execute(delete_stmt, {"d": monitor_date, "codes": codes})
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("开盘监测表去重删除失败，将直接追加：%s", exc)

        if table_exists and monitor_date and codes:
            deleted_total = 0
            run_pks = (
                df["run_pk"].dropna().unique().tolist() if "run_pk" in df.columns else []
            )
            for run_pk_val in run_pks:
                if pd.isna(run_pk_val):
                    continue
                deleted_total += self._delete_existing_run_rows(
                    table, monitor_date, int(run_pk_val), codes
                )
            if deleted_total > 0:
                self.logger.info("检测到同 run_pk 旧快照：已覆盖删除 %s 条。", deleted_total)

        if df.empty:
            self.logger.info("本次开盘监测结果全部为重复快照，跳过写入。")
            return

        try:
            self.db_writer.write_dataframe(df, table, if_exists="append")
            self.logger.info("开盘监测结果已写入表 %s：%s 条", table, len(df))
        except Exception as exc:  # noqa: BLE001
            self.logger.error("写入开盘监测表失败：%s", exc)

    def load_open_monitor_view_data(
        self, monitor_date: str, run_pk: int | None
    ) -> pd.DataFrame:
        view = self.params.open_monitor_view or self.params.open_monitor_wide_view
        fallback_view = self.params.open_monitor_wide_view
        if not (view and monitor_date and self._table_exists(view)):
            if fallback_view and self._table_exists(fallback_view):
                view = fallback_view
            else:
                return pd.DataFrame()

        clauses = ["`monitor_date` = :d"]
        params: dict[str, Any] = {"d": monitor_date}
        if run_pk:
            clauses.append("`run_pk` = :b")
            params["b"] = run_pk
        where_clause = " AND ".join(clauses)
        stmt = text(
            f"""
            SELECT *
            FROM `{view}`
            WHERE {where_clause}
            ORDER BY `checked_at` DESC, `sig_date` DESC, `code`
            """
        )
        try:
            with self.engine.begin() as conn:
                return pd.read_sql_query(stmt, conn, params=params)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取开盘监测视图 %s 失败：%s", view, exc)
            return pd.DataFrame()
