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

    t = ts.time()
    if t < auction_start:
        return "PREOPEN"
    if lunch_break_start <= t < lunch_break_end:
        return "BREAK"
    if t >= market_close:
        return "POSTCLOSE"

    minute_of_day = ts.hour * 60 + ts.minute
    slot_minute = (minute_of_day // window_minutes) * window_minutes
    slot_time = dt.datetime.combine(ts.date(), dt.time(slot_minute // 60, slot_minute % 60))
    return slot_time.strftime("%Y-%m-%d %H:%M")


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
            ORDER BY `date` DESC
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
                        ORDER BY `sig_date` DESC
                        LIMIT :lookback
                        """
                    ),
                    conn,
                    params={
                        "latest_trade_date": latest_trade_date,
                        "strategy_code": self.params.strategy_code,
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
            ORDER BY `sig_date` DESC, `code`
            """
        ).bindparams(bindparam("dates", expanding=True))

        try:
            with self.engine.begin() as conn:
                events_df = pd.read_sql_query(
                    stmt,
                    conn,
                    params={"dates": signal_dates, "strategy_code": self.params.strategy_code},
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

        # fix: 监控候选在拉行情/评估前按有效期剔除过期信号，避免过期 BUY 进入候选池
        cross_valid_days = int(getattr(self.params, "cross_valid_days", 3) or 3)
        pullback_valid_days = int(getattr(self.params, "pullback_valid_days", 5) or 5)

        def _infer_valid_days(row: pd.Series) -> int:
            kind = str(row.get("signal_kind") or "").upper()
            reason = str(row.get("sig_reason") or "")
            is_pullback = ("PULL" in kind) or ("回踩" in reason)
            return pullback_valid_days if is_pullback else cross_valid_days

        if "valid_days" not in events_df.columns:
            events_df["valid_days"] = events_df.apply(_infer_valid_days, axis=1)

        before = len(events_df)
        keep_mask = (
            events_df["signal_age"].isna()
            | events_df["valid_days"].isna()
            | (events_df["signal_age"] <= events_df["valid_days"])
        )
        events_df = events_df.loc[keep_mask].copy()
        dropped = before - len(events_df)
        if dropped:
            self.logger.info(
                f"已按信号有效期过滤过期 BUY 候选：移除 {dropped} 条（保留 {len(events_df)} 条）。"
            )

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
        self, latest_trade_date: str, codes: List[str], window: int = 20
    ) -> Dict[str, float]:
        if not latest_trade_date or not codes:
            return {}

        table = self._daily_table()
        if not self._table_exists(table):
            return {}

        try:
            end_date = pd.to_datetime(latest_trade_date).date()
        except Exception:
            return {}

        start_date = end_date - dt.timedelta(days=max(window * 4, 60))
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
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取历史成交量失败，将跳过盘中量比：%s", exc)
            return {}

        if df.empty or "volume" not in df.columns:
            return {}

        df["code"] = df["code"].astype(str)
        df["volume"] = df["volume"].apply(_to_float)
        avg_map: Dict[str, float] = {}
        for code, grp in df.groupby("code", sort=False):
            top = grp.sort_values("date", ascending=False).head(window)
            volumes = top["volume"].dropna()
            if not volumes.empty:
                avg_map[code] = float(volumes.mean())
        return avg_map

    def load_previous_strength(
        self, codes: List[str], as_of: dt.datetime | None = None
    ) -> Dict[str, float]:
        table = self.params.output_table
        if not codes or not self._table_exists(table):
            return {}

        if not self._column_exists(table, "signal_strength") or not self._column_exists(
            table, "checked_at"
        ):
            return {}

        stmt = text(
            f"""
            SELECT t1.`code`, t1.`signal_strength`
            FROM `{table}` t1
            JOIN (
                SELECT `code`, MAX(`checked_at`) AS latest_checked
                FROM `{table}`
                WHERE `code` IN :codes AND `signal_strength` IS NOT NULL {"AND `checked_at` < :as_of" if as_of else ""}
                GROUP BY `code`
            ) t2
              ON t1.`code` = t2.`code` AND t1.`checked_at` = t2.`latest_checked`
            WHERE t1.`code` IN :codes AND t1.`signal_strength` IS NOT NULL {"AND t1.`checked_at` < :as_of" if as_of else ""}
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

    def ensure_run_context(
        self,
        monitor_date: str,
        run_id: str,
        *,
        checked_at: dt.datetime | None,
        triggered_at: dt.datetime | None = None,
        env_index_snapshot_hash: str | None = None,
        params_json: str | None = None,
    ) -> int | None:
        table = getattr(self.params, "run_table", None)
        if not (table and monitor_date and run_id and self._table_exists(table)):
            return None

        parsed_monitor = pd.to_datetime(monitor_date, errors="coerce")
        monitor_date_val = parsed_monitor.date() if not pd.isna(parsed_monitor) else monitor_date
        payload = {
            "monitor_date": monitor_date_val,
            "run_id": run_id,
            "triggered_at": triggered_at,
            "checked_at": checked_at,
            "env_index_snapshot_hash": env_index_snapshot_hash,
            "params_json": params_json,
        }
        columns = list(payload.keys())
        update_clause = """
            `triggered_at` = COALESCE(`triggered_at`, VALUES(`triggered_at`)),
            `checked_at` = VALUES(`checked_at`),
            `env_index_snapshot_hash` = COALESCE(VALUES(`env_index_snapshot_hash`), `env_index_snapshot_hash`),
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


    def _ensure_run_pk_inplace(self, df: pd.DataFrame) -> None:
        """Ensure df has non-null run_pk derived from (monitor_date, run_id).

        Some upstream steps only carry (monitor_date, run_id) and forget to attach run_pk.
        This helper backfills run_pk via strategy_open_monitor_run so downstream joins are stable.
        """

        if df is None or df.empty:
            return

        if "run_id" not in df.columns or "monitor_date" not in df.columns:
            return

        if "run_pk" not in df.columns:
            df["run_pk"] = None


        need_mask = df["run_pk"].isna()
        if not need_mask.any():
            return

        def _norm_monitor_date(v):
            if v is None:
                return None
            # pandas Timestamp
            if isinstance(v, pd.Timestamp):
                if pd.isna(v):
                    return None
                return v.date().isoformat()
            # datetime/date
            if isinstance(v, dt.datetime):
                return v.date().isoformat()
            if isinstance(v, dt.date):
                return v.isoformat()
            s = str(v).strip()
            if not s or s.lower() == "nat":
                return None
            return s

        # Only iterate unique (monitor_date, run_id) that need backfill
        pairs = df.loc[need_mask, ["monitor_date", "run_id"]].drop_duplicates()
        for md, rid in pairs.itertuples(index=False, name=None):
            md2 = _norm_monitor_date(md)
            rid2 = str(rid).strip() if rid is not None else ""
            if not (md2 and rid2):
                continue

            run_pk = self.ensure_run_context(monitor_date=md2, run_id=rid2, checked_at=None)
            if run_pk is None:
                continue

            # assign into the rows still missing
            df.loc[(df["monitor_date"] == md) & (df["run_id"] == rid) & (df["run_pk"].isna()), "run_pk"] = int(run_pk)

    def env_snapshot_exists(self, monitor_date: str, run_id: str) -> bool:
        table = self.params.env_snapshot_table
        if not (table and monitor_date and run_id and self._table_exists(table)):
            return False

        stmt = text(
            f"""
            SELECT 1 FROM `{table}`
            WHERE `run_id` = :b AND `monitor_date` = :d
            LIMIT 1
            """
        )
        try:
            with self.engine.begin() as conn:
                row = conn.execute(stmt, {"b": run_id, "d": monitor_date}).fetchone()
                return row is not None
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("检查环境快照是否存在失败：%s", exc)
            return False

    def load_env_snapshot_row(
        self, monitor_date: str, run_id: str | None
    ) -> pd.DataFrame | None:
        table = self.params.env_snapshot_table
        if not (table and monitor_date and self._table_exists(table)):
            return None
        if run_id is None:
            self.logger.error("读取环境快照时缺少 run_id（monitor_date=%s）。", monitor_date)
            return None

        stmt = text(
            f"""
            SELECT * FROM `{table}`
            WHERE `run_id` = :b AND `monitor_date` = :d
            ORDER BY `checked_at` DESC
            LIMIT 1
            """
        )

        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": monitor_date, "b": run_id})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取环境快照失败：%s", exc)
            return None

        if df.empty:
            self.logger.error("未找到环境快照（monitor_date=%s, run_id=%s）。", monitor_date, run_id)
            return None

        return df

    def load_index_snapshot_by_hash(self, snapshot_hash: str | None) -> dict[str, Any]:
        if not snapshot_hash:
            return {}
        table = self.params.env_index_snapshot_table
        if not (table and self._table_exists(table)):
            return {}
        stmt = text(
            f"""
            SELECT *
            FROM `{table}`
            WHERE `snapshot_hash` = :h
            ORDER BY `checked_at` DESC
            LIMIT 1
            """
        )
        try:
            with self.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"h": snapshot_hash})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取指数环境快照失败：%s", exc)
            return {}
        if df.empty:
            return {}
        raw = df.iloc[0].to_dict()
        normalized = {
            "env_index_snapshot_hash": raw.get("snapshot_hash"),
            "env_index_code": raw.get("index_code"),
            "env_index_asof_trade_date": raw.get("asof_trade_date"),
            "env_index_live_trade_date": raw.get("live_trade_date"),
            "env_index_asof_close": raw.get("asof_close"),
            "env_index_asof_ma20": raw.get("asof_ma20"),
            "env_index_asof_ma60": raw.get("asof_ma60"),
            "env_index_asof_macd_hist": raw.get("asof_macd_hist"),
            "env_index_asof_atr14": raw.get("asof_atr14"),
            "env_index_live_open": raw.get("live_open"),
            "env_index_live_high": raw.get("live_high"),
            "env_index_live_low": raw.get("live_low"),
            "env_index_live_latest": raw.get("live_latest"),
            "env_index_live_pct_change": raw.get("live_pct_change"),
            "env_index_live_volume": raw.get("live_volume"),
            "env_index_live_amount": raw.get("live_amount"),
            "env_index_dev_ma20_atr": raw.get("dev_ma20_atr"),
            "env_index_gate_action": raw.get("gate_action"),
            "env_index_gate_reason": raw.get("gate_reason"),
            "env_index_position_cap": raw.get("position_cap"),
            "checked_at": raw.get("checked_at"),
        }
        return normalized

    def persist_env_snapshot(
        self,
        env_context: dict[str, Any] | None,
        monitor_date: str,
        run_id: str,
        checked_at: dt.datetime,
        env_weekly_gate_policy: str | None,
        *,
        run_pk: int | None = None,
    ) -> None:
        if not env_context:
            return

        table = self.params.env_snapshot_table
        if not table:
            return

        monitor_dt = pd.to_datetime(monitor_date, errors="coerce")
        monitor_date_val = monitor_dt.date() if not pd.isna(monitor_dt) else monitor_date

        payload: dict[str, Any] = {}
        weekly_scenario = env_context.get("weekly_scenario") if isinstance(env_context, dict) else {}
        if not isinstance(weekly_scenario, dict):
            weekly_scenario = {}

        index_score = _to_float(env_context.get("index_score"))
        regime = env_context.get("regime")
        position_hint = _to_float(env_context.get("position_hint"))
        if regime is not None:
            regime = str(regime).strip() or None
        if index_score is None or regime is None or position_hint is None:
            self.logger.error(
                "环境快照缺少指数环境字段：index_score=%s regime=%s position_hint=%s",
                index_score,
                regime,
                position_hint,
            )
            raise ValueError("环境快照缺少指数环境字段（index_score/regime/position_hint）")

        def _get_env(key: str) -> Any:  # noqa: ANN401
            if isinstance(env_context, dict) and env_context.get(key) not in (None, ""):
                return env_context.get(key)
            return weekly_scenario.get(key)

        payload["monitor_date"] = monitor_date_val
        payload["run_id"] = run_id
        payload["run_pk"] = run_pk
        payload["checked_at"] = checked_at
        env_weekly_asof = _get_env("weekly_asof_trade_date")
        if env_weekly_asof is not None:
            parsed_weekly = pd.to_datetime(env_weekly_asof, errors="coerce")
            env_weekly_asof = parsed_weekly.date() if not pd.isna(parsed_weekly) else env_weekly_asof
        payload["env_weekly_asof_trade_date"] = env_weekly_asof
        payload["env_weekly_risk_level"] = _get_env("weekly_risk_level")
        payload["env_weekly_scene"] = _get_env("weekly_scene_code")
        payload["env_weekly_gate_policy"] = env_weekly_gate_policy

        weekly_gate_action = (
            _get_env("weekly_gate_action")
            or _get_env("weekly_gate_policy")
            or env_weekly_gate_policy
        )
        payload["env_weekly_gate_action"] = weekly_gate_action
        payload["env_index_score"] = index_score
        payload["env_regime"] = regime
        payload["env_position_hint"] = position_hint
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
                "环境快照缺少 env_final_gate_action，已跳过写入（monitor_date=%s, run_id=%s）。",
                monitor_date,
                run_id,
            )
            return
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
        update_cols = [c for c in columns if c not in {"monitor_date", "run_id"}]
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
                "环境快照已写入表 %s（monitor_date=%s, run_id=%s）",
                table,
                monitor_date,
                run_id,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("写入环境快照失败：%s", exc)

    def persist_index_snapshot(
        self,
        snapshot: dict[str, Any],
        *,
        table: str,
    ) -> str | None:
        if not snapshot or not table:
            return None

        if not self._table_exists(table):
            self.logger.error("指数环境快照表 %s 不存在，已跳过写入。", table)
            return None

        for date_col in ["monitor_date", "asof_trade_date", "live_trade_date"]:
            if date_col in snapshot:
                parsed = pd.to_datetime(snapshot.get(date_col), errors="coerce")
                snapshot[date_col] = parsed.date() if not pd.isna(parsed) else snapshot.get(date_col)


        # 兜底：run_id 统一为非空字符串（否则无法和 run 表 join）
        if "run_id" in snapshot:
            rid = str(snapshot.get("run_id") or "").strip()
            snapshot["run_id"] = rid or None

        # 指数快照写入时，也顺手确保 run 记录存在（便于后续联表调试）
        if snapshot.get("monitor_date") and snapshot.get("run_id"):
            try:
                self.ensure_run_context(
                    monitor_date=snapshot.get("monitor_date"),
                    run_id=str(snapshot.get("run_id")),
                    checked_at=snapshot.get("checked_at"),
                    params_json=json.dumps({"phase":"ENV_INDEX_SNAPSHOT"}, ensure_ascii=False, separators=(",", ":")),
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("确保 run 记录存在时失败（可忽略）：%s", exc)

        columns = list(snapshot.keys())
        if "snapshot_hash" not in columns:
            return None

        col_clause = ", ".join(f"`{c}`" for c in columns)
        value_clause = ", ".join(f":{c}" for c in columns)
        stmt = text(
            f"""
            INSERT INTO `{table}` ({col_clause})
            VALUES ({value_clause})
            ON DUPLICATE KEY UPDATE `snapshot_hash` = `snapshot_hash`
            """
        )

        try:
            with self.engine.begin() as conn:
                conn.execute(stmt, snapshot)
            self.logger.info(
                "指数环境快照已写入表 %s（hash=%s）。",
                table,
                snapshot.get("snapshot_hash"),
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("写入指数环境快照失败：%s", exc)
            return None

        return str(snapshot.get("snapshot_hash") or "")

    def _delete_existing_run_rows(
        self, table: str, monitor_date: str, run_id: str, codes: List[str]
    ) -> int:
        if not (
            table
            and monitor_date
            and run_id
            and codes
            and self._table_exists(table)
        ):
            return 0

        stmt = text(
            "DELETE FROM `{table}` WHERE `monitor_date` = :d AND `run_id` = :b AND `code` IN :codes".format(
                table=table
            )
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    stmt, {"d": monitor_date, "b": run_id, "codes": codes}
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
            "run_id",
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
        if "run_id" not in quotes.columns:
            quotes["run_id"] = None
        quotes["run_id"] = quotes["run_id"].fillna(
            calc_run_id(dt.datetime.now(), self.params.run_id_minutes)
        )
        if "run_pk" not in quotes.columns:
            quotes["run_pk"] = None

        self._ensure_run_pk_inplace(quotes)
        quotes["code"] = quotes["code"].astype(str)

        monitor_dates = quotes["monitor_date"].dropna().astype(str).unique().tolist()
        if self._table_exists(table) and monitor_dates:
            for monitor in monitor_dates:
                run_ids = (
                    quotes.loc[quotes["monitor_date"].astype(str) == monitor, "run_id"]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()
                )
                for run_id in run_ids:
                    run_id_codes = quotes.loc[
                        (quotes["monitor_date"].astype(str) == monitor)
                        & (quotes["run_id"].astype(str) == run_id),
                        "code",
                    ].dropna().astype(str).unique().tolist()
                    if run_id_codes:
                        self._delete_existing_run_rows(
                            table,
                            monitor,
                            run_id,
                            run_id_codes,
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

        if "run_id" not in df.columns:
            df["run_id"] = None
        df["run_id"] = df["run_id"].fillna(
            calc_run_id(dt.datetime.now(), self.params.run_id_minutes)
        )
        if "run_pk" not in df.columns:
            df["run_pk"] = None

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
        df = df.drop_duplicates(subset=["monitor_date", "sig_date", "code", "run_id"])

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
            run_ids = (
                df["run_id"].dropna().astype(str).unique().tolist()
                if "run_id" in df.columns
                else []
            )
            for run_id_val in run_ids:
                if not run_id_val:
                    continue
                deleted_total += self._delete_existing_run_rows(
                    table, monitor_date, run_id_val, codes
                )
            if deleted_total > 0:
                self.logger.info("检测到同 run_id 旧快照：已覆盖删除 %s 条。", deleted_total)

        if df.empty:
            self.logger.info("本次开盘监测结果全部为重复快照，跳过写入。")
            return

        try:
            self.db_writer.write_dataframe(df, table, if_exists="append")
            self.logger.info("开盘监测结果已写入表 %s：%s 条", table, len(df))
        except Exception as exc:  # noqa: BLE001
            self.logger.error("写入开盘监测表失败：%s", exc)

    def load_open_monitor_view_data(
        self, monitor_date: str, run_id: str | None
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
        if run_id:
            clauses.append("`run_id` = :b")
            params["b"] = run_id
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
