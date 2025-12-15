"""开盘监测：检查“前一交易日收盘信号”在今日开盘是否仍可执行。

目标：
- 读取 strategy_ma5_ma20_signals 中“最新交易日”的 BUY 信号（通常是昨天收盘跑出来的）。
- 在开盘/集合竞价阶段拉取实时行情（今开/最新价），做二次过滤：
  - 高开过多（追高风险/买不到合理价）
  - 低开破位（跌破 MA20 / 大幅低开）
  - 涨停（大概率买不到）

输出：
- 可选写入 MySQL：strategy_ma5_ma20_open_monitor（默认 append）
- 可选导出 CSV 到 output/open_monitor

注意：
- 该脚本“只做监测与清单输出”，不下单。
- 实时行情默认使用 Eastmoney push2 接口；如需测试 AkShare，可在 config.yaml 将 open_monitor.quote_source=akshare。
"""

from __future__ import annotations

import datetime as dt
import json
import math
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from sqlalchemy import bindparam, text

from .config import get_section
from .db import DatabaseConfig, MySQLWriter
from .utils.logger import setup_logger


def _to_float(value: Any) -> float | None:  # noqa: ANN401
    try:
        if value is None:
            return None
        if isinstance(value, str):
            v = value.strip()
            if v in {"", "-", "--", "None", "nan"}:
                return None
            return float(v.replace(",", ""))
        if isinstance(value, (int, float)):
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                return None
            return float(value)
        return float(value)
    except Exception:
        return None


def _strip_baostock_prefix(code: str) -> str:
    code = str(code or "").strip()
    if code.startswith("sh.") or code.startswith("sz."):
        return code[3:]
    return code


def _to_baostock_code(exchange: str, symbol: str) -> str:
    ex = str(exchange or "").lower().strip()
    sym = str(symbol or "").strip()
    if ex in {"sh", "1"}:
        return f"sh.{sym}"
    if ex in {"sz", "0"}:
        return f"sz.{sym}"
    # fallback：猜测 6/9 为沪，0/3 为深
    if sym.startswith(("6", "9")):
        return f"sh.{sym}"
    return f"sz.{sym}"


def _to_eastmoney_secid(code: str) -> str:
    code = str(code or "").strip()
    if code.startswith("sh."):
        return f"1.{code[3:]}"
    if code.startswith("sz."):
        return f"0.{code[3:]}"
    # 尝试裸代码
    digits = _strip_baostock_prefix(code)
    if digits.startswith(("6", "9")):
        return f"1.{digits}"
    return f"0.{digits}"


@dataclass(frozen=True)
class OpenMonitorParams:
    """开盘监测参数（支持从 config.yaml 的 open_monitor 覆盖）。"""

    enabled: bool = True

    # 信号来源表：默认沿用 MA5-MA20 策略 signals_table
    signals_table: str = "strategy_ma5_ma20_signals"

    # 输出表：开盘检查结果
    output_table: str = "strategy_ma5_ma20_open_monitor"

    # 回看近 N 个交易日的 BUY 信号
    signal_lookback_days: int = 3

    # 行情来源：eastmoney / akshare（兼容：auto 将按 eastmoney 处理）
    quote_source: str = "eastmoney"

    # 候选有效期：回踩形态默认更长
    cross_valid_days: int = 3
    pullback_valid_days: int = 5

    # 核心过滤规则
    max_gap_up_pct: float = 0.05       # 今开相对昨收涨幅 > 5% → skip
    max_gap_up_atr_mult: float = 1.5   # 动态高开阈值：gap_up > min(max_gap_up_pct, atr_mult*ATR/昨收) → skip
    max_gap_down_pct: float = -0.03    # 今开相对昨收跌幅 < -3% → skip
    min_open_vs_ma20_pct: float = 0.0  # 今开 < MA20*(1+min_open_vs_ma20_pct) → skip（默认需站上 MA20）
    limit_up_trigger_pct: float = 9.7  # 涨跌幅 >= 9.7% → 视为涨停/接近涨停，skip

    # 追高过滤：入场价相对 MA5 乖离过大
    max_entry_vs_ma5_pct: float = 0.08  # 入场价 > MA5*(1+8%) → skip

    # 过期判定：信号后快速拉升
    expire_atr_mult: float = 1.2
    expire_pct_threshold: float = 0.07

    # 风控：开盘入场价为基准的 ATR 止损
    stop_atr_mult: float = 2.0         # stop_ref = entry - stop_atr_mult*ATR

    # 情绪过滤：信号日（昨日收盘）如果是接近涨停的大阳线，次日默认不追
    signal_day_limit_up_pct: float = 0.095  # 9.5%（兼容“接近涨停”）

    # 输出控制
    write_to_db: bool = True
    export_csv: bool = True
    export_top_n: int = 100
    output_subdir: str = "open_monitor"

    @classmethod
    def from_config(cls) -> "OpenMonitorParams":
        sec = get_section("open_monitor") or {}
        if not isinstance(sec, dict):
            sec = {}

        # 默认 signals_table 与策略保持一致
        strat = get_section("strategy_ma5_ma20_trend") or {}
        if isinstance(strat, dict):
            default_signals = strat.get("signals_table", cls.signals_table)
        else:
            default_signals = cls.signals_table

        def _get_bool(key: str, default: bool) -> bool:
            val = sec.get(key, default)
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                return val.strip().lower() in {"1", "true", "yes", "y", "on"}
            return bool(val)

        def _get_float(key: str, default: float) -> float:
            raw = sec.get(key, default)
            parsed = _to_float(raw)
            return default if parsed is None else float(parsed)

        def _get_int(key: str, default: int) -> int:
            raw = sec.get(key, default)
            try:
                return int(raw)
            except Exception:
                return default

        quote_source = str(sec.get("quote_source", cls.quote_source)).strip().lower() or "auto"
        # 路线A：auto 也按 eastmoney 处理，避免误以为会优先 AkShare
        if quote_source == "auto":
            quote_source = "eastmoney"

        return cls(
            enabled=_get_bool("enabled", cls.enabled),
            signals_table=str(sec.get("signals_table", default_signals)).strip() or default_signals,
            output_table=str(sec.get("output_table", cls.output_table)).strip() or cls.output_table,
            signal_lookback_days=_get_int("signal_lookback_days", cls.signal_lookback_days),
            quote_source=quote_source,
            cross_valid_days=_get_int("cross_valid_days", cls.cross_valid_days),
            pullback_valid_days=_get_int("pullback_valid_days", cls.pullback_valid_days),
            max_gap_up_pct=_get_float("max_gap_up_pct", cls.max_gap_up_pct),
            max_gap_up_atr_mult=_get_float("max_gap_up_atr_mult", cls.max_gap_up_atr_mult),
            max_gap_down_pct=_get_float("max_gap_down_pct", cls.max_gap_down_pct),
            min_open_vs_ma20_pct=_get_float("min_open_vs_ma20_pct", cls.min_open_vs_ma20_pct),
            limit_up_trigger_pct=_get_float("limit_up_trigger_pct", cls.limit_up_trigger_pct),

            max_entry_vs_ma5_pct=_get_float("max_entry_vs_ma5_pct", cls.max_entry_vs_ma5_pct),
            expire_atr_mult=_get_float("expire_atr_mult", cls.expire_atr_mult),
            expire_pct_threshold=_get_float("expire_pct_threshold", cls.expire_pct_threshold),
            stop_atr_mult=_get_float("stop_atr_mult", cls.stop_atr_mult),
            signal_day_limit_up_pct=_get_float("signal_day_limit_up_pct", cls.signal_day_limit_up_pct),

            write_to_db=_get_bool("write_to_db", cls.write_to_db),
            export_csv=_get_bool("export_csv", cls.export_csv),
            export_top_n=_get_int("export_top_n", cls.export_top_n),
            output_subdir=str(sec.get("output_subdir", cls.output_subdir)).strip() or cls.output_subdir,
        )


class MA5MA20OpenMonitorRunner:
    """开盘监测 Runner：读取前一交易日 BUY 信号 → 拉实时行情 → 输出可执行清单。"""

    def __init__(self) -> None:
        self.logger = setup_logger()
        self.params = OpenMonitorParams.from_config()
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())
        self.volume_ratio_threshold = self._resolve_volume_ratio_threshold()
        app_cfg = get_section("app") or {}
        self.index_codes = []
        if isinstance(app_cfg, dict):
            codes = app_cfg.get("index_codes", [])
            if isinstance(codes, (list, tuple)):
                self.index_codes = [str(c).strip() for c in codes if str(c).strip()]
        ak_cfg = get_section("akshare") or {}
        board_cfg = ak_cfg.get("board_industry", {}) if isinstance(ak_cfg, dict) else {}
        if not isinstance(board_cfg, dict):
            board_cfg = {}
        self.board_env_enabled = bool(board_cfg.get("enabled", False))
        self.board_spot_enabled = bool(board_cfg.get("spot_enabled", True))
        self.board_strength_history_days = board_cfg.get("history_days", 200)
        om_cfg = get_section("open_monitor") or {}
        threshold = _to_float(om_cfg.get("env_index_score_threshold"))
        self.env_index_score_threshold = threshold if threshold is not None else 2.0

    def _resolve_volume_ratio_threshold(self) -> float:
        strat = get_section("strategy_ma5_ma20_trend") or {}
        if isinstance(strat, dict):
            raw = strat.get("volume_ratio_threshold")
            parsed = _to_float(raw)
            if parsed is not None and parsed > 0:
                return float(parsed)
        return 1.5

    # -------------------------
    # DB helpers
    # -------------------------
    def _table_exists(self, table: str) -> bool:
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(text("SHOW TABLES LIKE :t"), conn, params={"t": table})
            return not df.empty
        except Exception:
            return False

    def _daily_table(self) -> str:
        """获取日线数据表名（用于补充计算“信号日涨幅”等信息）。"""

        strat = get_section("strategy_ma5_ma20_trend") or {}
        if isinstance(strat, dict):
            name = str(strat.get("daily_table") or "").strip()
            if name:
                return name
        return "history_daily_kline"

    def _load_signal_day_pct_change(self, signal_date: str, codes: List[str]) -> Dict[str, float]:
        """补充“信号日涨幅”（close vs 前一交易日 close）。

        用途：识别“信号日接近涨停/情绪极端”的场景，避免次日开盘追高。
        """

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
            with self.db_writer.engine.begin() as conn:
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

    def _is_trading_day(self, date_str: str, latest_trade_date: str | None = None) -> bool:
        """粗略判断是否为交易日（优先用日线表，其次用工作日）。"""

        try:
            d = dt.datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
        except Exception:  # noqa: BLE001
            return False

        if d.weekday() >= 5:
            return False

        # 盘中/当日：日线表大概率还没落库，直接按工作日视为交易日（节假日误跑属低频场景）
        if latest_trade_date and str(date_str)[:10] > str(latest_trade_date)[:10]:
            return True

        daily = self._daily_table()
        if not self._table_exists(daily):
            return True

        stmt = text(f"SELECT 1 FROM `{daily}` WHERE `date` = :d LIMIT 1")
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": str(d)})
            return not df.empty
        except Exception:  # noqa: BLE001
            return True

    def _load_trade_age_map(
        self, latest_trade_date: str, min_date: str, monitor_date: str | None
    ) -> Dict[str, int]:
        """返回 {date_str: trading_day_age}，0 表示监控基准日。"""

        base_date = latest_trade_date
        monitor_str = str(monitor_date or "").strip()
        if monitor_str and monitor_str > latest_trade_date and self._is_trading_day(monitor_str, latest_trade_date):
            base_date = monitor_str

        daily = self._daily_table()
        if not self._table_exists(daily):
            # 兜底：没有日线表时，只能用 monitor_str 作为 age=0 的基准
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
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"base_date": base_date, "min_date": min_date})
        except Exception:
            df = None

        dates = df["d"].dropna().astype(str).str[:10].tolist() if df is not None else []
        # 只在“确认为交易日/工作日盘中”时插入，避免周末误跑把周末插进去
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

    def _load_recent_buy_signals(self) -> Tuple[str | None, List[str], pd.DataFrame]:
        table = self.params.signals_table
        monitor_date = dt.date.today().isoformat()
        lookback = max(int(self.params.signal_lookback_days or 0), 1)

        try:
            with self.db_writer.engine.begin() as conn:
                max_df = pd.read_sql_query(
                    text(f"SELECT MAX(`date`) AS max_date FROM `{table}`"),
                    conn,
                )
                dates_df = pd.read_sql_query(
                    text(
                        f"""
                        SELECT DISTINCT `date`
                        FROM `{table}`
                        WHERE `signal` = 'BUY'
                        ORDER BY `date` DESC
                        LIMIT :n
                        """
                    ),
                    conn,
                    params={"n": lookback},
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.error("读取 signals_table=%s 失败：%s", table, exc)
            return None, [], pd.DataFrame()

        if max_df.empty:
            return None, [], pd.DataFrame()

        max_date = max_df.iloc[0].get("max_date")
        if pd.isna(max_date) or not str(max_date).strip():
            return None, [], pd.DataFrame()

        latest_trade_date = str(max_date)[:10]
        if dates_df.empty:
            self.logger.info("%s 没有任何 BUY 信号，跳过开盘监测。", latest_trade_date)
            return latest_trade_date, [], pd.DataFrame()

        signal_dates = [str(v)[:10] for v in dates_df["date"].tolist() if str(v).strip()]
        self.logger.info(
            "回看最近 %s 个交易日（最新=%s）BUY 信号：%s", lookback, latest_trade_date, signal_dates
        )

        stmt = text(
            f"""
            SELECT
              `date`,`code`,`close`,
              `ma5`,`ma20`,`ma60`,`ma250`,
              `vol_ratio`,`macd_hist`,`kdj_k`,`kdj_d`,`atr14`,`stop_ref`,
              `signal`,`reason`
            FROM `{table}`
            WHERE `date` IN :dates AND `signal` = 'BUY'
            """
        ).bindparams(bindparam("dates", expanding=True))

        with self.db_writer.engine.begin() as conn:
            try:
                df = pd.read_sql_query(stmt, conn, params={"dates": signal_dates})
            except Exception as exc:  # noqa: BLE001
                self.logger.error("读取 %s BUY 信号失败：%s", table, exc)
                return latest_trade_date, signal_dates, pd.DataFrame()

        if df.empty:
            self.logger.info("%s 内无 BUY 信号，跳过开盘监测。", signal_dates)
            return latest_trade_date, signal_dates, df

        df["code"] = df["code"].astype(str)
        df["date_str"] = df["date"].astype(str).str[:10]
        min_date = df["date_str"].min()
        trade_age_map = self._load_trade_age_map(latest_trade_date, str(min_date), monitor_date)
        df["signal_age"] = df["date_str"].map(trade_age_map)

        try:
            for d in signal_dates:
                codes = df.loc[df["date_str"] == d, "code"].dropna().unique().tolist()
                pct_map = self._load_signal_day_pct_change(d, codes)
                mask = df["date_str"] == d
                df.loc[mask, "_signal_day_pct_change"] = df.loc[mask, "code"].map(pct_map)
        except Exception:
            df["_signal_day_pct_change"] = None
        return latest_trade_date, signal_dates, df

    def _load_latest_snapshots(self, latest_trade_date: str, codes: List[str]) -> pd.DataFrame:
        if not latest_trade_date or not codes:
            return pd.DataFrame()

        table = self.params.signals_table
        stmt = text(
            f"""
            SELECT
              `date`,`code`,`close`,`ma5`,`ma20`,`ma60`,`ma250`,
              `vol_ratio`,`macd_hist`,`atr14`,`stop_ref`
            FROM `{table}`
            WHERE `date` = :d AND `code` IN :codes
            """
        ).bindparams(bindparam("codes", expanding=True))

        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn, params={"d": latest_trade_date, "codes": codes})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取最新指标失败，将跳过最新快照：%s", exc)
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        df["code"] = df["code"].astype(str)
        return df

    def _load_stock_industry_dim(self) -> pd.DataFrame:
        candidates = ["dim_stock_industry", "a_share_stock_industry"]
        for table in candidates:
            if not self._table_exists(table):
                continue
            try:
                with self.db_writer.engine.begin() as conn:
                    df = pd.read_sql_query(text(f"SELECT * FROM `{table}`"), conn)
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("读取 %s 失败：%s", table, exc)
                continue
            if not df.empty:
                df["code"] = df["code"].astype(str)
                return df
        return pd.DataFrame()

    def _load_board_constituent_dim(self) -> pd.DataFrame:
        table = "dim_stock_board_industry"
        if not self._table_exists(table):
            return pd.DataFrame()
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(text(f"SELECT * FROM `{table}`"), conn)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取 %s 失败：%s", table, exc)
            return pd.DataFrame()
        if not df.empty and "code" in df.columns:
            df["code"] = df["code"].astype(str)
        return df

    def _load_board_spot_strength(self) -> pd.DataFrame:
        if not (self.board_env_enabled and self.board_spot_enabled):
            return pd.DataFrame()
        if not self._table_exists("board_industry_spot"):
            return pd.DataFrame()

        stmt = text(
            """
            SELECT *
            FROM board_industry_spot
            WHERE ts = (SELECT MAX(ts) FROM board_industry_spot)
            """
        )
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(stmt, conn)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取 board_industry_spot 失败：%s", exc)
            return pd.DataFrame()

        if df.empty:
            return df

        rename_map = {}
        for col in df.columns:
            if "板块" in col and "名称" in col:
                rename_map[col] = "board_name"
            if "涨跌幅" in col or col in {"chg_pct", "涨跌幅(%)"}:
                rename_map[col] = "chg_pct"
            if "代码" in col:
                rename_map[col] = "board_code"
        df = df.rename(columns=rename_map)
        if "chg_pct" in df.columns:
            df["chg_pct"] = pd.to_numeric(df["chg_pct"], errors="coerce")
            df = df.sort_values(by="chg_pct", ascending=False).reset_index(drop=True)
            df["rank"] = df.index + 1
        if "board_code" in df.columns:
            df["board_code"] = df["board_code"].astype(str)
        return df

    def _load_index_trend(self, latest_trade_date: str) -> dict[str, Any]:
        if not self.index_codes or not self._table_exists("history_index_daily_kline"):
            return {"score": None, "detail": {}}

        stmt = text(
            """
            SELECT *
            FROM history_index_daily_kline
            WHERE `code` IN :codes AND `date` <= :d
            ORDER BY `code`, `date`
            """
        ).bindparams(bindparam("codes", expanding=True))
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(
                    stmt, conn, params={"codes": self.index_codes, "d": latest_trade_date}
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("读取指数日线失败：%s", exc)
            return {"score": None, "detail": {}}

        if df.empty:
            return {"score": None, "detail": {}}

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        numeric_cols = ["close", "ma20", "ma60", "ma250"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        detail: dict[str, dict[str, Any]] = {}
        for code, group in df.groupby("code"):
            grp = group.sort_values("date")
            grp["ma20"] = grp["close"].rolling(20, min_periods=1).mean()
            grp["ma60"] = grp["close"].rolling(60, min_periods=1).mean()
            grp["ma250"] = grp["close"].rolling(250, min_periods=1).mean()
            latest = grp.iloc[-1]
            score = 0
            for ma in ["ma20", "ma60", "ma250"]:
                ma_val = latest.get(ma)
                close_val = latest.get("close")
                if ma_val is not None and close_val is not None and close_val > ma_val:
                    score += 1
            detail[code] = {
                "close": latest.get("close"),
                "ma20": latest.get("ma20"),
                "ma60": latest.get("ma60"),
                "ma250": latest.get("ma250"),
                "score": score,
            }

        if not detail:
            return {"score": None, "detail": {}}

        avg_score = sum(v["score"] for v in detail.values()) / len(detail)
        return {"score": avg_score, "detail": detail}

    def _build_environment_context(self, latest_trade_date: str) -> dict[str, Any]:
        index_trend = self._load_index_trend(latest_trade_date)
        board_strength = self._load_board_spot_strength()
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

        return {"index": index_trend, "boards": board_map}

    # -------------------------
    # Quote fetch
    # -------------------------
    def _fetch_quotes(self, codes: List[str]) -> pd.DataFrame:
        """获取实时行情。

        路线A：默认直接走东财（eastmoney）以避免 AkShare 全市场实时接口不稳定。
        - quote_source=eastmoney/auto：直接东财
        - quote_source=akshare：仅在显式指定时才调用 AkShare
        """

        source = (self.params.quote_source or "eastmoney").strip().lower()
        if source == "akshare":
            return self._fetch_quotes_akshare(codes)
        # 兼容：auto 视为 eastmoney
        return self._fetch_quotes_eastmoney(codes)

    def _fetch_quotes_akshare(self, codes: List[str]) -> pd.DataFrame:
        try:
            import akshare as ak  # type: ignore
        except Exception as exc:  # noqa: BLE001
            self.logger.info("AkShare 不可用（将回退）：%s", exc)
            return pd.DataFrame()

        digits = {_strip_baostock_prefix(c) for c in codes}
        try:
            spot = ak.stock_zh_a_spot_em()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("AkShare 行情拉取失败（将回退）：%s", exc)
            return pd.DataFrame()

        if spot is None or getattr(spot, "empty", True):
            return pd.DataFrame()

        rename_map = {
            "代码": "symbol",
            "名称": "name",
            "最新价": "latest",
            "涨跌幅": "pct_change",
            "今开": "open",
            "昨收": "prev_close",
        }
        for k in list(rename_map.keys()):
            if k not in spot.columns:
                rename_map.pop(k, None)

        spot = spot.rename(columns=rename_map)
        if "symbol" not in spot.columns:
            return pd.DataFrame()

        spot["symbol"] = spot["symbol"].astype(str)
        spot = spot[spot["symbol"].isin(digits)].copy()
        if spot.empty:
            return pd.DataFrame()

        out = pd.DataFrame()
        out["code"] = spot["symbol"].apply(lambda x: _to_baostock_code("auto", str(x)))
        out["symbol"] = spot["symbol"].astype(str)
        out["name"] = spot.get("name", pd.Series([""] * len(spot))).astype(str)
        out["open"] = spot.get("open", pd.Series([None] * len(spot))).apply(_to_float)
        out["latest"] = spot.get("latest", pd.Series([None] * len(spot))).apply(_to_float)
        out["prev_close"] = spot.get("prev_close", pd.Series([None] * len(spot))).apply(_to_float)
        out["pct_change"] = spot.get("pct_change", pd.Series([None] * len(spot))).apply(_to_float)

        mapping = {_strip_baostock_prefix(c): c for c in codes}
        out["code"] = out["symbol"].map(mapping).fillna(out["code"])
        return out.reset_index(drop=True)

    def _urlopen_json_no_proxy(self, url: str, *, timeout: int = 10, retries: int = 2) -> Dict[str, Any]:
        """访问东财接口并返回 JSON（默认不使用环境代理）。

        说明：urllib 默认会读取环境变量代理；这里强制 ProxyHandler({})，避免被 HTTP(S)_PROXY 影响。
        """

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://quote.eastmoney.com/",
            "Connection": "close",
        }
        req = urllib.request.Request(url, headers=headers)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        last_exc: Exception | None = None
        for i in range(retries + 1):
            try:
                with opener.open(req, timeout=timeout) as resp:
                    raw = resp.read().decode("utf-8", errors="ignore")
                return json.loads(raw) if raw else {}
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if i < retries:
                    time.sleep(0.5 * (2**i))
                    continue
                raise

    def _fetch_quotes_eastmoney(self, codes: List[str]) -> pd.DataFrame:
        if not codes:
            return pd.DataFrame()

        base_url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
        fields = "f2,f3,f12,f14,f17,f18"
        secids = [_to_eastmoney_secid(c) for c in codes]

        batch_size = 80
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(secids), batch_size):
            part = secids[i : i + batch_size]
            query = {
                "fltt": "2",
                "invt": "2",
                "fields": fields,
                "secids": ",".join(part),
            }
            url = f"{base_url}?{urllib.parse.urlencode(query)}"
            try:
                payload = self._urlopen_json_no_proxy(url, timeout=10, retries=2)
            except Exception as exc:  # noqa: BLE001
                self.logger.error("Eastmoney 行情请求失败：%s", exc)
                continue

            data = (payload or {}).get("data") or {}
            diff = data.get("diff") or []
            if isinstance(diff, list):
                rows.extend([r for r in diff if isinstance(r, dict)])

        if not rows:
            return pd.DataFrame()

        out_rows: List[Dict[str, Any]] = []
        mapping = {_strip_baostock_prefix(c): c for c in codes}
        for r in rows:
            symbol = str(r.get("f12") or "").strip()
            name = str(r.get("f14") or "").strip()
            latest = _to_float(r.get("f2"))
            pct = _to_float(r.get("f3"))
            open_px = _to_float(r.get("f17"))
            prev_close = _to_float(r.get("f18"))

            code_guess = _to_baostock_code("auto", symbol)
            code = mapping.get(symbol, code_guess)

            out_rows.append(
                {
                    "code": code,
                    "symbol": symbol,
                    "name": name,
                    "open": open_px,
                    "latest": latest,
                    "prev_close": prev_close,
                    "pct_change": pct,
                }
            )

        return pd.DataFrame(out_rows)

    # -------------------------
    # Evaluate
    # -------------------------
    def _is_pullback_signal(self, signal_reason: str) -> bool:
        reason_text = str(signal_reason or "")
        lower = reason_text.lower()
        return ("回踩" in reason_text) and (("ma20" in lower) or ("ma 20" in lower))

    def _evaluate(
        self,
        signals: pd.DataFrame,
        quotes: pd.DataFrame,
        latest_snapshots: pd.DataFrame,
        latest_trade_date: str,
        env_context: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        if signals.empty:
            return pd.DataFrame()

        q = quotes.copy()
        if q.empty:
            out = signals.copy()
            out["monitor_date"] = dt.date.today().isoformat()
            out["open"] = None
            out["latest"] = None
            out["pct_change"] = None
            out["gap_pct"] = None
            out["action"] = "UNKNOWN"
            out["action_reason"] = "行情数据不可用"
            out["candidate_status"] = "UNKNOWN"
            out["status_reason"] = "行情数据不可用"
            out["checked_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return out

        q["code"] = q["code"].astype(str)
        merged = signals.merge(q, on="code", how="left", suffixes=("", "_q"))

        env_context = env_context or {}
        industry_dim = self._load_stock_industry_dim()
        if not industry_dim.empty:
            rename_map = {}
            if "industryClassification" in industry_dim.columns:
                rename_map["industryClassification"] = "industry_classification"
            industry_dim = industry_dim.rename(columns=rename_map)
            if "industry" not in industry_dim.columns and "industry_classification" in industry_dim.columns:
                industry_dim["industry"] = industry_dim["industry_classification"]
            merged = merged.merge(
                industry_dim[[c for c in ["code", "industry", "industry_classification"] if c in industry_dim.columns]],
                on="code",
                how="left",
            )
        board_dim = self._load_board_constituent_dim()
        if not board_dim.empty:
            cols = [c for c in ["code", "board_name", "board_code"] if c in board_dim.columns]
            if cols:
                deduped = board_dim.drop_duplicates(subset=["code"], keep="first")
                merged = merged.merge(deduped[cols], on="code", how="left")

        board_map: dict[str, Any] = env_context.get("boards", {}) if isinstance(env_context, dict) else {}
        index_score = None
        if isinstance(env_context, dict):
            index_section = env_context.get("index", {}) if isinstance(env_context.get("index"), dict) else {}
            index_score = index_section.get("score")

        if not latest_snapshots.empty:
            snap = latest_snapshots.copy()
            rename_map = {c: f"latest_{c}" for c in snap.columns if c not in {"code"}}
            snap = snap.rename(columns=rename_map)
            merged = merged.merge(snap, on="code", how="left")

        float_cols = [
            "close",
            "prev_close",
            "ma5",
            "ma20",
            "ma60",
            "ma250",
            "vol_ratio",
            "macd_hist",
            "atr14",
            "stop_ref",
            "open",
            "latest",
            "pct_change",
            "_signal_day_pct_change",
            "latest_close",
            "latest_ma5",
            "latest_ma20",
            "latest_ma60",
            "latest_ma250",
            "latest_vol_ratio",
            "latest_macd_hist",
            "latest_atr14",
            "latest_stop_ref",
        ]
        for col in float_cols:
            if col in merged.columns:
                merged[col] = merged.get(col).apply(_to_float)

        def _calc_gap(row: pd.Series) -> float | None:
            ref_close = row.get("prev_close")
            if ref_close is None:
                ref_close = row.get("close")

            px = row.get("open")
            if px is None or px <= 0:
                px = row.get("latest")

            if ref_close is None or px is None:
                return None
            if ref_close <= 0 or px <= 0:
                return None
            return (px - ref_close) / ref_close

        merged["gap_pct"] = merged.apply(_calc_gap, axis=1)

        max_up = self.params.max_gap_up_pct
        max_up_atr_mult = self.params.max_gap_up_atr_mult
        max_down = self.params.max_gap_down_pct
        min_vs_ma20 = self.params.min_open_vs_ma20_pct
        limit_up_trigger = self.params.limit_up_trigger_pct

        max_entry_vs_ma5 = self.params.max_entry_vs_ma5_pct
        stop_atr_mult = self.params.stop_atr_mult
        signal_day_limit_up = self.params.signal_day_limit_up_pct

        expire_atr_mult = self.params.expire_atr_mult
        expire_pct_threshold = self.params.expire_pct_threshold
        cross_valid_days = self.params.cross_valid_days
        pullback_valid_days = self.params.pullback_valid_days
        vol_threshold = self.volume_ratio_threshold

        actions: List[str] = []
        reasons: List[str] = []
        statuses: List[str] = []
        status_reasons: List[str] = []
        stop_refs: List[float | None] = []
        signal_stop_refs: List[float | None] = []
        valid_days_list: List[int | None] = []
        env_index_scores: List[float | None] = []
        board_statuses: List[str | None] = []
        board_ranks: List[float | None] = []
        board_chg_pcts: List[float | None] = []

        def _prefer_latest(row: pd.Series, key: str) -> float | None:
            latest_val = row.get(f"latest_{key}")
            if latest_val is not None and not pd.isna(latest_val):
                return latest_val
            val = row.get(key)
            return None if pd.isna(val) else val

        for _, row in merged.iterrows():
            action = "EXECUTE"
            reason = "OK"

            open_px = row.get("open")
            latest_px = row.get("latest")
            entry_px = open_px
            ma20 = _prefer_latest(row, "ma20")
            ma5 = _prefer_latest(row, "ma5")
            ma60 = _prefer_latest(row, "ma60")
            ma250 = _prefer_latest(row, "ma250")
            vol_ratio = _prefer_latest(row, "vol_ratio")
            macd_hist = _prefer_latest(row, "macd_hist")
            atr14 = _prefer_latest(row, "atr14")

            signal_stop_ref = _prefer_latest(row, "stop_ref")
            signal_close = row.get("close")
            current_close = _prefer_latest(row, "close")
            ref_close = row.get("prev_close")
            if ref_close is None:
                ref_close = signal_close

            gap = row.get("gap_pct")
            pct = row.get("pct_change")
            signal_day_pct = row.get("_signal_day_pct_change")
            signal_reason = str(row.get("reason") or "")
            signal_age = row.get("signal_age")

            used_latest_as_entry = False
            if entry_px is None or entry_px <= 0:
                entry_px = latest_px
                used_latest_as_entry = True
                if entry_px is None or entry_px <= 0:
                    action = "UNKNOWN"
                    reason = "无今开/最新价"
                else:
                    reason = "用最新价替代今开"

            stop_ref = signal_stop_ref
            if entry_px is not None and atr14 is not None and atr14 > 0 and stop_atr_mult > 0:
                stop_ref = entry_px - stop_atr_mult * atr14

            def _current_price() -> float | None:
                px = latest_px
                if px is None or px <= 0:
                    px = open_px
                if px is None or px <= 0:
                    px = current_close
                return px

            price_now = _current_price()

            valid_days = pullback_valid_days if self._is_pullback_signal(signal_reason) else cross_valid_days

            status = "ACTIVE"
            status_reason = "健康满足"

            if ma5 is not None and ma20 is not None and ma5 < ma20:
                status = "INVALID"
                status_reason = "MA5 下穿 MA20（死叉）"
            elif (
                price_now is not None
                and ma20 is not None
                and vol_ratio is not None
                and vol_ratio >= vol_threshold
                and price_now < ma20
            ):
                status = "INVALID"
                status_reason = "价格跌破 MA20 且前一交易日放量"
            elif (
                price_now is not None
                and signal_stop_ref is not None
                and price_now < signal_stop_ref
            ):
                status = "INVALID"
                status_reason = "跌破 ATR 止损参考价"
            elif valid_days > 0 and signal_age is not None and signal_age >= valid_days:
                status = "EXPIRED"
                status_reason = f"超过有效期 {valid_days} 个交易日"
            elif (
                price_now is not None
                and ma5 is not None
                and max_entry_vs_ma5 > 0
                and price_now > ma5 * (1.0 + max_entry_vs_ma5)
            ):
                status = "EXPIRED"
                status_reason = "入场价/最新价相对 MA5 乖离过大"
            elif price_now is not None and signal_close is not None and price_now > signal_close:
                gain = (price_now - signal_close) / signal_close
                atr_base = atr14 if atr14 is not None else row.get("atr14")
                atr_gain = None
                if atr_base is not None and atr_base > 0:
                    atr_gain = (price_now - signal_close) / atr_base
                if atr_gain is not None and atr_gain > expire_atr_mult:
                    status = "EXPIRED"
                    status_reason = f"信号后拉升超过 ATR×{expire_atr_mult:.2f}"
                elif gain > expire_pct_threshold:
                    status = "EXPIRED"
                    status_reason = f"信号后涨幅 {gain*100:.2f}% 超过阈值"
            else:
                trend_ok = (
                    current_close is not None
                    and ma60 is not None
                    and ma250 is not None
                    and ma20 is not None
                    and current_close > ma60
                    and current_close > ma250
                    and ma20 > ma60 > ma250
                )
                macd_ok = macd_hist is not None and macd_hist > 0
                vol_ok = vol_ratio is not None and vol_ratio >= vol_threshold

                if not trend_ok:
                    status = "INVALID"
                    status_reason = "多头排列/趋势破坏"
                elif (not macd_ok) or (not vol_ok):
                    status = "WAIT"
                    weak_reasons = []
                    if not macd_ok:
                        weak_reasons.append("MACD 动能转弱")
                    if not vol_ok:
                        weak_reasons.append("量能不足")
                    status_reason = "；".join(weak_reasons) if weak_reasons else "继续观察"

            if status in {"INVALID", "EXPIRED"}:
                action = "SKIP"
                reason = status_reason
            elif status == "WAIT":
                action = "WAIT"
                reason = status_reason

            if action == "EXECUTE" and signal_day_pct is not None and signal_day_pct >= signal_day_limit_up:
                action = "SKIP"
                reason = f"信号日涨幅 {signal_day_pct*100:.2f}% 接近涨停，次日不追"

            if action == "EXECUTE" and pct is not None and pct >= limit_up_trigger:
                action = "SKIP"
                reason = f"涨幅 {pct:.2f}% 接近/达到涨停"

            if action == "EXECUTE" and gap is not None and gap > 0:
                gap_up_threshold = max_up
                atr_based = None
                if ref_close is not None and ref_close > 0 and atr14 is not None and atr14 > 0 and max_up_atr_mult > 0:
                    atr_based = max_up_atr_mult * atr14 / ref_close
                    if atr_based > 0:
                        gap_up_threshold = min(max_up, atr_based)

                if gap > gap_up_threshold:
                    action = "SKIP"
                    if atr_based is None:
                        reason = f"高开 {gap*100:.2f}% 超过阈值 {gap_up_threshold*100:.2f}%"
                    else:
                        reason = (
                            f"高开 {gap*100:.2f}% 超过阈值 {gap_up_threshold*100:.2f}%"
                            f"（min(固定{max_up*100:.2f}%, ATR×{max_up_atr_mult:.1f}={atr_based*100:.2f}% )）"
                        )

            if action == "EXECUTE" and gap is not None and gap < max_down:
                action = "SKIP"
                reason = f"低开 {gap*100:.2f}% 低于阈值 {max_down*100:.2f}%"

            if action == "EXECUTE" and (ma20 is not None) and (entry_px is not None):
                threshold = ma20 * (1.0 + min_vs_ma20)
                if entry_px < threshold:
                    action = "SKIP"
                    px_label = "入场价(最新)" if used_latest_as_entry else "入场价"
                    reason = f"{px_label} {entry_px:.2f} 跌破 MA20 阈值 {threshold:.2f}"

            if action == "EXECUTE" and (ma5 is not None) and (entry_px is not None) and (max_entry_vs_ma5 > 0):
                threshold_ma5 = ma5 * (1.0 + max_entry_vs_ma5)
                if entry_px > threshold_ma5:
                    action = "SKIP"
                    px_label = "入场价(最新)" if used_latest_as_entry else "入场价"
                    reason = (
                        f"{px_label} {entry_px:.2f} 高于 MA5 阈值 {threshold_ma5:.2f}"
                        f"（>{max_entry_vs_ma5*100:.2f}%）"
                    )

            board_candidates: list[str] = []
            for key in [
                row.get("board_name"),
                row.get("board_code"),
                row.get("industry"),
                row.get("industry_classification"),
            ]:
                if pd.notna(key):
                    key_norm = str(key).strip()
                    if key_norm and key_norm.lower() != "nan":
                        board_candidates.append(key_norm)
            board_info = None
            board_status = None
            board_rank = None
            board_chg = None
            for key in board_candidates:
                board_info = board_map.get(key)
                if board_info:
                    break
            if isinstance(board_info, dict):
                board_status = board_info.get("status")
                board_rank = _to_float(board_info.get("rank"))
                board_chg = _to_float(board_info.get("chg_pct"))

            if (
                index_score is not None
                and action == "EXECUTE"
                and index_score < self.env_index_score_threshold
            ):
                action = "WAIT"
                reason = (
                    f"大盘趋势分数 {index_score:.1f} 低于阈值"
                    f" {self.env_index_score_threshold:.1f}，建议观望/减仓"
                )

            if board_status == "weak" and action == "EXECUTE":
                action = "WAIT"
                reason = "所属板块走势偏弱，建议降低优先级"
            elif board_status == "strong" and action == "WAIT" and action != "SKIP":
                rank_display = f"{board_rank:.0f}" if board_rank is not None else "-"
                reason = f"板块强势(rank={rank_display})，等待入场条件"

            env_index_scores.append(_to_float(index_score))
            board_statuses.append(board_status)
            board_ranks.append(board_rank)
            board_chg_pcts.append(board_chg)

            actions.append(action)
            reasons.append(reason)
            statuses.append(status)
            status_reasons.append(status_reason)
            stop_refs.append(_to_float(stop_ref))
            signal_stop_refs.append(_to_float(signal_stop_ref))
            valid_days_list.append(valid_days)

        merged["monitor_date"] = dt.date.today().isoformat()
        merged["latest_trade_date"] = latest_trade_date
        merged["action"] = actions
        merged["action_reason"] = reasons
        merged["candidate_status"] = statuses
        merged["status_reason"] = status_reasons
        merged["stop_ref"] = stop_refs
        merged["signal_stop_ref"] = signal_stop_refs
        merged["valid_days"] = valid_days_list
        merged["checked_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        merged["env_index_score"] = env_index_scores
        merged["board_status"] = board_statuses
        merged["board_rank"] = board_ranks
        merged["board_chg_pct"] = board_chg_pcts

        keep_cols = [
            "monitor_date",
            "latest_trade_date",
            "date",
            "signal_age",
            "valid_days",
            "code",
            "name",
            "close",
            "open",
            "latest",
            "pct_change",
            "gap_pct",
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
            "signal_stop_ref",
            "env_index_score",
            "industry",
            "board_name",
            "board_code",
            "board_status",
            "board_rank",
            "board_chg_pct",
            "signal",
            "reason",
            "candidate_status",
            "status_reason",
            "action",
            "action_reason",
            "checked_at",
        ]
        for col in keep_cols:
            if col not in merged.columns:
                merged[col] = None

        return merged[keep_cols].copy()

    # -------------------------
    # Persist & export
    # -------------------------
    def _persist_results(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        table = self.params.output_table
        if not self.params.write_to_db:
            return

        monitor_date = str(df.iloc[0].get("monitor_date") or "").strip()
        codes = df["code"].dropna().astype(str).unique().tolist()

        if monitor_date and codes and self._table_exists(table):
            delete_stmt = text(
                "DELETE FROM `{table}` WHERE `monitor_date` = :d AND `code` IN :codes".format(
                    table=table
                )
            ).bindparams(bindparam("codes", expanding=True))
            try:
                with self.db_writer.engine.begin() as conn:
                    conn.execute(delete_stmt, {"d": monitor_date, "codes": codes})
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("开盘监测表去重删除失败，将直接追加：%s", exc)

        try:
            self.db_writer.write_dataframe(df, table, if_exists="append")
            self.logger.info("开盘监测结果已写入表 %s：%s 条", table, len(df))
        except Exception as exc:  # noqa: BLE001
            self.logger.error("写入开盘监测表失败：%s", exc)

    def _export_csv(self, df: pd.DataFrame) -> None:
        if df.empty or (not self.params.export_csv):
            return

        app_sec = get_section("app") or {}
        base_dir = "output"
        if isinstance(app_sec, dict):
            base_dir = str(app_sec.get("output_dir", base_dir))

        outdir = Path(base_dir) / self.params.output_subdir
        outdir.mkdir(parents=True, exist_ok=True)

        monitor_date = str(df.iloc[0].get("monitor_date") or dt.date.today().isoformat())
        path = outdir / f"open_monitor_{monitor_date}.csv"

        export_df = df.copy()
        export_df["gap_pct"] = export_df["gap_pct"].apply(_to_float)
        # CSV 里把“状态正常且可执行”放在最前面，方便你开盘快速扫一眼
        action_rank = {"EXECUTE": 0, "WAIT": 1, "SKIP": 2, "UNKNOWN": 3}
        status_rank = {"ACTIVE": 0, "WAIT": 1, "EXPIRED": 2, "INVALID": 3, "UNKNOWN": 4}
        export_df["_action_rank"] = export_df["action"].map(action_rank).fillna(99)
        export_df["_status_rank"] = export_df["candidate_status"].map(status_rank).fillna(99)
        export_df = export_df.sort_values(
            by=["_status_rank", "_action_rank", "gap_pct"], ascending=[True, True, True]
        )
        export_df = export_df.drop(columns=["_action_rank", "_status_rank"], errors="ignore")
        if self.params.export_top_n > 0:
            export_df = export_df.head(self.params.export_top_n)

        export_df.to_csv(path, index=False, encoding="utf-8-sig")
        self.logger.info("开盘监测 CSV 已导出：%s", path)

    # -------------------------
    # Public run
    # -------------------------
    def run(self, *, force: bool = False) -> None:
        """执行开盘监测。

        - 默认遵循 config.yaml: open_monitor.enabled。
        - 当 force=True 时，即便 enabled=false 也会执行（用于单独运行脚本）。
        """

        if (not force) and (not self.params.enabled):
            self.logger.info("open_monitor.enabled=false，跳过开盘监测。")
            return

        if force and (not self.params.enabled):
            self.logger.info("open_monitor.enabled=false，但 force=True，仍将执行开盘监测。")

        latest_trade_date, signal_dates, signals = self._load_recent_buy_signals()
        if not latest_trade_date or signals.empty:
            return

        codes = signals["code"].dropna().astype(str).unique().tolist()
        self.logger.info("待监测标的数量：%s（信号日：%s）", len(codes), signal_dates)

        quotes = self._fetch_quotes(codes)
        if quotes.empty:
            self.logger.warning("未获取到任何实时行情，将输出 UNKNOWN 结果。")
        else:
            self.logger.info("实时行情已获取：%s 条", len(quotes))

        latest_snapshots = self._load_latest_snapshots(latest_trade_date, codes)
        env_context = self._build_environment_context(latest_trade_date)
        result = self._evaluate(
            signals, quotes, latest_snapshots, latest_trade_date, env_context
        )
        if result.empty:
            return

        summary = result["action"].value_counts(dropna=False).to_dict()
        self.logger.info("开盘监测结果统计：%s", summary)

        exec_df = result[result["action"] == "EXECUTE"].copy()
        exec_df["gap_pct"] = exec_df["gap_pct"].apply(_to_float)
        exec_df = exec_df.sort_values(by="gap_pct", ascending=True)
        top_n = min(30, len(exec_df))
        if top_n > 0:
            preview = exec_df[
                ["code", "name", "close", "open", "latest", "gap_pct", "action_reason"]
            ].head(top_n)
            self.logger.info(
                "可执行清单 Top%s（按 gap 由小到大）：\n%s",
                top_n,
                preview.to_string(index=False),
            )

        wait_df = result[result["action"] == "WAIT"].copy()
        wait_df["gap_pct"] = wait_df["gap_pct"].apply(_to_float)
        wait_df = wait_df.sort_values(by="gap_pct", ascending=True)
        wait_top = min(10, len(wait_df))
        if wait_top > 0:
            wait_preview = wait_df[
                ["code", "name", "close", "open", "latest", "gap_pct", "status_reason"]
            ].head(wait_top)
            self.logger.info(
                "WAIT 观察清单 Top%s（按 gap 由小到大）：\n%s",
                wait_top,
                wait_preview.to_string(index=False),
            )

        self._persist_results(result)
        self._export_csv(result)
