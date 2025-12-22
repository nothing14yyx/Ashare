"""MA5-MA20 顺势趋势波段系统

把“顺势 + MA5/MA20 触发 + 量价/指标过滤 + 风险第一 + 低频交易”落到你的程序里。

数据依赖：
  history_daily_kline（全量日线表；运行时按日期窗口截取，避免依赖 history_recent_xxx_days VIEW）

输出：
  - strategy_indicator_daily：指标明细（按信号计算窗口写入）
  - strategy_signal_events：信号事件（signal/reason/risk/stop 等）
      - signals_write_scope=latest：仅写入最新交易日（默认）
      - signals_write_scope=window：写入本次计算窗口内的全部交易日（用于回填历史/回测）
  - 默认通过 VIEW 列出全部 BUY 信号（历史）
    （v_strategy_signal_candidates；如需物理表可关闭 candidates_as_view）

说明：
  - 本实现先做“日线低频版本”作为选股/清单层。
  - 若要更严格的“分钟线执行层”（例如 60 分钟入场 / 30 分钟离场），建议只对 candidates 按需拉分钟线。
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.exc import OperationalError

from .chip_filter import ChipFilter
from .config import get_section
from .db import DatabaseConfig, MySQLWriter
from .indicator_utils import consecutive_true
from .schema_manager import (
    STRATEGY_CODE_MA5_MA20_TREND,
    TABLE_STRATEGY_CHIP_FILTER,
    TABLE_STRATEGY_INDICATOR_DAILY,
    TABLE_STRATEGY_SIGNAL_CANDIDATES,
    TABLE_STRATEGY_SIGNAL_EVENTS,
    VIEW_STRATEGY_SIGNAL_CANDIDATES,
)
from .utils import setup_logger


@dataclass(frozen=True)
class MA5MA20Params:
    """策略参数（支持从 config.yaml 的 strategy_ma5_ma20_trend 节覆盖）。"""

    enabled: bool = False
    universe_source: str = "top_liquidity"  # top_liquidity / universe / all
    lookback_days: int = 365

    # 日线数据来源表：默认直接用全量表（性能更稳），必要时你也可以在 config.yaml 覆盖
    daily_table: str = "history_daily_kline"

    # 放量确认：volume / vol_ma >= threshold
    volume_ratio_threshold: float = 1.5
    volume_ma_window: int = 5

    # 趋势过滤用均线（多头排列）
    trend_ma_short: int = 20
    trend_ma_mid: int = 60
    trend_ma_long: int = 250

    # 回踩买点：close 与 MA20 偏离比例
    pullback_band: float = 0.01

    # KDJ 低位阈值（可选增强：只做 reason 标记，不强制）
    kdj_low_threshold: float = 30.0

    # 输出表/视图
    indicator_table: str = TABLE_STRATEGY_INDICATOR_DAILY
    signal_events_table: str = TABLE_STRATEGY_SIGNAL_EVENTS
    candidates_table: str = (
        TABLE_STRATEGY_SIGNAL_CANDIDATES
    )  # 仅在 candidates_as_view=False 时写表

    # 可选：用视图替代 candidates 表（更简洁；候选清单实时从 signals 最新日筛选）
    candidates_as_view: bool = True
    candidates_view: str = VIEW_STRATEGY_SIGNAL_CANDIDATES

    # signals 写入范围：
    # - latest：仅写入最新交易日（默认，低开销）
    # - window：写入本次计算窗口内的全部交易日（用于回填历史/回测）
    signals_write_scope: str = "latest"

    @classmethod
    def from_config(cls) -> "MA5MA20Params":
        sec = get_section("strategy_ma5_ma20_trend")
        if not sec:
            return cls()
        kwargs = {}
        indicator_table = sec.get("indicator_table")
        if indicator_table is None:
            indicator_table = sec.get("signals_indicator_table")
        if indicator_table is not None:
            kwargs["indicator_table"] = str(indicator_table).strip()
        events_table = sec.get("signal_events_table")
        if events_table is None:
            events_table = sec.get("signals_table")
        if events_table is not None:
            kwargs["signal_events_table"] = str(events_table).strip()
        for k in cls.__dataclass_fields__.keys():  # type: ignore[attr-defined]
            if k in sec:
                kwargs[k] = sec[k]
        return cls(**kwargs)


def _ensure_datetime(series: pd.Series) -> pd.Series:
    if np.issubdtype(series.dtype, np.datetime64):
        return series
    return pd.to_datetime(series, errors="coerce")


def _normalize_list_date(series: pd.Series) -> pd.Series:
    raw = series.copy()
    as_str = raw.astype(str)
    digit_mask = as_str.str.fullmatch(r"\d{8}")
    parsed_digits = pd.to_datetime(as_str.where(digit_mask), format="%Y%m%d", errors="coerce")
    parsed_general = pd.to_datetime(raw, errors="coerce")
    parsed = parsed_digits.combine_first(parsed_general)
    parsed = parsed.where(~raw.isna(), pd.NaT)
    return parsed


def _to_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def _macd(
    close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """MACD: DIF/DEA/HIST(2*(DIF-DEA))"""
    dif = _ema(close, fast) - _ema(close, slow)
    dea = _ema(dif, signal)
    hist = 2 * (dif - dea)
    return dif, dea, hist


def _kdj(
    high: pd.Series, low: pd.Series, close: pd.Series, n: int = 9
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """标准 KDJ(9,3,3) 迭代实现。"""
    low_n = low.rolling(n, min_periods=1).min()
    high_n = high.rolling(n, min_periods=1).max()
    denom = (high_n - low_n).replace(0, np.nan)
    rsv = ((close - low_n) / denom * 100.0).fillna(0.0)

    k_vals: List[float] = []
    d_vals: List[float] = []
    k_prev = 50.0
    d_prev = 50.0
    for v in rsv.to_numpy():
        k_now = (2.0 / 3.0) * k_prev + (1.0 / 3.0) * float(v)
        d_now = (2.0 / 3.0) * d_prev + (1.0 / 3.0) * k_now
        k_vals.append(k_now)
        d_vals.append(d_now)
        k_prev, d_prev = k_now, d_now

    k = pd.Series(k_vals, index=close.index, name="kdj_k")
    d = pd.Series(d_vals, index=close.index, name="kdj_d")
    j = (3 * k - 2 * d).rename("kdj_j")
    return k, d, j


def _atr(high: pd.Series, low: pd.Series, preclose: pd.Series, n: int = 14) -> pd.Series:
    tr1 = (high - low).abs()
    tr2 = (high - preclose).abs()
    tr3 = (low - preclose).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=1).mean().rename("atr14")


def _rsi(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    avg_loss = loss.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.rename("rsi14")


def _split_exchange_symbol(code: str) -> Tuple[str, str]:
    code_s = str(code or "").strip()
    if not code_s:
        return "", ""
    if "." in code_s:
        ex, sym = code_s.split(".", 1)
        return ex.lower(), sym
    return "", code_s


class MA5MA20StrategyRunner:
    """从 MySQL 读取日线 → 计算指标 → 生成 MA5-MA20 信号 → 写回 MySQL。"""

    def __init__(self) -> None:
        self.logger = setup_logger()
        self.params = MA5MA20Params.from_config()
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())
        self.indicator_window = self._resolve_indicator_window()
        self._fundamentals_cache: pd.DataFrame | None = None
        self._stock_basic_cache: pd.DataFrame | None = None

    def _resolve_indicator_window(self) -> int:
        try:
            lookback = int(self.params.lookback_days)
        except Exception:
            self.logger.warning(
                "lookback_days=%s 解析失败，将回退默认 365 天。",
                self.params.lookback_days,
            )
            return 365
        if lookback <= 0:
            self.logger.warning(
                "lookback_days=%s 无效，需为正整数，将回退默认 365 天。",
                self.params.lookback_days,
            )
            return 365
        return lookback

    def _daily_table_name(self) -> str:
        tbl = (getattr(self.params, "daily_table", "") or "").strip()
        if tbl:
            return tbl
        return f"history_recent_{int(self.params.lookback_days)}_days"

    def _get_latest_trade_date(self) -> dt.date:
        tbl = self._daily_table_name()
        stmt = text(f"SELECT MAX(`date`) AS max_date FROM `{tbl}`")
        with self.db_writer.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
        if not row or not row.get("max_date"):
            raise RuntimeError(f"{tbl} 为空，无法运行策略。请先运行 python start.py")
        v = row["max_date"]
        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts):
            raise RuntimeError(f"无法解析最新交易日：{v!r}")
        return ts.date()

    def _load_universe_codes(self, latest_date: dt.date) -> List[str]:
        """按配置选择选股池来源：top_liquidity / universe / all"""
        source = (self.params.universe_source or "top_liquidity").strip().lower()
        # all：表示“全市场”，不要先把 3000+ codes 拉到 Python（更慢）；后续 SQL 直接按日期窗口读取
        if source == "all":
            return []
        with self.db_writer.engine.begin() as conn:
            if source == "top_liquidity":
                stmt = text("SELECT `code` FROM `a_share_top_liquidity` WHERE `date` = :d")
                codes = [r[0] for r in conn.execute(stmt, {"d": latest_date}).fetchall()]
                if codes:
                    return [str(c) for c in codes]

            if source == "universe":
                stmt = text("SELECT `code` FROM `a_share_universe` WHERE `date` = :d")
                codes = [r[0] for r in conn.execute(stmt, {"d": latest_date}).fetchall()]
                if codes:
                    return [str(c) for c in codes]

        # all：退化为 stock_list
        with self.db_writer.engine.begin() as conn:
            stmt = text("SELECT `code` FROM `a_share_stock_list`")
            codes = [r[0] for r in conn.execute(stmt).fetchall()]
        return [str(c) for c in codes]

    def _resolve_snapshot_buy_lookback(self) -> int:
        open_monitor_cfg = get_section("open_monitor") or {}
        if not isinstance(open_monitor_cfg, dict):
            return 1

        def _to_int(value) -> int:
            try:
                return int(value)
            except Exception:  # noqa: BLE001
                return 0

        lookback_days = _to_int(open_monitor_cfg.get("signal_lookback_days"))
        cross_days = _to_int(open_monitor_cfg.get("cross_valid_days"))
        pullback_days = _to_int(open_monitor_cfg.get("pullback_valid_days"))
        valid_days_max = max(cross_days, pullback_days)
        resolved = max(lookback_days, valid_days_max + 1)
        return resolved if resolved > 0 else 1

    def _load_recent_buy_codes(self, latest_date: dt.date) -> set[str]:
        table = self.params.signal_events_table
        if not self._table_exists(table):
            return set()

        lookback = self._resolve_snapshot_buy_lookback()
        base_table = self._daily_table_name()
        date_col = "date"
        if not self._table_exists(base_table):
            if self._table_exists(self.params.indicator_table):
                base_table = self.params.indicator_table
                date_col = "trade_date"
            else:
                base_table = table
                date_col = "sig_date"
        base_date_str = latest_date.isoformat()

        try:
            with self.db_writer.engine.begin() as conn:
                trade_dates_df = pd.read_sql_query(
                    text(
                        f"""
                        SELECT DISTINCT CAST(`{date_col}` AS CHAR) AS d
                        FROM `{base_table}`
                        WHERE `{date_col}` <= :base_date
                        ORDER BY `{date_col}` DESC
                        LIMIT {lookback}
                        """
                    ),
                    conn,
                    params={"base_date": base_date_str},
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取快照补齐窗口失败：%s", exc)
            trade_dates_df = pd.DataFrame()

        trade_dates = (
            trade_dates_df["d"].dropna().astype(str).str[:10].tolist()
            if trade_dates_df is not None and not trade_dates_df.empty
            else []
        )
        if not trade_dates:
            return set()

        window_latest = trade_dates[0]
        window_earliest = trade_dates[-1]
        try:
            with self.db_writer.engine.begin() as conn:
                codes_df = pd.read_sql_query(
                    text(
                        f"""
                        SELECT DISTINCT `code`
                        FROM `{table}`
                        WHERE `signal` = 'BUY'
                          AND `strategy_code` = :strategy
                          AND `sig_date` <= :latest
                          AND `sig_date` >= :earliest
                        """
                    ),
                    conn,
                    params={
                        "latest": window_latest,
                        "earliest": window_earliest,
                        "strategy": STRATEGY_CODE_MA5_MA20_TREND,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取近期 BUY 信号代码失败：%s", exc)
            return set()

        if codes_df.empty or "code" not in codes_df.columns:
            return set()

        return set(codes_df["code"].dropna().astype(str).tolist())

    def _load_daily_kline(self, codes: List[str], end_date: dt.date) -> pd.DataFrame:
        """读取最近 indicator_window 天的日线数据，用于指标计算。"""
        tbl = self._daily_table_name()
        lookback = int(self.indicator_window)
        start_date = end_date - dt.timedelta(days=int(lookback * 2))  # 给交易日留冗余

        # 你的 history_daily_kline 的 date/code 是 TEXT，统一用 ISO 字符串做范围过滤（避免类型隐式转换）
        start_date_s = start_date.isoformat()
        end_date_s = end_date.isoformat()

        # 只取策略计算必需列，降低 I/O
        select_cols = "`date`,`code`,`high`,`low`,`close`,`preclose`,`volume`,`amount`"

        codes = [str(c) for c in (codes or []) if str(c).strip()]
        use_in = bool(codes) and len(codes) <= 2000

        with self.db_writer.engine.begin() as conn:
            if not codes:
                stmt = text(
                    f"""
                    SELECT {select_cols}
                    FROM `{tbl}`
                    WHERE `date` BETWEEN :start_date AND :end_date
                    ORDER BY `code`,`date`
                    """
                )
                df = pd.read_sql(stmt, conn, params={"start_date": start_date_s, "end_date": end_date_s})
            elif use_in:
                # IN 列表分批，避免 SQL 过长/解析慢
                chunk_size = 800
                stmt = (
                    text(
                        f"""
                        SELECT {select_cols}
                        FROM `{tbl}`
                        WHERE `code` IN :codes AND `date` BETWEEN :start_date AND :end_date
                        ORDER BY `code`,`date`
                        """
                    )
                    .bindparams(bindparam("codes", expanding=True))
                )
                parts: List[pd.DataFrame] = []
                for i in range(0, len(codes), chunk_size):
                    part_codes = codes[i : i + chunk_size]
                    part = pd.read_sql(
                        stmt,
                        conn,
                        params={"codes": part_codes, "start_date": start_date_s, "end_date": end_date_s},
                    )
                    if not part.empty:
                        parts.append(part)
                df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
            else:
                # 极端情况：codes 很多时退化为按日期读取（不建议用 all 做日线指标计算）
                stmt = text(
                    f"""
                    SELECT {select_cols}
                    FROM `{tbl}`
                    WHERE `date` BETWEEN :start_date AND :end_date
                    ORDER BY `code`,`date`
                    """
                )
                df = pd.read_sql(stmt, conn, params={"start_date": start_date_s, "end_date": end_date_s})

        if df.empty:
            raise RuntimeError(f"未能从 {tbl} 读取到任何日线数据。")

        df["date"] = _ensure_datetime(df["date"])
        df["code"] = df["code"].astype(str)

        df = _to_numeric(
            df,
            ["high", "low", "close", "preclose", "volume", "amount"],
        )
        df = df.dropna(subset=["date", "code", "close", "high", "low", "preclose"]).copy()

        df = df.sort_values(["code", "date"]).reset_index(drop=True)
        df = df.groupby("code", group_keys=False).tail(lookback).reset_index(drop=True)
        return df

    def _load_fundamentals_latest(self) -> pd.DataFrame:
        table = "fundamentals_latest_wide"
        try:
            with self.db_writer.engine.begin() as conn:
                return pd.read_sql(text(f"SELECT * FROM `{table}`"), conn)
        except Exception as exc:  # noqa: BLE001
            self.logger.info("未能读取 %s（将跳过基本面标签）：%s", table, exc)
            return pd.DataFrame()

    def _load_stock_basic(self) -> pd.DataFrame:
        table = "a_share_stock_basic"
        try:
            with self.db_writer.engine.begin() as conn:
                try:
                    df = pd.read_sql(
                        text("SELECT `code`,`code_name`,`ipoDate` FROM `a_share_stock_basic`"),
                        conn,
                    )
                except OperationalError as exc:
                    if "1054" in str(exc) or "Unknown column" in str(exc):
                        self.logger.info(
                            "读取 %s 字段 ['code', 'code_name', 'ipoDate'] 失败，将回退基础字段：%s",
                            table,
                            exc,
                        )
                        return pd.read_sql(
                            text(f"SELECT `code`,`code_name` FROM `{table}`"), conn
                        )
                    raise

                if "ipoDate" in df.columns:
                    df["ipoDate"] = _normalize_list_date(df["ipoDate"])
                return df
        except Exception as exc:  # noqa: BLE001
            self.logger.info(
                "读取 %s 失败，将跳过 ST 标签与板块限幅识别：%s",
                table,
                exc,
            )
            return pd.DataFrame()

    def _compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """为每个 code 计算均线、量能、MACD、KDJ、ATR。"""
        df = df.sort_values(["code", "date"]).copy()

        # 用 groupby().rolling() 替代 transform(lambda...)，减少 Python 层开销
        g_close = df.groupby("code", sort=False)["close"]
        g_vol = df.groupby("code", sort=False)["volume"]

        df["ma5"] = g_close.rolling(5, min_periods=5).mean().reset_index(level=0, drop=True)
        df["ma10"] = g_close.rolling(10, min_periods=10).mean().reset_index(level=0, drop=True)
        df["ma20"] = g_close.rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
        df["ma60"] = g_close.rolling(60, min_periods=60).mean().reset_index(level=0, drop=True)
        df["ma250"] = g_close.rolling(250, min_periods=250).mean().reset_index(level=0, drop=True)

        vol_win = int(self.params.volume_ma_window)
        df["vol_ma"] = g_vol.rolling(vol_win, min_periods=vol_win).mean().reset_index(level=0, drop=True)
        df["vol_ratio"] = df["volume"] / df["vol_ma"]

        def _add_oscillators(sub: pd.DataFrame) -> pd.DataFrame:
            group_code = getattr(sub, "name", None)
            close = sub["close"]
            high = sub["high"]
            low = sub["low"]
            preclose = sub["preclose"]

            dif, dea, hist = _macd(close)
            k, d, j = _kdj(high, low, close)
            atr = _atr(high, low, preclose)
            rsi = _rsi(close)

            sub = sub.copy()
            sub["macd_dif"] = dif
            sub["macd_dea"] = dea
            sub["macd_hist"] = hist
            sub["prev_macd_hist"] = hist.shift(1)
            sub["kdj_k"] = k
            sub["kdj_d"] = d
            sub["kdj_j"] = j
            sub["atr14"] = atr
            sub["rsi14"] = rsi
            # pandas>=2.2, include_groups=False 会去掉分组列，主动恢复 code
            if "code" not in sub.columns:
                sub["code"] = group_code
            return sub

        # pandas 新版本会提示 groupby.apply 将来不再包含分组列，提前兼容
        try:
            df = (
                df.groupby("code", group_keys=False)
                .apply(_add_oscillators, include_groups=False)  # pandas>=2.2
                .reset_index(drop=True)
            )
        except TypeError:
            # 兼容旧 pandas（没有 include_groups 参数）
            df = df.groupby("code", group_keys=False).apply(_add_oscillators).reset_index(drop=True)
        return df

    def _select_column(
        self, df: pd.DataFrame, candidates: List[str], *, contains: str | None = None
    ) -> str | None:
        for col in candidates:
            if col in df.columns:
                return col
        if contains:
            contains_lower = contains.lower()
            for col in df.columns:
                if contains_lower in col.lower():
                    return col
        return None

    def _build_fundamental_risk_map(self, fundamentals: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        if fundamentals.empty:
            return {}

        netprofit_col = self._select_column(
            fundamentals,
            ["profit_netProfit", "profit_netprofit", "profit_net_profit"],
            contains="netprofit",
        )
        yoy_col = self._select_column(
            fundamentals,
            ["growth_YOYNI", "growth_YOYPNI", "growth_YOYEPSBasic"],
            contains="yoy",
        )

        if netprofit_col is None and yoy_col is None:
            self.logger.info(
                "fundamentals_latest_wide 缺少净利润/同比列，跳过基本面风险标签。"
            )
            return {}

        merged = fundamentals[
            [c for c in ["code", netprofit_col, yoy_col] if c in fundamentals.columns]
        ].copy()
        if merged.empty:
            return {}

        for col in [netprofit_col, yoy_col]:
            if col and col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors="coerce")

        risk_map: Dict[str, Dict[str, str]] = {}
        for _, row in merged.iterrows():
            code = str(row.get("code") or "").strip()
            if not code:
                continue

            tags: list[str] = []
            notes: list[str] = []

            netprofit_val = row.get(netprofit_col) if netprofit_col else None
            if (
                netprofit_val is not None
                and not pd.isna(netprofit_val)
                and float(netprofit_val) <= 0
            ):
                tags.append("NO_PROFIT")
                notes.append(f"净利润为 {float(netprofit_val):.2f}，缺乏业绩支撑")

            yoy_val = row.get(yoy_col) if yoy_col else None
            if yoy_val is not None and not pd.isna(yoy_val) and float(yoy_val) < 0:
                tags.append("WEAK_GROWTH")
                notes.append(f"净利润同比 {float(yoy_val):.2f}% 为负")

            if tags:
                risk_map[code] = {
                    "tag": "|".join(tags),
                    "note": "；".join(notes),
                }

        return risk_map

    def _build_board_masks(
        self, codes: pd.Series, stock_basic: pd.DataFrame
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        parts = codes.map(_split_exchange_symbol)
        code_parts = pd.DataFrame(parts.tolist(), columns=["exchange", "symbol"]).fillna("")

        symbols = code_parts["symbol"].astype(str)
        exchanges = code_parts["exchange"].astype(str)

        mask_bj = (exchanges == "bj") | symbols.str.startswith(("43", "83"))
        mask_growth = symbols.str.startswith(
            ("300", "301", "302", "303", "688", "689")
        )

        mask_st = pd.Series(False, index=codes.index)
        if not stock_basic.empty:
            cols_lower = {c.lower(): c for c in stock_basic.columns}
            code_col = cols_lower.get("code")
            name_col = cols_lower.get("code_name") or cols_lower.get("name")
            if code_col and name_col and name_col in stock_basic.columns:
                base = stock_basic[[code_col, name_col]].dropna().copy()
                base[code_col] = base[code_col].astype(str)
                base[name_col] = base[name_col].astype(str)
                base = base.drop_duplicates(subset=[code_col], keep="last")
                names = base.set_index(code_col)[name_col]

                def _lookup_name(raw_code: str) -> str:
                    direct = names.get(raw_code, "")
                    if isinstance(direct, pd.Series):
                        direct = direct.iloc[0] if not direct.empty else ""
                    if direct:
                        return str(direct)
                    ex, sym = _split_exchange_symbol(raw_code)
                    if sym and sym in names:
                        return str(names.get(sym, ""))
                    if ex and f"{ex}.{sym}" in names:
                        return str(names.get(f"{ex}.{sym}", ""))
                    return ""

                mask_st = codes.map(lambda c: _lookup_name(str(c))).str.upper().str.contains("ST")

        return mask_growth, mask_bj, mask_st

    def _generate_signals(
        self,
        df: pd.DataFrame,
        fundamentals: pd.DataFrame | None = None,
        stock_basic: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """生成 BUY/SELL/HOLD 信号，并给出 reason。"""
        p = self.params

        def _prev_state(mask: pd.Series) -> pd.Series:
            """分组取前一日布尔状态，兼容 pandas 新版的 fillna 行为。"""

            return (
                mask.groupby(df["code"])
                .shift(1)
                .astype("boolean")
                .fillna(False)
                .infer_objects(copy=False)
                .astype(bool)
            )

        # 1) 趋势过滤：多头排列 + 价格站上中长均线
        trend_ok = (
            (df["close"] > df["ma60"])
            & (df["close"] > df["ma250"])
            & (df["ma20"] > df["ma60"])
            & (df["ma60"] > df["ma250"])
        )

        # 2) MA5/MA20 金叉/死叉
        ma5_gt_ma20 = (df["ma5"] > df["ma20"]).astype(bool)
        prev_ma5_gt_ma20 = _prev_state(ma5_gt_ma20)
        cross_up = ma5_gt_ma20 & (~prev_ma5_gt_ma20)
        cross_down = (~ma5_gt_ma20) & prev_ma5_gt_ma20
        prev_macd_hist = pd.to_numeric(df.get("prev_macd_hist"), errors="coerce")
        hist_cross_up = (df["macd_hist"] > 0) & (prev_macd_hist <= 0)
        hist_cross_down = (df["macd_hist"] < 0) & (prev_macd_hist >= 0)

        # 3) 放量确认
        vol_ok = df["vol_ratio"] >= float(p.volume_ratio_threshold)

        # 4) MACD 过滤（DIF 上穿 DEA 或 HIST>0）
        macd_gt = (df["macd_dif"] > df["macd_dea"]).astype(bool)
        prev_macd_gt = _prev_state(macd_gt)
        macd_cross_up = macd_gt & (~prev_macd_gt)
        macd_ok = macd_cross_up | (df["macd_hist"] > 0)
        macd_event = np.select(
            [hist_cross_up, hist_cross_down],
            ["MACD_HIST_CROSS_UP", "MACD_HIST_CROSS_DOWN"],
            default="",
        )

        # 5) KDJ 低位金叉（可选增强：只作为 reason 标记）
        kdj_gt = (df["kdj_k"] > df["kdj_d"]).astype(bool)
        prev_kdj_gt = _prev_state(kdj_gt)
        kdj_cross_up = kdj_gt & (~prev_kdj_gt)
        kdj_low = df["kdj_k"] <= float(p.kdj_low_threshold)
        kdj_ok = kdj_cross_up & kdj_low

        # 6) 趋势回踩（close 接近 MA20 + MA5 向上）
        pullback_band = float(p.pullback_band)
        pullback_near = ((df["close"] - df["ma20"]).abs() / df["ma20"]) <= pullback_band
        ma5_up = (df["ma5"] - df["ma5"].groupby(df["code"]).shift(1)) > 0

        buy_cross = trend_ok & cross_up & vol_ok & macd_ok
        buy_pullback = trend_ok & pullback_near & ma5_up & macd_ok
        buy_macd_confirm = hist_cross_up & (df["close"] >= df["ma20"])
        pattern_series = self._detect_price_patterns(df)
        buy_pattern = (pattern_series == "W_BOTTOM_CONFIRMED") & (df["close"] > df["ma20"])

        # 卖出：死叉 或 跌破 MA20 且放量（趋势破坏）
        sell_without_hist = cross_down | ((df["close"] < df["ma20"]) & vol_ok)
        sell = sell_without_hist | hist_cross_down
        prev_ma5 = df.groupby("code")["ma5"].shift(1)
        prev_ma20 = df.groupby("code")["ma20"].shift(1)
        dead_cross = (df["ma5"] < df["ma20"]) & (prev_ma5 > prev_ma20) & (df["vol_ratio"] < 1.0)
        reduce_mask = (df["ma5"] < df["ma20"]) & (df["vol_ratio"] < 1.0) & (~dead_cross)

        # 用 pandas Series 拼接原因，避免 numpy.ndarray 没有 strip 的问题
        reason = pd.Series("", index=df.index, dtype="object")
        reason = reason.mask(buy_cross, "MA5上穿MA20（金叉）+放量+MACD")

        def _append(base: pd.Series, cond: pd.Series, text_: str) -> pd.Series:
            add = np.where(base.eq(""), text_, base + "|" + text_)
            return base.mask(cond, pd.Series(add, index=base.index, dtype="object"))

        reason = _append(reason, buy_pullback, "趋势回踩MA20")
        reason = _append(reason, kdj_ok & (buy_cross | buy_pullback), "KDJ低位金叉")
        reason = _append(reason, buy_macd_confirm, "MACD柱翻红")
        reason = _append(reason, buy_pattern, "W底突破")

        # 卖出原因优先覆盖
        reason = reason.mask(sell_without_hist, "MA5下穿MA20（死叉）或跌破MA20放量")
        reason = reason.mask(hist_cross_down, "MACD柱翻绿/死叉")
        reason = reason.mask(dead_cross, "死叉+缩量清")
        reason = reason.mask(reduce_mask, "弱势缩量减仓")
        reason = reason.mask(reason.eq(""), "观望")

        signal = np.select(
            [sell | dead_cross, reduce_mask, buy_cross | buy_pullback | buy_pattern, buy_macd_confirm],
            ["SELL", "REDUCE", "BUY", "BUY_CONFIRM"],
            default="HOLD",
        )

        out = df.copy()
        out["signal"] = signal
        out["reason"] = reason.to_numpy()
        # 风险参考：2*ATR 作为“初始止损价”参考（你也可以换成 MA20 跌破止损）
        out["stop_ref"] = out["close"] - 2.0 * out["atr14"]

        # 妖股/过热风险：短期涨幅+涨停次数+乖离
        out["ret_10"] = out.groupby("code", sort=False)["close"].pct_change(periods=10)
        out["ret_20"] = out.groupby("code", sort=False)["close"].pct_change(periods=20)
        pct_change = np.where(
            out["preclose"] > 0,
            (out["close"] - out["preclose"]) / out["preclose"],
            np.nan,
        )
        out["ma20_bias"] = np.where(
            out["ma20"] != 0, (out["close"] - out["ma20"]) / out["ma20"], np.nan
        )

        def _format_pct(value: float | None) -> str:
            if value is None or pd.isna(value):
                return "N/A"
            return f"{value*100:.2f}%"
        code_series = out["code"].astype(str)
        stock_basic_df = stock_basic if stock_basic is not None else pd.DataFrame()
        mask_growth, mask_bj, mask_st = self._build_board_masks(code_series, stock_basic_df)
        list_date_col = self._select_column(
            stock_basic_df,
            ["ipoDate"],
        )
        code_col = self._select_column(stock_basic_df, ["code"], contains="code")
        listing_days = pd.Series(pd.NA, index=out.index, dtype="Int64")
        if list_date_col and code_col and list_date_col in stock_basic_df.columns:
            base = stock_basic_df[[code_col, list_date_col]].dropna().copy()
            base[code_col] = base[code_col].astype(str)
            base[list_date_col] = pd.to_datetime(base[list_date_col], errors="coerce")
            base = base.dropna(subset=[list_date_col]).drop_duplicates(subset=[code_col])
            list_date_map = base.set_index(code_col)[list_date_col]
            mapped_dates = out["code"].map(list_date_map)
            listing_days = (out["date"] - mapped_dates).dt.days
        out["listing_days"] = listing_days
        limit_up_threshold = pd.Series(
            np.where(mask_bj, 0.295, np.where(mask_growth, 0.195, 0.097)),
            index=out.index,
            dtype=float,
        )

        mania_ret = (out["ret_20"] >= 0.35).fillna(False)
        limit_up_daily = pct_change >= limit_up_threshold
        out["limit_up_cnt_20"] = (
            limit_up_daily.groupby(out["code"], sort=False)
            .rolling(20, min_periods=1)
            .sum()
            .reset_index(level=0, drop=True)
        )
        mania_limit = (out["limit_up_cnt_20"] >= 2).fillna(False)
        mania_bias = (out["ma20_bias"] >= 0.15).fillna(False)
        mania_mask = mania_ret | mania_limit | mania_bias

        ret20_fmt = out["ret_20"].apply(_format_pct)
        bias_fmt = out["ma20_bias"].apply(_format_pct)
        mania_notes_ret = np.where(mania_ret, "20日涨幅 " + ret20_fmt + " 过高", "")
        mania_notes_limit = np.where(
            mania_limit, "20日内涨停 " + out["limit_up_cnt_20"].fillna(0).astype(int).astype(str) + " 次", ""
        )
        mania_notes_bias = np.where(mania_bias, "MA20 乖离 " + bias_fmt + " 偏离成本", "")
        mania_notes = [
            "；".join([v for v in items if v])
            for items in zip(mania_notes_ret, mania_notes_limit, mania_notes_bias)
        ]

        yearline_enabled = out["ma250"].notna()
        if "listing_days" in out.columns and out["listing_days"].notna().any():
            yearline_enabled = yearline_enabled & out["listing_days"].ge(250).fillna(False)
        below_ma250_mask = (
            (out["close"] < out["ma250"]) & yearline_enabled
        ).astype("boolean")
        above_ma250_mask = (
            (out["close"] >= out["ma250"]) & yearline_enabled
        ).astype("boolean")
        prev_below_ma250 = (
            below_ma250_mask.groupby(out["code"], sort=False).shift(1).fillna(False).astype("boolean")
        )
        below_ma250_streak = (
            below_ma250_mask.groupby(out["code"], sort=False)
            .transform(consecutive_true)
            .astype("Int64")
        )
        above_ma250_streak = (
            above_ma250_mask.groupby(out["code"], sort=False)
            .transform(consecutive_true)
            .astype("Int64")
        )
        ma250_slope = out.groupby("code", sort=False)["ma250"].diff()
        yearline_break_confirmed = below_ma250_streak >= 3
        yearline_break_warn = (~yearline_break_confirmed) & (
            below_ma250_mask & (ma250_slope < 0)
        )
        yearline_reclaim_confirmed = (above_ma250_streak >= 2) & prev_below_ma250

        yearline_state = np.select(
            [
                yearline_break_confirmed,
                yearline_reclaim_confirmed & yearline_enabled,
                below_ma250_mask,
                yearline_enabled,
            ],
            [
                "BREAK_CONFIRMED",
                "RECLAIM_CONFIRMED",
                "BELOW_1_2D",
                "ABOVE",
            ],
            default="NO_DATA",
        )

        yearline_tags_confirmed = np.where(
            yearline_break_confirmed, "YEARLINE_BREAK_CONFIRMED", ""
        )
        yearline_tags_warn = np.where(yearline_break_warn, "YEARLINE_BREAK_WARN", "")
        yearline_notes_confirmed = np.where(
            yearline_break_confirmed, "年线连续3日收盘跌破，趋势破位风险", ""
        )
        yearline_notes_warn = np.where(
            yearline_break_warn,
            "年线下穿且年线走弱，警惕有效跌破",
            "",
        )

        fund_df = fundamentals if fundamentals is not None else pd.DataFrame()
        fund_risk_map = self._build_fundamental_risk_map(fund_df)
        fund_tags = code_series.map(lambda c: (fund_risk_map.get(c) or {}).get("tag", ""))
        fund_notes = code_series.map(lambda c: (fund_risk_map.get(c) or {}).get("note", ""))

        mania_tags = np.where(mania_mask, "MANIA", "")
        st_tags = np.where(mask_st.fillna(False), "ST", "")
        st_notes = np.where(mask_st.fillna(False), "风险警示 ST", "")

        risk_tags = [
            "|".join([t for t in (mt, stt, ft, ytc, ytw) if t])
            for mt, stt, ft, ytc, ytw in zip(
                mania_tags,
                st_tags,
                fund_tags,
                yearline_tags_confirmed,
                yearline_tags_warn,
            )
        ]
        risk_notes = [
            "；".join([v for v in (mn, sn, fn, ync, ynw) if v])
            for mn, sn, fn, ync, ynw in zip(
                mania_notes,
                st_notes,
                fund_notes,
                yearline_notes_confirmed,
                yearline_notes_warn,
            )
        ]

        out["yearline_state"] = yearline_state
        out["risk_tag"] = risk_tags
        out["risk_note"] = risk_notes
        out["runup_pct"] = out.groupby("code", sort=False)["close"].pct_change(periods=5)
        fear_score = ((out["kdj_k"] - 50.0) / 50.0) * (1 - out["vol_ratio"])
        out["fear_score"] = fear_score.clip(-3, 3)
        wave_type = np.select(
            [
                (out["rsi14"] < 30) & (out["ma20_bias"] < -0.05),
                out["ma20_bias"].abs() < 0.02,
            ],
            ["OVERSOLD_REVERT", "RANGE"],
            default="TREND_PULLBACK",
        )
        out["wave_type"] = wave_type
        out["pattern_tag"] = pattern_series
        out["macd_event"] = macd_event
        out["hist_cross_up"] = hist_cross_up
        out["hist_cross_down"] = hist_cross_down

        out = self._attach_chip_factors(out)
        out = self._decide_final_action(out)

        extras: List[str] = []
        for _, row in out.iterrows():
            payload = {
                "raw_signal": row.get("raw_signal"),
                "macd_event": row.get("macd_event"),
                "hist_cross_up": bool(row.get("hist_cross_up", False)),
                "hist_cross_down": bool(row.get("hist_cross_down", False)),
                "chip_score": row.get("chip_score"),
                "chip_reason": row.get("chip_reason"),
                "gdhs_delta_pct": row.get("gdhs_delta_pct"),
                "gdhs_announce_date": (
                    row.get("gdhs_announce_date").isoformat()
                    if isinstance(row.get("gdhs_announce_date"), (dt.date, pd.Timestamp))
                    else None
                ),
                "age_days": row.get("age_days"),
                "deadzone_hit": bool(row.get("deadzone_hit", False)),
                "stale_hit": bool(row.get("stale_hit", False)),
                "chip_penalty": row.get("chip_penalty"),
                "chip_note": row.get("chip_note"),
                "gate": row.get("gate_tag"),
                "fear_score": row.get("fear_score"),
                "wave_type": row.get("wave_type"),
                "pattern_tag": row.get("pattern_tag"),
                "runup_pct": row.get("runup_pct"),
            }
            payload = {k: v for k, v in payload.items() if v is not None and not pd.isna(v)}
            extras.append(json.dumps(payload, ensure_ascii=False))
        out["extra_json"] = extras
        return out

    def _detect_price_patterns(self, df: pd.DataFrame) -> pd.Series:
        if df.empty:
            return pd.Series(dtype="object")

        pattern = pd.Series("", index=df.index, dtype="object")
        grouped = df.groupby("code", sort=False)
        for code, sub in grouped:
            lows = pd.to_numeric(sub.get("low"), errors="coerce")
            highs = pd.to_numeric(sub.get("high"), errors="coerce")
            closes = pd.to_numeric(sub.get("close"), errors="coerce")
            vol_ratio = pd.to_numeric(sub.get("vol_ratio"), errors="coerce")

            neckline = highs.rolling(15, min_periods=5).max()
            valley_recent = lows.rolling(20, min_periods=10).min()
            valley_prev = valley_recent.shift(5)
            valley_close = (valley_recent.notna()) & (valley_prev.notna())
            w_bottom = valley_close & ((valley_recent - valley_prev).abs() / valley_prev.clip(lower=1e-6) <= 0.03)
            confirm = closes > neckline
            if not vol_ratio.isna().all():
                confirm = confirm & (vol_ratio > 1.2)
            pattern.loc[sub.index] = np.where(confirm & w_bottom, "W_BOTTOM_CONFIRMED", "")

            flag_slope = closes.diff().rolling(5, min_periods=5).mean()
            flag_std = closes.rolling(5, min_periods=5).std()
            flag_range_ok = (flag_std / closes.replace(0, np.nan)) < 0.02
            flag_body = flag_slope.abs() < (closes.abs() * 0.001)
            breakout = closes > closes.rolling(10, min_periods=5).max()
            if not vol_ratio.isna().all():
                breakout = breakout & (vol_ratio > 1.2)
            pattern.loc[sub.index] = np.where(
                breakout & flag_range_ok & flag_body,
                "FLAG_BREAKOUT",
                pattern.loc[sub.index],
            )
        return pattern

    def _attach_chip_factors(self, signals: pd.DataFrame) -> pd.DataFrame:
        def _fill_vol_ratio(sig_df: pd.DataFrame, dates: List[dt.date], code_list: List[str]) -> pd.DataFrame:
            if "vol_ratio" in sig_df.columns and not sig_df["vol_ratio"].isna().any():
                return sig_df
            try:
                vr_stmt = text(
                    """
                    SELECT `trade_date`AS date, code, vol_ratio
                    FROM strategy_indicator_daily
                    WHERE `trade_date` IN :dates AND code IN :codes
                    """
                ).bindparams(bindparam("dates", expanding=True), bindparam("codes", expanding=True))
                with self.db_writer.engine.begin() as conn:
                    vr_df = pd.read_sql_query(
                        vr_stmt,
                        conn,
                        params={"dates": dates, "codes": code_list},
                    )
            except Exception:
                vr_df = pd.DataFrame()

            if vr_df.empty:
                return sig_df

            # 关键：merge 键 dtype 必须一致，否则会报：
            # ValueError: You are trying to merge on datetime64[ns] and object columns for key 'date'
            sig_df = sig_df.copy()
            vr_df = vr_df.copy()
            if "date" in sig_df.columns:
                sig_df["date"] = pd.to_datetime(sig_df["date"], errors="coerce")
            sig_df["code"] = sig_df["code"].astype(str)
            vr_df["date"] = pd.to_datetime(vr_df["date"], errors="coerce")
            vr_df["code"] = vr_df["code"].astype(str)
            sig_df = sig_df.dropna(subset=["date", "code"])
            vr_df = vr_df.dropna(subset=["date", "code"])
            if sig_df.empty or vr_df.empty:
                return sig_df
            merged_sig = sig_df.merge(vr_df, on=["date", "code"], how="left", suffixes=("", "_ind"))
            merged_sig["vol_ratio"] = pd.to_numeric(merged_sig["vol_ratio"], errors="coerce").fillna(
                pd.to_numeric(merged_sig.get("vol_ratio_ind"), errors="coerce")
            )
            if "vol_ratio_ind" in merged_sig.columns:
                merged_sig = merged_sig.drop(columns=["vol_ratio_ind"])
            return merged_sig

        if signals.empty:
            signals["chip_score"] = np.nan
            signals["gdhs_delta_pct"] = np.nan
            signals["gdhs_announce_date"] = pd.NaT
            signals["chip_reason"] = None
            signals["chip_penalty"] = np.nan
            signals["chip_note"] = None
            signals["age_days"] = np.nan
            signals["deadzone_hit"] = False
            signals["stale_hit"] = False
            return signals

        signals = signals.copy()
        signals["date"] = pd.to_datetime(signals["date"], errors="coerce")
        signals = signals.dropna(subset=["date", "code"])
        signals["code"] = signals["code"].astype(str)
        if signals.empty:
            return signals

        codes = signals["code"].unique().tolist()
        sig_dates = signals["date"].dt.date.unique().tolist()
        chip_table_exists = self._table_exists(TABLE_STRATEGY_CHIP_FILTER)
        chip_df = pd.DataFrame()

        if chip_table_exists:
            stmt = (
                text(
                    f"""
                    SELECT *
                    FROM `{TABLE_STRATEGY_CHIP_FILTER}`
                    WHERE `sig_date` IN :dates AND `code` IN :codes
                    """
                ).bindparams(bindparam("dates", expanding=True), bindparam("codes", expanding=True))
            )
            with self.db_writer.engine.begin() as conn:
                try:
                    chip_df = pd.read_sql_query(
                        stmt,
                        conn,
                        params={"dates": sig_dates, "codes": codes},
                    )
                except Exception:
                    chip_df = pd.DataFrame()

        if chip_df.empty:
            signals = _fill_vol_ratio(signals, sig_dates, codes)
            chip_cols = [
                "date",
                "code",
                "vol_ratio",
                "close",
                "ma20",
                "runup_pct",
                "fear_score",
                "pct_chg",
                "pct_change",
                "change_pct",
                "ret_1d",
                "ret",
            ]
            chip_inputs = [c for c in chip_cols if c in signals.columns]
            chip_df = ChipFilter().apply(signals[chip_inputs].copy())

        stale_mask = pd.Series(False, index=chip_df.index)
        if not chip_df.empty:
            chip_reason_col = chip_df.get("chip_reason")
            if chip_reason_col is None:
                chip_reason_col = pd.Series(pd.NA, index=chip_df.index, dtype="object")
            vol_source = (
                chip_df["vol_ratio"]
                if "vol_ratio" in chip_df.columns
                else pd.Series(np.nan, index=chip_df.index)
            )
            vol_col = pd.to_numeric(vol_source, errors="coerce")
            chip_df["chip_reason"] = chip_reason_col
            stale_mask = (
                chip_reason_col.isna()
                | (chip_reason_col == "")
                | chip_reason_col.eq("DATA_MISSING")
                | vol_col.isna()
            )
        if not chip_df.empty and stale_mask.any():
            stale_rows = chip_df.loc[stale_mask, ["sig_date", "code"]].dropna()
            if not stale_rows.empty:
                stale_dates = pd.to_datetime(stale_rows["sig_date"], errors="coerce").dt.date.dropna().unique().tolist()
                stale_codes = stale_rows["code"].astype(str).unique().tolist()
                re_sig = signals[signals["date"].dt.date.isin(stale_dates) & signals["code"].isin(stale_codes)].copy()
                refreshed = pd.DataFrame()
                if not re_sig.empty:
                    re_sig = _fill_vol_ratio(re_sig, stale_dates, stale_codes)
                    refresh_cols = [
                        "date",
                        "code",
                        "vol_ratio",
                        "close",
                        "ma20",
                        "runup_pct",
                        "fear_score",
                        "pct_chg",
                        "pct_change",
                        "change_pct",
                        "ret_1d",
                        "ret",
                    ]
                    refresh_inputs = [c for c in refresh_cols if c in re_sig.columns]
                    refreshed = ChipFilter().apply(re_sig[refresh_inputs].copy())
                if not refreshed.empty:
                    refreshed = refreshed.copy()
                    refreshed["sig_date"] = pd.to_datetime(refreshed["sig_date"], errors="coerce")
                    chip_df = chip_df[~stale_mask].copy()
                    concat_frames = []
                    for df in (chip_df, refreshed):
                        if df is None or df.empty:
                            continue
                        if not df.notna().any().any():
                            continue
                        concat_frames.append(df)
                    chip_df = pd.concat(concat_frames, ignore_index=True) if concat_frames else pd.DataFrame()
                    chip_df = chip_df.drop_duplicates(subset=["sig_date", "code"], keep="last")

        if chip_df.empty:
            signals["chip_score"] = np.nan
            signals["gdhs_delta_pct"] = np.nan
            signals["gdhs_announce_date"] = pd.NaT
            signals["chip_reason"] = "DATA_MISSING_GDHS"
            signals["chip_penalty"] = np.nan
            signals["chip_note"] = "DATA_MISSING_GDHS"
            signals["age_days"] = np.nan
            signals["deadzone_hit"] = False
            signals["stale_hit"] = False
            return signals

        chip_df = chip_df.rename(columns={"announce_date": "gdhs_announce_date"})
        chip_df["sig_date"] = pd.to_datetime(chip_df["sig_date"], errors="coerce")
        chip_df["code"] = chip_df["code"].astype(str)
        merged = pd.merge(
            signals,
            chip_df,
            how="left",
            left_on=["date", "code"],
            right_on=["sig_date", "code"],
            suffixes=("", "_chip"),
        )
        merged["gdhs_delta_pct"] = pd.to_numeric(merged.get("gdhs_delta_pct"), errors="coerce")
        merged["gdhs_announce_date"] = pd.to_datetime(
            merged.get("gdhs_announce_date"), errors="coerce"
        )
        merged["chip_score"] = pd.to_numeric(merged.get("chip_score"), errors="coerce")
        merged["chip_reason"] = merged.get("chip_reason")
        merged["chip_penalty"] = pd.to_numeric(merged.get("chip_penalty"), errors="coerce")
        merged["chip_note"] = merged.get("chip_note")
        merged["age_days"] = pd.to_numeric(merged.get("age_days"), errors="coerce")
        merged["deadzone_hit"] = merged.get("deadzone_hit", False)
        merged["stale_hit"] = merged.get("stale_hit", False)
        gdhs_missing = merged["gdhs_announce_date"].isna() & merged["gdhs_delta_pct"].isna()
        chip_all_missing = gdhs_missing & merged["chip_score"].isna()
        vol_missing = merged["vol_ratio"].isna()
        missing_reason_mask = merged["chip_reason"].isna() | (merged["chip_reason"] == "")
        missing_reason = np.select(
            [
            missing_reason_mask & (gdhs_missing | chip_all_missing),
            missing_reason_mask & vol_missing,
        ],
            ["DATA_MISSING_GDHS", "DATA_MISSING_VOL_RATIO"],
            default=None,
        )
        missing_reason_series = pd.Series(missing_reason, index=merged.index, dtype="object")
        missing_reason_mask = missing_reason_series.notna()
        merged.loc[missing_reason_mask, "chip_reason"] = missing_reason_series[missing_reason_mask].values
        merged.loc[missing_reason_mask, "chip_score"] = 0.0
        merged.loc[missing_reason_mask, "chip_note"] = merged.loc[missing_reason_mask, "chip_reason"]
        return merged

    def _decide_final_action(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        out = df.copy()
        out["raw_signal"] = out["signal"]
        final_action = pd.Series(out["signal"], index=out.index, dtype="object")
        final_reason = pd.Series(out["reason"], index=out.index, dtype="object")
        base_cap = 0.5
        final_cap = pd.Series(base_cap, index=out.index, dtype=float)
        gate_tag = pd.Series("", index=out.index, dtype="object")

        required_cols = ["close", "ma20", "ma5", "macd_hist"]
        missing_core = out[required_cols].isna().any(axis=1)
        final_action = final_action.mask(missing_core, "HOLD")
        final_reason = final_reason.mask(missing_core, "DATA_MISSING")
        final_cap = final_cap.mask(missing_core, 0.0)
        vol_missing = out["vol_ratio"].isna()
        final_reason = final_reason.mask(
            vol_missing & ~missing_core, "VOL_RATIO_FALLBACK"
        )
        final_cap = final_cap.mask(vol_missing & ~missing_core, 0.2)
        vol_ratio_filled = pd.to_numeric(out.get("vol_ratio"), errors="coerce").fillna(1.0)

        rsi = pd.to_numeric(out.get("rsi14"), errors="coerce")
        bias = pd.to_numeric(out.get("ma20_bias"), errors="coerce")
        env_gate = out.get("env_final_gate_action")
        oversold = (rsi < 30) & (bias < -0.05)
        final_cap = final_cap.mask(oversold, 0.2)
        gate_tag = gate_tag.mask(oversold, "C_GO")
        if oversold.any():
            base_reason = final_reason.fillna("").astype(str)
            c_go_reason = np.where(
                base_reason.eq(""),
                "C_GO",
                base_reason + "|C_GO",
            )
            final_reason = final_reason.mask(oversold, pd.Series(c_go_reason, index=final_reason.index))

        if env_gate is not None:
            env_gate_upper = out["env_final_gate_action"].astype(str).str.upper()
            gate_stop = env_gate_upper == "STOP"
            gate_wait = env_gate_upper == "WAIT"
            entry_mask = ~final_action.isin(["SELL", "REDUCE"])
            final_cap = final_cap.mask(gate_stop | gate_wait, 0.0)
            final_action = final_action.mask(
                (gate_stop | gate_wait) & entry_mask,
                "WAIT",
            )
            final_reason = final_reason.mask((gate_stop | gate_wait) & entry_mask, "ENV_GATE")

        runup_pct = pd.to_numeric(out.get("runup_pct"), errors="coerce")
        runup_exit = runup_pct > 0.15
        final_action = final_action.mask(runup_exit, "REDUCE")
        final_reason = final_reason.mask(runup_exit, "RUNUP>15%")
        final_cap = final_cap.mask(runup_exit, 0.0)

        prev_hist = pd.to_numeric(out.get("prev_macd_hist"), errors="coerce")
        macd_hist = pd.to_numeric(out.get("macd_hist"), errors="coerce")
        hist_diff = macd_hist - prev_hist
        macd_shrink = (
            hist_diff.groupby(out["code"], sort=False)
            .rolling(3, min_periods=3)
            .apply(lambda x: (x < 0).all(), raw=True)
            .reset_index(level=0, drop=True)
        )
        out["macd_shrink"] = macd_shrink
        macd_shrink_series = out.get("macd_shrink")
        if isinstance(macd_shrink_series, pd.Series) and macd_shrink_series.dtype != bool:
            macd_shrink_flag = (macd_shrink_series < 0).fillna(False)
        elif isinstance(macd_shrink_series, pd.Series):
            macd_shrink_flag = macd_shrink_series.fillna(False)
        else:
            macd_shrink_flag = pd.Series(False, index=out.index, dtype=bool)

        hist_cross_down_flag = out.get("hist_cross_down", False)
        if isinstance(hist_cross_down_flag, pd.Series):
            hist_cross_down_flag = hist_cross_down_flag.fillna(False)
        else:
            hist_cross_down_flag = bool(hist_cross_down_flag)

        exit_flag = macd_shrink_flag | hist_cross_down_flag
        soft_stop = (out["close"] > out["ma20"]) & (vol_ratio_filled < 1.0) & (
            exit_flag
        )
        final_action = final_action.mask(soft_stop, "SELL")
        final_reason = final_reason.mask(soft_stop, "动能衰减/缩量")
        final_cap = final_cap.mask(soft_stop, 0.0)

        reduce_mask = (out["ma5"] < out["ma20"]) & (vol_ratio_filled < 1.0)
        final_action = final_action.mask(reduce_mask & ~soft_stop, "REDUCE")
        final_reason = final_reason.mask(reduce_mask & ~soft_stop, "弱势缩量减仓")
        final_cap = final_cap.mask(reduce_mask, 0.0)

        chip_score = pd.to_numeric(out.get("chip_score"), errors="coerce")
        chip_reason = out.get("chip_reason")
        chip_reason = chip_reason.replace("", pd.NA) if isinstance(chip_reason, pd.Series) else chip_reason
        chip_penalty = pd.to_numeric(out.get("chip_penalty"), errors="coerce").fillna(0.0)
        raw_signal = out.get("raw_signal")
        raw_signal_upper = raw_signal.astype(str).str.upper() if isinstance(raw_signal, pd.Series) else None

        entry_mask = ~final_action.isin(["SELL", "REDUCE"])
        if raw_signal_upper is not None:
            sell_raw = raw_signal_upper == "SELL"
            final_action = final_action.mask(sell_raw, "SELL")
            final_cap = final_cap.mask(sell_raw, 0.0)

            buy_confirm_raw = raw_signal_upper == "BUY_CONFIRM"
            vol_guard = vol_ratio_filled < 1.0
            structure_weak = (out["ma5"] < out["ma20"]) | (out["macd_hist"] <= 0)
            chip_reason_str = chip_reason.astype("string") if isinstance(chip_reason, pd.Series) else pd.Series("", index=out.index, dtype="string")
            disperse_risk = chip_reason_str.eq("CHIP_DISPERSE_STRONG")
            outlier_risk = chip_reason_str.str.contains("OUTLIER", case=False, na=False)
            degrade_wait = buy_confirm_raw & entry_mask & (
                outlier_risk | (disperse_risk & (structure_weak | vol_guard))
            )
            # 筹码/数据异常：避免把 BUY_CONFIRM “硬拦”成 WAIT（会直接从 candidates 池消失）
            # 改为降级为 BUY + 小仓位，让信号还能进入候选池，但仓位明显受控
            final_action = final_action.mask(degrade_wait, "BUY")
            wait_reason = chip_reason.fillna("CHIP_WEAK") if isinstance(chip_reason, pd.Series) else "CHIP_WEAK"
            final_reason = final_reason.mask(degrade_wait, wait_reason)
            final_cap = final_cap.mask(degrade_wait, 0.1)

            weak_confirm = buy_confirm_raw & entry_mask & ~degrade_wait
            confirm_cap = np.where(chip_score >= 0.2, 0.2, 0.1)
            final_cap = final_cap.mask(weak_confirm, np.minimum(final_cap, confirm_cap))

        chip_score_filled = chip_score.fillna(0) if isinstance(chip_score, pd.Series) else 0.0
        hold_like = entry_mask & ~final_action.isin(["WAIT"])
        cap_tier = np.select(
            [chip_score_filled >= 0.5, chip_score_filled >= 0, chip_score_filled < 0],
            [0.3, 0.2, 0.1],
            default=final_cap,
        )
        final_cap = final_cap.mask(hold_like, np.minimum(final_cap, cap_tier))

        chip_missing = (
            chip_reason.astype("string").str.startswith("DATA_MISSING").fillna(False)
            if isinstance(chip_reason, pd.Series)
            else pd.Series(False, index=out.index)
        )
        if isinstance(chip_reason, pd.Series):
            missing_reason_fill = chip_reason
        else:
            missing_reason_fill = pd.Series(chip_reason, index=out.index)
        final_reason = final_reason.mask(chip_missing & final_reason.isna(), missing_reason_fill)

        entry_mask = ~final_action.isin(["SELL", "REDUCE"])
        final_cap = final_cap.mask(entry_mask, np.maximum(0.0, final_cap - chip_penalty))
        vol_ratio = pd.to_numeric(out.get("vol_ratio"), errors="coerce")
        low_vol_entry = vol_ratio_filled < 1.5
        final_cap = final_cap.mask(entry_mask & low_vol_entry & (final_cap > 0), 0.3)

        exit_mask = final_action.isin(["SELL", "REDUCE"])
        final_cap = final_cap.mask(exit_mask, 0.0)

        out["final_action"] = final_action.fillna("HOLD")
        out["final_reason"] = final_reason.fillna("观望")
        out["final_cap"] = final_cap
        out["gate_tag"] = gate_tag.replace("", None)
        return out

    def _clear_table(self, table: str) -> None:
        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE `{table}`"))
        except Exception:
            pass

    def _table_exists(self, table: str) -> bool:
        if not table:
            return False
        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1"))
            return True
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("检查表 %s 是否存在失败：%s", table, exc)
            return False

    def _write_indicator_daily(
        self, latest_date: dt.date, signals: pd.DataFrame, codes: List[str]
    ) -> None:
        table = self.params.indicator_table
        scope = (getattr(self.params, "signals_write_scope", "latest") or "latest").strip().lower()
        if scope not in {"latest", "window"}:
            self.logger.warning("signals_write_scope=%s 无效，已回退为 latest。", scope)
            scope = "latest"

        base = signals[signals["date"].dt.date == latest_date].copy() if scope == "latest" else signals.copy()
        if base.empty:
            self.logger.warning("signals_write_scope=%s 下无任何指标行，已跳过写入。", scope)
            return

        keep_cols = [
            "date",
            "code",
            "close",
            "volume",
            "amount",
            "ma5",
            "ma10",
            "ma20",
            "ma60",
            "ma250",
            "vol_ratio",
            "macd_dif",
            "macd_dea",
            "macd_hist",
            "prev_macd_hist",
            "kdj_k",
            "kdj_d",
            "kdj_j",
            "atr14",
            "rsi14",
            "ret_10",
            "ret_20",
            "limit_up_cnt_20",
            "ma20_bias",
            "yearline_state",
        ]
        indicator_df = base[keep_cols].copy()
        indicator_df = indicator_df.rename(columns={"date": "trade_date"})
        indicator_df["trade_date"] = pd.to_datetime(indicator_df["trade_date"]).dt.date
        indicator_df["code"] = indicator_df["code"].astype(str)
        # 防止 DataFrame 自身重复键（trade_date, code）导致一次写入就撞主键
        indicator_df = indicator_df.drop_duplicates(subset=["trade_date", "code"], keep="last").copy()

        if scope == "latest":
            delete_stmt = (
                text(f"DELETE FROM `{table}` WHERE `trade_date` = :d AND `code` IN :codes")
                .bindparams(bindparam("codes", expanding=True))
            )
            del_codes = indicator_df["code"].tolist()
            with self.db_writer.engine.begin() as conn:
                conn.execute(delete_stmt, {"d": latest_date, "codes": del_codes})
        else:
            start_d = min(indicator_df["trade_date"])
            end_d = max(indicator_df["trade_date"])
            codes_clean = [str(c) for c in (codes or []) if str(c).strip()]
            delete_by_date_only = (not codes_clean) or (len(codes_clean) > 2000)
            with self.db_writer.engine.begin() as conn:
                if delete_by_date_only:
                    delete_stmt = text(
                        f"DELETE FROM `{table}` WHERE `trade_date` BETWEEN :start_date AND :end_date"
                    )
                    conn.execute(delete_stmt, {"start_date": start_d, "end_date": end_d})
                else:
                    delete_stmt = (
                        text(
                            f"""
                            DELETE FROM `{table}`
                            WHERE `trade_date` BETWEEN :start_date AND :end_date
                              AND `code` IN :codes
                            """
                        )
                        .bindparams(bindparam("codes", expanding=True))
                    )
                    chunk_size = 800
                    for i in range(0, len(codes_clean), chunk_size):
                        part_codes = codes_clean[i : i + chunk_size]
                        conn.execute(
                            delete_stmt,
                            {"start_date": start_d, "end_date": end_d, "codes": part_codes},
                        )

            self.logger.info(
                "signals_write_scope=window：指标已覆盖写入 %s~%s（%s）。",
                start_d,
                end_d,
                "all-codes" if delete_by_date_only else f"{len(codes_clean)} codes",
            )

        self.db_writer.write_dataframe(indicator_df, table, if_exists="append")

    def _write_signal_events(
        self, latest_date: dt.date, signals: pd.DataFrame, codes: List[str]
    ) -> None:
        table = self.params.signal_events_table
        scope = (getattr(self.params, "signals_write_scope", "latest") or "latest").strip().lower()
        if scope not in {"latest", "window"}:
            self.logger.warning("signals_write_scope=%s 无效，已回退为 latest。", scope)
            scope = "latest"

        base = signals[signals["date"].dt.date == latest_date].copy() if scope == "latest" else signals.copy()
        if base.empty:
            self.logger.warning("signals_write_scope=%s 下无任何信号事件，已跳过写入。", scope)
            return

        keep_cols = [
            "date",
            "code",
            "signal",
            "final_action",
            "final_reason",
            "final_cap",
            "reason",
            "risk_tag",
            "risk_note",
            "stop_ref",
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
        ]
        events_df = base[keep_cols].copy()
        events_df = events_df.rename(columns={"date": "sig_date"})
        events_df["sig_date"] = pd.to_datetime(events_df["sig_date"]).dt.date
        events_df["code"] = events_df["code"].astype(str)
        events_df["strategy_code"] = STRATEGY_CODE_MA5_MA20_TREND

        # 防止同一批信号里存在重复键，导致唯一约束/主键冲突（strategy_code, sig_date, code）
        events_df = events_df.drop_duplicates(subset=["strategy_code", "sig_date", "code"], keep="last").copy()
        for col in ["risk_tag", "risk_note", "reason", "chip_reason"]:
            if col in events_df.columns:
                events_df[col] = (
                    events_df[col]
                    .fillna("")
                    .astype(str)
                    .str.slice(0, 250)
                )
        for col in ["final_reason", "macd_event", "wave_type"]:
            if col in events_df.columns:
                events_df[col] = (
                    events_df[col]
                    .fillna("")
                    .astype(str)
                    .str.slice(0, 255)
                )
        if "chip_note" in events_df.columns:
            events_df["chip_note"] = (
                events_df["chip_note"]
                .fillna("")
                .astype(str)
                .str.slice(0, 255)
                .replace("", pd.NA)
            )
        events_df["gdhs_announce_date"] = pd.to_datetime(
            events_df.get("gdhs_announce_date"), errors="coerce"
        ).dt.date
        events_df["extra_json"] = None
        if "extra_json" in base.columns:
            events_df["extra_json"] = base["extra_json"].fillna("").astype(str)
        if "chip_score" in events_df.columns:
            events_df["chip_score"] = pd.to_numeric(events_df["chip_score"], errors="coerce")
        if "chip_penalty" in events_df.columns:
            events_df["chip_penalty"] = pd.to_numeric(events_df["chip_penalty"], errors="coerce")
        if "age_days" in events_df.columns:
            events_df["age_days"] = pd.to_numeric(events_df["age_days"], errors="coerce").astype("Int64")
        for flag_col in ["deadzone_hit", "stale_hit"]:
            if flag_col in events_df.columns:
                events_df[flag_col] = events_df[flag_col].astype(bool)

        if scope == "latest":
            delete_stmt = (
                text(
                    f"""
                    DELETE FROM `{table}`
                    WHERE `sig_date` = :d AND `code` IN :codes AND `strategy_code` = :strategy
                    """
                ).bindparams(bindparam("codes", expanding=True))
            )
            del_codes = events_df["code"].tolist()
            with self.db_writer.engine.begin() as conn:
                conn.execute(
                    delete_stmt,
                    {
                        "d": latest_date,
                        "codes": del_codes,
                        "strategy": STRATEGY_CODE_MA5_MA20_TREND,
                    },
                )
        else:
            start_d = min(events_df["sig_date"])
            end_d = max(events_df["sig_date"])
            codes_clean = [str(c) for c in (codes or []) if str(c).strip()]
            delete_by_date_only = (not codes_clean) or (len(codes_clean) > 2000)
            with self.db_writer.engine.begin() as conn:
                if delete_by_date_only:
                    delete_stmt = text(
                        f"""
                        DELETE FROM `{table}`
                        WHERE `sig_date` BETWEEN :start_date AND :end_date
                          AND `strategy_code` = :strategy
                        """
                    )
                    conn.execute(
                        delete_stmt,
                        {"start_date": start_d, "end_date": end_d, "strategy": STRATEGY_CODE_MA5_MA20_TREND},
                    )
                else:
                    delete_stmt = (
                        text(
                            f"""
                            DELETE FROM `{table}`
                            WHERE `sig_date` BETWEEN :start_date AND :end_date
                              AND `strategy_code` = :strategy
                              AND `code` IN :codes
                            """
                        ).bindparams(bindparam("codes", expanding=True))
                    )
                    chunk_size = 800
                    for i in range(0, len(codes_clean), chunk_size):
                        part_codes = codes_clean[i : i + chunk_size]
                        conn.execute(
                            delete_stmt,
                            {
                                "start_date": start_d,
                                "end_date": end_d,
                                "codes": part_codes,
                                "strategy": STRATEGY_CODE_MA5_MA20_TREND,
                            },
                        )
            self.logger.info(
                "signals_write_scope=window：事件已覆盖写入 %s~%s（%s）。",
                start_d,
                end_d,
                "all-codes" if delete_by_date_only else f"{len(codes_clean)} codes",
            )

        self.db_writer.write_dataframe(events_df, table, if_exists="append")

    def _write_candidates(self, latest_date: dt.date, signals: pd.DataFrame) -> None:
        tbl = self.params.candidates_table

        latest = signals[signals["date"].dt.date == latest_date].copy()
        if latest.empty:
            self.logger.warning("最新交易日 %s 无任何信号行，已跳过 candidates。", latest_date)
            return

        action_col = "final_action" if "final_action" in latest.columns else "signal"
        cands = latest[latest[action_col].isin(["BUY", "BUY_CONFIRM"])].copy()
        if cands.empty:
            self._clear_table(tbl)
            self.logger.info("最新交易日 %s 未筛出 BUY 候选（低频策略：空仓等待）。", latest_date)
            return

        cands = cands.sort_values(["vol_ratio", "macd_hist"], ascending=False)
        keep_cols = [
            "date",
            "code",
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
            "yearline_state",
            "risk_tag",
            "risk_note",
            "reason",
            "final_action",
            "final_reason",
            "final_cap",
        ]
        cands = cands[keep_cols].copy()
        cands = cands.rename(columns={"date": "sig_date"})
        cands["sig_date"] = pd.to_datetime(cands["sig_date"]).dt.date
        cands["code"] = cands["code"].astype(str)

        self._clear_table(tbl)
        self.db_writer.write_dataframe(cands, tbl, if_exists="append")

    def _precompute_chip_filter(self, signals: pd.DataFrame) -> None:
        """策略运行结束后触发筹码预计算，盘前准备 ready signals。"""

        if signals.empty:
            return

        chip_inputs = [
            "date",
            "sig_date",
            "code",
            "vol_ratio",
            "close",
            "ma20",
            "runup_pct",
            "fear_score",
            "pct_chg",
            "pct_change",
            "change_pct",
            "ret_1d",
            "ret",
        ]
        sig_for_chip = signals[[c for c in chip_inputs if c in signals.columns]].copy()
        if "sig_date" not in sig_for_chip.columns and "date" in sig_for_chip.columns:
            sig_for_chip = sig_for_chip.rename(columns={"date": "sig_date"})
        if "sig_date" not in sig_for_chip.columns:
            return
        sig_for_chip["sig_date"] = pd.to_datetime(sig_for_chip["sig_date"], errors="coerce")
        if "date" not in sig_for_chip.columns:
            sig_for_chip["date"] = sig_for_chip["sig_date"]
        sig_for_chip = sig_for_chip.dropna(subset=["sig_date", "code"])
        if sig_for_chip.empty:
            return

        try:
            ChipFilter().apply(sig_for_chip)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("筹码预计算失败（已跳过，不影响信号写入）：%s", exc)

    def run(self, *, force: bool = False) -> None:
        """执行 MA5-MA20 策略。

        - 默认遵循 config.yaml: strategy_ma5_ma20_trend.enabled。
        - 当 force=True 时，即便 enabled=false 也会执行（用于单独运行脚本）。
        """

        if (not force) and (not self.params.enabled):
            self.logger.info("strategy_ma5_ma20_trend.enabled=false，已跳过 MA5-MA20 策略运行。")
            return

        if force and (not self.params.enabled):
            self.logger.info("strategy_ma5_ma20_trend.enabled=false，但 force=True，仍将执行 MA5-MA20 策略。")

        self.logger.debug(
            "MA5-MA20 参数：lookback_days=%s indicator_window=%s",
            self.params.lookback_days,
            self.indicator_window,
        )

        daily_tbl = self._daily_table_name()
        latest_date = self._get_latest_trade_date()
        signal_codes = self._load_universe_codes(latest_date)
        recent_buy_codes = self._load_recent_buy_codes(latest_date)
        source = (self.params.universe_source or "top_liquidity").strip().lower()
        calc_codes_set: set[str] = set()
        if source == "all" and not signal_codes:
            calc_codes = []
        else:
            calc_codes_set = set(signal_codes).union(recent_buy_codes)
            calc_codes = sorted(calc_codes_set)
        self.logger.info(
            "MA5-MA20 策略：日线表=%s，信号选股池来源=%s，信号 codes=%s，快照补齐=%s（新增 %s 个近期 BUY code）",
            daily_tbl,
            self.params.universe_source,
            len(signal_codes),
            len(calc_codes),
            max(len(calc_codes_set) - len(set(signal_codes)), 0),
        )

        daily = self._load_daily_kline(calc_codes, latest_date)
        self.logger.info("MA5-MA20 策略：读取日线 %s 行（%s 只股票）。", len(daily), daily["code"].nunique())

        ind = self._compute_indicators(daily)
        fundamentals = self._load_fundamentals_latest() if self._fundamentals_cache is None else self._fundamentals_cache
        stock_basic = self._load_stock_basic() if self._stock_basic_cache is None else self._stock_basic_cache
        self._fundamentals_cache = fundamentals
        self._stock_basic_cache = stock_basic
        sig = self._generate_signals(ind, fundamentals, stock_basic)
        sig = sig.dropna(subset=["date", "code"])
        sig["date"] = pd.to_datetime(sig["date"], errors="coerce")
        sig["code"] = sig["code"].astype(str)
        sig = (
            sig.sort_values(["code", "date"])
            .drop_duplicates(subset=["code", "date"], keep="last")
            .reset_index(drop=True)
        )
        sig_for_candidates = sig.copy()
        sig_for_candidates["code"] = sig_for_candidates["code"].astype(str)

        sig["code"] = sig["code"].astype(str)
        snapshot_only_codes = calc_codes_set - set(signal_codes) if calc_codes_set else set()
        if snapshot_only_codes:
            sig_for_candidates = sig_for_candidates[~sig_for_candidates["code"].isin(snapshot_only_codes)]
        sig_for_write = sig.copy()
        if snapshot_only_codes:
            snapshot_mask = sig_for_write["code"].isin(snapshot_only_codes)
            latest_mask = sig_for_write["date"].dt.date == latest_date
            sig_for_write.loc[snapshot_mask, "signal"] = "SNAPSHOT"
            sig_for_write.loc[snapshot_mask, "final_action"] = "SNAPSHOT"
            sig_for_write.loc[snapshot_mask, "final_reason"] = "SNAPSHOT_ONLY"
            sig_for_write.loc[snapshot_mask, "reason"] = "SNAPSHOT_ONLY"
            sig_for_write = pd.concat(
                [sig_for_write[~snapshot_mask], sig_for_write[snapshot_mask & latest_mask]],
                ignore_index=True,
            )

        self._write_indicator_daily(latest_date, sig, calc_codes)
        self._write_signal_events(latest_date, sig_for_write, calc_codes)
        if not bool(getattr(self.params, "candidates_as_view", False)):
            self._write_candidates(latest_date, sig_for_candidates)

        self._precompute_chip_filter(sig_for_write)

        latest_sig = sig[sig["date"].dt.date == latest_date]
        dup_count = int(latest_sig.duplicated(subset=["code", "date"]).sum())
        self.logger.info(
            "MA5-MA20 策略自检：latest_sig 行数=%s，唯一 code 数=%s，重复(code,date)=%s",
            len(latest_sig),
            latest_sig["code"].nunique(),
            dup_count,
        )
        action_col = "final_action" if "final_action" in latest_sig.columns else "signal"
        action_series = latest_sig[action_col].fillna("HOLD").astype(str)
        counts = action_series.value_counts(dropna=False)
        order = ["BUY", "BUY_CONFIRM", "SELL", "REDUCE", "HOLD", "WAIT"]
        ordered_counts = {k: int(counts.get(k, 0)) for k in order}
        other_count = int(counts.drop(labels=order, errors="ignore").sum())
        self.logger.info(
            "MA5-MA20 策略完成：最终动作(final_action)统计（最新交易日：%s）："
            "BUY=%s, BUY_CONFIRM=%s, SELL=%s, REDUCE=%s, HOLD=%s, WAIT=%s, OTHER=%s",
            latest_date,
            ordered_counts["BUY"],
            ordered_counts["BUY_CONFIRM"],
            ordered_counts["SELL"],
            ordered_counts["REDUCE"],
            ordered_counts["HOLD"],
            ordered_counts["WAIT"],
            other_count,
        )
