"""MA5-MA20 顺势趋势波段系统

把“顺势 + MA5/MA20 触发 + 量价/指标过滤 + 风险第一 + 低频交易”落到你的程序里。

数据依赖（你现有的日线窗口表）：
  history_recent_{lookback_days}_days  （默认 lookback_days=365 -> history_recent_365_days）

输出两张 MySQL 表：
  - strategy_ma5_ma20_signals：最新交易日每只股票的信号快照（BUY/SELL/HOLD + reason）
  - strategy_ma5_ma20_candidates：仅保留 BUY 候选清单（符合“多数时间等待，少数时间出手”）

说明：
  - 本实现先做“日线低频版本”作为选股/清单层。
  - 若要更严格的“分钟线执行层”（例如 60 分钟入场 / 30 分钟离场），建议只对 candidates 按需拉分钟线。
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

from .config import get_section
from .db import DatabaseConfig, MySQLWriter
from .utils import setup_logger


@dataclass(frozen=True)
class MA5MA20Params:
    """策略参数（支持从 config.yaml 的 strategy_ma5_ma20_trend 节覆盖）。"""

    enabled: bool = False
    universe_source: str = "top_liquidity"  # top_liquidity / universe / all
    lookback_days: int = 365

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

    # 输出表
    signals_table: str = "strategy_ma5_ma20_signals"
    candidates_table: str = "strategy_ma5_ma20_candidates"

    @classmethod
    def from_config(cls) -> "MA5MA20Params":
        sec = get_section("strategy_ma5_ma20_trend")
        if not sec:
            return cls()
        kwargs = {}
        for k in cls.__dataclass_fields__.keys():  # type: ignore[attr-defined]
            if k in sec:
                kwargs[k] = sec[k]
        return cls(**kwargs)


def _ensure_datetime(series: pd.Series) -> pd.Series:
    if np.issubdtype(series.dtype, np.datetime64):
        return series
    return pd.to_datetime(series, errors="coerce")


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


class MA5MA20StrategyRunner:
    """从 MySQL 读取日线 → 计算指标 → 生成 MA5-MA20 信号 → 写回 MySQL。"""

    def __init__(self) -> None:
        self.logger = setup_logger()
        self.params = MA5MA20Params.from_config()
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())

    def _daily_table_name(self) -> str:
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

    def _load_daily_kline(self, codes: List[str], end_date: dt.date) -> pd.DataFrame:
        """读取最近 lookback_days 的日线数据。"""
        tbl = self._daily_table_name()
        lookback = int(self.params.lookback_days)
        start_date = end_date - dt.timedelta(days=int(lookback * 2))  # 给交易日留冗余

        # codes 太多时避免 IN 超长：先按日期取，再在 Pandas 侧过滤
        use_in = len(codes) <= 2000

        if use_in:
            stmt = (
                text(
                    f"""
                    SELECT
                        `date`,`code`,`open`,`high`,`low`,`close`,`preclose`,`volume`,`amount`,
                        `tradestatus`,`isST`,`pctChg`
                    FROM `{tbl}`
                    WHERE `code` IN :codes AND `date` BETWEEN :start_date AND :end_date
                    ORDER BY `code`,`date`
                    """
                )
                .bindparams(bindparam("codes", expanding=True))
            )
            params = {"codes": codes, "start_date": start_date, "end_date": end_date}
        else:
            stmt = text(
                f"""
                SELECT
                    `date`,`code`,`open`,`high`,`low`,`close`,`preclose`,`volume`,`amount`,
                    `tradestatus`,`isST`,`pctChg`
                FROM `{tbl}`
                WHERE `date` BETWEEN :start_date AND :end_date
                ORDER BY `code`,`date`
                """
            )
            params = {"start_date": start_date, "end_date": end_date}

        with self.db_writer.engine.begin() as conn:
            df = pd.read_sql(stmt, conn, params=params)

        if df.empty:
            raise RuntimeError(f"未能从 {tbl} 读取到任何日线数据。")

        df["date"] = _ensure_datetime(df["date"])
        df["code"] = df["code"].astype(str)

        df = _to_numeric(
            df,
            ["open", "high", "low", "close", "preclose", "volume", "amount", "pctChg"],
        )
        df = df.dropna(subset=["date", "code", "close", "high", "low", "preclose"]).copy()

        if (not use_in) and codes:
            df = df[df["code"].isin(set(map(str, codes)))].copy()

        df = df.sort_values(["code", "date"]).reset_index(drop=True)
        df = df.groupby("code", group_keys=False).tail(lookback).reset_index(drop=True)
        return df

    def _compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """为每个 code 计算均线、量能、MACD、KDJ、ATR。"""
        df = df.sort_values(["code", "date"]).copy()

        g_close = df.groupby("code")["close"]
        g_vol = df.groupby("code")["volume"]

        df["ma5"] = g_close.transform(lambda s: s.rolling(5, min_periods=5).mean())
        df["ma10"] = g_close.transform(lambda s: s.rolling(10, min_periods=10).mean())
        df["ma20"] = g_close.transform(lambda s: s.rolling(20, min_periods=20).mean())
        df["ma60"] = g_close.transform(lambda s: s.rolling(60, min_periods=60).mean())
        df["ma250"] = g_close.transform(lambda s: s.rolling(250, min_periods=250).mean())

        vol_win = int(self.params.volume_ma_window)
        df["vol_ma"] = g_vol.transform(lambda s: s.rolling(vol_win, min_periods=vol_win).mean())
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

            sub = sub.copy()
            sub["macd_dif"] = dif
            sub["macd_dea"] = dea
            sub["macd_hist"] = hist
            sub["kdj_k"] = k
            sub["kdj_d"] = d
            sub["kdj_j"] = j
            sub["atr14"] = atr
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

    def _generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成 BUY/SELL/HOLD 信号，并给出 reason。"""
        p = self.params

        # 1) 趋势过滤：多头排列 + 价格站上中长均线
        trend_ok = (
            (df["close"] > df["ma60"])
            & (df["close"] > df["ma250"])
            & (df["ma20"] > df["ma60"])
            & (df["ma60"] > df["ma250"])
        )

        # 2) MA5/MA20 金叉/死叉
        ma5_gt_ma20 = (df["ma5"] > df["ma20"]).astype(bool)
        prev_ma5_gt_ma20 = (
            ma5_gt_ma20.groupby(df["code"])
            .shift(1)
            .fillna(False)
            .infer_objects(copy=False)
            .astype(bool)
        )
        cross_up = ma5_gt_ma20 & (~prev_ma5_gt_ma20)
        cross_down = (~ma5_gt_ma20) & prev_ma5_gt_ma20

        # 3) 放量确认
        vol_ok = df["vol_ratio"] >= float(p.volume_ratio_threshold)

        # 4) MACD 过滤（DIF 上穿 DEA 或 HIST>0）
        macd_gt = (df["macd_dif"] > df["macd_dea"]).astype(bool)
        prev_macd_gt = (
            macd_gt.groupby(df["code"])
            .shift(1)
            .fillna(False)
            .infer_objects(copy=False)
            .astype(bool)
        )
        macd_cross_up = macd_gt & (~prev_macd_gt)
        macd_ok = macd_cross_up | (df["macd_hist"] > 0)

        # 5) KDJ 低位金叉（可选增强：只作为 reason 标记）
        kdj_gt = (df["kdj_k"] > df["kdj_d"]).astype(bool)
        prev_kdj_gt = (
            kdj_gt.groupby(df["code"])
            .shift(1)
            .fillna(False)
            .infer_objects(copy=False)
            .astype(bool)
        )
        kdj_cross_up = kdj_gt & (~prev_kdj_gt)
        kdj_low = df["kdj_k"] <= float(p.kdj_low_threshold)
        kdj_ok = kdj_cross_up & kdj_low

        # 6) 趋势回踩（close 接近 MA20 + MA5 向上）
        pullback_band = float(p.pullback_band)
        pullback_near = ((df["close"] - df["ma20"]).abs() / df["ma20"]) <= pullback_band
        ma5_up = (df["ma5"] - df["ma5"].groupby(df["code"]).shift(1)) > 0

        buy_cross = trend_ok & cross_up & vol_ok & macd_ok
        buy_pullback = trend_ok & pullback_near & ma5_up & macd_ok

        # 卖出：死叉 或 跌破 MA20 且放量（趋势破坏）
        sell = cross_down | ((df["close"] < df["ma20"]) & vol_ok)

        # 用 pandas Series 拼接原因，避免 numpy.ndarray 没有 strip 的问题
        reason = pd.Series("", index=df.index, dtype="object")
        reason = reason.mask(buy_cross, "MA5上穿MA20（金叉）+放量+MACD")

        def _append(base: pd.Series, cond: pd.Series, text_: str) -> pd.Series:
            add = np.where(base.eq(""), text_, base + "|" + text_)
            return base.mask(cond, pd.Series(add, index=base.index, dtype="object"))

        reason = _append(reason, buy_pullback, "趋势回踩MA20")
        reason = _append(reason, kdj_ok & (buy_cross | buy_pullback), "KDJ低位金叉")

        # 卖出原因优先覆盖
        reason = reason.mask(sell, "MA5下穿MA20（死叉）或跌破MA20放量")
        reason = reason.mask(reason.eq(""), "观望")

        signal = np.where(sell, "SELL", np.where(buy_cross | buy_pullback, "BUY", "HOLD"))

        out = df.copy()
        out["signal"] = signal
        out["reason"] = reason.to_numpy()
        # 风险参考：2*ATR 作为“初始止损价”参考（你也可以换成 MA20 跌破止损）
        out["stop_ref"] = out["close"] - 2.0 * out["atr14"]
        return out

    def _ensure_signals_table(self, table: str) -> None:
        """确保 signals 表存在（包含主键，避免重复）。"""
        create_stmt = text(
            f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
              `date` DATE NOT NULL,
              `code` VARCHAR(20) NOT NULL,
              `close` DOUBLE NULL,
              `volume` DOUBLE NULL,
              `amount` DOUBLE NULL,
              `ma5` DOUBLE NULL,
              `ma10` DOUBLE NULL,
              `ma20` DOUBLE NULL,
              `ma60` DOUBLE NULL,
              `ma250` DOUBLE NULL,
              `vol_ratio` DOUBLE NULL,
              `macd_dif` DOUBLE NULL,
              `macd_dea` DOUBLE NULL,
              `macd_hist` DOUBLE NULL,
              `kdj_k` DOUBLE NULL,
              `kdj_d` DOUBLE NULL,
              `kdj_j` DOUBLE NULL,
              `atr14` DOUBLE NULL,
              `stop_ref` DOUBLE NULL,
              `signal` VARCHAR(10) NULL,
              `reason` VARCHAR(255) NULL,
              PRIMARY KEY (`date`, `code`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        with self.db_writer.engine.begin() as conn:
            conn.execute(create_stmt)

    def _ensure_candidates_table(self, table: str) -> None:
        """确保 candidates 表存在（用于无 BUY 时清空表）。"""
        create_stmt = text(
            f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
              `date` DATE NOT NULL,
              `code` VARCHAR(20) NOT NULL,
              `close` DOUBLE NULL,
              `ma5` DOUBLE NULL,
              `ma20` DOUBLE NULL,
              `ma60` DOUBLE NULL,
              `ma250` DOUBLE NULL,
              `vol_ratio` DOUBLE NULL,
              `macd_hist` DOUBLE NULL,
              `kdj_k` DOUBLE NULL,
              `kdj_d` DOUBLE NULL,
              `atr14` DOUBLE NULL,
              `stop_ref` DOUBLE NULL,
              `reason` VARCHAR(255) NULL,
              PRIMARY KEY (`date`, `code`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        with self.db_writer.engine.begin() as conn:
            conn.execute(create_stmt)

    def _clear_table(self, table: str) -> None:
        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE `{table}`"))
        except Exception:
            pass

    def _write_signals(self, latest_date: dt.date, signals: pd.DataFrame) -> None:
        tbl = self.params.signals_table
        self._ensure_signals_table(tbl)

        latest = signals[signals["date"].dt.date == latest_date].copy()
        if latest.empty:
            self.logger.warning("未找到最新交易日 %s 的信号行。", latest_date)
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
            "kdj_k",
            "kdj_d",
            "kdj_j",
            "atr14",
            "stop_ref",
            "signal",
            "reason",
        ]
        latest = latest[keep_cols].copy()
        latest["date"] = pd.to_datetime(latest["date"]).dt.date
        latest["code"] = latest["code"].astype(str)

        # 先删除当天同 code 的旧数据，再 append 写入（保证幂等）
        delete_stmt = (
            text(f"DELETE FROM `{tbl}` WHERE `date` = :d AND `code` IN :codes")
            .bindparams(bindparam("codes", expanding=True))
        )
        codes = latest["code"].tolist()
        with self.db_writer.engine.begin() as conn:
            conn.execute(delete_stmt, {"d": latest_date, "codes": codes})

        self.db_writer.write_dataframe(latest, tbl, if_exists="append")

    def _write_candidates(self, latest_date: dt.date, signals: pd.DataFrame) -> None:
        tbl = self.params.candidates_table
        self._ensure_candidates_table(tbl)

        latest = signals[signals["date"].dt.date == latest_date].copy()
        if latest.empty:
            self.logger.warning("最新交易日 %s 无任何信号行，已跳过 candidates。", latest_date)
            return

        cands = latest[latest["signal"] == "BUY"].copy()
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
            "reason",
        ]
        cands = cands[keep_cols].copy()
        cands["date"] = pd.to_datetime(cands["date"]).dt.date
        cands["code"] = cands["code"].astype(str)

        self._clear_table(tbl)
        self.db_writer.write_dataframe(cands, tbl, if_exists="append")

    def run(self) -> None:
        if not self.params.enabled:
            self.logger.info("strategy_ma5_ma20_trend.enabled=false，已跳过 MA5-MA20 策略运行。")
            return

        daily_tbl = self._daily_table_name()
        latest_date = self._get_latest_trade_date()
        codes = self._load_universe_codes(latest_date)
        self.logger.info(
            "MA5-MA20 策略：日线表=%s，选股池来源=%s，codes=%s",
            daily_tbl,
            self.params.universe_source,
            len(codes),
        )

        daily = self._load_daily_kline(codes, latest_date)
        self.logger.info("MA5-MA20 策略：读取日线 %s 行（%s 只股票）。", len(daily), daily["code"].nunique())

        ind = self._compute_indicators(daily)
        sig = self._generate_signals(ind)

        self._write_signals(latest_date, sig)
        self._write_candidates(latest_date, sig)

        latest_sig = sig[sig["date"].dt.date == latest_date]
        cnt_buy = int((latest_sig["signal"] == "BUY").sum())
        cnt_sell = int((latest_sig["signal"] == "SELL").sum())
        cnt_hold = int((latest_sig["signal"] == "HOLD").sum())
        self.logger.info(
            "MA5-MA20 策略完成：%s BUY / %s SELL / %s HOLD（最新交易日：%s）",
            cnt_buy,
            cnt_sell,
            cnt_hold,
            latest_date,
        )
