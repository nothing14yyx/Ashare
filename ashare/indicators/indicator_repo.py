"""指标相关数据访问层 (Repository) 与计算服务。"""

from __future__ import annotations

import logging
from typing import Tuple, List, Dict, Any
import datetime as dt

import numpy as np
import pandas as pd
from sqlalchemy import text, bindparam

from ashare.indicators.indicator_utils import atr, macd
from ashare.core.db import MySQLWriter


class IndicatorRepository:
    """负责日线/周线指标相关的数据读写。"""

    def __init__(self, db_writer: MySQLWriter, logger: logging.Logger) -> None:
        self.db_writer = db_writer
        self.logger = logger

    def fetch_board_history(self, trade_date: str, lookback_days: int = 90) -> pd.DataFrame:
        """获取板块历史行情。"""
        start_date = (pd.to_datetime(trade_date) - dt.timedelta(days=lookback_days)).date().isoformat()
        stmt = text(
            """
            SELECT date, board_name, `收盘` as close
            FROM board_industry_hist_daily
            WHERE date BETWEEN :start AND :end
            ORDER BY date ASC
            """
        )
        with self.db_writer.engine.connect() as conn:
            return pd.read_sql(stmt, conn, params={"start": start_date, "end": trade_date})

    def fetch_board_spot(self) -> pd.DataFrame:
        """获取板块快照（用于补全 board_code）。"""
        stmt = text("SELECT board_name, board_code FROM board_industry_spot")
        with self.db_writer.engine.connect() as conn:
            return pd.read_sql(stmt, conn)

    def persist_board_rotation(self, trade_date: str, df: pd.DataFrame) -> None:
        """持久化板块轮动计算结果。"""
        if df.empty:
            return
        # 先清理旧数据
        with self.db_writer.engine.begin() as conn:
            conn.execute(text("DELETE FROM strategy_board_rotation WHERE date = :d"), {"d": trade_date})
        
        self.db_writer.write_dataframe(df, "strategy_board_rotation", if_exists="append")

    def fetch_index_trade_dates(self, code: str | list[str], start: dt.date, end: dt.date) -> pd.DataFrame:
        """获取指数交易日历。"""
        stmt = text("SELECT DISTINCT date as trade_date FROM history_index_daily_kline WHERE code IN :c AND date BETWEEN :s AND :e")
        with self.db_writer.engine.connect() as conn:
            return pd.read_sql(stmt.bindparams(bindparam("c", expanding=True)), 
                               conn, params={"c": code, "s": start.isoformat(), "e": end.isoformat()})

    def fetch_index_daily_kline(self, code: str | list[str], start: dt.date, end: dt.date) -> pd.DataFrame:
        """获取指数日线 K 线。"""
        stmt = text("SELECT * FROM history_index_daily_kline WHERE code IN :c AND date BETWEEN :s AND :e ORDER BY date ASC")
        with self.db_writer.engine.connect() as conn:
            return pd.read_sql(stmt.bindparams(bindparam("c", expanding=True)), 
                               conn, params={"c": code, "s": start.isoformat(), "e": end.isoformat()})


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


def _rsi(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    avg_loss = loss.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.rename("rsi14")


class TechnicalIndicatorService:
    """负责将原始行情转换为技术指标宽表。"""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger(__name__)

    def compute_all(self, df: pd.DataFrame, volume_ma_window: int = 5) -> pd.DataFrame:
        """为每个 code 批量计算均线、量能、MACD、KDJ、ATR 等核心指标。"""
        if df.empty:
            return df

        df = df.sort_values(["code", "date"]).copy()
        
        # 1. 均线类计算 (Vectorized)
        g_close = df.groupby("code", sort=False)["close"]
        g_vol = df.groupby("code", sort=False)["volume"]

        df["ma5"] = g_close.rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
        df["ma10"] = g_close.rolling(10, min_periods=1).mean().reset_index(level=0, drop=True)
        df["ma20"] = g_close.rolling(20, min_periods=1).mean().reset_index(level=0, drop=True)
        df["ma60"] = g_close.rolling(60, min_periods=1).mean().reset_index(level=0, drop=True)
        df["ma250"] = g_close.rolling(250, min_periods=1).mean().reset_index(level=0, drop=True)

        # 2. 量能类
        df["vol_ma"] = g_vol.rolling(volume_ma_window, min_periods=1).mean().reset_index(level=0, drop=True)
        df["vol_ratio"] = df["volume"] / df["vol_ma"].replace(0, np.nan)
        df["avg_volume_20"] = g_vol.rolling(20, min_periods=1).mean().reset_index(level=0, drop=True)
        
        # === 核心增强：一分钟选股法相关指标 ===
        # 成交额全市场排名 (按日分组)
        df["turnover_rank"] = df.groupby("date")["amount"].rank(ascending=False)
        df["is_top_turnover"] = df["turnover_rank"] <= 50 # 略微放宽到前50名
        
        # 5日线位置
        df["above_ma5"] = df["close"] > df["ma5"]

        # 3. 价格变动与涨停统计
        df["ret_1"] = g_close.pct_change(1)
        df["ret_10"] = g_close.pct_change(10)
        df["ret_20"] = g_close.pct_change(20)
        
        # 简单的涨停统计 (假设 9.9% 以上算涨停)
        df["is_limit_up"] = df["ret_1"] >= 0.099
        df["limit_up_cnt_20"] = df.groupby("code")["is_limit_up"].rolling(20, min_periods=1).sum().reset_index(level=0, drop=True)

        # 4. 衍生指标 (MACD, KDJ, ATR, RSI)
        def _add_oscillators(sub: pd.DataFrame) -> pd.DataFrame:
            group_code = getattr(sub, "name", None)
            close = sub["close"]
            high = sub["high"]
            low = sub["low"]
            preclose = sub.get("preclose")
            if preclose is None:
                preclose = close.shift(1)

            dif, dea, hist = macd(close)
            k, d, j = _kdj(high, low, close)
            atr_val = atr(high, low, preclose)
            rsi = _rsi(close)

            sub = sub.copy()
            sub["macd_dif"] = dif
            sub["macd_dea"] = dea
            sub["macd_hist"] = hist
            sub["prev_macd_hist"] = hist.shift(1)
            sub["kdj_k"] = k
            sub["kdj_d"] = d
            sub["kdj_j"] = j
            sub["atr14"] = atr_val
            sub["rsi14"] = rsi
            
            # 计算 MA20 乖离率等补充指标
            sub["ma20_bias"] = (sub["close"] - sub["ma20"]) / sub["ma20"].replace(0, np.nan)
            
            # 恐慌/贪婪分：结合 KDJ 与量比
            # 这里简单代理逻辑：KDJ 高位且量缩 = 贪婪/过热；KDJ 低位且量缩 = 磨底
            sub["fear_score"] = ((sub["kdj_k"] - 50.0) / 50.0) * (1 - sub.get("vol_ratio", 1.0))
            sub["fear_score"] = sub["fear_score"].clip(-3, 3)

            # 年线状态
            sub["yearline_state"] = np.where(sub["close"] > sub["ma250"], "ABOVE", "BELOW")

            if "code" not in sub.columns:
                sub["code"] = group_code
            return sub

        try:
            df = (
                df.groupby("code", group_keys=False)
                .apply(_add_oscillators, include_groups=False)
                .reset_index(drop=True)
            )
        except TypeError:
            df = df.groupby("code", group_keys=False).apply(_add_oscillators).reset_index(drop=True)
            
        return df
