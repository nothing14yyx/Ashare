"""趋势跟随 顺势趋势波段系统

核心逻辑：趋势确认 + 主力行为识别 + 风险门控
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

from ashare.strategies.chip_filter import ChipFilter
from ashare.core.config import get_section
from ashare.core.db import DatabaseConfig, MySQLWriter
from ashare.indicators.indicator_utils import atr, consecutive_true, macd
from ashare.core.schema_manager import TABLE_STRATEGY_CHIP_FILTER
from ashare.strategies.strategy_data_repo import StrategyDataRepository
from ashare.strategies.strategy_store import StrategyStore
from ashare.strategies.ma5_ma20_params import MA5MA20Params
from ashare.utils import setup_logger


def _split_exchange_symbol(code: str) -> Tuple[str, str]:
    code_s = str(code or "").strip()
    if not code_s: return "", ""
    if "." in code_s:
        ex, sym = code_s.split(".", 1)
        return ex.lower(), sym
    return "", code_s


class TrendFollowingStrategyRunner:
    """趋势跟随策略运行器：基于成品指标生成买卖信号。"""

    def __init__(self) -> None:
        self.logger = setup_logger()
        self.params = MA5MA20Params.from_config()
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())
        self.store = StrategyStore(self.db_writer, self.params, self.logger)
        self.data_repo = StrategyDataRepository(self.db_writer, self.logger)

    def _get_latest_trade_date(self) -> dt.date:
        stmt = text("SELECT MAX(`trade_date`) AS max_date FROM `strategy_indicator_daily`")
        with self.db_writer.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
        if not row or not row.get("max_date"):
            raise RuntimeError("指标表为空，请先运行 Pipeline 2")
        return pd.to_datetime(row["max_date"]).date()

    def _generate_signals(
        self,
        df: pd.DataFrame,
        fundamentals: pd.DataFrame | None = None,
        stock_basic: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """核心信号生成逻辑。"""
        p = self.params
        
        # 1. 基础状态准备
        df = df.sort_values(["code", "date"]).copy()
        prev_ma5 = df.groupby("code")["ma5"].shift(1)
        prev_ma20 = df.groupby("code")["ma20"].shift(1)
        prev_macd_hist = df.groupby("code")["macd_hist"].shift(1)
        prev_vol = df.groupby("code")["volume"].shift(1)
        pct_chg = df.groupby("code")["close"].pct_change()

        # 2. 趋势与金叉判定
        trend_ok = (df["close"] > df["ma60"]) & (df["ma20"] > df["ma60"])
        cross_up = (df["ma5"] > df["ma20"]) & (prev_ma5 <= prev_ma20)
        macd_ok = (df["macd_hist"] > 0) | ((df["macd_hist"] > prev_macd_hist) & (df["macd_hist"] > -0.1))
        vol_ok = df["vol_ratio"] >= 1.2

        # 3. 量价铁律增强 (主力信号)
        # 地量长阳
        low_vol_long_bull = (df["vol_ratio"] < 0.8) & (pct_chg > 0.05)
        # 倍量放大
        double_vol = (df["volume"] > prev_vol * 1.9)
        # 近15天有涨停 (股性)
        is_limit_up = pct_chg > 0.098
        has_limit_up_15d = is_limit_up.groupby(df["code"]).rolling(15, min_periods=1).max().reset_index(level=0, drop=True).fillna(0).astype(bool)
        
        main_force_ok = low_vol_long_bull | (double_vol & has_limit_up_15d)

        # 4. 买入判定
        buy_cross = trend_ok & cross_up & vol_ok & macd_ok & main_force_ok
        buy_pullback = trend_ok & (df["ma20_bias"].abs() < 0.02) & (df["ma5"] > prev_ma5) & macd_ok
        buy_macd_confirm = (df["macd_hist"] > 0) & (prev_macd_hist <= 0) & (df["close"] > df["ma20"])

        # 5. 卖出与风险判定
        dead_cross = (df["ma5"] < df["ma20"]) & (prev_ma5 >= prev_ma20)
        stagnation_at_high = (df["vol_ratio"] > 2.0) & (pct_chg.abs() < 0.01) & (df["ma20_bias"] > 0.1)
        sell = dead_cross | stagnation_at_high | ((df["close"] < df["ma20"]) & vol_ok)
        
        reduce_mask = (df["ma5"] < df["ma20"]) & (df["close"] < df["ma20"]) & (df["vol_ratio"] < 1.0) & (~dead_cross)

        # 6. 构造输出
        out = df.copy()
        out["signal"] = np.select(
            [sell, reduce_mask, buy_cross | buy_pullback, buy_macd_confirm],
            ["SELL", "REDUCE", "BUY", "BUY_CONFIRM"],
            default="HOLD"
        )
        
        # 补全高级字段
        out["macd_event"] = np.select(
            [(df["macd_hist"] > 0) & (prev_macd_hist <= 0), (df["macd_hist"] < 0) & (prev_macd_hist >= 0)],
            ["HIST_UP", "HIST_DOWN"],
            default=None
        )
        out["wave_type"] = np.select(
            [(df["rsi14"] < 30) & (df["ma20_bias"] < -0.05), (df["ma20_bias"].abs() < 0.02)],
            ["OVERSOLD", "RANGE"],
            default="TRENDING"
        )
        out["stop_ref"] = out["close"] - 2.0 * out["atr14"]
        
        # 记录原因
        reasons = pd.Series("观望", index=df.index)
        reasons = reasons.mask(buy_cross, "趋势跟随：主力倍量/地量拉升金叉")
        reasons = reasons.mask(buy_pullback, "趋势跟随：缩量回踩MA20")
        reasons = reasons.mask(buy_macd_confirm, "趋势跟随：MACD翻红确认")
        reasons = reasons.mask(stagnation_at_high, "风险：高位放量滞涨")
        out["reason"] = reasons

        return out

    def _attach_chip_factors(self, signals: pd.DataFrame) -> pd.DataFrame:
        """从 strategy_chip_filter 表中关联筹码因子。"""
        if signals.empty: return signals
        
        codes = signals["code"].unique().tolist()
        latest_date = signals["date"].dt.date.max()
        
        stmt = text("SELECT * FROM strategy_chip_filter WHERE sig_date = :d AND code IN :codes")
        try:
            with self.db_writer.engine.connect() as conn:
                chip_df = pd.read_sql(stmt.bindparams(bindparam("codes", expanding=True)), 
                                     conn, params={"d": latest_date, "codes": codes})
            
            if not chip_df.empty:
                chip_df = chip_df.rename(columns={"sig_date": "date"})
                chip_df["date"] = pd.to_datetime(chip_df["date"])
                signals = signals.merge(chip_df, on=["date", "code"], how="left", suffixes=("", "_chip"))
        except Exception as e:
            self.logger.warning("关联筹码因子失败: %s", e)
            
        return signals

    def _decide_final_action(self, df: pd.DataFrame) -> pd.DataFrame:
        """最终决策层：平衡个股筹码与板块轮动地位。"""
        out = df.copy()
        out["final_action"] = out["signal"]
        out["final_reason"] = out["reason"]
        
        # 1. 准备权重因子
        # 确保 chip_score 列存在并转换为数值 Series
        if "chip_score" in out.columns:
            chip_series = pd.to_numeric(out["chip_score"], errors="coerce").fillna(0)
        else:
            chip_series = pd.Series(0.0, index=out.index)
        
        chip_status = np.select(
            [chip_series >= 0.5, chip_series <= -0.5],
            [1, -1],
            default=0
        )
        
        # 板块因子：从 rotation_phase 转换
        # 假设：leader=2, improving=1, weakening=-1, lagging=-2
        board_phase = out.get("rotation_phase", "neutral")
        board_factor = np.select(
            [board_phase == "leader", board_phase == "improving", board_phase == "weakening", board_phase == "lagging"],
            [2, 1, -1, -2],
            default=0
        )
        
        # 2. 综合质量分 (Quality Score)
        # 逻辑：板块权重略高于个股筹码（顺势而为）
        quality_score = (board_factor * 1.5) + (chip_status * 1.0)
        
        # 3. 动态调整信号
        is_buy = out["signal"].isin(["BUY", "BUY_CONFIRM"])
        
        # 情况 A：板块和筹码双差 -> 拦截
        stop_mask = is_buy & (quality_score <= -2)
        out.loc[stop_mask, "final_action"] = "WAIT"
        out.loc[stop_mask, "final_reason"] = out.loc[stop_mask, "final_reason"] + "|共振走弱(板块+筹码)"
        
        # 情况 B：板块强但筹码差 -> 允许轻仓博弈
        reduce_cap_mask = is_buy & (board_factor >= 1) & (chip_status == -1)
        # (此时保持 BUY 状态，但后面会降低 final_cap)
        
        # 4. 动态计算仓位 (final_cap)
        # 基础仓位 0.5
        base_cap = 0.5
        # 根据质量分线性调整仓位 (从 0.1 到 0.8)
        # 简单的线性映射
        calculated_cap = base_cap + (quality_score * 0.1)
        out["quality_score"] = quality_score # 显式存入 DF
        out["final_cap"] = np.where(out["final_action"].isin(["BUY", "BUY_CONFIRM"]), calculated_cap, 0.0)
        out["final_cap"] = out["final_cap"].clip(0.0, 0.8) # 封顶 0.8，防止过热
        
        return out

    def run(self, *, force: bool = False) -> None:
        """执行趋势跟随策略。"""
        try:
            latest_date = self._get_latest_trade_date()
            
            # 1. 获取候选池
            stmt = text("SELECT `code` FROM `a_share_top_liquidity` WHERE `trade_date` = :d")
            with self.db_writer.engine.connect() as conn:
                df_liq = pd.read_sql(stmt, conn, params={"d": latest_date})
            
            candidate_codes = df_liq["code"].unique().tolist() if not df_liq.empty else []
            if not candidate_codes:
                self.logger.warning("当前无候选流动性标的。")
                return

            # 2. 加载指标数据
            ind = self.data_repo.load_indicator_daily(candidate_codes, latest_date, lookback=100)
            if ind.empty:
                self.logger.error("指标加载失败。")
                return

            # === 核心增强：加载板块轮动信息 ===
            stmt_rot = text("""
                SELECT b.code, r.rotation_phase 
                FROM dim_stock_board_industry b
                JOIN strategy_board_rotation r ON b.board_name = r.board_name
                WHERE r.date = :d
            """)
            try:
                with self.db_writer.engine.connect() as conn:
                    df_rot = pd.read_sql(stmt_rot, conn, params={"d": latest_date})
                if not df_rot.empty:
                    ind = ind.merge(df_rot, on="code", how="left")
            except Exception as e:
                self.logger.warning("读取板块轮动数据失败（将以 neutral 运行）: %s", e)

            # 3. 产生初步信号
            sig = self._generate_signals(ind)
            
            # 4. 关联筹码因子
            sig = self._attach_chip_factors(sig)
            
            # 5. 最终执行决策 (现在包含板块权重了)
            sig = self._decide_final_action(sig)
            
            # 6. 只保留当天的信号写入
            sig_to_write = sig[sig["date"].dt.date == latest_date].copy()
            
            # 补齐 extra_json (包含平衡后的决策证据)
            extras = []
            for _, row in sig_to_write.iterrows():
                payload = {
                    "chip_score": row.get("chip_score"),
                    "board_phase": row.get("rotation_phase"),
                    "quality_score": row.get("quality_score"),
                    "wave_type": row.get("wave_type")
                }
                extras.append(json.dumps({k: v for k, v in payload.items() if v is not None}, ensure_ascii=False))
            sig_to_write["extra_json"] = extras

            # 7. 入库
            self.store.write_signal_events(latest_date, sig_to_write, candidate_codes)
            
            self.logger.info("趋势跟随策略扫描完成，信号已更新（已平衡筹码与板块权重）。")
            
        except Exception as e:
            self.logger.error("策略执行异常: %s", e, exc_info=True)
