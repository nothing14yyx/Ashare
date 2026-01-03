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
from ashare.core.schema_manager import STRATEGY_CODE_MA5_MA20_TREND
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
        """核心信号生成逻辑（HardGate/BaseSignal/SoftFactor 分层）。"""
        p = self.params

        # 1) 基础状态准备
        df = df.sort_values(["code", "date"]).copy()
        required_cols = ["close", "ma5", "ma20", "ma60", "ma20_bias", "vol_ratio", "atr14"]
        for col in required_cols:
            if col not in df.columns:
                df[col] = np.nan
        for col in ["rsi14", "macd_hist", "volume", "ma250"]:
            if col not in df.columns:
                df[col] = np.nan
        if "kdj_k" not in df.columns:
            df["kdj_k"] = np.nan

        prev_ma5 = df.groupby("code")["ma5"].shift(1)
        prev_ma20 = df.groupby("code")["ma20"].shift(1)
        prev_macd_hist = df.groupby("code")["macd_hist"].shift(1) if "macd_hist" in df.columns else None
        prev_vol = df.groupby("code")["volume"].shift(1) if "volume" in df.columns else None
        pct_chg = df.groupby("code")["close"].pct_change()

        def _as_bool_series(series: pd.Series | None) -> pd.Series:
            if series is None:
                return pd.Series(False, index=df.index)
            s = pd.Series(series, index=df.index)
            try:
                s = s.astype("boolean")
            except Exception:
                s = s.astype(bool)
            s = s.fillna(False)
            return s.astype(bool)

        # 2) HardGate（仅极少门槛）
        missing_mask = df[required_cols].isna().any(axis=1)
        untradeable_mask = _as_bool_series(df.get("one_word_limit_up"))
        env_gate_mask = pd.Series(False, index=df.index)
        for col in ["env_gate_action", "env_final_gate_action", "gate_action", "env_action"]:
            if col in df.columns:
                env_gate_mask = env_gate_mask | df[col].fillna("").astype(str).str.upper().isin(
                    ["ALLOW_NONE", "STOP"]
                )
        hard_gate_mask = missing_mask | untradeable_mask | env_gate_mask

        # 3) BaseSignal（仅核心买卖点，不新增硬过滤）
        close = df["close"]
        ma5 = df["ma5"]
        ma20 = df["ma20"]
        ma60 = df["ma60"]

        trend_ok = (close > ma60) & (ma20 > ma60)
        cross_up = (ma5 > ma20) & (prev_ma5 <= prev_ma20)
        buy_cross = trend_ok & cross_up
        buy_pullback = trend_ok & (df["ma20_bias"].abs() < p.pullback_band) & (ma5 > prev_ma5)
        base_buy = buy_cross | buy_pullback

        dead_cross = (ma5 < ma20) & (prev_ma5 >= prev_ma20)
        stagnation_at_high = (
            (df["vol_ratio"] >= p.stagnation_vol_ratio_threshold)
            & (pct_chg.abs() < p.stagnation_pct_abs_threshold)
            & (df["ma20_bias"] > p.stagnation_ma20_bias_threshold)
        )
        base_sell = dead_cross | stagnation_at_high | (close < ma20)
        base_reduce = (ma5 < ma20) & (close < ma20) & (~dead_cross)

        # 4) SoftFactor（升级/降级/加权/风控标签）
        vol_ok = df["vol_ratio"] >= p.volume_ratio_threshold
        macd_hist = df["macd_hist"] if "macd_hist" in df.columns else pd.Series(0.0, index=df.index)
        if prev_macd_hist is None:
            prev_macd_hist = pd.Series(0.0, index=df.index)
        macd_ok = (macd_hist > 0) | ((macd_hist > prev_macd_hist) & (macd_hist > -0.1))

        low_vol_long_bull = (df["vol_ratio"] < 0.8) & (pct_chg > 0.05)
        double_vol = (
            (df["volume"] > prev_vol * 1.9)
            if prev_vol is not None and "volume" in df.columns
            else pd.Series(False, index=df.index)
        )
        is_limit_up = pct_chg > 0.098
        has_limit_up_15d = (
            is_limit_up.groupby(df["code"])
            .rolling(15, min_periods=1)
            .max()
            .reset_index(level=0, drop=True)
            .fillna(0)
            .astype(bool)
        )
        main_force_ok = low_vol_long_bull | (double_vol & has_limit_up_15d)

        bull_engulf = _as_bool_series(df.get("bull_engulf"))
        bear_engulf = _as_bool_series(df.get("bear_engulf"))
        engulf_body_atr = df["engulf_body_atr"] if "engulf_body_atr" in df.columns else pd.Series(np.nan, index=df.index)

        engulf_confirm = (
            bull_engulf
            & (df["ma20_bias"].abs() <= p.engulf_near_ma20_band)
            & (engulf_body_atr >= p.engulf_body_atr_threshold)
            & (df["vol_ratio"] >= p.engulf_vol_ratio_threshold)
        )
        if p.buy_confirm_require_ma5_ge_ma20:
            engulf_confirm = engulf_confirm & (ma5 >= ma20)

        risk_reversal = (
            bear_engulf
            & (df["ma20_bias"] >= p.engulf_risk_ma20_bias_threshold)
            & (engulf_body_atr >= p.engulf_body_atr_threshold)
        )

        wyckoff_score = pd.Series(0.0, index=df.index)
        wyckoff_confirm = pd.Series(False, index=df.index)
        if bool(getattr(p, "wyckoff_enabled", False)):
            try:
                from ashare.strategies.ma_wyckoff_model import MAWyckoffStrategy

                model = MAWyckoffStrategy()

                def _apply(sub: pd.DataFrame) -> pd.DataFrame:
                    return model.build_factors(sub)

                try:
                    wyckoff_df = (
                        df.groupby("code", group_keys=False)
                        .apply(_apply, include_groups=False)
                    )
                except TypeError:
                    wyckoff_df = df.groupby("code", group_keys=False).apply(_apply)
                if "wyckoff_score" in wyckoff_df.columns:
                    wyckoff_score = wyckoff_df["wyckoff_score"].fillna(0.0)
                if "wyckoff_confirm" in wyckoff_df.columns:
                    wyckoff_confirm = wyckoff_df["wyckoff_confirm"].fillna(False).astype(bool)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("wyckoff 软因子计算失败：%s", exc)

        # 5) 组合输出（优先级：HardGate -> RiskReversal -> Sell -> Reduce -> Buy）
        signal = np.select(
            [hard_gate_mask, risk_reversal, base_sell, base_reduce, base_buy],
            ["HOLD", "REDUCE", "SELL", "REDUCE", "BUY"],
            default="HOLD",
        )

        # 买入升级（Soft Confirm）
        confirm_mask = base_buy & (
            engulf_confirm
            | (wyckoff_score >= p.wyckoff_confirm_min_score)
            | (vol_ok & macd_ok & main_force_ok)
        )
        soft_lookback = max(int(getattr(p, "soft_confirm_lookback_days", 0) or 0), 0)
        if soft_lookback > 1:
            recent_engulf = (
                engulf_confirm.groupby(df["code"])
                .rolling(soft_lookback, min_periods=1)
                .max()
                .reset_index(level=0, drop=True)
                .fillna(False)
                .astype(bool)
            )
            recent_wyckoff = (
                (wyckoff_score >= p.wyckoff_confirm_min_score)
                .groupby(df["code"])
                .rolling(soft_lookback, min_periods=1)
                .max()
                .reset_index(level=0, drop=True)
                .fillna(False)
                .astype(bool)
            )
            recent_force = (
                (vol_ok & macd_ok & main_force_ok)
                .groupby(df["code"])
                .rolling(soft_lookback, min_periods=1)
                .max()
                .reset_index(level=0, drop=True)
                .fillna(False)
                .astype(bool)
            )
            recent_confirm = recent_engulf | recent_wyckoff | recent_force
        else:
            recent_confirm = pd.Series(False, index=df.index)

        soft_confirm_recent = base_buy & (~confirm_mask) & recent_confirm
        signal = np.where(
            (signal == "BUY") & (confirm_mask | soft_confirm_recent),
            "BUY_CONFIRM",
            signal,
        )

        # 6) 构造输出
        out = df.copy()
        out["signal"] = signal

        out["macd_event"] = np.select(
            [(macd_hist > 0) & (prev_macd_hist <= 0), (macd_hist < 0) & (prev_macd_hist >= 0)],
            ["HIST_UP", "HIST_DOWN"],
            default=None,
        )
        out["wave_type"] = np.select(
            [(df["rsi14"] < 30) & (df["ma20_bias"] < -0.05), (df["ma20_bias"].abs() < 0.02)],
            ["OVERSOLD", "RANGE"],
            default="TRENDING",
        )
        out["stop_ref"] = out["close"] - 2.0 * out["atr14"]

        # 软因子仓位缩放
        cap_scale = pd.Series(1.0, index=df.index)
        cap_scale = cap_scale.mask(confirm_mask, 1.1)
        if soft_confirm_recent.any():
            cap_scale = cap_scale.mask(
                soft_confirm_recent, float(getattr(p, "soft_confirm_cap_scale", 0.5))
            )
            out.loc[soft_confirm_recent, "risk_tag"] = "SOFT_CONFIRM_RECENT"
        base_buy_count = int(base_buy.sum())
        if base_buy_count > 0 and base_buy_count < int(p.min_signals):
            cap_scale = cap_scale.mask(base_buy, p.min_signals_cap_scale)
            out.loc[base_buy, "risk_tag"] = "MIN_SIGNALS_SCALE"
        out["cap_scale"] = cap_scale
        out["wyckoff_score"] = wyckoff_score
        out["wyckoff_confirm"] = wyckoff_confirm
        if "engulf_score" in df.columns:
            out["engulf_score"] = df["engulf_score"]

        out["risk_tag"] = out.get("risk_tag", None)
        out["risk_note"] = None
        out.loc[hard_gate_mask, "risk_tag"] = "HARD_GATE"
        out.loc[untradeable_mask, "risk_tag"] = "UNTRADEABLE"
        out.loc[risk_reversal, "risk_tag"] = "ENGULF_REVERSAL"

        # 恐惧分（用于监控与复盘）
        kdj_k = pd.to_numeric(df.get("kdj_k"), errors="coerce")
        rsi14 = pd.to_numeric(df.get("rsi14"), errors="coerce")
        base_osc = kdj_k.where(kdj_k.notna(), rsi14)
        fear_score = ((base_osc - 50.0) / 50.0) * (1 - df["vol_ratio"].fillna(1.0))
        out["fear_score"] = fear_score.clip(-3, 3)

        # 记录原因
        reasons = pd.Series("观望", index=df.index)
        reasons = reasons.mask(buy_cross, "趋势跟随：金叉")
        reasons = reasons.mask(buy_pullback, "趋势跟随：回踩MA20")
        reasons = reasons.mask(stagnation_at_high, "风险：高位放量滞涨")
        reasons = reasons.mask(risk_reversal, "风险：吞没反转")
        reasons = reasons.mask(signal == "BUY_CONFIRM", reasons + "|软确认")
        reasons = reasons.mask(soft_confirm_recent, reasons + "|软确认_回看")
        out["reason"] = reasons

        # 漏斗计数日志
        funnel = {
            "rows": int(len(df)),
            "hard_gate": int(hard_gate_mask.sum()),
            "trend_ok": int(trend_ok.sum()),
            "buy_cross": int(buy_cross.sum()),
            "buy_pullback": int(buy_pullback.sum()),
            "base_buy": int(base_buy.sum()),
            "sell": int(base_sell.sum()),
            "reduce": int(base_reduce.sum()),
            "bull_engulf": int(bull_engulf.sum()),
            "bear_engulf": int(bear_engulf.sum()),
            "wyckoff_confirm": int(wyckoff_confirm.sum()),
            "signal_buy": int((out["signal"] == "BUY").sum()),
            "signal_buy_confirm": int((out["signal"] == "BUY_CONFIRM").sum()),
            "signal_reduce": int((out["signal"] == "REDUCE").sum()),
            "signal_sell": int((out["signal"] == "SELL").sum()),
        }
        self.logger.info("signal_funnel=%s", funnel)

        return out

    def _attach_chip_factors(self, signals: pd.DataFrame) -> pd.DataFrame:
        """从 strategy_chip_filter 表中关联筹码因子。"""
        if signals.empty: return signals
        
        codes = signals["code"].unique().tolist()
        latest_date = signals["date"].dt.date.max()
        
        select_cols = [
            "sig_date",
            "code",
            "chip_score",
            "gdhs_delta_pct",
            "announce_date",
            "chip_reason",
            "chip_penalty",
            "chip_note",
        ]
        cols_sql = ", ".join(f"`{col}`" for col in select_cols)
        stmt = text(
            f"SELECT {cols_sql} FROM strategy_chip_filter WHERE sig_date = :d AND code IN :codes"
        )
        
        chip_dfs = []
        chunk_size = 1000
        total = len(codes)
        self.logger.info("正在关联筹码数据，涉及 %d 只股票，正在分批查询...", total)

        try:
            with self.db_writer.engine.connect() as conn:
                for i in range(0, total, chunk_size):
                    batch_codes = codes[i : i + chunk_size]
                    try:
                        batch_df = pd.read_sql(
                            stmt.bindparams(bindparam("codes", expanding=True)), 
                            conn, 
                            params={"d": latest_date, "codes": batch_codes}
                        )
                        if not batch_df.empty:
                            chip_dfs.append(batch_df)
                    except Exception as e:
                        self.logger.warning("批次 %d 查询筹码失败: %s", i, e)

            if chip_dfs:
                chip_df = pd.concat(chip_dfs, ignore_index=True)
                chip_df = chip_df.rename(columns={"sig_date": "date"})
                if "announce_date" in chip_df.columns:
                    chip_df = chip_df.rename(columns={"announce_date": "gdhs_announce_date"})
                chip_df["date"] = pd.to_datetime(chip_df["date"])
                signals = signals.merge(chip_df, on=["date", "code"], how="left", suffixes=("", "_chip"))
                self.logger.info("筹码数据关联完成，匹配记录数: %d", len(chip_df))
            else:
                try:
                    cnt_row = conn.execute(
                        text("SELECT COUNT(*) AS cnt FROM strategy_chip_filter WHERE sig_date = :d"),
                        {"d": latest_date},
                    ).mappings().first()
                    cnt = int(cnt_row["cnt"]) if cnt_row and cnt_row.get("cnt") is not None else 0
                except Exception:
                    cnt = 0
                if cnt == 0:
                    self.logger.info(
                        "未查询到任何筹码数据（sig_date=%s 无记录，可能刚生成需下一轮生效）。",
                        latest_date,
                    )
                else:
                    self.logger.info(
                        "筹码表已有数据，但当前候选未匹配（sig_date=%s, table_rows=%s）。",
                        latest_date,
                        cnt,
                    )

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
            [
                board_phase.isin(["leader", "leading"]),
                board_phase == "improving",
                board_phase == "weakening",
                board_phase == "lagging",
            ],
            [2, 1, -1, -2],
            default=0
        )
        
        # 2. 综合质量分 (Quality Score)
        # 逻辑：板块权重略高于个股筹码（顺势而为）
        wyckoff_score = pd.to_numeric(out.get("wyckoff_score"), errors="coerce").fillna(0.0)
        engulf_score = pd.to_numeric(out.get("engulf_score"), errors="coerce").fillna(0.0)
        quality_score = (
            (board_factor * 1.5)
            + (chip_status * 1.0)
            + (wyckoff_score * float(getattr(self.params, "wyckoff_score_weight", 0.0)))
            + (engulf_score * float(getattr(self.params, "engulf_score_weight", 0.0)))
        )
        
        # 3. 动态调整信号
        is_buy = out["signal"].isin(["BUY", "BUY_CONFIRM"])
        
        # 情况 A：板块和筹码双差 -> 拦截
        stop_threshold = float(getattr(self.params, "quality_stop_threshold", -3.0))
        soft_threshold = float(getattr(self.params, "quality_soft_threshold", -1.0))
        stop_mask = is_buy & (quality_score <= stop_threshold)
        out.loc[stop_mask, "final_action"] = "WAIT"
        out.loc[stop_mask, "final_reason"] = out.loc[stop_mask, "final_reason"] + "|共振走弱(板块+筹码)"

        soft_mask = is_buy & (~stop_mask) & (quality_score < soft_threshold)
        if soft_mask.any():
            out.loc[soft_mask, "final_reason"] = (
                out.loc[soft_mask, "final_reason"].fillna("").astype(str)
                + "|共振偏弱降仓"
            )
            out.loc[soft_mask, "risk_tag"] = "QUALITY_SOFT_DOWNGRADE"
        
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
        cap_scale = pd.to_numeric(out.get("cap_scale"), errors="coerce").fillna(1.0)
        out["cap_scale"] = cap_scale
        out["final_cap"] = (out["final_cap"] * cap_scale).clip(0.0, 0.8)
        cap_scale_reduce = 0.5
        if reduce_cap_mask.any():
            out.loc[reduce_cap_mask, "final_cap"] = (
                out.loc[reduce_cap_mask, "final_cap"] * cap_scale_reduce
            ).clip(0.0, 0.8)
            out.loc[reduce_cap_mask, "cap_scale"] = cap_scale_reduce
            out.loc[reduce_cap_mask, "final_reason"] = (
                out.loc[reduce_cap_mask, "final_reason"].fillna("").astype(str)
                + f"|CAP_ADJUSTED={cap_scale_reduce}"
            )

        soft_cap_scale = float(getattr(self.params, "quality_soft_cap_scale", 0.7))
        if soft_mask.any():
            out.loc[soft_mask, "final_cap"] = (
                out.loc[soft_mask, "final_cap"] * soft_cap_scale
            ).clip(0.0, 0.8)
            out.loc[soft_mask, "cap_scale"] = (
                out.loc[soft_mask, "cap_scale"].fillna(1.0) * soft_cap_scale
            )
            out.loc[soft_mask, "final_reason"] = (
                out.loc[soft_mask, "final_reason"].fillna("").astype(str)
                + f"|CAP_ADJUSTED={soft_cap_scale}"
            )
        
        return out

    def run(self, *, force: bool = False) -> None:
        """执行趋势跟随策略。"""
        enabled = bool(getattr(self.params, "enabled", False))
        self.logger.info(
            "strategy_code=%s enabled=%s force=%s",
            STRATEGY_CODE_MA5_MA20_TREND,
            enabled,
            force,
        )
        if not enabled and not force:
            self.logger.info("策略开关关闭，跳过执行。")
            return
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
            lookback = int(getattr(self.params, "lookback_days", 100) or 100)
            lookback = max(60, lookback)
            ind = self.data_repo.load_indicator_daily(candidate_codes, latest_date, lookback=lookback)
            if ind.empty:
                self.logger.error("指标加载失败。")
                return

            # === 核心增强：加载板块轮动信息 ===
            stmt_board_dim = text(
                """
                SELECT `code`, `board_code`, `board_name`
                FROM dim_stock_board_industry
                WHERE `code` IN :codes
                """
            )
            stmt_rot = text(
                """
                SELECT `board_code`, `board_name`, `rotation_phase`
                FROM strategy_board_rotation
                WHERE `date` = :d
                """
            )
            try:
                with self.db_writer.engine.connect() as conn:
                    df_board = pd.read_sql(
                        stmt_board_dim.bindparams(bindparam("codes", expanding=True)),
                        conn,
                        params={"codes": candidate_codes},
                    )
                    df_rot = pd.read_sql(stmt_rot, conn, params={"d": latest_date})
                df_board.columns = [str(c).strip().lower() for c in df_board.columns]
                df_rot.columns = [str(c).strip().lower() for c in df_rot.columns]
                if not df_board.empty:
                    if "board_code" not in df_board.columns:
                        df_board["board_code"] = ""
                    if "board_name" not in df_board.columns:
                        df_board["board_name"] = ""
                    df_board["board_code"] = df_board["board_code"].fillna("").astype(str)
                    df_board["board_name"] = df_board["board_name"].fillna("").astype(str)
                    df_board["board_code_missing"] = df_board["board_code"].str.strip().eq("")
                    df_board = df_board.sort_values(
                        ["code", "board_code_missing", "board_code", "board_name"]
                    ).drop_duplicates(subset=["code"], keep="first")
                    df_board = df_board.drop(columns=["board_code_missing"])
                    ind = ind.merge(df_board, on="code", how="left")
                if "board_code" not in ind.columns:
                    ind["board_code"] = ""
                if "board_name" not in ind.columns:
                    ind["board_name"] = ""
                if not df_rot.empty:
                    if "board_code" not in df_rot.columns:
                        df_rot["board_code"] = ""
                    if "board_name" not in df_rot.columns:
                        df_rot["board_name"] = ""
                    df_rot["board_code"] = df_rot["board_code"].fillna("").astype(str)
                    df_rot["board_name"] = df_rot["board_name"].fillna("").astype(str)
                    rot_by_code = df_rot[df_rot["board_code"].str.strip().ne("")].drop_duplicates(
                        subset=["board_code"], keep="last"
                    )
                    rot_by_name = df_rot.drop_duplicates(subset=["board_name"], keep="last")
                    rot_code_map = rot_by_code.set_index("board_code")["rotation_phase"]
                    rot_name_map = rot_by_name.set_index("board_name")["rotation_phase"]
                    ind["rotation_phase"] = ind["board_code"].map(rot_code_map)
                    ind["rotation_phase"] = ind["rotation_phase"].fillna(
                        ind["board_name"].map(rot_name_map)
                    )
            except Exception as e:
                self.logger.warning("读取板块轮动数据失败（将以 neutral 运行）: %s", e)

            # 3. 产生初步信号
            sig = self._generate_signals(ind)
            
            # 4. 关联筹码因子
            sig = self._attach_chip_factors(sig)
            
            # 5. 最终执行决策 (现在包含板块权重了)
            sig = self._decide_final_action(sig)
            
            # 6. 按 signals_write_scope 选择写入范围
            scope = str(getattr(self.params, "signals_write_scope", "latest") or "latest").strip().lower()
            if scope == "window":
                sig_to_write = sig.copy()
                window_days = int(getattr(self.params, "signals_write_window_days", 0) or 0)
                if window_days > 0:
                    start_date = latest_date - dt.timedelta(days=max(window_days - 1, 0))
                    sig_to_write = sig_to_write[
                        sig_to_write["date"].dt.date >= start_date
                    ].copy()
            else:
                sig_to_write = sig[sig["date"].dt.date == latest_date].copy()
            
            # 补齐 extra_json (包含平衡后的决策证据)
            extras = []
            for _, row in sig_to_write.iterrows():
                payload = {
                    "chip_score": row.get("chip_score"),
                    "board_phase": row.get("rotation_phase"),
                    "quality_score": row.get("quality_score"),
                    "wave_type": row.get("wave_type"),
                    "cap_scale": row.get("cap_scale"),
                    "wyckoff_score": row.get("wyckoff_score"),
                    "engulf_score": row.get("engulf_score"),
                }
                extras.append(json.dumps({k: v for k, v in payload.items() if v is not None}, ensure_ascii=False))
            sig_to_write["extra_json"] = extras

            # 7. 入库
            self.logger.info("正在将 %d 条信号事件写入数据库...", len(sig_to_write))
            self.store.write_signal_events(latest_date, sig_to_write, candidate_codes)
            
            self.logger.info("趋势跟随策略扫描完成，信号已更新（已平衡筹码与板块权重）。")
            
        except Exception as e:
            self.logger.error("策略执行异常: %s", e, exc_info=True)
