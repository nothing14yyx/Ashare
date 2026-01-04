"""低吸形态识别模块。

核心逻辑：
基于当天分钟线（分时图），判断当前价格相对于均价线（VWAP）的位置、量能配合情况，
识别“回踩稳固”、“分歧转一致”等低吸机会。
"""

from __future__ import annotations

import pandas as pd
import numpy as np


def detect_low_suck_signal(
    df_minute: pd.DataFrame, 
    ref_close_yesterday: float | None = None
) -> dict:
    """分析分时数据，返回低吸信号强度与理由。
    
    Args:
        df_minute: 包含 time, price, volume, avg_price(可选) 的分钟线
        ref_close_yesterday: 昨日收盘价，用于计算涨幅
        
    Returns:
        dict: {
            "strength": "STRONG" | "MEDIUM" | "WEAK" | "NONE",
            "reason": str,
            "score": float (0-100),
            "suggested_action": "GO" | "WAIT"
        }
    """
    if df_minute is None or df_minute.empty or len(df_minute) < 30:
        return {"strength": "NONE", "reason": "数据不足(少于30分钟)", "score": 0}

    # 提取关键序列
    price = df_minute["price"]
    volume = df_minute["volume"]
    avg_price = df_minute.get("avg_price")
    
    current_price = price.iloc[-1]
    
    # 如果没有均价线，尝试现场计算
    if avg_price is None or avg_price.isnull().all():
        cum_vol = volume.cumsum()
        cum_amt = (price * volume).cumsum()
        avg_price = cum_amt / cum_vol.replace(0, 1)
        
    current_avg = avg_price.iloc[-1]
    
    # === 核心指标计算 ===
    
    # 1. 均线乖离率 (Deviation from VWAP)
    dev_pct = (current_price - current_avg) / current_avg
    
    # 2. 趋势稳定性 (最近30分钟波动率)
    recent_window = 30
    if len(price) > recent_window:
        recent_prices = price.tail(recent_window)
        recent_std = recent_prices.pct_change().std()
        # 趋势倾向: 1=涨, -1=跌
        trend_slope = (recent_prices.iloc[-1] - recent_prices.iloc[0]) / recent_prices.iloc[0]
    else:
        recent_std = 0.01 # 默认值
        trend_slope = 0
        
    # 3. 量能配合：最近10分钟 vs 最近60分钟
    recent_vol = volume.tail(10).mean()
    recent_vol_base = volume.tail(60).mean()
    vol_ratio = recent_vol / recent_vol_base if recent_vol_base > 0 else 1.0
    
    # 4. 当日/近段低位位置
    day_low = price.min()
    recent_low = price.tail(60).min()
    
    # 5. 均价线斜率 (新增：防止阴跌接飞刀)
    # 计算分时均价线的斜率（Trend Slope of VWAP）
    if len(avg_price) > 30:
        avg_price_slope = (avg_price.iloc[-1] - avg_price.iloc[0]) / avg_price.iloc[0]
    else:
        avg_price_slope = 0.0

    # === 判定逻辑 ===
    strength = "NONE"
    reasons = []
    score = 50 # 基准分
    
    # A. 均价线附近回踩（允许轻微下破）
    if -0.005 <= dev_pct <= 0.003:
        if vol_ratio < 0.7:
            reasons.append("缩量回踩均线")
            score += 20
        else:
            reasons.append("均线附近")
            score += 8

    # B. 低位回拉（接近日内低点后回升）
    if current_price <= day_low * 1.01 and current_price >= recent_low * 1.003:
        reasons.append("接近当日低位回拉")
        score += 20

    # C. 震荡稳定（避免放量冲高后回落）
    if recent_std is not None and recent_std < 0.004 and trend_slope > -0.003:
        reasons.append("低波动稳定")
        score += 10
        
    # [新增] 阴跌识别惩罚
    # 如果均价线本身在快速下坠，扣重分
    if avg_price_slope < -0.015: 
        reasons.append("趋势向下(阴跌)")
        score -= 30  # 大幅扣分，直接打死

    # D. 偏离过大惩罚
    if dev_pct > 0.02:
        reasons.append("偏离均线过高")
        score -= 15
    elif dev_pct < -0.02:
        reasons.append("跌破均线过深")
        score -= 15

    # D. 昨收盘硬约束 (若是绿盘，低吸需更谨慎)
    if ref_close_yesterday:
        chg_pct = (current_price - ref_close_yesterday) / ref_close_yesterday
        reasons.append(f"涨幅{chg_pct*100:.2f}%")
        if chg_pct < -0.03:
            score -= 10
            reasons.append("跌幅偏深")
        elif chg_pct > 0.05:
            score -= 8
            reasons.append("涨幅偏大")

    # 最终评级修正
    if score >= 80: strength = "STRONG"
    elif score >= 60: strength = "MEDIUM"
    elif score >= 45: strength = "WEAK"
    else: strength = "NONE"

    return {
        "strength": strength,
        "reason": "; ".join(reasons) if reasons else "无明显低吸特征",
        "score": score,
        "dev_vwap": dev_pct,
        "vol_ratio": vol_ratio,
        "suggested_action": "GO" if score >= 60 else "WAIT"
    }
