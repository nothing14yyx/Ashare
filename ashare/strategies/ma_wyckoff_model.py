import pandas as pd
import numpy as np
from ashare.indicators.wyckoff import WyckoffAnalyzer
from ashare.indicators.indicator_utils import macd

# 定义动作常量
ACTION_BUY_STRONG = "BUY_STRONG" # 重仓 (均线 + 量价背离确认)
ACTION_BUY_LIGHT = "BUY_LIGHT"   # 轻仓 (仅均线)
ACTION_REDUCE = "REDUCE"         # 减仓 (量价顶背离/动能衰竭)
ACTION_SELL = "SELL"             # 清仓 (均线死叉/破位)
ACTION_HOLD = "HOLD"             # 持有

WYCKOFF_SCORE_MAP = {
    ACTION_BUY_STRONG: 2.0,
    ACTION_BUY_LIGHT: 1.0,
    ACTION_REDUCE: -1.0,
    ACTION_SELL: -2.0,
    ACTION_HOLD: 0.0,
}

def calculate_ma(series: pd.Series, window: int) -> pd.Series:
    """简单移动平均 (SMA)"""
    return series.rolling(window=window).mean()

class MAWyckoffStrategy:
    """
    MA + Wyckoff 融合策略 (4档动作模型)
    
    整合逻辑：
    1. MA5/MA20 提供趋势基础 (Trend)
    2. Wyckoff EFI & MACD Divergence 提供动能健康度诊断 (Momentum Health)
    3. 4档信号系统优化仓位管理
    """
    
    def __init__(self, 
                 ma_short=5, 
                 ma_long=20, 
                 efi_window=60, 
                 divergence_lookback=30,
                 confirmation_window=10):
        self.ma_short = ma_short
        self.ma_long = ma_long
        self.efi_window = efi_window
        self.divergence_lookback = divergence_lookback
        self.confirmation_window = confirmation_window # 金叉前 N 天寻找背离确认

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        运行策略计算，返回带有 'signal' 和 'action_level' 的 DataFrame
        """
        if df.empty:
            return df

        df = df.copy()

        # ----------------------------------------
        # 1. 基础指标计算
        # ----------------------------------------
        # 均线
        df['ma_short'] = calculate_ma(df['close'], self.ma_short)
        df['ma_long'] = calculate_ma(df['close'], self.ma_long)
        
        # MACD (用于背离)
        # indicator_utils.macd 返回 (dif, dea, hist)
        df['macd'], df['macd_signal'], df['macd_hist'] = macd(df['close'])
        
        # Wyckoff EFI & Z-Score
        df['efi'] = WyckoffAnalyzer.calculate_efi(df)
        df['efi_z'] = WyckoffAnalyzer.calculate_z_score(df['efi'], window=self.efi_window)
        
        # 背离检测 (基于 MACD)
        # 注意：这里使用 macd_hist 还是 macd 线本身可以调整，通常用 macd 线更稳
        df = WyckoffAnalyzer.detect_divergence(
            df, 
            indicator_col='macd', 
            price_col='close', 
            lookback=self.divergence_lookback
        )

        # ----------------------------------------
        # 2. 逻辑状态判定
        # ----------------------------------------
        
        # A. 趋势状态
        df['trend_bullish'] = df['ma_short'] > df['ma_long']
        df['golden_cross'] = (df['ma_short'] > df['ma_long']) & (df['ma_short'].shift(1) <= df['ma_long'].shift(1))
        df['death_cross'] = (df['ma_short'] < df['ma_long']) & (df['ma_short'].shift(1) >= df['ma_long'].shift(1))

        # B. 动能衰竭信号 (用于 REDUCE)
        # 1. 顶背离
        # 2. EFI 高位死叉 (Z > 1 且今日跌破昨日) -> 简化版高位转弱
        df['efi_weakness'] = (df['efi_z'] > 1.0) & (df['efi'] < df['efi'].shift(1))
        df['signal_reduce'] = df['bearish_divergence'] | df['efi_weakness']

        # C. 底部确认信号 (用于 BUY_STRONG)
        # 逻辑：在金叉当天的过去 confirmation_window 天内，是否发生过 底背离 或 EFI低位金叉
        # 我们使用 rolling max 来检查过去 N 天是否有 True
        df['has_recent_bull_div'] = df['bullish_divergence'].rolling(window=self.confirmation_window).max() > 0
        
        # EFI 低位转强: Z < -1 且回升
        df['efi_strength'] = (df['efi_z'] < -1.0) & (df['efi'] > df['efi'].shift(1))
        df['has_recent_efi_strength'] = df['efi_strength'].rolling(window=self.confirmation_window).max() > 0
        
        df['is_confirmed_bottom'] = df['has_recent_bull_div'] | df['has_recent_efi_strength']

        # ----------------------------------------
        # 3. 动作分级 (Action Generation)
        # ----------------------------------------
        df['action'] = ACTION_HOLD # 默认
        
        # 逐行遍历生成最终动作 (为了逻辑清晰，虽然慢一点但比向量化更易读)
        # 也可以用 np.select 优化
        
        conditions = [
            # SELL: 死叉
            df['death_cross'],
            
            # REDUCE: 趋势虽好(Trend Bullish)，但出现衰竭信号
            (df['trend_bullish']) & (df['signal_reduce']),
            
            # BUY_STRONG: 金叉 且 有底部确认
            (df['golden_cross']) & (df['is_confirmed_bottom']),
            
            # BUY_LIGHT: 金叉 但 无底部确认
            (df['golden_cross']) & (~df['is_confirmed_bottom'])
        ]
        
        choices = [
            ACTION_SELL,
            ACTION_REDUCE,
            ACTION_BUY_STRONG,
            ACTION_BUY_LIGHT
        ]
        
        # 应用逻辑 (注意顺序：np.select 优先级是从上到下)
        # 这里有一个问题：REDUCE 可能会覆盖 BUY (如果同一天既金叉又背离？不太可能，但需注意)
        # 均线刚金叉很难立刻顶背离，除非极端情况。我们把 SELL 放在最前，BUY 放在 REDUCE 前面？
        # 不，REDUCE 是在持仓过程中发生的。BUY 是在金叉瞬间发生的。
        # 我们的 golden_cross 只有一天 True。
        
        # 修正逻辑顺序：
        # 1. SELL (最高优先级)
        # 2. BUY_STRONG / BUY_LIGHT (金叉时刻)
        # 3. REDUCE (非金叉时刻，持仓中)
        
        df['action'] = np.select(
            [
                df['death_cross'],                                      # 1. 必须跑
                (df['golden_cross']) & (df['is_confirmed_bottom']),     # 2. 完美买点
                (df['golden_cross']),                                   # 3. 普通买点 (fallback)
                (df['trend_bullish']) & (df['signal_reduce'])           # 4. 持仓中预警
            ],
            [
                ACTION_SELL,
                ACTION_BUY_STRONG,
                ACTION_BUY_LIGHT,
                ACTION_REDUCE
            ],
            default=ACTION_HOLD
        )

        return df

    def build_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """输出可被趋势策略消费的 wyckoff_confirm / wyckoff_score。"""
        if df.empty:
            return df
        out = self.run(df)
        out["wyckoff_score"] = out["action"].map(WYCKOFF_SCORE_MAP).fillna(0.0)
        out["wyckoff_confirm"] = out["action"].isin([ACTION_BUY_STRONG])
        return out
