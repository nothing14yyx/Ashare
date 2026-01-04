import pandas as pd
import numpy as np
from scipy.signal import argrelextrema

class WyckoffAnalyzer:
    """
    威科夫量价分析工具库 (Wyckoff Volume-Price Analyzer)
    
    核心功能：
    1. EFI (Elder's Force Index) 计算
    2. EFI 的 Z-Score 标准化 (解决不同个股波动率差异问题)
    3. 严格的 MACD/价格 背离检测 (基于极值点)
    """

    @staticmethod
    def calculate_efi(df: pd.DataFrame, period: int = 13) -> pd.Series:
        """
        计算强力指数 (EFI - Elder's Force Index)
        
        Logic:
            Raw Force = (Close - Prev_Close) * Volume
            EFI = EMA(Raw Force, period)
            
        Args:
            df: 包含 'close', 'volume' 列的 DataFrame
            period: EMA 平滑周期，默认 13
            
        Returns:
            pd.Series: EFI 指标线
        """
        # 确保数据有效
        if len(df) < period:
            return pd.Series(0, index=df.index)

        # 1. 计算价格变化
        price_change = df['close'].diff()
        
        # 2. 计算原始 Force (努力 * 结果)
        # 注意：如果成交量很大但价格不动，Force 接近 0
        raw_force = price_change * df['volume']
        
        # 3. 平滑处理
        efi = raw_force.ewm(span=period, adjust=False).mean()
        
        return efi

    @staticmethod
    def calculate_z_score(series: pd.Series, window: int = 60) -> pd.Series:
        """
        计算滚动 Z-Score，用于定义相对的“高位”和“低位”
        
        Formula: (Value - Mean) / StdDev
        
        Args:
            series: 输入指标序列 (如 EFI)
            window: 滚动窗口大小 (如 60 天)
            
        Returns:
            pd.Series: Z-Score 序列
        """
        roll = series.rolling(window=window)
        mean = roll.mean()
        std = roll.std()
        
        # 避免除以零
        z_score = (series - mean) / (std.replace(0, 1))
        
        return z_score

    @staticmethod
    def detect_divergence(
        df: pd.DataFrame, 
        indicator_col: str = 'macd',
        price_col: str = 'close',
        order: int = 5,
        lookback: int = 30
    ) -> pd.DataFrame:
        """
        检测价格与指标的背离 (Divergence)
        
        Args:
            df: 数据源
            indicator_col: 指标列名 (通常是 MACD 或 EFI)
            price_col: 价格列名
            order: 极值点判断窗口 (前后 order 天都比当前小/大才算极值)
            lookback: 回溯寻找上一个极值点的时间范围
            
        Returns:
            df: 包含 'bullish_divergence' (底背离) 和 'bearish_divergence' (顶背离) bool 列
        """
        # 初始化结果列
        df['bullish_divergence'] = False
        df['bearish_divergence'] = False
        
        # 必须有足够数据
        if len(df) < lookback:
            return df

        prices = df[price_col].values
        indicators = df[indicator_col].values
        
        # 1. 寻找价格的局部低点 (Local Minima) 和 高点 (Local Maxima)
        # argrelextrema 返回的是索引数组
        price_min_indices = argrelextrema(prices, np.less, order=order)[0]
        price_max_indices = argrelextrema(prices, np.greater, order=order)[0]
        
        # 2. 判定底背离 (Bullish Divergence)
        # 逻辑：当前是价格低点 -> 找上一个价格低点 -> 对比价格和指标
        for current_idx in price_min_indices:
            # 找到 lookback 范围内的上一个低点
            prev_candidates = price_min_indices[
                (price_min_indices < current_idx) & 
                (price_min_indices >= current_idx - lookback)
            ]
            
            if len(prev_candidates) > 0:
                prev_idx = prev_candidates[-1] # 最近的一个
                
                price_curr = prices[current_idx]
                price_prev = prices[prev_idx]
                ind_curr = indicators[current_idx]
                ind_prev = indicators[prev_idx]
                
                # 严格定义：价格创新低 AND 指标抬高
                if price_curr < price_prev and ind_curr > ind_prev:
                    # 标记在 current_idx
                    df.at[df.index[current_idx], 'bullish_divergence'] = True

        # 3. 判定顶背离 (Bearish Divergence)
        # 逻辑：当前是价格高点 -> 找上一个价格高点 -> 对比价格和指标
        for current_idx in price_max_indices:
            prev_candidates = price_max_indices[
                (price_max_indices < current_idx) & 
                (price_max_indices >= current_idx - lookback)
            ]
            
            if len(prev_candidates) > 0:
                prev_idx = prev_candidates[-1]
                
                price_curr = prices[current_idx]
                price_prev = prices[prev_idx]
                ind_curr = indicators[current_idx]
                ind_prev = indicators[prev_idx]
                
                # 严格定义：价格创新高 AND 指标走低
                if price_curr > price_prev and ind_curr < ind_prev:
                    df.at[df.index[current_idx], 'bearish_divergence'] = True
                    
        return df
