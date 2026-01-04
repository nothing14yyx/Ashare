from dataclasses import dataclass
from ashare.core.config import get_section
from ashare.core.schema_manager import (
    TABLE_STRATEGY_INDICATOR_DAILY,
    TABLE_STRATEGY_SIGNAL_EVENTS,
)

@dataclass(frozen=True)
class MA5MA20Params:
    """策略参数（支持从 config.yaml 的 strategy_ma5_ma20_trend 节覆盖）。"""

    enabled: bool = False
    lookback_days: int = 365

    # 日线数据来源表：默认直接用全量表（性能更稳），必要时你也可以在 config.yaml 覆盖
    daily_table: str = "history_daily_kline"

    # 放量确认：volume / vol_ma >= threshold
    volume_ratio_threshold: float = 1.5
    volume_ma_window: int = 5

    # 低信号保护：当 base_buy 信号过少时，仍输出但降仓
    min_signals: int = 3
    min_signals_cap_scale: float = 0.5

    # 高位滞涨阈值（风险提示/卖出）
    stagnation_vol_ratio_threshold: float = 2.0
    stagnation_pct_abs_threshold: float = 0.01
    stagnation_ma20_bias_threshold: float = 0.1

    # 趋势过滤用均线（多头排列）
    trend_ma_short: int = 20
    trend_ma_mid: int = 60
    trend_ma_long: int = 250

    # 回踩买点：close 与 MA20 偏离比例
    pullback_band: float = 0.01
    # 接近买点预警（NEAR_SIGNAL）
    near_ma20_band: float = 0.02
    near_cross_gap_band: float = 0.005
    near_signal_macd_required: bool = False

    # KDJ 低位阈值（可选增强：只做 reason 标记，不强制）
    kdj_low_threshold: float = 30.0

    # BUY_CONFIRM 收紧开关：要求 MA5 >= MA20
    buy_confirm_require_ma5_ge_ma20: bool = True

    # 吞没形态软因子阈值
    engulf_body_atr_threshold: float = 0.6
    engulf_vol_ratio_threshold: float = 1.2
    engulf_near_ma20_band: float = 0.02
    engulf_score_weight: float = 0.3
    engulf_risk_ma20_bias_threshold: float = 0.06

    # Wyckoff 软因子
    wyckoff_enabled: bool = True
    wyckoff_confirm_min_score: float = 1.0
    wyckoff_score_weight: float = 0.5

    # 软确认回看窗口（用于避免仅当日信号过稀）
    soft_confirm_lookback_days: int = 10
    soft_confirm_cap_scale: float = 0.5

    # 质量分拦截与降级
    quality_stop_threshold: float = -3.0
    quality_soft_threshold: float = -1.0
    quality_soft_cap_scale: float = 0.7

    # 一字涨停阈值（用于 HardGate，不可交易）
    one_word_limit_up_pct: float = 0.095

    # 输出表/视图
    indicator_table: str = TABLE_STRATEGY_INDICATOR_DAILY
    signal_events_table: str = TABLE_STRATEGY_SIGNAL_EVENTS

    # signals 写入范围：
    # - latest：仅写入最新交易日（默认，低开销）
    # - window：写入本次计算窗口内的全部交易日（用于回填历史/回测）
    signals_write_scope: str = "latest"
    signals_write_batch_days: int = 5
    signals_write_window_days: int = 10
    valid_days: int = 3

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
