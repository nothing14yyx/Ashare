"""运行筹码过滤计算。"""

from __future__ import annotations

import pandas as pd

from ashare.chip_filter import ChipFilter
from ashare.db import DatabaseConfig, MySQLWriter
from ashare.open_monitor import OpenMonitorParams
from ashare.open_monitor_repo import OpenMonitorRepository
from ashare.schema_manager import ensure_schema
from ashare.utils.logger import setup_logger


def _build_chip_inputs(signals: pd.DataFrame) -> pd.DataFrame:
    if signals.empty:
        return pd.DataFrame()

    inputs = pd.DataFrame()
    inputs["sig_date"] = pd.to_datetime(signals.get("sig_date"), errors="coerce")
    inputs["date"] = inputs["sig_date"]
    if "code" not in signals.columns:
        return pd.DataFrame()
    inputs["code"] = signals["code"].astype(str)

    mapping = {
        "sig_vol_ratio": "vol_ratio",
        "sig_close": "close",
        "sig_ma20": "ma20",
        "sig_fear_score": "fear_score",
        "sig_macd_hist": "macd_hist",
    }
    for src, dest in mapping.items():
        if src in signals.columns:
            inputs[dest] = signals[src]

    inputs = inputs.dropna(subset=["sig_date", "code"])
    return inputs


def main() -> int:
    ensure_schema()
    logger = setup_logger()
    params = OpenMonitorParams.from_config()
    db_writer = MySQLWriter(DatabaseConfig.from_env())
    repo = OpenMonitorRepository(db_writer.engine, logger, params)

    latest_trade_date, signal_dates, signals = repo.load_recent_buy_signals()
    if not latest_trade_date or signals.empty:
        logger.warning("未获取到可用信号，已跳过筹码计算。")
        return 0

    logger.info("筹码过滤输入信号数=%s（信号日=%s）", len(signals), signal_dates)
    inputs = _build_chip_inputs(signals)
    if inputs.empty:
        logger.warning("筹码过滤输入为空，已跳过。")
        return 0

    chip_df = ChipFilter().apply(inputs)
    rowcount = 0 if chip_df is None else len(chip_df)
    logger.info("筹码过滤写入完成，行数=%s。", rowcount)
    return rowcount


if __name__ == "__main__":
    main()
