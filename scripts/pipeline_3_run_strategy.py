import sys
import os
import logging
from datetime import datetime

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from sqlalchemy import text

from ashare.utils.logger import setup_logger
from ashare.strategies.trend_following_strategy import TrendFollowingStrategyRunner
from ashare.strategies.chip_filter import ChipFilter
from ashare.core.schema_manager import STRATEGY_CODE_MA5_MA20_TREND

def main():
    setup_logger()
    logger = logging.getLogger("Pipeline_3_Strategy")
    
    logger.info("==============================================")
    logger.info(f"开始执行流水线 3: 趋势跟随策略扫描 - {datetime.now()}")
    logger.info("==============================================")

    try:
        # 运行核心策略 (Trend Following Strategy)
        # 该 runner 会自动读取指标、生成信号并预计算筹码分
        logger.info(">>> 正在启动趋势跟随策略扫描...")
        runner = TrendFollowingStrategyRunner()
        force_env = str(os.getenv("ASHARE_STRATEGY_FORCE", "")).strip().lower()
        force = force_env in {"1", "true", "yes", "y", "on"}
        runner.run(force=force)

        # 筹码因子计算（写入 strategy_chip_filter）
        repo = runner.db_writer
        table = runner.params.signal_events_table
        stmt_latest = text(
            f"SELECT MAX(`sig_date`) AS latest_sig_date FROM `{table}` WHERE `strategy_code` = :strategy"
        )
        with repo.engine.begin() as conn:
            row = conn.execute(
                stmt_latest, {"strategy": STRATEGY_CODE_MA5_MA20_TREND}
            ).mappings().first()
        latest_date = row.get("latest_sig_date") if row else None
        if latest_date:
            stmt = text(
                f"""
                SELECT
                  e.`sig_date`,
                  e.`code`,
                  e.`signal`,
                  e.`final_action`,
                  e.`risk_tag`,
                  ind.`close`,
                  ind.`ma20`,
                  ind.`vol_ratio`,
                  ind.`atr14`,
                  ind.`macd_hist`,
                  ind.`kdj_k`,
                  ind.`rsi14`,
                  ind.`ma20_bias`,
                  ind.`yearline_state`
                FROM `{table}` e
                LEFT JOIN `{runner.params.indicator_table}` ind
                  ON ind.`trade_date` = e.`sig_date` AND ind.`code` = e.`code`
                WHERE e.`strategy_code` = :strategy AND e.`sig_date` = :d
                """
            )
            with repo.engine.begin() as conn:
                sig_df = pd.read_sql(
                    stmt,
                    conn,
                    params={
                        "strategy": STRATEGY_CODE_MA5_MA20_TREND,
                        "d": latest_date,
                    },
                )
            if sig_df.empty:
                logger.warning("筹码计算跳过：最新信号日=%s 但无信号行。", latest_date)
            else:
                chip = ChipFilter()
                result = chip.apply(sig_df)
                logger.info("筹码计算完成：写入 %s 行。", len(result))
        else:
            logger.warning("筹码计算跳过：未找到最新 sig_date。")
        
        logger.info("==============================================")
        logger.info("流水线 3: 策略执行完成！")
        logger.info("动态视图 strategy_ready_signals 已自动更新。")
        logger.info("请继续执行 pipeline_4_execution_monitor.py 进行实盘评估。")
        logger.info("==============================================")

    except Exception as e:
        logger.error(f"流水线 3 执行失败: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
