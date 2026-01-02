import sys
import os
import logging
from datetime import datetime

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ashare.utils.logger import setup_logger
from ashare.core.app import AshareApp
from ashare.indicators.market_indicator_runner import MarketIndicatorRunner
from ashare.monitor.open_monitor import MA5MA20OpenMonitorRunner
from ashare.indicators.market_indicator_builder import MarketIndicatorBuilder

def main():
    setup_logger()
    logger = logging.getLogger("Pipeline_2_Process")
    
    logger.info("==============================================")
    logger.info(f"开始执行流水线 2: 数据预处理与指标计算 - {datetime.now()}")
    logger.info("==============================================")

    try:
        # 初始化基础 Repository 和 Builder
        mon_runner = MA5MA20OpenMonitorRunner()
        builder = MarketIndicatorBuilder(env_builder=mon_runner.repo, logger=logger)
        mi_runner = MarketIndicatorRunner(repo=mon_runner.repo, builder=builder, logger=logger)

        # 1. 刷新 Universe (构建宽表、流动性筛选、基本面过滤)
        logger.info(">>> 步骤 1/4: 刷新交易 Universe (Building Universe)...")
        app = AshareApp()
        app.run_universe_builder()
        
        # 获取基准日期
        latest_date = app._infer_latest_trade_day_from_db("history_daily_kline")

        # 2. 预计算技术指标 (Data Processing)
        logger.info(">>> 步骤 2/4: 运行个股技术指标预计算 (Technical Indicators)...")
        mi_runner.run_technical_indicators(latest_date=latest_date)

        # 3. 更新全市场日线指标 (Market Indicators)
        logger.info(">>> 步骤 3/4: 运行全市场指标计算 (Daily Market Indicators)...")
        mi_runner.run_daily_indicator(mode="incremental")

        # 4. 计算大盘周线环境 (Market Environment)
        logger.info(">>> 步骤 4/4: 运行大盘周线环境评估 (Weekly Market Indicators)...")
        mi_runner.run_weekly_indicator(mode="incremental")

        logger.info("==============================================")
        logger.info("流水线 2: 数据处理完成！")
        logger.info("请继续执行 pipeline_3_run_strategy.py 生成交易信号。")
        logger.info("==============================================")

    except Exception as e:
        logger.error(f"流水线 2 执行失败: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()