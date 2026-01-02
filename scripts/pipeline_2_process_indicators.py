import sys
import os
import logging
import subprocess
from datetime import datetime

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ashare.utils.logger import setup_logger

def run_step(name, python_code):
    logger = logging.getLogger("Pipeline_2_Process")
    logger.info(f"==============================================")
    logger.info(f">>> 正在启动步骤: {name}")
    try:
        # 使用 -c 执行 Python 代码，彻底隔离环境
        result = subprocess.run(
            [sys.executable, "-c", python_code],
            check=True
        )
        logger.info(f">>> 步骤 {name} 已成功完成。")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f">>> 步骤 {name} 失败 (Exit Code: {e.returncode})")
        return False

def main():
    setup_logger()
    logger = logging.getLogger("Pipeline_2_Process")
    
    logger.info("==============================================")
    logger.info(f"开始执行流水线 2: 数据预处理与指标计算 - {datetime.now()}")
    logger.info("==============================================")

    # 1. 刷新交易 Universe
    # 这一步已经由 Pipeline 1 或 之前的尝试完成，为了稳健再跑一遍
    cmd_1 = "from ashare.core.app import AshareApp; AshareApp().run_universe_builder()"
    run_step("Building Universe", cmd_1)

    # 2. 核心指标批量加工 (合并技术指标、全市场日线环境、周线环境)
    # 将所有计算逻辑合并到一个子进程，避免频繁初始化连接
    cmd_calc = """
import logging
import datetime as dt
from ashare.indicators.market_indicator_runner import MarketIndicatorRunner
from ashare.monitor.open_monitor import MA5MA20OpenMonitorRunner
from ashare.indicators.market_indicator_builder import MarketIndicatorBuilder
from ashare.core.app import AshareApp

logger = logging.getLogger('ashare')
mon = MA5MA20OpenMonitorRunner()
builder = MarketIndicatorBuilder(env_builder=mon.repo, logger=logger)
mi = MarketIndicatorRunner(repo=mon.repo, builder=builder, logger=logger)

# 推断日期
latest_date = AshareApp()._infer_latest_trade_day_from_db('history_daily_kline')

print(f">>> [Subprocess] 正在计算技术指标 (latest_date={latest_date})...")
mi.run_technical_indicators(latest_date=latest_date)

print(f">>> [Subprocess] 正在计算日线市场环境...")
mi.run_daily_indicator(mode='incremental')

print(f">>> [Subprocess] 正在计算周线大盘趋势...")
mi.run_weekly_indicator(mode='incremental')
"""
    if run_step("Full Processing & Indicators", cmd_calc):
        logger.info("==============================================")
        logger.info("流水线 2: 所有指标加工任务执行成功！")
        logger.info("==============================================")
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()