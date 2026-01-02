import sys
import os
import logging
from datetime import datetime

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ashare.utils.logger import setup_logger
from ashare.strategies.trend_following_strategy import TrendFollowingStrategyRunner

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
        runner.run(force=True)  # force=True 确保在 pipeline 中始终运行
        
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
