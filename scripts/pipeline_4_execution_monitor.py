import sys
import os
import logging
from datetime import datetime

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ashare.utils.logger import setup_logger
from ashare.monitor.open_monitor import MA5MA20OpenMonitorRunner

def main():
    setup_logger()
    logger = logging.getLogger("Pipeline_4_Monitor")
    
    logger.info("==============================================")
    logger.info(f"开始执行流水线 4: 实盘/盘前执行监测 - {datetime.now()}")
    logger.info("==============================================")

    try:
        # 运行开盘监测 (Open Monitor)
        # 它会基于 strategy_ready_signals 视图中的信号，结合实时行情给出决策
        logger.info(">>> 正在启动 Open Monitor 监测引擎...")
        runner = MA5MA20OpenMonitorRunner()
        runner.run()
        
        logger.info("==============================================")
        logger.info("流水线 4: 执行监测完成！")
        logger.info("决策结果已入库：strategy_open_monitor_eval")
        logger.info("==============================================")

    except Exception as e:
        logger.error(f"流水线 4 执行失败: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()