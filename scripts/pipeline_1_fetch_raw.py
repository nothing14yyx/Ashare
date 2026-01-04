import sys
import os
import logging
from datetime import datetime

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ashare.utils.logger import setup_logger
from ashare.core.app import AshareApp
from ashare.core.schema_manager import ensure_schema

def main():
    setup_logger()
    logger = logging.getLogger("Pipeline_1_Fetch")
    
    logger.info("==============================================")
    logger.info(f"开始执行流水线 1: 原始数据采集 - {datetime.now()}")
    logger.info("==============================================")

    try:
        ensure_schema()
        # 采集原始数据 (Baostock/Akshare/LHB/Margin/GDHS)
        logger.info(">>> 正在启动数据抓取引擎 (Baostock/AkShare)...")
        app = AshareApp()
        app.run()
        
        logger.info("==============================================")
        logger.info("流水线 1: 数据采集完成。")
        logger.info("请继续执行 pipeline_2_process_indicators.py 进行数据预处理。")
        logger.info("==============================================")

    except Exception as e:
        logger.error(f"流水线 1 执行失败: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
