import sys
import os
import logging
import datetime as dt

# 添加项目根目录到 sys.path，确保能找到 ashare 包
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ashare.core.config import load_config, get_section
from ashare.data.akshare_fetcher import AkshareDataFetcher
from ashare.core.db import DatabaseConfig, MySQLWriter
from sqlalchemy import text

def debug():
    print("=== AShare 环境与配置自检工具 ===")
    
    # 1. 配置加载测试
    try:
        config = load_config()
        print(f"[OK] 配置文件加载成功。项数: {len(config)}, 顶级节点: {list(config.keys())}")
    except Exception as e:
        print(f"[ERROR] 配置文件加载失败: {e}")
        return

    # 2. 关键节点检查
    app_cfg = get_section("app")
    print("\n--- [app] 配置检查 ---")
    for k in ['index_codes', 'fetch_index_kline']:
        print(f"{k}: {app_cfg.get(k)}")

    # 3. 数据库连接与视图重构检查
    print("\n--- 数据库与视图状态检查 ---")
    try:
        db_writer = MySQLWriter(DatabaseConfig.from_env())
        with db_writer.engine.connect() as conn:
            # 检查我们重构的 ready_signals 视图
            res = conn.execute(text("SELECT COUNT(*) FROM strategy_ready_signals")).scalar()
            print(f"[OK] 数据库连接成功。")
            print(f"[OK] strategy_ready_signals 视图可用，当前就绪信号数: {res}")
            
            # 检查 candidates 视图
            res_cand = conn.execute(text("SELECT COUNT(*) FROM strategy_candidates")).scalar()
            print(f"[OK] strategy_candidates 视图可用，当前候选总数: {res_cand}")
    except Exception as e:
        print(f"[ERROR] 数据库或视图检查失败: {e}")

    # 4. 数据源库加载测试
    print("\n--- 数据源依赖检查 ---")
    try:
        from ashare.data.baostock_session import BaostockSession
        bs = BaostockSession()
        print("[OK] Baostock 依赖库加载成功。")
    except Exception as e:
        print(f"[ERROR] Baostock 加载失败: {e}")

    try:
        fetcher = AkshareDataFetcher()
        print("[OK] AkshareDataFetcher 加载成功。")
    except Exception as e:
        print(f"[ERROR] AkshareDataFetcher 加载失败: {e}")

    print("\n=== 自检完成 ===")

if __name__ == "__main__":
    debug()
