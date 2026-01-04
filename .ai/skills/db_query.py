import sys
import os
import json
import pandas as pd
from sqlalchemy import text

# 1. 路径修复：确保能引用到 ashare 包
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ashare.core.db import DatabaseConfig, MySQLWriter

def query_db(sql_query: str, limit: int = 20):
    try:
        # 复用项目现有的连接配置
        db = MySQLWriter(DatabaseConfig.from_env())
        
        # 简单的安全检查：如果是 SELECT 且没有 LIMIT，自动加上 LIMIT
        sql_lower = sql_query.lower().strip()
        read_prefixes = ("select", "show", "describe", "explain")
        is_read_query = sql_lower.startswith(read_prefixes)
        if sql_lower.startswith("select") and "limit" not in sql_lower and "count" not in sql_lower:
            sql_query += f" LIMIT {limit}"

        if is_read_query:
            with db.engine.connect() as conn:
                df = pd.read_sql(text(sql_query), conn)

            # 日期格式化为 ISO 字符串，处理 NaN
            result = df.to_json(orient="records", date_format="iso", force_ascii=False)
            print(result)
        else:
            with db.engine.begin() as conn:
                result = conn.execute(text(sql_query))
            print(json.dumps({"status": "ok", "rows_affected": result.rowcount}, ensure_ascii=False))
        
    except Exception as e:
        # 输出标准 JSON 错误格式，方便 AI 解析
        error_msg = json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False)
        print(error_msg)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"status": "error", "message": "Usage: python db_query.py <sql_query>"}))
        sys.exit(1)
        
    # 获取命令行传入的 SQL
    # 注意：如果 SQL 包含空格，调用时需要用引号包裹
    query = sys.argv[1]
    query_db(query)
