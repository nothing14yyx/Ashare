"""
Sector Rotation Analyzer Skill
Usage: python .ai/skills/sector_rotation_analyzer.py [date]
"""
import sys
import pandas as pd
from sqlalchemy import text
from ashare.core.db import MySQLWriter, DatabaseConfig
import datetime as dt

def analyze_sector_rotation(target_date: str = None):
    db = MySQLWriter(DatabaseConfig.from_env())
    
    if not target_date:
        target_date = dt.date.today().isoformat()
        
    print(f"正在读取板块历史行情 (基准日: {target_date})...")
    
    # 读取最近 90 天数据，确保覆盖目标日期前的窗口
    stmt = text("""
        SELECT date, board_name, `收盘` as close 
        FROM board_industry_hist_daily 
        WHERE date <= :target_date 
          AND date >= DATE_SUB(:target_date, INTERVAL 90 DAY)
        ORDER BY date ASC
    """)
    
    with db.engine.connect() as conn:
        df = pd.read_sql(stmt, conn, params={"target_date": target_date})
    
    if df.empty:
        print("未找到板块历史数据。")
        return

    # 转换日期格式
    df['date'] = pd.to_datetime(df['date'])
    
    # 2. 数据透视：行=日期，列=板块，值=收盘价
    pivot_df = df.pivot_table(index='date', columns='board_name', values='close')
    pivot_df = pivot_df.ffill()
    
    # 获取实际分析的日期（可能是 target_date 或最近的一个交易日）
    latest_date = pivot_df.index[-1]
    print(f"实际分析日期: {latest_date.date()}")
    
    # 3. 计算收益率
    # 长期强度：20日涨跌幅
    ret_20d = pivot_df.pct_change(20).iloc[-1]
    # 短期动量：5日涨跌幅
    ret_5d = pivot_df.pct_change(5).iloc[-1]
    
    # 4. 构建分析 DataFrame
    metrics = pd.DataFrame({
        'ret_20d': ret_20d,
        'ret_5d': ret_5d
    }).dropna()
    
    # 5. 标准化排名 (0~100分，100为最强)
    metrics['rank_trend'] = metrics['ret_20d'].rank(pct=True) * 100
    metrics['rank_mom'] = metrics['ret_5d'].rank(pct=True) * 100
    
    # 6. 划分象限
    def classify_rotation(row):
        # 阈值设为 50 (中位数)
        strong_trend = row['rank_trend'] >= 50
        strong_mom = row['rank_mom'] >= 50
        
        if strong_trend and strong_mom:
            return "🚀 领涨 (Leading)"
        elif not strong_trend and strong_mom:
            return "📈 转强 (Improving)"
        elif strong_trend and not strong_mom:
            return "📉 转弱 (Weakening)"
        else:
            return "🥀 滞后 (Lagging)"

    metrics['phase'] = metrics.apply(classify_rotation, axis=1)
    
    # 7. 输出报告
    print("\n====== 板块轮动分析报告 (Top 5 per Phase) ======")
    
    for phase in ["🚀 领涨 (Leading)", "📈 转强 (Improving)", "📉 转弱 (Weakening)", "🥀 滞后 (Lagging)"]:
        subset = metrics[metrics['phase'] == phase]
        
        if "Leading" in phase or "Weakening" in phase:
            subset = subset.sort_values(by='rank_trend', ascending=False)
        else:
            subset = subset.sort_values(by='rank_mom', ascending=False)
            
        print(f"\n{phase} - 共 {len(subset)} 个板块:")
        print(f"{ '板块名称':<12} {'20日涨幅':<10} {'5日涨幅':<10} {'趋势分':<6} {'动量分':<6}")
        print("-" * 60)
        
        for name, row in subset.head(5).iterrows():
            print(f"{name:<12} {row['ret_20d']:.2%}     {row['ret_5d']:.2%}     {row['rank_trend']:.0f}     {row['rank_mom']:.0f}")

    # 8. 特别关注
    print("\n====== ⚡ 重点关注：异动爆发 (转强且动量>90) ======")
    breakout = metrics[(metrics['phase'] == "📈 转强 (Improving)") & (metrics['rank_mom'] > 90)]
    if not breakout.empty:
        for name, row in breakout.sort_values('ret_5d', ascending=False).iterrows():
            print(f"🔥 {name}: 20日 {row['ret_20d']:.2%}, 5日 {row['ret_5d']:.2%}")
    else:
        print("无")

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else None
    analyze_sector_rotation(target)