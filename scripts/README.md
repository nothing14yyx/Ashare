# AShare 自动化流水线 (Pipelines)

项目提供了四段式的自动化流水线，严格区分了数据生命周期的不同阶段。

## 流水线概览

| 顺序 | 脚本名称 | 阶段 | 核心任务 |
| :--- | :--- | :--- | :--- |
| 1 | `pipeline_1_fetch_raw.py` | **数据准备** | 从 Baostock/AkShare 抓取原始日线、基本面、龙虎榜等数据存入历史表。 |
| 2 | `pipeline_2_process_indicators.py` | **数据处理** | 基于历史数据计算全市场指标，先周线后日线环境，并补齐板块轮动状态。 |
| 3 | `pipeline_3_run_strategy.py` | **策略运行** | 扫描 Universe，应用 MA5-MA20 策略生成买卖信号，并预计算筹码分数。 |
| 4 | `pipeline_4_execution_monitor.py` | **实盘监控** | 结合实时行情（Snapshot）对信号进行临场评估，给出最终执行建议。 |

## 执行指南

建议每日收盘后按顺序运行 Pipeline 1-3，次日开盘前或盘中运行 Pipeline 4。

```bash
# 每日收盘后
python scripts/pipeline_1_fetch_raw.py
python scripts/pipeline_2_process_indicators.py
python scripts/pipeline_3_run_strategy.py

# 次日交易时段
python scripts/pipeline_4_execution_monitor.py
```

## 注意事项
- **Pipeline 3** 执行后，数据库视图 `strategy_ready_signals` 会自动汇总所有就绪信号。
- **Pipeline 4** 的结果存储在 `strategy_open_monitor_eval` 表中。
