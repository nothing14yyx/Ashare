# A股量化策略分析系统

基于 Baostock 与 AkShare 的 A 股量化分析平台，覆盖数据采集、指标加工、趋势策略与开盘监测，支持数据库自动建表与日常流水线化运行。

## 项目概览

- **数据采集**：Baostock 日线/指数/基本面，AkShare 龙虎榜/两融/股东户数等行为数据
- **指标与环境**：技术指标、日线市场环境、周线通道与市场状态
- **策略执行**：MA5-MA20 趋势跟随（含量价与动量确认、回踩与形态判断等）
- **开盘监测**：基于前一交易日信号的盘前/盘中执行评估（EXECUTE/WAIT/STOP）
- **数据库管理**：Schema 自动创建与校验，保证表结构一致

## 项目结构

```
AShare/
├── ashare/                 # 核心模块（core/data/indicators/strategies/monitor/utils）
├── scripts/                # 日常流水线脚本（pipeline_*.py）
├── docs/                   # 项目文档
├── output/                 # 统一导出目录（日志/导出文件等）
├── config.yaml             # 主配置文件
├── requirements.txt        # 依赖包
└── start.py                # 旧版入口（当前建议使用 scripts 流水线）
```

## 安装与配置

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 数据库配置
在 `config.yaml` 中配置 MySQL：

```yaml
database:
  host: 127.0.0.1
  port: 3306
  user: root
  password: ""
  db_name: ashare
```

### 3. 代理配置（可选）
如需代理网络：

```yaml
proxy:
  http: http://127.0.0.1:7890
  https: http://127.0.0.1:7890
```

## 使用流程（推荐）

建议每日收盘后运行 Pipeline 1-3，次日交易时段运行 Pipeline 4。

```bash
# 1) 数据采集
python -m scripts.pipeline_1_fetch_raw

# 2) 指标处理（日线/周线/技术指标）
python -m scripts.pipeline_2_process_indicators

# 3) 策略扫描（趋势跟随）
python -m scripts.pipeline_3_run_strategy

# 4) 开盘/盘中执行监测
python -m scripts.pipeline_4_execution_monitor
```

如需定时执行开盘监测：

```bash
python -m scripts.run_open_monitor_scheduler
```

## 关键配置

### app
- `app.output_dir`: 统一导出目录（默认 `output`）
- `app.fetch_daily_kline`: 是否拉取/更新日线 K 线
- `app.fetch_stock_meta`: 是否拉取股票元数据

### strategy_ma5_ma20_trend
- `enabled`: 是否启用 MA5-MA20 趋势策略
- `universe_source`: 选股池来源（top_liquidity/universe/all）
- `signals_write_scope`: 信号写入范围（latest/window）

### open_monitor
- `quote_source`: 行情来源（eastmoney/akshare）
- `signal_lookback_days`: 回看信号天数
- `interval_minutes`: 调度间隔（供调度器/去重桶使用）

## 核心表与视图

- `history_daily_kline`: 日线 K 线历史
- `strategy_indicator_daily`: 日线指标
- `strategy_signal_events`: 策略信号事件
- `strategy_ready_signals`（视图）: 可执行信号视图
- `strategy_open_monitor_eval`: 开盘监测评估结果

## 注意事项

- 输出文件统一在根目录 `output/`，该目录已加入 `.gitignore`。
- 数据采集建议在交易日收盘后执行，保证数据完整性。
- 运行脚本请保持在项目根目录，优先使用 `python -m scripts.xxx`。

## 许可证

本项目仅供学习和研究使用。