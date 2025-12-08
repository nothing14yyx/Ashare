# A 股数据获取示例

本项目基于 [AKShare](https://akshare.akfamily.xyz/introduction.html) 提供快速示例，演示如何拉取 A 股实时行情与历史数据。接口均使用新浪端口，避免东财（em）风控导致的连接报错。项目入口为 `start.py`，无需额外命令行参数即可运行。

## 运行方式
1. 安装依赖：`pip install akshare pandas`
2. 运行脚本：`python start.py`

运行后会在 `output/` 目录下生成：
- `realtime_quotes.csv`：配置内股票的实时行情。
- `history_recent_<days>_days.csv`：配置内所有股票最近一段时间的历史行情合并数据。
- `{industry|concept}_boards_realtime.csv`：按行业/概念聚合的实时涨跌幅、成交额/成交量汇总。
- `{industry|concept}_boards_recent_<days>_days.csv`：对实时涨幅靠前的板块计算最近行情的累计涨幅与成交放大倍数。

如需修改股票列表或日期范围，可调整 `src/config.py` 中的默认配置。
