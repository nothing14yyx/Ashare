# A 股数据获取示例

本项目基于 [AKShare](https://akshare.akfamily.xyz/introduction.html) 提供快速示例，演示如何拉取 A 股实时行情与历史数据。项目入口为 `start.py`，无需额外命令行参数即可运行。

## 运行方式
1. 安装依赖：`pip install akshare pandas`
2. 运行脚本：`python start.py`

运行后会在 `output/` 目录下生成：
- `realtime_quotes.csv`：配置内股票的实时行情。
- `history_<code>_<start>_<end>.csv`：对应股票的历史行情数据。

如需修改股票列表或日期范围，可调整 `src/config.py` 中的默认配置。
