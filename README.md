# A 股数据采集示例（Baostock 版）

基于 [Baostock](http://www.baostock.com/) 的示例项目，展示如何登录会话、批量拉取股票列表与近 30 日日线数据，并按成交额筛选高流动性标的。

## 功能
- 自动登录 Baostock，获取最近一个交易日的股票列表并保存到 `output/a_share_stock_list.csv`。
- 拉取全市场最近 30 个交易日的日线数据，统一保存为 `output/history_recent_30_days.csv`。
- 根据日线成交额构建候选池，剔除 ST/退市标的与停牌记录。
- 输出成交额排序，便于快速定位高流动性标的：`output/a_share_top_liquidity.csv`。
- 提供脚本化入口 `start.py`，直接运行即可完成上述流程。

## 使用方式
1. 确保已安装依赖（`pip install -r requirements.txt`）。
2. 在项目根目录直接运行启动脚本：
   ```bash
   python start.py
   ```
3. 输出文件默认保存在仓库根目录下的 `output/` 文件夹：
   - `a_share_stock_list.csv`: 最近交易日的股票列表
   - `history_recent_30_days.csv`: 全市场最近 30 个交易日的日线数据
   - `a_share_universe.csv`: 剔除 ST、退市与停牌标的后的候选池
   - `a_share_top_liquidity.csv`: 按成交额排序的高流动性标的列表

## 注意事项
- Baostock 需要登录才能正常请求接口，项目内的 `BaostockSession` 会自动处理登录与登出。
- 若网络环境不稳定导致个别标的的历史日线为空，脚本会自动跳过并继续处理其他标的。
- 生成候选池与流动性排序均依赖日线中的 `amount` 字段，如需调整排序逻辑可修改 `AshareUniverseBuilder`。
