# A 股数据采集示例（Baostock 版）

基于 [Baostock](http://www.baostock.com/) 的示例项目，展示如何登录会话、批量拉取股票列表与近 n 日日线数据，并按成交额筛选高流动性标的。

## 功能
- 自动登录 Baostock，获取最近一个交易日的股票列表并写入 MySQL 表 `a_share_stock_list`。
- 拉取全市场最近 n 个交易日的日线数据，统一写入表 `history_recent_n_days`。
- 根据日线成交额构建候选池，剔除 ST/退市标的与停牌记录。
- 输出成交额排序，便于快速定位高流动性标的：写入表 `a_share_top_liquidity`。
- 可选启用 Akshare 行为证据采集：
  - 龙虎榜详情写入 `a_share_lhb_detail`
  - 两融明细写入 `a_share_margin_detail`
  - 北向持股排行写入 `a_share_hsgt_hold_rank`
  - 股东户数汇总与明细写入 `a_share_gdhs` 与 `a_share_gdhs_detail`
- 提供脚本化入口 `start.py`，直接运行即可完成上述流程。

## 使用方式
1. 确保已安装依赖（`pip install -r requirements.txt`）。
2. 在项目根目录直接运行启动脚本：
   ```bash
   python start.py
   ```
3. 数据将写入 MySQL 数据库 `ashare`（可通过环境变量覆盖连接信息）：
   - `a_share_stock_list`: 最近交易日的股票列表
   - `history_recent_n_days`: 全市场最近 n 个交易日的日线数据
   - `a_share_universe`: 剔除 ST、退市与停牌标的后的候选池
   - `a_share_top_liquidity`: 按成交额排序的高流动性标的列表

## 开盘监测（可选）
项目内置“开盘监测”脚本：读取前一交易日收盘信号（BUY），结合实时行情再次过滤，输出“可执行/不可执行”清单。

- 单次运行：
  ```bash
  python run_open_monitor.py
  ```

- 定时调度（每整 5 / 10 分钟触发一次）：
  ```bash
  python run_open_monitor_scheduler.py --interval 5
  # 或
  python run_open_monitor_scheduler.py --interval 10
  ```

## 注意事项
- Baostock 需要登录才能正常请求接口，项目内的 `BaostockSession` 会自动处理登录与登出。
- 若网络环境不稳定导致个别标的的历史日线为空，脚本会自动跳过并继续处理其他标的。
- 生成候选池与流动性排序均依赖日线中的 `amount` 字段，如需调整排序逻辑可修改 `AshareUniverseBuilder`。
