"""运行 MA5-MA20 顺势趋势波段系统。

用法：
  python run_ma5_ma20_trend_strategy.py

说明：
  - 读取 config.yaml 的 strategy_ma5_ma20_trend 配置。
  - 需要你已先运行 python start.py，把 history_recent_{N}_days 等表准备好。
"""

from ashare.ma5_ma20_trend_strategy import MA5MA20StrategyRunner
from ashare.schema_manager import ensure_schema


def main() -> None:
    # 独立执行脚本：不受 config.yaml 的 enabled 总开关限制
    ensure_schema()
    MA5MA20StrategyRunner().run(force=True)


if __name__ == "__main__":
    main()
