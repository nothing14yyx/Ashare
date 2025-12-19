"""项目启动脚本入口, 直接执行即可运行示例."""

from ashare.app import AshareApp
from ashare.ma5_ma20_trend_strategy import MA5MA20StrategyRunner
from ashare.schema_manager import ensure_schema


def main() -> None:
    ensure_schema()
    AshareApp().run()
    # 策略是否执行由 config.yaml: strategy_ma5_ma20_trend.enabled 控制
    MA5MA20StrategyRunner().run()


if __name__ == "__main__":
    main()
