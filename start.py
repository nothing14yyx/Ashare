"""项目启动脚本入口, 直接执行即可运行示例."""

import datetime as dt

from ashare.app import AshareApp
from ashare.env_snapshot_utils import resolve_weekly_asof_date
from ashare.ma5_ma20_trend_strategy import MA5MA20StrategyRunner
from ashare.open_monitor import MA5MA20OpenMonitorRunner
from ashare.schema_manager import ensure_schema


def main() -> None:
    ensure_schema()
    AshareApp().run()
    # 策略是否执行由 config.yaml: strategy_ma5_ma20_trend.enabled 控制
    MA5MA20StrategyRunner().run()

    # feat: 实盘前生成指数周线通道环境快照，避免 open_monitor 缺少 env_snapshot
    try:
        asof_date = resolve_weekly_asof_date(False)
        runner = MA5MA20OpenMonitorRunner()
        checked_at = dt.datetime.now()
        runner.build_and_persist_env_snapshot(
            asof_date,
            monitor_date=asof_date,
            run_id=f"WEEKLY_{asof_date}",
            checked_at=checked_at,
        )
        runner.logger.info("已生成指数周线通道环境快照（asof_date=%s）。", asof_date)
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] 生成指数周线通道环境快照失败：{exc}")


if __name__ == "__main__":
    main()
