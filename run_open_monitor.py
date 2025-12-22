# run_open_monitor.py
from __future__ import annotations

from ashare.open_monitor import MA5MA20OpenMonitorRunner
from ashare.schema_manager import ensure_schema


def main() -> None:
    # 确保表结构（与你项目其他入口一致）
    ensure_schema()

    # 单次执行 open_monitor（force=True：即使 open_monitor.enabled=false 也跑）
    runner = MA5MA20OpenMonitorRunner()
    runner.run(force=True)


if __name__ == "__main__":
    main()
