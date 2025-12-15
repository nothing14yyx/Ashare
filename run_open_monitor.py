"""运行开盘监测（基于前一交易日收盘信号）。

用法：
  python run_open_monitor.py

说明：
  - 读取 config.yaml 的 open_monitor 配置。
  - 默认读取 strategy_ma5_ma20_signals “最新交易日”的 BUY 信号。
  - 拉取实时行情后输出“可执行/不可执行”清单，并可选写入 DB / 导出 CSV。
"""

from ashare.open_monitor import MA5MA20OpenMonitorRunner


def main() -> None:
    # 独立执行脚本：不受 config.yaml 的 enabled 总开关限制
    MA5MA20OpenMonitorRunner().run(force=True)


if __name__ == "__main__":
    main()
