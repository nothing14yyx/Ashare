"""使用 stock_zh_a_daily 接口验证网络与代理配置的示例脚本.

直接运行本文件即可触发一次数据抓取, 便于排查网络问题。
"""
from __future__ import annotations

from ashare.fetcher import AshareDataFetcher
from ashare.config import ProxyConfig


def run_daily_check(symbol: str = "sh600000", adjust: str | None = "qfq") -> None:
    """尝试通过 stock_zh_a_daily 获取样例数据并打印结果."""

    fetcher = AshareDataFetcher(proxy_config=ProxyConfig.from_env())
    print(f"开始通过 stock_zh_a_daily 获取 {symbol} 的日线行情数据...")

    try:
        data = fetcher.daily_quotes(symbol=symbol, adjust=adjust)
    except (RuntimeError, ValueError) as exc:
        print(f"获取失败: {exc}")
        print("请检查网络或代理配置, 然后重试。")
        return

    if data.empty:
        print("接口返回为空, 请确认股票代码是否正确或稍后再试。")
        return

    print("成功获取到数据, 前 5 行预览:")
    print(data.head())


if __name__ == "__main__":
    run_daily_check()
