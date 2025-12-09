"""A 股数据获取工具的脚本入口."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

from .fetcher import AshareDataFetcher


class AshareApp:
    """通过脚本方式运行的数据获取工具."""

    def __init__(
        self, output_dir: str | Path = "output", proxies: dict[str, str] | None = None
    ):
        self.fetcher = AshareDataFetcher(proxies=proxies)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _print_interfaces(self, interfaces: Iterable[str]) -> None:
        preview = list(interfaces)
        print(f"数据字典共发现 {len(preview)} 个 A 股接口, 前 10 个预览:")
        for name in preview[:10]:
            print(f" - {name}")

    def _save_sample(self, df: pd.DataFrame, filename: str) -> Path:
        target = self.output_dir / filename
        df.to_csv(target, index=False)
        return target

    def run(self) -> None:
        """执行数据获取示例.

        1. 输出 A 股数据接口列表的摘要;
        2. 抓取实时行情, 给出前几只股票的代码与名称;
        3. 使用首只股票下载近 30 天的历史行情并写入 CSV。
        """

        try:
            interfaces = self.fetcher.available_interfaces()
        except RuntimeError as exc:
            print(f"获取数据字典失败: {exc}")
            print(
                "请确认当前网络或代理可访问 https://akshare.akfamily.xyz 。"
            )
            return

        self._print_interfaces(interfaces)

        try:
            symbols = self.fetcher.symbol_list().head(5)
        except RuntimeError as exc:
            print(f"实时行情获取失败: {exc}")
            print("若处于代理环境, 请确认 HTTPS 代理可用或在环境变量中配置。")
            return
        print("\n实时行情示例 (前 5 条):")
        print(symbols)

        if symbols.empty:
            print("未能获取到实时行情数据, 请检查网络环境或 AKShare 的可用性。")
            return

        sample_symbol = symbols.iloc[0]["代码"]
        end = date.today()
        start = end - timedelta(days=30)

        try:
            history = self.fetcher.history_quotes(
                symbol=sample_symbol,
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
                period="daily",
                adjust="qfq",
            )
        except RuntimeError as exc:
            print(f"历史行情获取失败: {exc}")
            print("请检查网络连通性或稍后重试。")
            return

        output_path = self._save_sample(history, f"{sample_symbol}_history.csv")
        print(f"\n已将 {sample_symbol} 的近 30 天历史行情保存至 {output_path}")


if __name__ == "__main__":
    AshareApp().run()
