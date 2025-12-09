"""A 股数据获取工具的脚本入口."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import ProxyConfig
from .fetcher import AshareDataFetcher


class AshareApp:
    """通过脚本方式运行的数据获取工具."""

    def __init__(
        self, output_dir: str | Path = "output", proxy_config: ProxyConfig | None = None
    ):
        self.fetcher = AshareDataFetcher(proxy_config=proxy_config)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _print_interfaces(self, interfaces: Iterable[str]) -> None:
        preview = list(interfaces)
        print(f"数据字典共发现 {len(preview)} 个 A 股接口, 前 10 个预览:")
        for name in preview[:10]:
            print(f" - {name}")

    def _save_interfaces(self, interfaces: Iterable[str]) -> Path:
        interfaces_df = pd.DataFrame({"interface": list(interfaces)})
        return self._save_sample(interfaces_df, "a_share_interfaces.csv")

    def _save_sample(self, df: pd.DataFrame, filename: str) -> Path:
        target = self.output_dir / filename
        df.to_csv(target, index=False)
        return target

    def _select_history_symbol(self, symbols: pd.DataFrame) -> str:
        """为历史行情查询挑选更可靠的示例代码."""

        codes = symbols["代码"].astype(str)
        pure_digits = codes[codes.str.fullmatch(r"\d{6}")]
        if not pure_digits.empty:
            return pure_digits.iloc[0]

        trimmed = codes.str.replace(r"^[a-zA-Z]{2}", "", regex=True)
        trimmed_digits = trimmed[trimmed.str.fullmatch(r"\d{6}")]
        if not trimmed_digits.empty:
            return trimmed_digits.iloc[0]

        return "000001"

    def run(self) -> None:
        """执行数据获取示例.

        1. 输出 A 股数据接口列表的摘要;
        2. 抓取实时行情, 给出前几只股票的代码与名称;
        3. 使用首只股票下载近 30 天的历史行情并写入 CSV。
        """

        try:
            interfaces = self.fetcher.available_interfaces()
        except RuntimeError as exc:
            print(f"加载数据字典失败: {exc}")
            print("无法校验接口列表, 请先解决网络问题后重试。")
            return

        self._print_interfaces(interfaces)
        saved_interfaces = self._save_interfaces(interfaces)
        print(f"已将全部接口名称保存至 {saved_interfaces}")

        try:
            symbols = self.fetcher.symbol_list().head(5)
        except RuntimeError as exc:
            print(f"获取实时行情失败: {exc}")
            print("请检查当前网络或代理配置, 重新运行脚本。")
            return

        print("\n实时行情示例 (前 5 条):")
        print(symbols)

        if symbols.empty:
            print("未能获取到实时行情数据, 请检查网络环境或 AKShare 的可用性。")
            return

        sample_symbol = self._select_history_symbol(symbols)
        print(f"将使用 {sample_symbol} 作为历史行情示例股票。")
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
            fallback_symbol = "000001"
            if sample_symbol != fallback_symbol:
                print(
                    f"获取 {sample_symbol} 的历史行情失败, 尝试使用备用股票 {fallback_symbol}。"
                )
                try:
                    history = self.fetcher.history_quotes(
                        symbol=fallback_symbol,
                        start_date=start.strftime("%Y%m%d"),
                        end_date=end.strftime("%Y%m%d"),
                        period="daily",
                        adjust="qfq",
                    )
                    sample_symbol = fallback_symbol
                except RuntimeError as fallback_exc:
                    print(f"获取历史行情失败: {fallback_exc}")
                    print("请确认网络连通性或稍后重试。")
                    return
            else:
                print(f"获取历史行情失败: {exc}")
                print("请确认网络连通性或稍后重试。")
                return

        output_path = self._save_sample(history, f"{sample_symbol}_history.csv")
        print(f"\n已将 {sample_symbol} 的近 30 天历史行情保存至 {output_path}")


if __name__ == "__main__":
    AshareApp().run()
