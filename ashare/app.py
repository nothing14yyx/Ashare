"""A 股接口清单导出脚本入口."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import ProxyConfig
from .core_fetcher import AshareCoreFetcher
from .fetcher import AshareDataFetcher
from .universe import AshareUniverseBuilder


class AshareApp:
    """通过脚本方式导出 A 股接口清单的应用."""

    def __init__(
        self,
        output_dir: str | Path = "output",
        proxy_config: ProxyConfig | None = None,
        top_liquidity_count: int = 100,
    ):
        self.core_fetcher = AshareCoreFetcher()
        self.fetcher = AshareDataFetcher(proxy_config=proxy_config)
        self.universe_builder = AshareUniverseBuilder(
            top_liquidity_count=top_liquidity_count,
            fetcher=self.core_fetcher,
        )
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

    def run(self) -> None:
        """执行接口清单导出示例.

        1. 输出 A 股数据接口列表的摘要;
        2. 将接口清单写入 CSV 便于查阅;
        3. 导出全市场实时行情与最近 30 日历史日线数据;
        4. 构建剔除 ST/停牌标的的候选池并筛选高流动性标的。
        """

        interfaces: list[str] | None = None
        try:
            interfaces = self.fetcher.available_interfaces()
        except RuntimeError as exc:
            print(f"加载数据字典失败: {exc}")
            print("数据字典仅用于接口清单导出，不影响交易数据流程。")

        if interfaces:
            self._print_interfaces(interfaces)
            saved_interfaces = self._save_interfaces(interfaces)
            print(f"已将全部接口名称保存至 {saved_interfaces}")

        try:
            realtime_quotes_path = self.export_realtime_quotes(self.output_dir)
        except RuntimeError as exc:
            print(f"导出实时行情失败: {exc}")
        else:
            print(f"已导出全市场实时行情至 {realtime_quotes_path}")

        try:
            history_path = self.export_recent_daily_history(self.output_dir, days=30)
        except RuntimeError as exc:
            print(f"导出最近 30 个交易日的日线数据失败: {exc}")
        else:
            print(f"已导出最近 30 个交易日的历史数据至 {history_path}")

        try:
            universe_df = self.universe_builder.build_universe()
        except RuntimeError as exc:
            print(f"生成当日候选池失败: {exc}")
            return

        universe_path = self._save_sample(universe_df, "a_share_universe.csv")
        print(f"已生成剔除 ST/停牌的候选池: {universe_path}")

        try:
            top_liquidity = self.universe_builder.pick_top_liquidity(universe_df)
        except RuntimeError as exc:
            print(f"挑选成交额前 {self.universe_builder.top_liquidity_count} 名失败: {exc}")
            return

        top_liquidity_path = self._save_sample(
            top_liquidity, "a_share_top_liquidity.csv"
        )
        print(
            "已将成交额排序结果写入 "
            f"{top_liquidity_path}, 可用于盘中选板块龙头或流动性筛选。"
        )

    def export_realtime_quotes(self, output_dir: Path) -> Path:
        """使用 AshareCoreFetcher 导出全市场实时行情到 CSV。"""
        df = self.core_fetcher.get_realtime_all_a()
        if df.empty:
            raise RuntimeError("导出实时行情失败：A 股实时行情为空。")

        out_path = output_dir / "realtime_quotes.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"已导出全市场实时行情至 {out_path}")
        return out_path

    def export_recent_daily_history(self, output_dir: Path, days: int = 30) -> Path:
        """
        拉取最近 N 日（例如 30 日）的日线数据，并导出到 CSV。

        简化版本建议：
        - 先用 get_realtime_all_a() 拿到全市场代码列表；
        - 对每个代码，使用 get_daily_a_sina(...) 拉取一段时间；
        - 只取最近 `days` 个交易日的数据；
        - 合并成一个大 DataFrame（增加一列代码），写出 CSV。
        """
        spot_df = self.core_fetcher.get_realtime_all_a()
        if spot_df.empty or "代码" not in spot_df.columns:
            raise RuntimeError("导出历史日线失败：无法获取全市场代码列表。")

        history_frames = []
        for code in spot_df["代码"].astype(str):
            symbol = self._to_sina_symbol(code)
            daily_df = self.core_fetcher.get_daily_a_sina(symbol=symbol, adjust="")
            if daily_df.empty:
                continue

            trimmed = daily_df.tail(days).copy()
            trimmed.insert(0, "代码", code)
            history_frames.append(trimmed)

        if not history_frames:
            raise RuntimeError("导出历史日线失败：所有股票的日线数据均为空。")

        combined = pd.concat(history_frames, ignore_index=True)
        out_path = output_dir / f"history_recent_{days}_days.csv"
        combined.to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path

    @staticmethod
    def _to_sina_symbol(code: str) -> str:
        """将六位代码转换为新浪日线接口需要的带交易所前缀的代码."""
        normalized = code.strip()
        if not normalized:
            return normalized

        if normalized.startswith(("5", "6", "9")):
            prefix = "sh"
        else:
            prefix = "sz"
        return f"{prefix}{normalized}"


if __name__ == "__main__":
    AshareApp().run()
