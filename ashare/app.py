"""A 股接口清单导出脚本入口."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import ProxyConfig
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
        self.fetcher = AshareDataFetcher(proxy_config=proxy_config)
        self.universe_builder = AshareUniverseBuilder(
            top_liquidity_count=top_liquidity_count
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
        2. 将接口清单写入 CSV 便于查阅。
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


if __name__ == "__main__":
    AshareApp().run()
