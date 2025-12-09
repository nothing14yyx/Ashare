"""A 股接口清单导出脚本入口."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import ProxyConfig
from .fetcher import AshareDataFetcher


class AshareApp:
    """通过脚本方式导出 A 股接口清单的应用."""

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


if __name__ == "__main__":
    AshareApp().run()
