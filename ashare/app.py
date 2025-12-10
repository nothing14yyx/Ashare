"""基于 Baostock 的示例脚本入口."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd

from .baostock_core import BaostockDataFetcher
from .baostock_session import BaostockSession
from .universe import AshareUniverseBuilder


class AshareApp:
    """通过脚本方式导出 Baostock 数据的应用."""

    def __init__(
        self,
        output_dir: str | Path = "output",
        top_liquidity_count: int = 100,
    ) -> None:
        self.session = BaostockSession()
        self.fetcher = BaostockDataFetcher(self.session)
        self.universe_builder = AshareUniverseBuilder(
            top_liquidity_count=top_liquidity_count,
        )
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _save_sample(self, df: pd.DataFrame, filename: str) -> Path:
        target = self.output_dir / filename
        df.to_csv(target, index=False)
        return target

    def _export_stock_list(self, trade_date: str) -> Path:
        stock_df = self.fetcher.get_stock_list(trade_date)
        if stock_df.empty:
            raise RuntimeError("获取股票列表失败：返回为空。")

        path = self._save_sample(stock_df, "a_share_stock_list.csv")
        print(f"已保存 {len(stock_df)} 只股票的列表至 {path}")
        return path

    def _export_recent_daily_history(
        self, stock_df: pd.DataFrame, end_date: str, days: int = 30
    ) -> Tuple[pd.DataFrame, Path]:
        if stock_df.empty or "code" not in stock_df.columns:
            raise RuntimeError("导出历史日线失败：股票列表为空或缺少 code 列。")

        end_day = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
        start_day = (end_day - dt.timedelta(days=days * 3)).isoformat()

        history_frames: list[pd.DataFrame] = []
        for code in stock_df["code"]:
            daily_df = self.fetcher.get_kline(
                code=code,
                start_date=start_day,
                end_date=end_date,
                freq="d",
                adjustflag="1",
            )
            if daily_df.empty:
                continue

            numeric_cols = ["amount", "volume", "close", "open", "high", "low"]
            for col in numeric_cols:
                if col in daily_df.columns:
                    daily_df[col] = pd.to_numeric(daily_df[col], errors="coerce")

            history_frames.append(daily_df)

        if not history_frames:
            raise RuntimeError("导出历史日线失败：全部股票均未返回数据。")

        combined = pd.concat(history_frames, ignore_index=True)
        combined_path = self._save_sample(
            combined, f"history_recent_{days}_days.csv"
        )
        print(
            "已导出最近 {days} 个交易日的历史数据，共 {rows} 行，路径：{path}".format(
                days=days, rows=len(combined), path=combined_path
            )
        )
        return combined, combined_path

    def _print_preview(self, interfaces: Iterable[str]) -> None:
        preview = list(interfaces)
        print(f"已发现 {len(preview)} 个项目组件，前 10 个预览：")
        for name in preview[:10]:
            print(f" - {name}")

    def run(self) -> None:
        """执行 Baostock 数据导出与候选池筛选示例。"""

        # 1) 预览当前模块内可用组件（示例信息输出）
        self._print_preview(
            [
                "BaostockSession",
                "BaostockDataFetcher",
                "AshareUniverseBuilder",
            ]
        )

        # 2) 获取最近交易日并导出股票列表
        latest_trade_day = self.fetcher.get_latest_trading_date()
        print(f"最近交易日：{latest_trade_day}")
        try:
            stock_list_path = self._export_stock_list(latest_trade_day)
        except RuntimeError as exc:
            print(f"导出股票列表失败: {exc}")
            return

        stock_df = pd.read_csv(stock_list_path)

        # 3) 导出最近 30 日历史日线
        try:
            history_df, history_path = self._export_recent_daily_history(
                stock_df, latest_trade_day, days=30
            )
        except RuntimeError as exc:
            print(f"导出最近 30 个交易日的日线数据失败: {exc}")
            return

        # 4) 构建候选池并挑选成交额前 N 名
        try:
            universe_df = self.universe_builder.build_universe(stock_df, history_df)
        except RuntimeError as exc:
            print(f"生成当日候选池失败: {exc}")
            return

        universe_path = self._save_sample(universe_df, "a_share_universe.csv")
        print(f"已生成候选池：{universe_path}")

        try:
            top_liquidity = self.universe_builder.pick_top_liquidity(universe_df)
        except RuntimeError as exc:
            print(f"挑选成交额前 {self.universe_builder.top_liquidity_count} 名失败: {exc}")
            return

        top_liquidity_path = self._save_sample(
            top_liquidity, "a_share_top_liquidity.csv"
        )
        print(
            "已将成交额排序结果写入 {path}，可用于筛选高流动性标的。".format(
                path=top_liquidity_path
            )
        )

        # 5) 提示历史日线路径
        print(f"历史日线数据已保存：{history_path}")


if __name__ == "__main__":
    AshareApp().run()
