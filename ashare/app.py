"""基于 Baostock 的示例脚本入口."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd
from tqdm import tqdm

from .baostock_core import BaostockDataFetcher
from .baostock_session import BaostockSession
from .universe import AshareUniverseBuilder
from .utils import setup_logger


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
        self.logger = setup_logger(self.output_dir)

    def _save_sample(self, df: pd.DataFrame, filename: str) -> Path:
        target = self.output_dir / filename
        df.to_csv(target, index=False)
        return target

    def _export_stock_list(self, trade_date: str) -> Path:
        stock_df = self.fetcher.get_stock_list(trade_date)
        if stock_df.empty:
            raise RuntimeError("获取股票列表失败：返回为空。")

        path = self._save_sample(stock_df, "a_share_stock_list.csv")
        self.logger.info("已保存 %s 只股票的列表至 %s", len(stock_df), path)
        return path

    def _export_recent_daily_history(
        self, stock_df: pd.DataFrame, end_date: str, days: int = 30
    ) -> Tuple[pd.DataFrame, Path]:
        if stock_df.empty or "code" not in stock_df.columns:
            raise RuntimeError("导出历史日线失败：股票列表为空或缺少 code 列。")

        end_day = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
        start_day = (end_day - dt.timedelta(days=days + 5)).isoformat()

        history_frames: list[pd.DataFrame] = []
        total = len(stock_df)
        self.logger.info("开始导出 %s 只股票的 %s 日历史数据", total, days)

        for idx, code in enumerate(
            tqdm(stock_df["code"], desc="数据拉取进度"), start=1
        ):
            try:
                daily_df = self.fetcher.get_kline(
                    code=code,
                    start_date=start_day,
                    end_date=end_date,
                    freq="d",
                    adjustflag="1",
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("股票 %s 数据拉取失败: %s", code, exc)
                continue

            if daily_df.empty:
                continue

            numeric_cols = ["amount", "volume", "close", "open", "high", "low"]
            for col in numeric_cols:
                if col in daily_df.columns:
                    daily_df[col] = pd.to_numeric(daily_df[col], errors="coerce")

            history_frames.append(daily_df)

            if idx % 500 == 0 or idx == total:
                self.logger.info("已完成 %s/%s 支股票的数据拉取", idx, total)

        if not history_frames:
            raise RuntimeError("导出历史日线失败：全部股票均未返回数据。")

        combined = pd.concat(history_frames, ignore_index=True)
        combined_path = self._save_sample(
            combined, f"history_recent_{days}_days.csv"
        )
        self.logger.info(
            "已导出最近 %s 个交易日的历史数据，共 %s 行，路径：%s",
            days,
            len(combined),
            combined_path,
        )
        return combined, combined_path

    def _print_preview(self, interfaces: Iterable[str]) -> None:
        preview = list(interfaces)
        self.logger.info("已发现 %s 个项目组件，前 10 个预览：", len(preview))
        for name in preview[:10]:
            self.logger.info(" - %s", name)

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
        self.logger.info("最近交易日：%s", latest_trade_day)
        try:
            stock_list_path = self._export_stock_list(latest_trade_day)
        except RuntimeError as exc:
            self.logger.error("导出股票列表失败: %s", exc)
            return

        stock_df = pd.read_csv(stock_list_path)

        # 3) 导出最近 30 日历史日线
        try:
            history_df, history_path = self._export_recent_daily_history(
                stock_df, latest_trade_day, days=30
            )
        except RuntimeError as exc:
            self.logger.error("导出最近 30 个交易日的日线数据失败: %s", exc)
            return

        # 4) 构建候选池并挑选成交额前 N 名
        try:
            universe_df = self.universe_builder.build_universe(stock_df, history_df)
        except RuntimeError as exc:
            self.logger.error("生成当日候选池失败: %s", exc)
            return

        universe_path = self._save_sample(universe_df, "a_share_universe.csv")
        self.logger.info("已生成候选池：%s", universe_path)

        try:
            top_liquidity = self.universe_builder.pick_top_liquidity(universe_df)
        except RuntimeError as exc:
            self.logger.error(
                "挑选成交额前 %s 名失败: %s",
                self.universe_builder.top_liquidity_count,
                exc,
            )
            return

        top_liquidity_path = self._save_sample(
            top_liquidity, "a_share_top_liquidity.csv"
        )
        self.logger.info(
            "已将成交额排序结果写入 %s，可用于筛选高流动性标的。",
            top_liquidity_path,
        )

        # 5) 提示历史日线路径
        self.logger.info("历史日线数据已保存：%s", history_path)


if __name__ == "__main__":
    AshareApp().run()
