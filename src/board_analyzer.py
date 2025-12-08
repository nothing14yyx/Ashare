"""板块与概念聚合分析工具。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, List, Sequence

import pandas as pd

from src.akshare_client import AKShareClient


@dataclass
class BoardDefinition:
    """板块/概念定义与成分股列表。"""

    code: str
    name: str
    board_type: str
    symbols: list[str]

    @property
    def id(self) -> str:
        return f"{self.board_type}:{self.code or self.name}"


class BoardAnalyzer:
    """结合实时与历史行情，计算板块强弱与成交放大情况。"""

    def __init__(self, client: AKShareClient, logger: logging.Logger | None = None):
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    def fetch_board_components(self, board_type: str = "industry") -> list[BoardDefinition]:
        """获取板块/概念与其成分股映射。"""

        board_type = board_type.lower()
        if board_type not in {"industry", "concept"}:
            raise ValueError("board_type 仅支持 industry 或 concept")

        if board_type == "industry":
            boards_df = self.client.fetch_board_industries()
        else:
            boards_df = self.client.fetch_board_concepts()

        boards: list[BoardDefinition] = []
        for _, row in boards_df.iterrows():
            board_code = str(row.get("代码") or row.get("code") or "").strip()
            board_name = str(row.get("名称") or row.get("name") or "").strip()
            if not board_name:
                continue

            if board_type == "industry":
                cons_df = self.client.fetch_board_industry_cons(board_code or board_name)
            else:
                cons_df = self.client.fetch_board_concept_cons(board_code or board_name)

            codes = [self.client._normalize_code(code) for code in cons_df.get("代码", [])]
            if not codes:
                continue

            boards.append(
                BoardDefinition(
                    code=board_code,
                    name=board_name,
                    board_type=board_type,
                    symbols=codes,
                )
            )

        if not boards:
            raise LookupError("未能获取到任何板块成分，请检查网络或数据源是否可用")

        self.logger.info("共获取到 %s 个 %s 板块", len(boards), board_type)
        return boards

    @staticmethod
    def filter_boards(
        boards: Sequence[BoardDefinition], names: Iterable[str]
    ) -> list[BoardDefinition]:
        target_names = {name.strip() for name in names if name}
        return [board for board in boards if board.name in target_names]

    def compute_realtime_snapshot(self, boards: Sequence[BoardDefinition]) -> pd.DataFrame:
        """基于实时行情，统计各板块当日涨跌与成交情况。"""

        unique_codes = sorted({code for board in boards for code in board.symbols})
        quotes = self.client.fetch_realtime_quotes(unique_codes)

        numeric_columns = ["涨跌幅", "成交量", "成交额", "涨跌额", "最新价"]
        for column in numeric_columns:
            if column in quotes:
                quotes[column] = pd.to_numeric(quotes[column], errors="coerce")

        results: list[dict[str, object]] = []
        for board in boards:
            board_quotes = quotes[quotes["代码"].isin(board.symbols)]
            if board_quotes.empty:
                continue

            results.append(
                {
                    "板块名称": board.name,
                    "板块代码": board.code,
                    "类型": board.board_type,
                    "成分股数": len(board.symbols),
                    "平均涨跌幅": board_quotes["涨跌幅"].mean(),
                    "合计成交额": board_quotes["成交额"].sum(),
                    "合计成交量": board_quotes["成交量"].sum(),
                }
            )

        snapshot = pd.DataFrame(results)
        if snapshot.empty:
            raise LookupError("实时板块统计为空，可能未能获取到实时行情数据")

        snapshot.sort_values("平均涨跌幅", ascending=False, inplace=True)
        snapshot.reset_index(drop=True, inplace=True)
        return snapshot

    def compute_recent_history_metrics(
        self,
        boards: Sequence[BoardDefinition],
        n_days: int,
        adjust: str | None = "qfq",
    ) -> pd.DataFrame:
        """基于最近行情，计算板块涨幅与成交放大倍数。"""

        unique_codes: List[str] = sorted({code for board in boards for code in board.symbols})
        history = self.client.fetch_recent_history(unique_codes, n_days=n_days, adjust=adjust)

        history["收盘"] = pd.to_numeric(history["收盘"], errors="coerce")
        history["成交量"] = pd.to_numeric(history["成交量"], errors="coerce")
        history["成交额"] = pd.to_numeric(history["成交额"], errors="coerce")
        history["日期_dt"] = pd.to_datetime(history["日期"], errors="coerce")

        metrics: list[dict[str, object]] = []
        for board in boards:
            board_history = history[history["代码"].isin(board.symbols)].copy()
            if board_history.empty:
                continue

            board_history.sort_values("日期_dt", inplace=True)
            latest_date = board_history["日期_dt"].max()
            previous_dates = (
                board_history.loc[board_history["日期_dt"] < latest_date, "日期_dt"].unique()
            )
            previous_date = previous_dates.max() if len(previous_dates) else None

            latest_slice = board_history[board_history["日期_dt"] == latest_date]
            prev_slice = (
                board_history[board_history["日期_dt"] == previous_date]
                if previous_date is not None
                else pd.DataFrame(columns=board_history.columns)
            )

            stock_returns = (
                board_history.groupby("代码")
                .apply(self._calc_stock_return)
                .dropna()
            )

            latest_amount = latest_slice["成交额"].sum()
            prev_amount = prev_slice["成交额"].sum()
            amount_ratio = (latest_amount / prev_amount) if prev_amount else None

            latest_volume = latest_slice["成交量"].sum()
            prev_volume = prev_slice["成交量"].sum()
            volume_ratio = (latest_volume / prev_volume) if prev_volume else None

            metrics.append(
                {
                    "板块名称": board.name,
                    "板块代码": board.code,
                    "类型": board.board_type,
                    "成分股数": len(board.symbols),
                    "平均累计涨幅": stock_returns.mean() if not stock_returns.empty else None,
                    "最新成交额": latest_amount,
                    "成交额放大倍数": amount_ratio,
                    "最新成交量": latest_volume,
                    "成交量放大倍数": volume_ratio,
                }
            )

        board_metrics = pd.DataFrame(metrics)
        if board_metrics.empty:
            raise LookupError("板块历史统计为空，可能未能获取到历史行情数据")

        board_metrics.sort_values("平均累计涨幅", ascending=False, inplace=True)
        board_metrics.reset_index(drop=True, inplace=True)
        return board_metrics

    @staticmethod
    def _calc_stock_return(group: pd.DataFrame) -> float | None:
        if group.empty:
            return None

        first_close = group.iloc[0]["收盘"]
        last_close = group.iloc[-1]["收盘"]
        if pd.isna(first_close) or pd.isna(last_close) or first_close == 0:
            return None

        return (last_close - first_close) / first_close * 100
