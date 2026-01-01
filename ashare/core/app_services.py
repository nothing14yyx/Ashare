"""AshareApp orchestration services."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import bindparam, text

from ashare.core.schema_manager import TABLE_A_SHARE_UNIVERSE

if TYPE_CHECKING:
    from ashare.core.app import AshareApp


class DataIngestService:
    """负责数据采集与元数据加载。"""

    def __init__(self, app: "AshareApp") -> None:
        self.app = app
        self.logger = app.logger

    def resolve_latest_trade_day(self) -> str:
        if self.app.use_baostock:
            return self.app.fetcher.get_latest_trading_date()
        return self.app._infer_latest_trade_day_from_db(  # noqa: SLF001
            base_table="history_daily_kline"
        )

    def export_index_daily_history(self, latest_trade_day: str) -> None:
        try:
            self.app._export_index_daily_history(latest_trade_day)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("拉取指数日线失败：%s", exc)

    def load_stock_list(self, latest_trade_day: str) -> pd.DataFrame:
        if self.app.fetch_stock_meta:
            try:
                return self.app._export_stock_list(latest_trade_day)  # noqa: SLF001
            except RuntimeError as exc:
                self.logger.error("导出股票列表失败: %s", exc)
                raise

        stock_df = self.app._load_table("a_share_stock_list")  # noqa: SLF001
        if stock_df.empty:
            stock_df = self.app._build_stock_list_from_history(  # noqa: SLF001
                base_table="history_daily_kline",
                trade_day=latest_trade_day,
            )
        if stock_df.empty:
            msg = (
                "股票列表为空：已关闭 Baostock 元数据拉取，"
                "但数据库中也没有可用的 a_share_stock_list/history_daily_kline。"
            )
            self.logger.error(msg)
            raise RuntimeError(msg)
        return stock_df

    def load_stock_basic(self) -> pd.DataFrame | None:
        if self.app.fetch_stock_meta:
            try:
                return self.app._export_stock_basic()  # noqa: SLF001
            except RuntimeError as exc:
                self.logger.warning(
                    "导出证券基本资料失败: %s，将跳过上市状态与上市天数过滤。", exc
                )
                return None

        stock_basic_df = self.app._load_table("a_share_stock_basic")  # noqa: SLF001
        if stock_basic_df.empty:
            self.logger.warning(
                "已关闭 Baostock 元数据拉取，且数据库中未找到 a_share_stock_basic，将跳过上市状态与上市天数过滤。"
            )
            return None
        return stock_basic_df

    def load_stock_industry(self) -> pd.DataFrame:
        if self.app.fetch_stock_meta:
            try:
                return self.app._export_stock_industry()  # noqa: SLF001
            except RuntimeError as exc:
                self.logger.warning(
                    "导出行业分类数据失败: %s，将继续主流程（不做行业映射）。",
                    exc,
                )
                return pd.DataFrame()

        return self.app._load_table("a_share_stock_industry")  # noqa: SLF001

    def refresh_board_industry(self, latest_trade_day: str) -> None:
        if not self.app.board_industry_enabled:
            return

        board_spot = pd.DataFrame()
        try:
            board_spot = self.app._export_board_industry_spot()  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("板块快照刷新失败：%s", exc)
        try:
            self.app._export_board_industry_constituents(board_spot)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("板块成份股维表刷新失败：%s", exc)
        try:
            self.app._export_board_industry_history(  # noqa: SLF001
                latest_trade_day, spot=board_spot
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("板块数据刷新失败：%s", exc)

    def load_index_membership(self, latest_trade_day: str) -> dict:
        if self.app.fetch_stock_meta:
            return self.app._export_index_members(latest_trade_day)  # noqa: SLF001
        return self.app._load_index_membership_from_db()  # noqa: SLF001


class FundamentalService:
    """负责基础面宽表刷新与读取。"""

    def __init__(self, app: "AshareApp") -> None:
        self.app = app
        self.logger = app.logger

    def load_fundamentals(
        self,
        latest_trade_day: str,
        index_membership: dict,
        stock_df: pd.DataFrame,
    ) -> pd.DataFrame:
        fundamentals_wide = pd.DataFrame()
        try:
            if self.app.refresh_fundamentals:
                fundamental_codes: set[str] = set().union(*index_membership.values())
                if not fundamental_codes:
                    fallback_count = min(
                        self.app.universe_builder.top_liquidity_count, len(stock_df)
                    )
                    fundamental_codes = set(stock_df["code"].head(fallback_count))

                self.logger.info(
                    "基础面刷新开关已开启，本次将对 %s 支股票刷新季频财务数据。",
                    len(fundamental_codes),
                )
                fundamentals_wide = self.app.fundamental_manager.refresh_all(
                    sorted(fundamental_codes),
                    latest_trade_day,
                    quarterly_lookback=4,
                    report_lookback_years=0,
                    adjust_lookback_years=0,
                    update_reports=False,
                    update_corporate_actions=False,
                    update_macro=False,
                )
            else:
                self.logger.info(
                    "基础面刷新开关已关闭，本次仅使用数据库中已有的财务表构建宽表。"
                )
                fundamentals_wide = self.app.fundamental_manager.build_latest_wide()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "基础面阶段出现异常，将继续主流程（只使用技术面过滤）: %s",
                exc,
            )
        return fundamentals_wide


class HistoryKlineService:
    """负责历史日线数据增量导出。"""

    def __init__(self, app: "AshareApp") -> None:
        self.app = app

    def export_daily_history(
        self, stock_df: pd.DataFrame, latest_trade_day: str
    ) -> tuple[pd.DataFrame, str]:
        return self.app._export_daily_history_incremental(  # noqa: SLF001
            stock_df,
            latest_trade_day,
            base_table="history_daily_kline",
            window_days=self.app.history_days,
            fetch_enabled=self.app.fetch_daily_kline,
        )


class UniverseService:
    """负责候选池构建与落库。"""

    def __init__(self, app: "AshareApp") -> None:
        self.app = app
        self.logger = app.logger

    def build_universe(
        self,
        stock_df: pd.DataFrame,
        history_df: pd.DataFrame,
        *,
        stock_basic_df: pd.DataFrame | None,
        industry_df: pd.DataFrame,
        index_membership: dict,
    ) -> pd.DataFrame:
        return self.app.universe_builder.build_universe(
            stock_df,
            history_df,
            stock_basic_df=stock_basic_df,
            industry_df=industry_df,
            index_membership=index_membership,
        )

    def apply_fundamental_filters(
        self, universe_df: pd.DataFrame, fundamentals_wide: pd.DataFrame
    ) -> pd.DataFrame:
        if fundamentals_wide.empty:
            self.logger.info("未生成财务宽表，跳过基本面过滤。")
            return universe_df

        merged = universe_df.merge(fundamentals_wide, on="code", how="left")

        def _select_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
            for col in candidates:
                if col in df.columns:
                    return col
            return None

        def _filter_numeric(
            df: pd.DataFrame, candidates: list[str], predicate, desc: str
        ) -> pd.DataFrame:
            target_col = _select_column(df, candidates)
            if target_col is None:
                self.logger.info("缺少 %s 指标列，跳过该条件。", desc)
                return df

            series = pd.to_numeric(df[target_col], errors="coerce")
            before = len(df)
            df = df[predicate(series)]
            self.logger.info("%s 过滤：%s -> %s", desc, before, len(df))
            return df

        merged = _filter_numeric(
            merged,
            ["profit_roeAvg", "profit_roe", "dupont_dupontROE"],
            lambda s: s > 0,
            "ROE 为正",
        )
        merged = _filter_numeric(
            merged,
            ["balance_liabilityToAsset", "balance_assetLiabRatio"],
            lambda s: s < 0.75,
            "资产负债率 < 75%",
        )
        merged = _filter_numeric(
            merged,
            ["cash_flow_CFOToNP", "cash_flow_CFOToOR", "cash_flow_CFOToGr"],
            lambda s: s > 0,
            "经营现金流为正（按 CFO 比率代理）",
        )
        merged = _filter_numeric(
            merged,
            ["growth_YOYNI", "growth_YOYPNI", "growth_YOYEPSBasic"],
            lambda s: s > 0,
            "净利润或 EPS 同比为正",
        )

        return merged

    def persist_universe_snapshot(self, universe_df: pd.DataFrame) -> bool:
        universe_snapshot_table = TABLE_A_SHARE_UNIVERSE
        self.logger.info(
            "已生成候选池 表=%s 行数=%s",
            universe_snapshot_table,
            len(universe_df),
        )

        if universe_df.empty:
            self.logger.warning("候选池为空，跳过 Universe 表落库与后续选股。")
            return False

        if not self.app._table_exists(universe_snapshot_table):  # noqa: SLF001
            self.logger.error(
                "Universe 表 %s 不存在（SchemaManager 未完成建表？），跳过落库与后续选股。",
                universe_snapshot_table,
            )
            return False

        df = universe_df.copy()
        if "tradestatus" in df.columns and "tradeStatus" not in df.columns:
            df = df.rename(columns={"tradestatus": "tradeStatus"})

        universe_columns = [
            "date",
            "code",
            "code_name",
            "tradeStatus",
            "amount",
            "volume",
            "open",
            "high",
            "low",
            "close",
            "ipoDate",
            "type",
            "status",
            "in_hs300",
            "in_zz500",
            "in_sz50",
        ]
        for col in universe_columns:
            if col not in df.columns:
                df[col] = None
        df = df[universe_columns]

        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["ipoDate"] = pd.to_datetime(df["ipoDate"], errors="coerce").dt.date

        dates = sorted(set(df["date"].dropna().astype(str)))
        if not dates:
            self.logger.warning(
                "候选池缺少有效 date 字段，跳过 Universe 表落库与后续选股。"
            )
            return False

        delete_stmt = text(
            f"DELETE FROM `{universe_snapshot_table}` WHERE `date` IN :dates"
        ).bindparams(bindparam("dates", expanding=True))

        with self.app.db_writer.engine.begin() as conn:
            conn.execute(delete_stmt, {"dates": dates})
            df.to_sql(
                universe_snapshot_table,
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

        self.logger.info(
            "Universe 表快照已写入 %s：rows=%s, dates=%s",
            universe_snapshot_table,
            len(df),
            dates,
        )
        return True

    def pick_top_liquidity(self, universe_df: pd.DataFrame) -> pd.DataFrame:
        return self.app.universe_builder.pick_top_liquidity(universe_df)

    def sync_external_signals(self, latest_trade_day: str, focus_df: pd.DataFrame) -> None:
        if self.app.external_signal_manager is None:
            self.logger.info("Akshare 行为证据层未启用，跳过外部信号同步。")
            return

        focus_codes = self._extract_focus_codes(focus_df)
        try:
            self.app.external_signal_manager.sync_daily_signals(
                latest_trade_day, focus_codes
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("行为证据同步阶段出现异常: %s", exc)

    @staticmethod
    def _extract_focus_codes(df: pd.DataFrame) -> list[str]:
        if df.empty or "code" not in df.columns:
            return []
        return df["code"].astype(str).dropna().tolist()
