"""基于 Baostock 的示例脚本入口."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd
from tqdm import tqdm

from .akshare_fetcher import AkshareDataFetcher
from .baostock_core import BaostockDataFetcher
from .baostock_session import BaostockSession
from .config import ProxyConfig, get_section
from .db import DatabaseConfig, MySQLWriter
from .external_signal_manager import ExternalSignalManager
from .fundamental_manager import FundamentalDataManager
from .universe import AshareUniverseBuilder
from .utils import setup_logger


class AshareApp:
    """通过脚本方式导出 Baostock 数据的应用."""

    def __init__(
        self,
        output_dir: str | Path = "output",
        top_liquidity_count: int | None = None,
        history_days: int | None = None,
        min_listing_days: int | None = None,
    ) -> None:
        # 保持入口参数兼容性
        self.output_dir = Path(output_dir)

        # 日志改为写到项目根目录的 ashare.log，不再跟 output 绑在一起
        self.logger = setup_logger()

        # 从 config.yaml 读取基础面刷新开关
        app_cfg = get_section("app")
        refresh_flag = app_cfg.get("refresh_fundamentals", False)
        if isinstance(refresh_flag, str):
            refresh_flag = refresh_flag.strip().lower() in {
                "1",
                "true",
                "yes",
                "y",
                "on",
            }
        self.refresh_fundamentals: bool = bool(refresh_flag)

        self.history_days = (
            history_days
            if history_days is not None
            else self._read_int_from_env("ASHARE_HISTORY_DAYS", 30)
        )
        resolved_top_liquidity = (
            top_liquidity_count
            if top_liquidity_count is not None
            else self._read_int_from_env("ASHARE_TOP_LIQUIDITY_COUNT", 100)
        )
        resolved_min_listing_days = (
            min_listing_days
            if min_listing_days is not None
            else self._read_int_from_env("ASHARE_MIN_LISTING_DAYS", 60)
        )
        self.logger.info(
            "参数配置：history_days=%s, top_liquidity_count=%s, min_listing_days=%s",
            self.history_days,
            resolved_top_liquidity,
            resolved_min_listing_days,
        )

        self.db_config = DatabaseConfig.from_env()
        self.db_writer = MySQLWriter(self.db_config)

        proxy_config = ProxyConfig.from_env()
        proxy_config.apply_to_environment()
        self.logger.info(
            "代理配置: HTTP=%s, HTTPS=%s", proxy_config.http, proxy_config.https
        )

        self.session = BaostockSession()
        self.fetcher = BaostockDataFetcher(self.session)
        self.universe_builder = AshareUniverseBuilder(
            top_liquidity_count=resolved_top_liquidity,
            min_listing_days=resolved_min_listing_days,
        )
        self.fundamental_manager = FundamentalDataManager(
            self.fetcher, self.db_writer, self.logger
        )
        akshare_cfg = get_section("akshare")
        self.akshare_enabled = akshare_cfg.get("enabled", False)
        self.external_signal_manager: ExternalSignalManager | None = None
        if self.akshare_enabled:
            try:
                akshare_fetcher = AkshareDataFetcher()
                self.external_signal_manager = ExternalSignalManager(
                    akshare_fetcher, self.db_writer, self.logger, akshare_cfg
                )
            except ImportError as exc:  # noqa: BLE001
                self.akshare_enabled = False
                self.logger.warning(
                    "Akshare 行为证据层初始化失败，已自动关闭：%s", exc
                )
        else:
            self.logger.info("Akshare 行为证据层已关闭，跳过相关初始化。")

    def _save_sample(self, df: pd.DataFrame, table_name: str) -> str:
        self.db_writer.write_dataframe(df, table_name)
        return table_name

    def _read_int_from_env(self, name: str, default: int) -> int:
        raw_value = os.getenv(name)
        if raw_value is None:
            return default

        try:
            parsed = int(raw_value)
            if parsed > 0:
                return parsed
            self.logger.warning("环境变量 %s 必须为正整数，已回退默认值 %s", name, default)
        except ValueError:
            self.logger.warning("环境变量 %s 解析失败，已回退默认值 %s", name, default)

        return default

    def _get_trading_days_between(
        self, start_day: dt.date, end_day: dt.date
    ) -> list[dt.date]:
        calendar_df = self.fetcher.get_trade_calendar(
            start_day.isoformat(), end_day.isoformat()
        )
        if calendar_df.empty:
            return []

        if "is_trading_day" in calendar_df.columns:
            calendar_df = calendar_df[
                calendar_df["is_trading_day"].astype(str) == "1"
            ]

        trading_days = (
            pd.to_datetime(calendar_df["calendar_date"], errors="coerce")
            .dt.date.dropna()
            .tolist()
        )
        return sorted([day for day in trading_days if day <= end_day])

    def _get_recent_trading_days(self, end_day: dt.date, days: int) -> list[dt.date]:
        lookback = max(days * 3, days + 20)
        max_lookback = 365

        while True:
            start_day = end_day - dt.timedelta(days=lookback)
            trading_days = self._get_trading_days_between(start_day, end_day)

            if len(trading_days) >= days or lookback >= max_lookback:
                break

            lookback = min(lookback + days, max_lookback)

        if len(trading_days) < days:
            raise RuntimeError(
                f"在 {lookback} 天的回看范围内未能找到 {days} 个交易日。"
            )

        return trading_days[-days:]

    def _export_stock_list(self, trade_date: str) -> pd.DataFrame:
        stock_df = self.fetcher.get_stock_list(trade_date)
        if stock_df.empty:
            raise RuntimeError("获取股票列表失败：返回为空。")

        table_name = self._save_sample(stock_df, "a_share_stock_list")
        self.logger.info("已保存 %s 只股票的列表至表 %s", len(stock_df), table_name)
        return stock_df

    def _export_stock_basic(self) -> pd.DataFrame:
        stock_basic_df = self.fetcher.get_stock_basic()
        if stock_basic_df.empty:
            raise RuntimeError("获取证券基本资料失败：返回为空。")

        table_name = self._save_sample(stock_basic_df, "a_share_stock_basic")
        self.logger.info("已保存证券基本资料至表 %s", table_name)
        return stock_basic_df

    def _export_stock_industry(self) -> pd.DataFrame:
        industry_df = self.fetcher.get_stock_industry()
        if industry_df.empty:
            raise RuntimeError("获取行业分类信息失败：返回为空。")

        table_name = self._save_sample(industry_df, "a_share_stock_industry")
        self.logger.info("已保存行业分类信息至表 %s", table_name)
        return industry_df

    def _export_index_members(self, latest_trade_day: str) -> dict[str, set[str]]:
        index_membership: dict[str, set[str]] = {}
        for index_name in ("hs300", "zz500", "sz50"):
            try:
                members_df = self.fetcher.get_index_members(index_name, latest_trade_day)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("获取 %s 成分股失败: %s", index_name, exc)
                continue

            if members_df.empty:
                self.logger.warning("指数 %s 成分股返回为空，已跳过。", index_name)
                continue

            table_name = self._save_sample(
                members_df, f"index_{index_name}_members"
            )
            self.logger.info("已保存 %s 成分股至表 %s", index_name, table_name)
            index_membership[index_name] = set(members_df.get("code", []))

        return index_membership

    def _export_recent_daily_history(
        self, stock_df: pd.DataFrame, end_date: str, days: int = 30
    ) -> Tuple[pd.DataFrame, str]:
        if stock_df.empty or "code" not in stock_df.columns:
            raise RuntimeError("导出历史日线失败：股票列表为空或缺少 code 列。")

        end_day = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
        recent_trading_days = self._get_recent_trading_days(end_day, days)
        start_day = recent_trading_days[0].isoformat()

        history_frames: list[pd.DataFrame] = []
        total = len(stock_df)
        success_count = 0
        empty_codes: list[str] = []
        failed_codes: list[str] = []
        self.logger.info(
            "开始导出 %s 只股票的最近 %s 个交易日历史数据，窗口 [%s, %s]",
            total,
            days,
            start_day,
            end_date,
        )

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
                failed_codes.append(code)
                continue

            if daily_df.empty:
                empty_codes.append(code)
                continue

            history_frames.append(daily_df)
            success_count += 1

            if idx % 500 == 0 or idx == total:
                self.logger.info("已完成 %s/%s 支股票的数据拉取", idx, total)

        if not history_frames:
            raise RuntimeError("导出历史日线失败：全部股票均未返回数据。")

        self.logger.info(
            "日线拉取完成：成功 %s/%s，空数据 %s，失败 %s",
            success_count,
            total,
            len(empty_codes),
            len(failed_codes),
        )
        if empty_codes:
            self.logger.debug("完全未返回数据的股票：%s", ", ".join(sorted(empty_codes)))
        if failed_codes:
            self.logger.debug("请求失败的股票：%s", ", ".join(sorted(failed_codes)))

        combined = pd.concat(history_frames, ignore_index=True)
        combined_table = self._save_sample(combined, f"history_recent_{days}_days")
        self.logger.info(
            "已导出最近 %s 个交易日的历史数据，共 %s 行，表名：%s",
            days,
            len(combined),
            combined_table,
        )
        return combined, combined_table

    def _export_daily_history_incremental(
        self,
        stock_df: pd.DataFrame,
        end_date: str,
        base_table: str = "history_daily_kline",
        window_days: int = 30,
    ) -> Tuple[pd.DataFrame, str]:
        """
        增量更新日线数据，并返回最近 window_days 天的切片。

        - 首次运行或表为空时：调用冷启动逻辑拉取 window_days 天并写入基础表。
        - 后续运行：仅拉取缺失的交易日数据并追加到基础表，然后从基础表切片。
        """

        if stock_df.empty or "code" not in stock_df.columns:
            raise RuntimeError("导出历史日线失败：股票列表为空或缺少 code 列。")

        end_day = dt.datetime.strptime(end_date, "%Y-%m-%d").date()

        with self.db_writer.engine.begin() as conn:
            try:
                existing = pd.read_sql(
                    f"SELECT MAX(`date`) AS max_date FROM `{base_table}`",
                    conn,
                )
                last_date_raw = existing["max_date"].iloc[0]
            except Exception:  # noqa: BLE001
                last_date_raw = None

        last_date_value: dt.date | None = None
        if isinstance(last_date_raw, pd.Timestamp):
            last_date_value = last_date_raw.date()
        elif isinstance(last_date_raw, dt.date):
            last_date_value = last_date_raw
        elif isinstance(last_date_raw, str) and last_date_raw:
            last_date_value = dt.datetime.strptime(last_date_raw, "%Y-%m-%d").date()

        if last_date_value is None:
            self.logger.info(
                "历史表 %s 不存在或为空，执行冷启动：拉取最近 %s 天日线。",
                base_table,
                window_days,
            )
            history_df, _ = self._export_recent_daily_history(
                stock_df, end_date, days=window_days
            )
            self.db_writer.write_dataframe(
                history_df,
                base_table,
                if_exists="replace",
            )
        elif last_date_value >= end_day:
            self.logger.info(
                "历史日线表 %s 已包含截至 %s 的数据，跳过增量拉取。",
                base_table,
                end_date,
            )
        else:
            trade_start = last_date_value + dt.timedelta(days=1)
            new_trading_days = self._get_trading_days_between(trade_start, end_day)
            if not new_trading_days:
                self.logger.info(
                    "最近交易日仍为 %s，暂无需要增量的交易日。",
                    last_date_value.isoformat(),
                )
                new_trading_days = []

            start_day = new_trading_days[0].isoformat() if new_trading_days else None
            self.logger.info(
                "开始增量拉取 %s 至 %s 的日线数据（原有截至 %s）。",
                start_day or "无新增交易日",
                end_date,
                last_date_value.isoformat(),
            )

            history_frames: list[pd.DataFrame] = []
            total = len(stock_df)
            success_count = 0
            empty_codes: list[str] = []
            failed_codes: list[str] = []

            if start_day is not None:
                for idx, code in enumerate(
                    tqdm(stock_df["code"], desc="增量数据拉取进度"),
                    start=1,
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
                        self.logger.warning("股票 %s 增量数据拉取失败: %s", code, exc)
                        failed_codes.append(code)
                        continue

                    if daily_df.empty:
                        empty_codes.append(code)
                        continue

                    history_frames.append(daily_df)
                    success_count += 1

                    if idx % 500 == 0 or idx == total:
                        self.logger.info(
                            "增量已完成 %s/%s 支股票的数据拉取",
                            idx,
                            total,
                        )

            if history_frames:
                new_rows = pd.concat(history_frames, ignore_index=True)
                self.db_writer.write_dataframe(
                    new_rows,
                    base_table,
                    if_exists="append",
                )
                self.logger.info(
                    "增量拉取完成：成功 %s/%s，空数据 %s，失败 %s",
                    success_count,
                    total,
                    len(empty_codes),
                    len(failed_codes),
                )
                if empty_codes:
                    self.logger.debug(
                        "增量阶段完全未返回数据的股票：%s",
                        ", ".join(sorted(empty_codes)),
                    )
                if failed_codes:
                    self.logger.debug(
                        "增量阶段请求失败的股票：%s",
                        ", ".join(sorted(failed_codes)),
                    )
            else:
                self.logger.info("本次没有任何新的日线数据可写入。")

        window_trading_days = self._get_recent_trading_days(end_day, window_days)
        window_start = window_trading_days[0].isoformat()
        query = f"SELECT * FROM `{base_table}` WHERE `date` >= '{window_start}'"

        with self.db_writer.engine.begin() as conn:
            recent_df = pd.read_sql(query, conn)

        if recent_df.empty:
            raise RuntimeError(
                f"从表 {base_table} 切出最近 {window_days} 天数据失败：结果为空。"
            )

        if "date" not in recent_df.columns:
            raise RuntimeError(
                f"表 {base_table} 缺少 date 列，无法进行时间窗口切片。"
            )

        window_day_set = {day for day in window_trading_days}
        recent_df["date"] = pd.to_datetime(recent_df["date"])
        recent_df = recent_df[
            recent_df["date"].dt.date.isin(window_day_set)
        ].copy()

        if recent_df.empty:
            raise RuntimeError(
                "从表 {table} 中切出最近 {days} 天数据失败：结果为空。".format(
                    table=base_table,
                    days=window_days,
                )
            )

        return recent_df, base_table

    def _extract_focus_codes(self, df: pd.DataFrame) -> list[str]:
        if df.empty:
            return []
        if "code" not in df.columns:
            return []
        codes = df["code"].astype(str).dropna().tolist()
        return codes

    def _sync_external_signals(
        self, latest_trade_day: str, focus_df: pd.DataFrame
    ) -> None:
        if self.external_signal_manager is None:
            self.logger.info("Akshare 行为证据层未启用，跳过外部信号同步。")
            return

        focus_codes = self._extract_focus_codes(focus_df)
        try:
            self.external_signal_manager.sync_daily_signals(
                latest_trade_day, focus_codes
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("行为证据同步阶段出现异常: %s", exc)

    def _print_preview(self, interfaces: Iterable[str]) -> None:
        preview = list(interfaces)
        self.logger.info("已发现 %s 个项目组件，前 10 个预览：", len(preview))
        for name in preview[:10]:
            self.logger.info(" - %s", name)

    def _apply_fundamental_filters(
        self, universe_df: pd.DataFrame, fundamentals_df: pd.DataFrame
    ) -> pd.DataFrame:
        if fundamentals_df.empty:
            self.logger.info("未生成财务宽表，跳过基本面过滤。")
            return universe_df

        merged = universe_df.merge(fundamentals_df, on="code", how="left")

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

    def run(self) -> None:
        """执行 Baostock 数据导出与候选池筛选示例。"""

        try:
            # 1) 预览当前模块内可用组件（示例信息输出）
            self._print_preview(
                [
                    "BaostockSession",
                    "BaostockDataFetcher",
                    "AshareUniverseBuilder",
                    "FundamentalDataManager",
                ]
            )

            # 2) 获取最近交易日并导出股票列表
            latest_trade_day = self.fetcher.get_latest_trading_date()
            self.logger.info("最近交易日：%s", latest_trade_day)
            try:
                stock_df = self._export_stock_list(latest_trade_day)
            except RuntimeError as exc:
                self.logger.error("导出股票列表失败: %s", exc)
                return

            try:
                stock_basic_df = self._export_stock_basic()
            except RuntimeError as exc:
                self.logger.warning(
                    "导出证券基本资料失败: %s，将跳过上市状态与上市天数过滤。",
                    exc,
                )
                stock_basic_df = None

            try:
                industry_df = self._export_stock_industry()
            except RuntimeError as exc:
                self.logger.error("导出行业分类数据失败: %s", exc)
                return

            index_membership = self._export_index_members(latest_trade_day)
            fundamentals_wide = pd.DataFrame()
            try:
                if self.refresh_fundamentals:
                    fundamental_codes: set[str] = set().union(
                        *index_membership.values()
                    )
                    if not fundamental_codes:
                        fallback_count = min(
                            self.universe_builder.top_liquidity_count, len(stock_df)
                        )
                        fundamental_codes = set(
                            stock_df["code"].head(fallback_count)
                        )

                    self.logger.info(
                        "基础面刷新开关已开启，本次将对 %s 支股票刷新季频财务数据。",
                        len(fundamental_codes),
                    )
                    fundamentals_wide = self.fundamental_manager.refresh_all(
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
                    fundamentals_wide = self.fundamental_manager.build_latest_wide()
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "基础面阶段出现异常，将继续主流程（只使用技术面过滤）: %s",
                    exc,
                )

            # 3) 导出最近 N 日历史日线（增量模式）
            try:
                history_df, history_table = self._export_daily_history_incremental(
                    stock_df,
                    latest_trade_day,
                    base_table="history_daily_kline",
                    window_days=self.history_days,
                )
            except RuntimeError as exc:
                self.logger.error(
                    "导出最近 %s 个交易日的日线数据失败: %s",
                    self.history_days,
                    exc,
                )
                return

            # 4) 构建候选池并挑选成交额前 N 名
            try:
                universe_df = self.universe_builder.build_universe(
                    stock_df,
                    history_df,
                    stock_basic_df=stock_basic_df,
                    industry_df=industry_df,
                    index_membership=index_membership,
                )
            except RuntimeError as exc:
                self.logger.error("生成当日候选池失败: %s", exc)
                return

            try:
                universe_df = self._apply_fundamental_filters(
                    universe_df, fundamentals_wide
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("基本面过滤阶段出现异常，保留未过滤结果: %s", exc)

            universe_table = self._save_sample(universe_df, "a_share_universe")
            self.logger.info("已生成候选池：表 %s", universe_table)

            try:
                top_liquidity = self.universe_builder.pick_top_liquidity(universe_df)
            except RuntimeError as exc:
                self.logger.error(
                    "挑选成交额前 %s 名失败: %s",
                    self.universe_builder.top_liquidity_count,
                    exc,
                )
                return

            top_liquidity_table = self._save_sample(
                top_liquidity, "a_share_top_liquidity"
            )
            self.logger.info(
                "已将成交额排序结果写入表 %s，可用于筛选高流动性标的。",
                top_liquidity_table,
            )

            self._sync_external_signals(latest_trade_day, top_liquidity)

            # 5) 提示历史日线路径
            self.logger.info("历史日线数据已保存至表：%s", history_table)
        finally:
            self.db_writer.dispose()

if __name__ == "__main__":
    AshareApp().run()
