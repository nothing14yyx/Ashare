"""财务与宏观数据采集与入库管理模块。"""

from __future__ import annotations

import datetime as dt
from typing import Iterable

import pandas as pd
from sqlalchemy import text

from .baostock_core import BaostockDataFetcher
from .db import MySQLWriter


class FundamentalDataManager:
    """负责财务数据、公司公告与宏观序列的更新与宽表生成。"""

    def __init__(
        self,
        fetcher: BaostockDataFetcher,
        db_writer: MySQLWriter,
        logger,
    ) -> None:
        self.fetcher = fetcher
        self.db_writer = db_writer
        self.logger = logger

    def _recent_quarters(
        self, latest_date: dt.date, lookback_quarters: int
    ) -> list[tuple[int, int]]:
        if lookback_quarters <= 0:
            return []

        month = latest_date.month
        current_quarter = (month - 1) // 3 + 1
        year = latest_date.year

        quarters: list[tuple[int, int]] = []
        for _ in range(lookback_quarters):
            quarters.append((year, current_quarter))
            current_quarter -= 1
            if current_quarter == 0:
                current_quarter = 4
                year -= 1

        quarters.reverse()
        return quarters

    def _load_existing(self, table: str) -> pd.DataFrame:
        with self.db_writer.engine.begin() as conn:
            try:
                return pd.read_sql(text(f"SELECT * FROM `{table}`"), conn)
            except Exception:  # noqa: BLE001
                return pd.DataFrame()

    def _upsert_dataframe(
        self, df: pd.DataFrame, table: str, subset: Iterable[str] | None
    ) -> None:
        if df.empty:
            self.logger.info("表 %s 本次无新增数据，跳过写入。", table)
            return

        existing = self._load_existing(table)
        combined = pd.concat([existing, df], ignore_index=True)
        if subset:
            combined = combined.drop_duplicates(subset=list(subset), keep="last")
        combined.reset_index(drop=True, inplace=True)
        self.db_writer.write_dataframe(combined, table, if_exists="replace")
        self.logger.info("表 %s 已更新，当前总行数：%s", table, len(combined))

    def _fetch_quarterly_table(
        self,
        table: str,
        fetch_fn,
        codes: list[str],
        quarter_pairs: list[tuple[int, int]],
    ) -> None:
        frames: list[pd.DataFrame] = []
        for year, quarter in quarter_pairs:
            for code in codes:
                try:
                    df = fetch_fn(code=code, year=year, quarter=quarter)
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning(
                        "拉取 %s 年 Q%s %s 数据失败: %s", year, quarter, code, exc
                    )
                    continue

                if df.empty:
                    continue

                df["code"] = code
                df["year"] = year
                df["quarter"] = quarter
                frames.append(df)

        if not frames:
            self.logger.warning("%s 在指定季度内无任何返回，跳过写入。", table)
            return

        combined = pd.concat(frames, ignore_index=True)
        self._upsert_dataframe(combined, table, subset=["code", "year", "quarter"])

    def update_quarterly_fundamentals(
        self,
        codes: list[str],
        latest_trade_day: str,
        lookback_quarters: int = 8,
    ) -> None:
        latest_date = dt.datetime.strptime(latest_trade_day, "%Y-%m-%d").date()
        quarter_pairs = self._recent_quarters(latest_date, lookback_quarters)

        task_map = {
            "fundamentals_quarter_profit": self.fetcher.get_profit_data,
            "fundamentals_quarter_growth": self.fetcher.get_growth_data,
            "fundamentals_quarter_balance": self.fetcher.get_balance_data,
            "fundamentals_quarter_cashflow": self.fetcher.get_cash_flow_data,
            "fundamentals_quarter_operation": self.fetcher.get_operation_data,
            "fundamentals_quarter_dupont": self.fetcher.get_dupont_data,
        }

        for table, fetch_fn in task_map.items():
            self.logger.info(
                "开始更新 %s，季度窗口：%s", table, quarter_pairs
            )
            self._fetch_quarterly_table(table, fetch_fn, codes, quarter_pairs)

    def update_company_reports(
        self,
        codes: list[str],
        start_date: str,
        end_date: str,
    ) -> None:
        express_frames: list[pd.DataFrame] = []
        forecast_frames: list[pd.DataFrame] = []

        for code in codes:
            try:
                express_df = self.fetcher.get_performance_express_report(
                    code=code, start_date=start_date, end_date=end_date
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("拉取 %s 业绩快报失败: %s", code, exc)
                express_df = pd.DataFrame()

            if not express_df.empty:
                express_df["code"] = code
                express_frames.append(express_df)

            try:
                forecast_df = self.fetcher.get_forecast_report(
                    code=code, start_date=start_date, end_date=end_date
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("拉取 %s 业绩预告失败: %s", code, exc)
                forecast_df = pd.DataFrame()

            if not forecast_df.empty:
                forecast_df["code"] = code
                forecast_frames.append(forecast_df)

        if express_frames:
            express_combined = pd.concat(express_frames, ignore_index=True)
            subset = [col for col in ["code", "pubDate", "statDate"] if col in express_combined]
            self._upsert_dataframe(
                express_combined,
                "a_share_performance_express",
                subset=subset or None,
            )
        else:
            self.logger.info("本次无业绩快报数据返回。")

        if forecast_frames:
            forecast_combined = pd.concat(forecast_frames, ignore_index=True)
            subset = [col for col in ["code", "profitForcastPubDate", "statDate"] if col in forecast_combined]
            self._upsert_dataframe(
                forecast_combined,
                "a_share_forecast_report",
                subset=subset or None,
            )
        else:
            self.logger.info("本次无业绩预告数据返回。")

    def update_corporate_actions(
        self,
        codes: list[str],
        start_date: str,
        end_date: str,
        start_year: int,
        end_year: int,
    ) -> None:
        dividend_frames: list[pd.DataFrame] = []
        for code in codes:
            for year in range(start_year, end_year + 1):
                try:
                    df = self.fetcher.get_dividend_data(code=code, year=year)
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("拉取 %s %s 年分红数据失败: %s", code, year, exc)
                    continue
                if df.empty:
                    continue
                df["code"] = code
                df["year"] = year
                dividend_frames.append(df)

        if dividend_frames:
            dividend_combined = pd.concat(dividend_frames, ignore_index=True)
            subset = [col for col in ["code", "dividendDate"] if col in dividend_combined]
            self._upsert_dataframe(
                dividend_combined, "a_share_dividend_events", subset=subset or None
            )
        else:
            self.logger.info("分红数据为空，跳过写入。")

        adjust_frames: list[pd.DataFrame] = []
        for code in codes:
            try:
                df = self.fetcher.get_adjust_factor(
                    code=code, start_date=start_date, end_date=end_date
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("拉取 %s 复权因子失败: %s", code, exc)
                continue
            if df.empty:
                continue
            df["code"] = code
            adjust_frames.append(df)

        if adjust_frames:
            adjust_combined = pd.concat(adjust_frames, ignore_index=True)
            subset = [col for col in ["code", "tradeDate"] if col in adjust_combined]
            self._upsert_dataframe(
                adjust_combined, "a_share_adjust_factor", subset=subset or None
            )
        else:
            self.logger.info("复权因子数据为空，跳过写入。")

    def update_macro_series(self, start_date: str, end_date: str) -> None:
        macro_tasks = {
            "macro_deposit_rate": self.fetcher.get_deposit_rate_data,
            "macro_loan_rate": self.fetcher.get_loan_rate_data,
            "macro_rrr": self.fetcher.get_required_reserve_ratio_data,
            "macro_money_supply_month": self.fetcher.get_money_supply_data_month,
            "macro_money_supply_year": self.fetcher.get_money_supply_data_year,
        }

        for table, fetch_fn in macro_tasks.items():
            try:
                df = fetch_fn(start_date=start_date, end_date=end_date)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("拉取宏观数据 %s 失败: %s", table, exc)
                continue

            if df.empty:
                self.logger.info("宏观表 %s 返回为空，跳过。", table)
                continue

            self._upsert_dataframe(df, table, subset=None)

        if hasattr(self.fetcher, "get_shibor_data"):
            try:
                df_shibor = self.fetcher.get_shibor_data(
                    start_date=start_date, end_date=end_date
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Shibor 数据拉取失败: %s", exc)
                return

            if df_shibor.empty:
                self.logger.info("Shibor 数据为空，跳过写入。")
            else:
                self._upsert_dataframe(df_shibor, "macro_shibor", subset=None)
        else:
            self.logger.info("当前 Baostock 版本缺少 Shibor 接口，已跳过。")

    def build_latest_wide(self) -> pd.DataFrame:
        table_prefix = {
            "fundamentals_quarter_profit": "profit",
            "fundamentals_quarter_growth": "growth",
            "fundamentals_quarter_balance": "balance",
            "fundamentals_quarter_cashflow": "cash_flow",
            "fundamentals_quarter_operation": "operation",
            "fundamentals_quarter_dupont": "dupont",
        }

        merged: pd.DataFrame | None = None
        for table, prefix in table_prefix.items():
            df = self._load_existing(table)
            if df.empty:
                continue

            sort_cols = [col for col in ["year", "quarter"] if col in df.columns]
            if sort_cols:
                df = df.sort_values(sort_cols)
            latest = df.groupby("code", as_index=False).tail(1)

            rename_map = {
                col: f"{prefix}_{col}"
                for col in latest.columns
                if col != "code"
            }
            latest = latest.rename(columns=rename_map)

            if merged is None:
                merged = latest
            else:
                merged = merged.merge(latest, on="code", how="outer")

        if merged is None:
            self.logger.warning("未能生成基础宽表，所有基础表均为空。")
            return pd.DataFrame()

        merged = merged.reset_index(drop=True)
        self.db_writer.write_dataframe(merged, "fundamentals_latest_wide")
        self.logger.info("已生成宽表 fundamentals_latest_wide，共 %s 行。", len(merged))
        return merged

    def refresh_all(
        self,
        codes: list[str],
        latest_trade_day: str,
        quarterly_lookback: int = 8,
        report_lookback_years: int = 2,
        adjust_lookback_years: int = 1,
        update_reports: bool = True,
        update_corporate_actions: bool = True,
        update_macro: bool = True,
    ) -> pd.DataFrame:
        if not codes:
            self.logger.warning("股票代码为空，跳过财务与宏观数据更新。")
            return pd.DataFrame()

        self.update_quarterly_fundamentals(
            codes, latest_trade_day, lookback_quarters=quarterly_lookback
        )

        end_date = latest_trade_day

        if update_reports and report_lookback_years > 0:
            start_date = (
                dt.datetime.strptime(latest_trade_day, "%Y-%m-%d").date()
                - dt.timedelta(days=365 * report_lookback_years)
            ).isoformat()
            self.update_company_reports(codes, start_date=start_date, end_date=end_date)

        if update_corporate_actions and adjust_lookback_years > 0:
            adjust_start = (
                dt.datetime.strptime(latest_trade_day, "%Y-%m-%d").date()
                - dt.timedelta(days=365 * adjust_lookback_years)
            ).isoformat()
            adjust_year_start = dt.datetime.strptime(adjust_start, "%Y-%m-%d").year
            adjust_year_end = dt.datetime.strptime(end_date, "%Y-%m-%d").year
            self.update_corporate_actions(
                codes,
                start_date=adjust_start,
                end_date=end_date,
                start_year=adjust_year_start,
                end_year=adjust_year_end,
            )

        if update_macro:
            macro_start = "2000-01-01"
            self.update_macro_series(start_date=macro_start, end_date=end_date)

        return self.build_latest_wide()
