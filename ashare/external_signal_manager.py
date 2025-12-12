"""基于 Akshare 的行为证据数据同步管理器."""

from __future__ import annotations

from typing import Iterable, Dict, Any

import pandas as pd
from sqlalchemy import bindparam, text

from .akshare_fetcher import AkshareDataFetcher
from .db import MySQLWriter


class ExternalSignalManager:
    """负责从 Akshare 拉取龙虎榜、两融、北向持股与股东户数等信号并入库。"""

    def __init__(
        self,
        fetcher: AkshareDataFetcher,
        db_writer: MySQLWriter,
        logger,
        config: Dict[str, Any] | None = None,
    ) -> None:
        self.fetcher = fetcher
        self.db_writer = db_writer
        self.logger = logger
        self.config = config or {}

    def _to_yyyymmdd(self, value: Any) -> str | None:  # noqa: ANN401
        if pd.isna(value):
            return None
        text_value = str(value).strip()
        if not text_value:
            return None
        if text_value.isdigit() and len(text_value) >= 8:
            return text_value[-8:]

        try:
            parsed = pd.to_datetime(text_value, errors="coerce")
        except Exception:  # noqa: BLE001
            parsed = pd.NaT

        if pd.isna(parsed):
            return None
        return parsed.strftime("%Y%m%d")

    def _rename_first(self, df: pd.DataFrame, candidates: list[str], target: str) -> None:
        for col in candidates:
            if col in df.columns and col != target:
                df.rename(columns={col: target}, inplace=True)
                break

    def _ensure_column(self, df: pd.DataFrame, column: str, value: Any) -> None:  # noqa: ANN401
        if column not in df.columns:
            df[column] = value

    def _upsert(
        self,
        df: pd.DataFrame,
        table: str,
        subset: Iterable[str] | None,
        period_delete_by_code: bool = False,
    ) -> None:
        if df.empty:
            self.logger.info("表 %s 本次无新增数据，跳过写入。", table)
            return

        deduped = df.copy()
        if subset:
            deduped.drop_duplicates(subset=list(subset), keep="last", inplace=True)

        delete_filters: list[Dict[str, str]] = []
        if "trade_date" in deduped.columns:
            key_cols = ["trade_date"]
            for optional in ("indicator", "exchange", "market"):
                if optional in deduped.columns:
                    key_cols.append(optional)

            for _, row in (
                deduped[key_cols].dropna(subset=["trade_date"]).drop_duplicates().iterrows()
            ):
                delete_filters.append({col: str(row[col]) for col in key_cols})
        elif "period" in deduped.columns:
            if period_delete_by_code and {"period", "code"}.issubset(deduped.columns):
                period_code = (
                    deduped[["period", "code"]]
                    .dropna(subset=["period", "code"])
                    .drop_duplicates()
                )
                for _, row in period_code.iterrows():
                    delete_filters.append(
                        {"period": str(row["period"]), "code": str(row["code"])}
                    )
            else:
                for period in deduped["period"].dropna().astype(str).unique():
                    delete_filters.append({"period": period})

        if delete_filters:
            try:
                with self.db_writer.engine.begin() as conn:
                    for filter_values in delete_filters:
                        conditions = [f"{col} = :{col}" for col in filter_values]
                        stmt = text(
                            f"DELETE FROM `{table}` WHERE " + " AND ".join(conditions)
                        ).bindparams(
                            *(bindparam(col) for col in filter_values)
                        )
                        conn.execute(stmt, filter_values)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "删除表 %s 分区时出现异常（可能表不存在），已跳过删除：%s",
                    table,
                    exc,
                )

        self.db_writer.write_dataframe(deduped, table, if_exists="append")
        self.logger.info("表 %s 已写入 %s 行。", table, len(deduped))

    def _normalize_code_column(self, df: pd.DataFrame) -> None:
        self._rename_first(
            df,
            [
                "code",
                "代码",
                "证券代码",
                "股票代码",
                "SECURITY_CODE",
                "SECURITYCODE",
                "symbol",
                "标的证券代码",
            ],
            "code",
        )
        if "code" in df.columns:
            df["code"] = (
                df["code"]
                .astype(str)
                .str.strip()
                .apply(self._format_code_with_prefix)
            )

    def _format_code_with_prefix(self, code: str) -> str:
        if not code or pd.isna(code):
            return ""
        text_code = str(code).strip().lower()
        if text_code in {"", "nan", "none", "null"}:
            return ""
        if text_code.startswith(("sh.", "sz.", "bj.")):
            return text_code

        numeric_text = text_code
        if "." in text_code and not text_code.startswith(("sh.", "sz.", "bj.")):
            left = text_code.split(".", 1)[0]
            if left.isdigit():
                numeric_text = left

        digits = numeric_text
        if digits.isdigit() and len(digits) > 6:
            digits = digits[-6:]
        if not digits.isdigit():
            return ""
        digits = digits.zfill(6)

        if digits.startswith(("60", "68", "69")):
            return f"sh.{digits}"
        if digits.startswith(("00", "30", "20")):
            return f"sz.{digits}"
        if digits.startswith(("83", "87", "43", "40")):
            return f"bj.{digits}"
        return f"sz.{digits}"

    def _normalize_trade_date(self, df: pd.DataFrame, trade_date: str) -> None:
        if "trade_date" not in df.columns:
            for col in [
                "trade_date",
                "TRADE_DATE",
                "交易日期",
                "日期",
                "上榜日",
                "信用交易日期",
            ]:
                if col in df.columns:
                    df["trade_date"] = df[col]
                    break

        normalized_default = self._to_yyyymmdd(trade_date) or trade_date.replace("-", "")
        self._ensure_column(df, "trade_date", normalized_default)
        df["trade_date"] = df["trade_date"].apply(
            lambda x: self._to_yyyymmdd(x) or normalized_default
        )
        for col in ["上榜日", "信用交易日期", "日期", "交易日期"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: self._to_yyyymmdd(x) or normalized_default
                )

    def _normalize_period(self, df: pd.DataFrame, period: str | None = None) -> None:
        self._rename_first(
            df,
            [
                "股东户数统计截止日-本次",
                "股东户数统计截止日",
                "截止日期",
                "报告期",
                "date",
            ],
            "period",
        )
        fallback_period = self._to_yyyymmdd(period) if period else None
        self._ensure_column(df, "period", fallback_period or period or "")
        df["period"] = df["period"].apply(
            lambda x: self._to_yyyymmdd(x)
            or fallback_period
            or ("" if pd.isna(x) else str(x))
        )

    def _normalize_exchange(self, df: pd.DataFrame, exchange: str) -> None:
        self._rename_first(df, ["exchange", "交易所"], "exchange")
        self._ensure_column(df, "exchange", exchange)

    def _normalize_market(self, df: pd.DataFrame, market: str) -> None:
        self._rename_first(df, ["market", "渠道"], "market")
        self._ensure_column(df, "market", market)

    def _normalize_indicator(self, df: pd.DataFrame, indicator: str) -> None:
        self._rename_first(df, ["indicator", "排行类型"], "indicator")
        self._ensure_column(df, "indicator", indicator)

    def _normalize_symbol(self, code: str) -> str:
        if pd.isna(code):
            return ""
        if "." in code:
            return code.split(".", 1)[1]
        return code

    def sync_lhb_detail(self, trade_date: str) -> pd.DataFrame:
        try:
            df = self.fetcher.get_lhb_detail(trade_date)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("龙虎榜详情获取失败: %s", exc)
            return pd.DataFrame()

        if df.empty:
            self.logger.info("龙虎榜 %s 返回为空。", trade_date)
            return df

        self._normalize_code_column(df)
        self._normalize_trade_date(df, trade_date)
        subset = [
            col
            for col in ["code", "trade_date", "上榜日", "上榜原因"]
            if col in df.columns
        ]
        self._upsert(df, "a_share_lhb_detail", subset=subset or None)
        return df

    def sync_margin_detail(self, trade_date: str, exchanges: Iterable[str]) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for exchange in exchanges:
            try:
                df = self.fetcher.get_margin_detail(trade_date, exchange)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("两融明细 %s 获取失败: %s", exchange, exc)
                continue

            if df.empty:
                self.logger.info("两融明细 %s 在 %s 返回为空。", exchange, trade_date)
                continue

            self._normalize_code_column(df)
            self._normalize_trade_date(df, trade_date)
            self._normalize_exchange(df, exchange)
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        subset = [col for col in ["exchange", "trade_date", "code"] if col in combined.columns]
        self._upsert(combined, "a_share_margin_detail", subset=subset or None)
        return combined

    def sync_hsgt_hold_rank(
        self, market_list: Iterable[str], indicator: str, trade_date: str
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for market in market_list:
            try:
                df = self.fetcher.get_hsgt_hold_rank(market=market, indicator=indicator)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("北向持股排行 %s 获取失败: %s", market, exc)
                continue

            if df.empty:
                self.logger.info("北向持股排行 %s 返回为空。", market)
                continue

            self._normalize_code_column(df)
            self._normalize_trade_date(df, trade_date)
            self._normalize_market(df, market)
            self._normalize_indicator(df, indicator)
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        subset = [
            col
            for col in ["market", "indicator", "trade_date", "code"]
            if col in combined.columns
        ]
        self._upsert(combined, "a_share_hsgt_hold_rank", subset=subset or None)
        return combined

    def sync_shareholder_counts(
        self, trade_date: str, focus_codes: list[str] | None = None
    ) -> pd.DataFrame:
        gdhs_cfg = self.config.get("gdhs", {})
        period = gdhs_cfg.get("period") or "最新"
        try:
            summary_df = self.fetcher.get_shareholder_count(period)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("股东户数汇总获取失败: %s", exc)
            summary_df = pd.DataFrame()

        if not summary_df.empty:
            period_from_data = summary_df.get("股东户数统计截止日-本次")
            if period_from_data is not None:
                period = str(period_from_data.iloc[0])
            self._normalize_code_column(summary_df)
            self._normalize_period(summary_df, period)
            summary_subset = [col for col in ["code", "period"] if col in summary_df.columns]
            self._upsert(summary_df, "a_share_gdhs", subset=summary_subset or None)
        else:
            self.logger.info("股东户数汇总在 %s 返回为空。", period)

        if not gdhs_cfg.get("detail_enabled", True):
            return summary_df

        if not focus_codes:
            focus_codes = []
        top_n = gdhs_cfg.get("detail_top_n", 100)
        normalized_codes = [self._normalize_symbol(code) for code in focus_codes]
        normalized_codes = normalized_codes[:top_n]

        frames = self.fetcher.batch_get_shareholder_count_detail(normalized_codes)
        if not frames:
            self.logger.info("股东户数明细在指定代码范围内无返回。")
            return summary_df

        detail_df = pd.concat(frames, ignore_index=True)
        period_from_detail = detail_df.get("股东户数统计截止日")
        if period_from_detail is not None:
            period = str(period_from_detail.iloc[0])
        self._normalize_period(detail_df, period)
        self._normalize_code_column(detail_df)
        subset = [col for col in ["code", "period"] if col in detail_df.columns]
        self._upsert(
            detail_df,
            "a_share_gdhs_detail",
            subset=subset or None,
            period_delete_by_code=True,
        )
        return summary_df

    def sync_daily_signals(
        self, trade_date: str, focus_codes: list[str] | None = None
    ) -> None:
        ak_cfg = self.config
        if not ak_cfg.get("enabled", False):
            self.logger.info("Akshare 行为证据开关关闭，已跳过所有相关采集。")
            return

        lhb_cfg = ak_cfg.get("lhb", {})
        if lhb_cfg.get("enabled", True):
            self.sync_lhb_detail(trade_date)
        else:
            self.logger.info("龙虎榜采集已关闭。")

        margin_cfg = ak_cfg.get("margin", {})
        if margin_cfg.get("enabled", True):
            exchanges = margin_cfg.get("exchanges", ["sse", "szse"])
            self.sync_margin_detail(trade_date, exchanges)
        else:
            self.logger.info("两融采集已关闭。")

        hsgt_cfg = ak_cfg.get("hsgt", {})
        if hsgt_cfg.get("enabled", True):
            markets = hsgt_cfg.get("markets", ["沪股通", "深股通"])
            indicator = hsgt_cfg.get("indicator", "5日排行")
            self.sync_hsgt_hold_rank(markets, indicator, trade_date)
        else:
            self.logger.info("北向持股采集已关闭。")

        gdhs_cfg = ak_cfg.get("gdhs", {})
        if gdhs_cfg.get("enabled", True):
            self.sync_shareholder_counts(trade_date, focus_codes)
        else:
            self.logger.info("股东户数采集已关闭。")
