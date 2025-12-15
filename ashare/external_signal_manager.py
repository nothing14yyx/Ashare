"""基于 Akshare 的行为证据数据同步管理器."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable

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

    # =========================
    # Utils
    # =========================
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

    def _yyyymmdd_to_iso(self, yyyymmdd: str) -> str:
        if not yyyymmdd or len(yyyymmdd) != 8:
            return str(yyyymmdd)
        return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"

    def _iso_to_yyyymmdd(self, iso_date: str) -> str:
        ymd = self._to_yyyymmdd(iso_date)
        return ymd or str(iso_date).replace("-", "")

    def _shift_yyyymmdd(self, yyyymmdd: str, delta_days: int) -> str:
        base = pd.to_datetime(yyyymmdd, format="%Y%m%d", errors="coerce")
        if pd.isna(base):
            return yyyymmdd
        shifted = base + pd.Timedelta(days=delta_days)
        return shifted.strftime("%Y%m%d")

    def _rename_first(self, df: pd.DataFrame, candidates: list[str], target: str) -> None:
        for col in candidates:
            if col in df.columns and col != target:
                df.rename(columns={col: target}, inplace=True)
                break

    def _ensure_column(self, df: pd.DataFrame, column: str, value: Any) -> None:  # noqa: ANN401
        if column not in df.columns:
            df[column] = value

    def _ensure_df(self, value: Any) -> pd.DataFrame:  # noqa: ANN401
        if value is None:
            return pd.DataFrame()
        if isinstance(value, pd.DataFrame):
            return value
        try:
            return pd.DataFrame(value)
        except Exception:  # noqa: BLE001
            return pd.DataFrame()

    # =========================
    # DB
    # =========================
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
                    delete_filters.append({"period": str(row["period"]), "code": str(row["code"])})
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
                        ).bindparams(*(bindparam(col) for col in filter_values))
                        conn.execute(stmt, filter_values)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "删除表 %s 分区时出现异常（可能表不存在），已跳过删除：%s",
                    table,
                    exc,
                )

        self.db_writer.write_dataframe(deduped, table, if_exists="append")
        self.logger.info("表 %s 已写入 %s 行。", table, len(deduped))

    # =========================
    # Normalization
    # =========================
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
                df["code"].astype(str).str.strip().apply(self._format_code_with_prefix)
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
        code = str(code)
        if "." in code:
            return code.split(".", 1)[1]
        return code

    # =========================
    # Trade-date backoff (核心：取最近一次可用数据)
    # =========================
    def _iter_recent_trading_dates(
        self,
        requested_trade_date_iso: str,
        max_backoff_days: int,
        skip_today: bool,
    ) -> list[str]:
        """按交易日回退（优先）。

        设计目标：不引入额外配置/开关，也不改调用方签名。
        - 优先使用本地数据库里“指数日线”表的日期序列作为交易日历；
        - 若表不存在/为空/查询失败，则返回空列表，由调用方回退到自然日逻辑。

        返回值：ISO 日期列表（YYYY-MM-DD），按“最近 -> 更早”顺序。
        返回条数与旧逻辑保持一致：最多返回 max_backoff_days + 1 次尝试的候选日期。
        """

        req_iso = str(requested_trade_date_iso or "").strip()
        if not req_iso:
            return []

        # 多取 1 条：如果 req_iso 本身在表里且 skip_today=True，需要丢掉第一条
        limit = int(max_backoff_days) + 2
        if limit <= 0:
            return []

        sql = text(
            f"SELECT DISTINCT `date` AS d FROM `history_index_daily_kline` "
            "WHERE `date` <= :req ORDER BY `date` DESC "
            f"LIMIT {limit}"
        )

        try:
            with self.db_writer.engine.connect() as conn:
                rows = conn.execute(sql, {"req": req_iso}).fetchall()
        except Exception:
            return []

        dates: list[str] = []
        for row in rows:
            if not row:
                continue
            value = row[0]
            if value is None:
                continue
            iso = str(value).strip()
            if iso:
                dates.append(iso)

        if not dates:
            return []

        if skip_today and dates and dates[0] == req_iso:
            dates = dates[1:]

        # 旧逻辑：最多尝试 max_backoff_days + 1 次
        return dates[: int(max_backoff_days) + 1]

    def _iter_recent_dates(
        self,
        requested_trade_date: str,
        max_backoff_days: int,
        skip_today: bool,
    ) -> list[str]:
        """
        返回 ISO 日期列表（YYYY-MM-DD），按“最近 -> 更早”顺序。

        为避免国庆/春节等长假导致“自然日回退不够而全空”，这里默认优先按交易日回退：
        - 能从本地交易日历拿到候选日期 → 用交易日序列；
        - 否则回退到自然日（兼容旧行为）。
        """
        req_ymd = self._to_yyyymmdd(requested_trade_date)
        if not req_ymd:
            return []

        req_iso = self._yyyymmdd_to_iso(req_ymd)

        trading_dates = self._iter_recent_trading_dates(
            requested_trade_date_iso=req_iso,
            max_backoff_days=max_backoff_days,
            skip_today=skip_today,
        )
        if trading_dates:
            return trading_dates

        # fallback：自然日回退（旧逻辑）
        start_offset = 1 if skip_today else 0
        dates: list[str] = []
        for offset in range(start_offset, max_backoff_days + 1 + start_offset):
            ymd = self._shift_yyyymmdd(req_ymd, -offset)
            dates.append(self._yyyymmdd_to_iso(ymd))
        return dates

    def _fetch_trade_date_df(
        self,
        fetch_fn: Callable[[str], Any],
        requested_trade_date: str,
        max_backoff_days: int,
        skip_today: bool,
        case_name: str,
    ) -> tuple[pd.DataFrame, str | None]:
        """
        尝试获取“最近一次可用”的 trade-date 数据：
        - 只要不是 exception，就认为接口可用（success_empty 也可用）
        - 返回 (df, used_date_iso)
        """
        candidates = self._iter_recent_dates(
            requested_trade_date=requested_trade_date,
            max_backoff_days=max_backoff_days,
            skip_today=skip_today,
        )
        if not candidates:
            return pd.DataFrame(), None

        last_exc: Exception | None = None
        for idx, used_iso in enumerate(candidates, start=1):
            try:
                raw = fetch_fn(used_iso)
                df = self._ensure_df(raw)
                if df is None:
                    raise TypeError("akshare returned None")
                # 注意：有些接口在非交易日/未更新时会“成功返回空 DF”；
                # 如果还没到最后一次尝试，则继续回退到更早日期。
                if getattr(df, "empty", False) and idx < len(candidates):
                    self.logger.info(
                        "%s 返回空数据（date=%s, try=%s/%s），继续回退。",
                        case_name, used_iso, idx, len(candidates)
                    )
                    continue
                return df, used_iso
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                self.logger.warning(
                    "%s 获取失败（date=%s, try=%s/%s）: %s",
                    case_name,
                    used_iso,
                    idx,
                    len(candidates),
                    exc,
                )

        # 全部失败
        if last_exc is not None:
            self.logger.warning("%s 回退尝试全部失败: %s", case_name, last_exc)
        return pd.DataFrame(), None

    # =========================
    # Sync tasks
    # =========================
    def sync_lhb_detail(self, trade_date: str) -> pd.DataFrame:
        ak_cfg = self.config
        lhb_cfg = ak_cfg.get("lhb", {}) if isinstance(ak_cfg.get("lhb", {}), dict) else {}

        # 关键：默认不取当天，直接取“最近一次可用”（通常是上一交易日）
        skip_today = bool(lhb_cfg.get("skip_today", ak_cfg.get("skip_today", True)))
        max_backoff_days = int(lhb_cfg.get("max_backoff_days", ak_cfg.get("max_backoff_days", 5)))

        df, used_iso = self._fetch_trade_date_df(
            fetch_fn=lambda d: self.fetcher.get_lhb_detail(d),
            requested_trade_date=trade_date,
            max_backoff_days=max_backoff_days,
            skip_today=skip_today,
            case_name="龙虎榜详情",
        )

        if df.empty:
            self.logger.info("龙虎榜返回为空（used_date=%s）。", used_iso or trade_date)
            return df

        used_date_for_norm = used_iso or trade_date
        self._normalize_code_column(df)
        self._normalize_trade_date(df, used_date_for_norm)
        subset = [col for col in ["code", "trade_date", "上榜日", "上榜原因"] if col in df.columns]
        self._upsert(df, "a_share_lhb_detail", subset=subset or None)
        return df

    def sync_margin_detail(self, trade_date: str, exchanges: Iterable[str]) -> pd.DataFrame:
        ak_cfg = self.config
        margin_cfg = ak_cfg.get("margin", {}) if isinstance(ak_cfg.get("margin", {}), dict) else {}

        skip_today = bool(margin_cfg.get("skip_today", ak_cfg.get("skip_today", True)))
        max_backoff_days = int(margin_cfg.get("max_backoff_days", ak_cfg.get("max_backoff_days", 5)))

        frames: list[pd.DataFrame] = []

        for exchange in exchanges:
            df, used_iso = self._fetch_trade_date_df(
                fetch_fn=lambda d, ex=exchange: self.fetcher.get_margin_detail(d, ex),
                requested_trade_date=trade_date,
                max_backoff_days=max_backoff_days,
                skip_today=skip_today,
                case_name=f"两融明细 {exchange}",
            )

            if df.empty:
                self.logger.info("两融明细 %s 返回为空（used_date=%s）。", exchange, used_iso or trade_date)
                continue

            used_date_for_norm = used_iso or trade_date
            self._normalize_code_column(df)
            self._normalize_trade_date(df, used_date_for_norm)
            self._normalize_exchange(df, exchange)
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        subset = [col for col in ["exchange", "trade_date", "code"] if col in combined.columns]
        self._upsert(combined, "a_share_margin_detail", subset=subset or None)
        return combined

    def sync_hsgt_hold_rank(self, market_list: Iterable[str], indicator: str, trade_date: str) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for market in market_list:
            try:
                df = self.fetcher.get_hsgt_hold_rank(market=market, indicator=indicator)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("北向持股排行 %s 获取失败: %s", market, exc)
                continue

            if df is None or df.empty:
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
        subset = [col for col in ["market", "indicator", "trade_date", "code"] if col in combined.columns]
        self._upsert(combined, "a_share_hsgt_hold_rank", subset=subset or None)
        return combined

    def sync_shareholder_counts(self, trade_date: str, focus_codes: list[str] | None = None) -> pd.DataFrame:
        gdhs_cfg = self.config.get("gdhs", {}) if isinstance(self.config.get("gdhs", {}), dict) else {}
        period = gdhs_cfg.get("period") or "最新"

        try:
            summary_df = self.fetcher.get_shareholder_count(period)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("股东户数汇总获取失败: %s", exc)
            summary_df = pd.DataFrame()

        summary_df = self._ensure_df(summary_df)

        if not summary_df.empty:
            period_from_data = summary_df.get("股东户数统计截止日-本次")
            if period_from_data is not None and not period_from_data.empty:
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
        top_n = int(gdhs_cfg.get("detail_top_n", 100))
        normalized_codes = [self._normalize_symbol(code) for code in focus_codes][:top_n]

        try:
            frames = self.fetcher.batch_get_shareholder_count_detail(normalized_codes)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("股东户数明细批量获取失败: %s", exc)
            return summary_df

        if not frames:
            self.logger.info("股东户数明细在指定代码范围内无返回。")
            return summary_df

        detail_df = pd.concat([self._ensure_df(x) for x in frames if x is not None], ignore_index=True)
        if detail_df.empty:
            self.logger.info("股东户数明细拼接后为空。")
            return summary_df

        period_from_detail = detail_df.get("股东户数统计截止日")
        if period_from_detail is not None and not period_from_detail.empty:
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

    def sync_daily_signals(self, trade_date: str, focus_codes: list[str] | None = None) -> None:
        ak_cfg = self.config
        if not ak_cfg.get("enabled", False):
            self.logger.info("Akshare 行为证据开关关闭，已跳过所有相关采集。")
            return

        # 这里保证：任何一个子任务失败，都不会把“行为证据同步阶段”整体打断
        lhb_cfg = ak_cfg.get("lhb", {})
        if isinstance(lhb_cfg, dict) and lhb_cfg.get("enabled", True):
            try:
                self.sync_lhb_detail(trade_date)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("龙虎榜同步阶段出现异常: %s", exc)
        else:
            self.logger.info("龙虎榜采集已关闭。")

        margin_cfg = ak_cfg.get("margin", {})
        if isinstance(margin_cfg, dict) and margin_cfg.get("enabled", True):
            exchanges = margin_cfg.get("exchanges", ["sse", "szse"])
            try:
                self.sync_margin_detail(trade_date, exchanges)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("两融同步阶段出现异常: %s", exc)
        else:
            self.logger.info("两融采集已关闭。")

        hsgt_cfg = ak_cfg.get("hsgt", {})
        if isinstance(hsgt_cfg, dict) and hsgt_cfg.get("enabled", True):
            markets = hsgt_cfg.get("markets", ["沪股通", "深股通"])
            indicator = hsgt_cfg.get("indicator", "5日排行")
            try:
                self.sync_hsgt_hold_rank(markets, indicator, trade_date)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("北向持股同步阶段出现异常: %s", exc)
        else:
            self.logger.info("北向持股采集已关闭。")

        gdhs_cfg = ak_cfg.get("gdhs", {})
        if isinstance(gdhs_cfg, dict) and gdhs_cfg.get("enabled", True):
            try:
                self.sync_shareholder_counts(trade_date, focus_codes)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("股东户数同步阶段出现异常: %s", exc)
        else:
            self.logger.info("股东户数采集已关闭。")
