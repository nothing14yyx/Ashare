from __future__ import annotations

import datetime as dt
from typing import Iterable, List, Tuple

import numpy as np

import pandas as pd

from ashare.core.db import DatabaseConfig, MySQLWriter
from ashare.core.schema_manager import TABLE_STRATEGY_CHIP_FILTER
from ashare.utils import setup_logger
from ashare.strategies.chip_filter_repo import ChipFilterRepository


class ChipFilter:
    """筹码因子计算：从股东户数明细读取并覆盖写入单表。"""

    def __init__(self) -> None:
        self.logger = setup_logger()
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())
        self.table = TABLE_STRATEGY_CHIP_FILTER
        self.repo = ChipFilterRepository(self.db_writer, self.logger)

    def _table_exists(self, table: str) -> bool:
        return self.repo.table_exists(table)

    def _resolve_gdhs_columns(
        self, table: str
    ) -> Tuple[str | None, str | None, str | None, str | None]:
        return self.repo.resolve_gdhs_columns(table)

    def _load_gdhs_detail(
        self, codes: Iterable[str], latest_date: str | dt.date, table: str
    ) -> pd.DataFrame:
        code_col, announce_col, delta_pct_col, delta_abs_col = self._resolve_gdhs_columns(table)
        if code_col is None or announce_col is None:
            return pd.DataFrame()

        df = self.repo.load_gdhs_detail(
            table, codes, code_col, announce_col, delta_pct_col, delta_abs_col
        )
        if df.empty:
            return pd.DataFrame()

        rename_map = {code_col: "code", announce_col: "announce_date"}
        if delta_pct_col:
            rename_map[delta_pct_col] = "gdhs_delta_pct"
        if delta_abs_col:
            rename_map[delta_abs_col] = "gdhs_delta_raw"
        df = df.rename(columns=rename_map)
        df["code"] = df["code"].astype(str)
        df["announce_date"] = pd.to_datetime(df["announce_date"], errors="coerce")
        latest_ts = pd.to_datetime(latest_date, errors="coerce")
        if pd.isna(latest_ts):
            latest_ts = pd.Timestamp.max
        df = df[df["announce_date"] <= latest_ts]
        df["gdhs_delta_pct"] = pd.to_numeric(df.get("gdhs_delta_pct"), errors="coerce")
        df["gdhs_delta_raw"] = pd.to_numeric(df.get("gdhs_delta_raw"), errors="coerce")
        return df.dropna(subset=["announce_date"]).sort_values(["code", "announce_date"]).reset_index(drop=True)

    def _write_table(self, df: pd.DataFrame) -> None:
        if df.empty or not self._table_exists(self.table):
            return
        sig_dates = df["sig_date"].dropna().unique().tolist()
        table_columns = self.repo.get_table_columns(self.table)
        if sig_dates:
            codes = df["code"].dropna().astype(str).unique().tolist()
            self.repo.delete_existing(self.table, sig_dates, codes)
        aligned = df.copy()
        if table_columns:
            aligned = aligned[[c for c in aligned.columns if c in table_columns]].copy()
        self.repo.write_dataframe(aligned, self.table)

    def apply(
        self,
        signals: pd.DataFrame,
        *,
        gdhs_table: str = "a_share_gdhs_detail",
        gdhs_summary_table: str | None = "a_share_gdhs",
    ) -> pd.DataFrame:
        if signals.empty:
            return pd.DataFrame()
        detail_exists = self._table_exists(gdhs_table)
        summary_exists = bool(gdhs_summary_table) and self._table_exists(gdhs_summary_table)
        if not detail_exists and not summary_exists:
            return pd.DataFrame()

        sig_df = signals.copy()
        date_col = (
            "date"
            if "date" in sig_df.columns
            else ("sig_date" if "sig_date" in sig_df.columns else None)
        )
        if date_col is None:
            self.logger.warning(
                "ChipFilter: signals 缺少 date/sig_date 列，无法计算筹码过滤。"
            )
            return pd.DataFrame()
        sig_df["sig_date"] = pd.to_datetime(sig_df[date_col], errors="coerce")
        sig_df = sig_df.dropna(subset=["sig_date", "code"])
        sig_df["code"] = sig_df["code"].astype(str)
        sig_df = sig_df.sort_values(["code", "sig_date"], ignore_index=True)
        if sig_df.empty:
            return pd.DataFrame()

        latest_date = sig_df["sig_date"].max()
        codes = sig_df["code"].unique().tolist()
        empty_cols = ["code", "announce_date", "gdhs_delta_pct", "gdhs_delta_raw"]

        detail_df = (
            self._load_gdhs_detail(codes, latest_date, gdhs_table)
            if detail_exists
            else pd.DataFrame(columns=empty_cols)
        )
        summary_df = (
            self._load_gdhs_detail(codes, latest_date, gdhs_summary_table)  # type: ignore[arg-type]
            if summary_exists
            else pd.DataFrame(columns=empty_cols)
        )

        frames: list[pd.DataFrame] = []
        if not detail_df.empty:
            detail_df = detail_df.copy()
            detail_df["__src"] = 0
            frames.append(detail_df)
        if not summary_df.empty:
            summary_df = summary_df.copy()
            summary_df["__src"] = 1
            frames.append(summary_df)

        chip_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=empty_cols)
        if not chip_df.empty:
            chip_df["code"] = chip_df["code"].astype(str)
            chip_df["announce_date"] = pd.to_datetime(chip_df["announce_date"], errors="coerce")
            chip_df["gdhs_delta_pct"] = pd.to_numeric(chip_df.get("gdhs_delta_pct"), errors="coerce")
            chip_df["gdhs_delta_raw"] = pd.to_numeric(chip_df.get("gdhs_delta_raw"), errors="coerce")
            chip_df = (
                chip_df.dropna(subset=["code", "announce_date"])
                .sort_values(["code", "announce_date", "__src"], ignore_index=True)
                .drop_duplicates(subset=["code", "announce_date"], keep="first")
                .drop(columns=["__src"], errors="ignore")
            )

        chip_df = chip_df.sort_values(["code", "announce_date"], ignore_index=True)

        merged_parts: list[pd.DataFrame] = []
        for code, sig_slice in sig_df.groupby("code", sort=False):
            chip_slice = chip_df[chip_df["code"] == code]

            if chip_slice.empty:
                part = sig_slice.copy()
                part["announce_date"] = pd.NaT
                part["gdhs_delta_pct"] = np.nan
                part["gdhs_delta_raw"] = np.nan
            else:
                part = pd.merge_asof(
                    sig_slice,
                    chip_slice,
                    left_on="sig_date",
                    right_on="announce_date",
                    direction="backward",
                    allow_exact_matches=True,
                )

            part = part.copy()
            part["code"] = code

            # 关键：统一 dtype，避免 concat 时因“空/全 NA 块”的 dtype 推断变化触发 FutureWarning
            part["announce_date"] = pd.to_datetime(part.get("announce_date"), errors="coerce")
            part["gdhs_delta_pct"] = pd.to_numeric(part.get("gdhs_delta_pct"), errors="coerce")
            part["gdhs_delta_raw"] = pd.to_numeric(part.get("gdhs_delta_raw"), errors="coerce")

            # 过滤空块 / 全 NA 块（FutureWarning 的根源之一）
            if part.empty:
                continue
            if not part.notna().any().any():
                continue

            merged_parts.append(part)

        if not merged_parts:
            return pd.DataFrame()

        merged = pd.concat(merged_parts, ignore_index=True)
        if "code" not in merged.columns:
            self.logger.warning("strategy_chip_filter 缺少 code 列，已跳过写入。")
            return pd.DataFrame()

        bad = merged["code"].isna() | (merged["code"].astype(str).str.strip() == "")
        if bad.any():
            self.logger.warning("strategy_chip_filter 丢弃 code 为空的行数=%s", int(bad.sum()))
            merged = merged.loc[~bad].copy()
        if merged.empty:
            return pd.DataFrame()

        merged["gdhs_delta_pct"] = pd.to_numeric(merged.get("gdhs_delta_pct"), errors="coerce")
        merged["vol_ratio"] = pd.to_numeric(merged.get("vol_ratio"), errors="coerce")
        merged["close"] = pd.to_numeric(merged.get("close"), errors="coerce")
        merged["ma20"] = pd.to_numeric(merged.get("ma20"), errors="coerce")
        merged["runup_pct"] = pd.to_numeric(merged.get("runup_pct"), errors="coerce")
        merged["fear_score"] = pd.to_numeric(merged.get("fear_score"), errors="coerce")
        merged["chip_delta"] = merged["gdhs_delta_pct"]

        sig_dt = pd.to_datetime(merged["sig_date"], errors="coerce")
        merged["age_days"] = (sig_dt - merged["announce_date"]).dt.days
        merged["deadzone_hit"] = merged["chip_delta"].abs() <= 5
        merged["stale_hit"] = merged["age_days"] > 120

        chip_reason = pd.Series(pd.NA, index=merged.index, dtype="object")
        chip_score = pd.Series(0.0, index=merged.index, dtype=float)
        chip_penalty = pd.Series(0.0, index=merged.index, dtype=float)
        chip_note = pd.Series(pd.NA, index=merged.index, dtype="object")

        missing_gdhs = merged["chip_delta"].isna() | merged["announce_date"].isna()
        chip_reason = chip_reason.mask(missing_gdhs, "DATA_MISSING_GDHS")
        missing_vol = merged["vol_ratio"].isna()
        vol_ratio_used = merged["vol_ratio"].fillna(1.0)
        chip_reason = chip_reason.mask(
            missing_vol & chip_reason.isna(), "DATA_MISSING_VOL_RATIO_FALLBACK"
        )

        delta_raw = pd.to_numeric(merged.get("gdhs_delta_raw"), errors="coerce")
        outlier_mask = (merged["chip_delta"].abs() > 80) | (delta_raw.abs() > 1_000_000)
        outlier_mask = outlier_mask.fillna(False)
        chip_reason = chip_reason.mask(outlier_mask & chip_reason.isna(), "DATA_OUTLIER_GDHS")

        chip_reason = chip_reason.mask(merged["stale_hit"] & chip_reason.isna(), "DATA_STALE_GDHS")

        can_eval = ~(
            missing_gdhs | outlier_mask | merged["stale_hit"].fillna(False)
        )
        concentrate = can_eval & (merged["chip_delta"] <= -10)
        disperse = can_eval & (merged["chip_delta"] >= 20)
        deadzone = can_eval & merged["deadzone_hit"]

        chip_score.loc[concentrate] += 0.6
        chip_score.loc[disperse] -= 0.6

        chip_reason = chip_reason.mask(concentrate, "CHIP_CONCENTRATE")
        chip_reason = chip_reason.mask(disperse, "CHIP_DISPERSE_STRONG")
        chip_reason = chip_reason.mask(deadzone, "CHIP_NEUTRAL")

        # 量价确认与情绪分歧扣分
        daily_drop_candidates = []
        for col in ["pct_chg", "pct_change", "change_pct", "ret_1d", "ret"]:
            if col in merged.columns:
                daily_drop_candidates.append(pd.to_numeric(merged[col], errors="coerce"))
        daily_drop = next((s for s in daily_drop_candidates if not s.isna().all()), pd.Series(np.nan, index=merged.index))
        weaken_price = (merged["close"] < merged["ma20"]) | (
            (vol_ratio_used > 1.5) & (daily_drop < 0)
        )
        chip_score.loc[can_eval & weaken_price.fillna(False)] -= 0.3

        risk_tag = merged.get("risk_tag")
        if isinstance(risk_tag, pd.Series):
            risk_tag = risk_tag.astype("string")
            mania_mask = risk_tag.str.contains("MANIA", case=False, na=False)
        else:
            mania_mask = pd.Series(False, index=merged.index)

        high_runup = merged["runup_pct"] > 0.2
        chip_score.loc[can_eval & (high_runup | mania_mask)] -= 0.3

        chip_score = chip_score.clip(-1.0, 1.0)
        chip_reason = chip_reason.mask(can_eval & chip_reason.isna(), "CHIP_NEUTRAL")

        data_issue_mask = chip_reason.isin({
            "DATA_MISSING_GDHS",
            "DATA_OUTLIER_GDHS",
            "DATA_STALE_GDHS",
        })
        chip_score = chip_score.where(~data_issue_mask, 0.0)
        chip_note = chip_note.mask(data_issue_mask, chip_reason)

        deadzone_flag = deadzone.fillna(False)
        chip_penalty = chip_penalty.mask(deadzone_flag, chip_penalty + 0.1)
        chip_note = chip_note.mask(deadzone_flag & chip_note.isna(), "DEADZONE_PENALTY")

        merged["chip_score"] = chip_score
        merged["chip_reason"] = chip_reason
        merged["chip_penalty"] = chip_penalty
        merged["chip_note"] = chip_note
        merged["sig_date"] = merged["sig_date"].dt.date
        merged["updated_at"] = dt.datetime.now()

        keep_cols = [
            "sig_date",
            "code",
            "announce_date",
            "gdhs_delta_pct",
            "gdhs_delta_raw",
            "chip_score",
            "chip_reason",
            "vol_ratio",
            "age_days",
            "deadzone_hit",
            "stale_hit",
            "chip_penalty",
            "chip_note",
            "updated_at",
        ]
        result = merged[keep_cols].copy()
        result["announce_date"] = pd.to_datetime(result.get("announce_date"), errors="coerce").dt.date
        try:
            self._write_table(result)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("写入 %s 失败：%s", self.table, exc)
        return result
