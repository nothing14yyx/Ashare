from __future__ import annotations

import datetime as dt
from typing import Iterable, List, Tuple

import pandas as pd
from sqlalchemy import bindparam, text

from .db import DatabaseConfig, MySQLWriter
from .schema_manager import TABLE_STRATEGY_CHIP_FILTER
from .utils import setup_logger


class ChipFilter:
    """筹码因子计算：从股东户数明细读取并覆盖写入单表。"""

    def __init__(self) -> None:
        self.logger = setup_logger()
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())
        self.table = TABLE_STRATEGY_CHIP_FILTER

    def _table_exists(self, table: str) -> bool:
        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1"))
            return True
        except Exception:
            return False

    def _resolve_gdhs_columns(
        self, table: str
    ) -> Tuple[str | None, str | None, str | None, str | None]:
        stmt = text(f"SELECT * FROM `{table}` LIMIT 0")
        with self.db_writer.engine.begin() as conn:
            meta_df = pd.read_sql_query(stmt, conn)
        columns = set(meta_df.columns)
        code_col = "code" if "code" in columns else None
        announce_candidates = [
            "公告日期",
            "股东户数公告日期",
            "股东户数统计截止日",
            "period",
        ]
        delta_pct_candidates = [
            "股东户数-增减比例",
            "增减比例",
            "holder_change_ratio",
        ]
        announce_col = next((c for c in announce_candidates if c in columns), None)
        delta_pct_col = next((c for c in delta_pct_candidates if c in columns), None)
        delta_abs_col = None
        if "股东户数-变动数量" in columns:
            delta_abs_col = "股东户数-变动数量"
        elif "股东户数-上次" in columns:
            delta_abs_col = "股东户数-上次"
        elif "holder_change" in columns:
            delta_abs_col = "holder_change"
        return code_col, announce_col, delta_pct_col, delta_abs_col

    def _load_gdhs_detail(
        self, codes: Iterable[str], latest_date: str | dt.date, table: str
    ) -> pd.DataFrame:
        code_col, announce_col, delta_pct_col, delta_abs_col = self._resolve_gdhs_columns(table)
        if code_col is None or announce_col is None:
            return pd.DataFrame()

        select_cols = [code_col, announce_col]
        for col in [delta_pct_col, delta_abs_col]:
            if col:
                select_cols.append(col)

        stmt = (
            text(
                f"""
                SELECT {",".join(f"`{c}`" for c in select_cols)}
                FROM `{table}`
                WHERE `{code_col}` IN :codes
                  AND `{announce_col}` <= :latest
                """
            ).bindparams(bindparam("codes", expanding=True))
        )
        with self.db_writer.engine.begin() as conn:
            try:
                df = pd.read_sql_query(stmt, conn, params={"codes": list(codes), "latest": str(latest_date)})
            except Exception:
                return pd.DataFrame()

        rename_map = {code_col: "code", announce_col: "announce_date"}
        if delta_pct_col:
            rename_map[delta_pct_col] = "gdhs_delta_pct"
        if delta_abs_col:
            rename_map[delta_abs_col] = "gdhs_delta_raw"
        df = df.rename(columns=rename_map)
        df["code"] = df["code"].astype(str)
        df["announce_date"] = pd.to_datetime(df["announce_date"], errors="coerce")
        df["gdhs_delta_pct"] = pd.to_numeric(df.get("gdhs_delta_pct"), errors="coerce")
        df["gdhs_delta_raw"] = pd.to_numeric(df.get("gdhs_delta_raw"), errors="coerce")
        return df.dropna(subset=["announce_date"]).sort_values(["code", "announce_date"]).reset_index(drop=True)

    def _write_table(self, df: pd.DataFrame) -> None:
        if df.empty or not self._table_exists(self.table):
            return
        sig_dates = df["sig_date"].dropna().unique().tolist()
        if sig_dates:
            delete_stmt = (
                text(
                    f"""
                    DELETE FROM `{self.table}`
                    WHERE `sig_date` IN :dates AND `code` IN :codes
                    """
                ).bindparams(bindparam("dates", expanding=True), bindparam("codes", expanding=True))
            )
            with self.db_writer.engine.begin() as conn:
                codes = df["code"].dropna().astype(str).unique().tolist()
                conn.execute(delete_stmt, {"dates": sig_dates, "codes": codes})
        self.db_writer.write_dataframe(df, self.table, if_exists="append")

    def apply(self, signals: pd.DataFrame, *, gdhs_table: str = "a_share_gdhs_detail") -> pd.DataFrame:
        if signals.empty:
            return pd.DataFrame()

        if not self._table_exists(gdhs_table):
            return pd.DataFrame()

        sig_df = signals.copy()
        sig_df["sig_date"] = pd.to_datetime(sig_df["date"], errors="coerce")
        sig_df = sig_df.dropna(subset=["sig_date", "code"])
        sig_df["code"] = sig_df["code"].astype(str)
        sig_df = sig_df.sort_values(["code", "sig_date"], ignore_index=True)
        if sig_df.empty:
            return pd.DataFrame()

        latest_date = sig_df["sig_date"].max()
        codes = sig_df["code"].unique().tolist()
        chip_df = self._load_gdhs_detail(codes, latest_date, gdhs_table)
        if chip_df.empty:
            return pd.DataFrame()

        chip_df = chip_df.sort_values(["code", "announce_date"], ignore_index=True)

        merged_parts = []
        for code, sig_slice in sig_df.groupby("code", sort=False):
            chip_slice = chip_df[chip_df["code"] == code]
            if chip_slice.empty:
                part = (
                    sig_slice.assign(
                        announce_date=pd.NaT,
                        gdhs_delta_pct=pd.NA,
                        gdhs_delta_raw=pd.NA,
                    )
                )
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
            part = part.dropna(how="all")
            if not part.empty:
                merged_parts.append(part)

        cleaned_parts = []
        for part in merged_parts:
            if part is None:
                continue
            trimmed = part.dropna(how="all")
            if not trimmed.empty:
                cleaned_parts.append(trimmed)

        if not cleaned_parts:
            return pd.DataFrame()

        merged = pd.concat(cleaned_parts, ignore_index=True)
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
        chip_reason = pd.Series("", index=merged.index, dtype="object")
        chip_reason = chip_reason.mask(
            merged["gdhs_delta_pct"].isna() | merged["announce_date"].isna(),
            "DATA_MISSING",
        )
        delta_raw = pd.to_numeric(merged.get("gdhs_delta_raw"), errors="coerce")
        outlier = (merged["gdhs_delta_pct"].abs() > 80) | (delta_raw.abs() > 1_000_000)
        outlier = outlier.fillna(False)
        chip_reason = chip_reason.mask(outlier, "DATA_OUTLIER_GDHS")
        chip_ok = (merged["gdhs_delta_pct"] < -5) & (merged["vol_ratio"] > 1.5)
        chip_ok = chip_ok.mask(outlier, False)
        chip_ok = chip_ok.where(~chip_reason.isin(["DATA_MISSING"]))
        chip_reject = chip_reason.isna() & (chip_ok == False)  # noqa: E712
        chip_reason = chip_reason.mask(chip_reject, "CHIP_REJECT")
        merged["chip_ok"] = chip_ok
        merged["chip_reason"] = chip_reason.replace("", None)
        merged["sig_date"] = merged["sig_date"].dt.date
        merged["updated_at"] = dt.datetime.now()

        keep_cols = [
            "sig_date",
            "code",
            "announce_date",
            "gdhs_delta_pct",
            "gdhs_delta_raw",
            "chip_ok",
            "chip_reason",
            "vol_ratio",
            "updated_at",
        ]
        result = merged[keep_cols].copy()
        result["announce_date"] = pd.to_datetime(result.get("announce_date"), errors="coerce").dt.date
        try:
            self._write_table(result)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("写入 %s 失败：%s", self.table, exc)
        return result
