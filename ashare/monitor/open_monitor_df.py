from __future__ import annotations

import pandas as pd

ASOF_RENAME_MAP = {
    "trade_date": "asof_trade_date",
    "close": "asof_close",
    "avg_volume_20": "asof_avg_volume_20",
    "ma5": "asof_ma5",
    "ma20": "asof_ma20",
    "ma60": "asof_ma60",
    "ma250": "asof_ma250",
    "vol_ratio": "asof_vol_ratio",
    "macd_hist": "asof_macd_hist",
    "kdj_k": "asof_kdj_k",
    "kdj_d": "asof_kdj_d",
    "atr14": "asof_atr14",
}

ASOF_SIG_COALESCE = (
    ("asof_close", "sig_close"),
    ("asof_ma5", "sig_ma5"),
    ("asof_ma20", "sig_ma20"),
    ("asof_ma60", "sig_ma60"),
    ("asof_ma250", "sig_ma250"),
    ("asof_vol_ratio", "sig_vol_ratio"),
    ("asof_macd_hist", "sig_macd_hist"),
    ("asof_atr14", "sig_atr14"),
    ("asof_stop_ref", "sig_stop_ref"),
)


def normalize_asof_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()

    if "code" in df.columns:
        df["code"] = df["code"].astype(str)

    rename_map = {
        src: dst
        for src, dst in ASOF_RENAME_MAP.items()
        if src in df.columns and dst not in df.columns
    }
    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def coalesce_asof_from_sig(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()

    if "asof_trade_date" in df.columns and "sig_date" in df.columns:
        mask = df["asof_trade_date"].notna()
        df["asof_trade_date"] = df["asof_trade_date"].where(mask, df["sig_date"])

    for target, fallback in ASOF_SIG_COALESCE:
        if target not in df.columns:
            df[target] = None
        if fallback not in df.columns:
            continue
        left = pd.to_numeric(df[target], errors="coerce")
        right = pd.to_numeric(df[fallback], errors="coerce")
        df[target] = left.fillna(right)

    return df
