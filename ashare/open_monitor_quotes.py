from __future__ import annotations

"""open_monitor 的行情抓取与字段标准化。

目标：
- 将 eastmoney / akshare 行情抓取逻辑从 open_monitor.py 拆分出来；
- 统一输出列名（live_open/live_high/live_low/live_latest/live_volume/live_amount...）。
"""

import json
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List

import pandas as pd

from .utils.convert import to_float as _to_float


def normalize_quotes_columns(df: pd.DataFrame) -> pd.DataFrame:
    """将不同来源行情列统一成 open_monitor 契约列。"""

    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame(
            columns=[
                "code",
                "live_open",
                "live_high",
                "live_low",
                "live_latest",
                "live_volume",
                "live_amount",
                "live_pct_change",
                "prev_close",
            ]
        )

    out = df.copy()

    mapping: Dict[str, str] = {
        # 统一英文列
        "open": "live_open",
        "high": "live_high",
        "low": "live_low",
        "latest": "live_latest",
        "volume": "live_volume",
        "amount": "live_amount",
        "pct_change": "live_pct_change",
        "gap_pct": "live_gap_pct",
        "intraday_vol_ratio": "live_intraday_vol_ratio",
        # 统一中文列（以 akshare spot 为主）
        "今开": "live_open",
        "最高": "live_high",
        "最低": "live_low",
        "最新价": "live_latest",
        "成交量": "live_volume",
        "成交额": "live_amount",
        "涨跌幅": "live_pct_change",
        "昨收": "prev_close",
    }

    rename_cols = {k: v for k, v in mapping.items() if k in out.columns and v not in out.columns}
    if rename_cols:
        out = out.rename(columns=rename_cols)

    # 确保统一列存在（即便为空行情，也保持契约列结构稳定）
    for col in (
        "live_open",
        "live_high",
        "live_low",
        "live_latest",
        "live_volume",
        "live_amount",
    ):
        if col not in out.columns:
            out[col] = None

    return out


def strip_baostock_prefix(code: str) -> str:
    code = str(code or "").strip()
    if code.startswith("sh.") or code.startswith("sz."):
        return code[3:]
    return code


def to_baostock_code(exchange: str, symbol: str) -> str:
    ex = str(exchange or "").lower().strip()
    sym = str(symbol or "").strip()
    if ex in {"sh", "1"}:
        return f"sh.{sym}"
    if ex in {"sz", "0"}:
        return f"sz.{sym}"
    # fallback：猜测 6/9 为沪，0/3 为深
    if sym.startswith(("6", "9")):
        return f"sh.{sym}"
    return f"sz.{sym}"


def to_eastmoney_secid(code: str) -> str:
    code = str(code or "").strip()
    if code.startswith("sh."):
        digits = strip_baostock_prefix(code)
        return f"1.{digits}"
    if code.startswith("sz."):
        digits = strip_baostock_prefix(code)
        return f"0.{digits}"
    # fallback：按 6/9 -> 沪
    digits = strip_baostock_prefix(code)
    if digits.startswith(("6", "9")):
        return f"1.{digits}"
    return f"0.{digits}"


def urlopen_json_no_proxy(url: str, *, timeout: int = 10, retries: int = 2) -> Dict[str, Any]:
    """访问东财接口并返回 JSON（默认不使用环境代理）。"""

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://quote.eastmoney.com/",
        "Connection": "close",
    }
    req = urllib.request.Request(url, headers=headers)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    last_exc: Exception | None = None
    for i in range(retries + 1):
        try:
            with opener.open(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            return json.loads(raw) if raw else {}
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if i < retries:
                time.sleep(0.5 * (2**i))
                continue
            raise last_exc


def fetch_quotes_akshare(
    codes: List[str],
    *,
    strict_quotes: bool = True,
    logger: Any = None,  # noqa: ANN401
) -> pd.DataFrame:
    try:
        import akshare as ak  # type: ignore
    except Exception as exc:  # noqa: BLE001
        if logger is not None:
            logger.info("AkShare 不可用（将回退）：%s", exc)
        return normalize_quotes_columns(pd.DataFrame())

    digits = {strip_baostock_prefix(c) for c in codes}
    try:
        spot = ak.stock_zh_a_spot_em()
    except Exception as exc:  # noqa: BLE001
        if logger is not None:
            logger.warning("AkShare 行情拉取失败（将回退）：%s", exc)
        return normalize_quotes_columns(pd.DataFrame())

    if spot is None or getattr(spot, "empty", True):
        return normalize_quotes_columns(pd.DataFrame())

    rename_map = {
        "代码": "symbol",
        "名称": "name",
        "最新价": "latest",
        "涨跌幅": "pct_change",
        "今开": "open",
        "昨收": "prev_close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
    }
    for k in list(rename_map.keys()):
        if k not in spot.columns:
            rename_map.pop(k, None)

    spot = spot.rename(columns=rename_map)
    if "symbol" not in spot.columns:
        return normalize_quotes_columns(pd.DataFrame())

    spot["symbol"] = spot["symbol"].astype(str)
    spot = spot[spot["symbol"].isin(digits)].copy()
    if spot.empty:
        return normalize_quotes_columns(pd.DataFrame())

    out = pd.DataFrame()
    out["code"] = spot["symbol"].apply(lambda x: to_baostock_code("auto", str(x)))
    out["symbol"] = spot["symbol"].astype(str)
    out["name"] = spot.get("name", pd.Series([""] * len(spot))).astype(str)
    out["open"] = spot.get("open", pd.Series([None] * len(spot))).apply(_to_float)
    out["latest"] = spot.get("latest", pd.Series([None] * len(spot))).apply(_to_float)
    out["prev_close"] = spot.get("prev_close", pd.Series([None] * len(spot))).apply(_to_float)
    out["high"] = spot.get("high", pd.Series([None] * len(spot))).apply(_to_float)
    out["low"] = spot.get("low", pd.Series([None] * len(spot))).apply(_to_float)
    out["volume"] = spot.get("volume", pd.Series([None] * len(spot))).apply(_to_float)
    out["amount"] = spot.get("amount", pd.Series([None] * len(spot))).apply(_to_float)
    out["pct_change"] = spot.get("pct_change", pd.Series([None] * len(spot))).apply(_to_float)

    mapping = {strip_baostock_prefix(c): c for c in codes}
    out["code"] = out["symbol"].map(mapping).fillna(out["code"])
    out = normalize_quotes_columns(out)
    required = [
        "code",
        "live_open",
        "live_high",
        "live_low",
        "live_latest",
        "live_volume",
        "live_amount",
    ]
    missing = [c for c in required if c not in out.columns]
    if missing:
        msg = f"akshare 行情缺少统一列：{missing}"
        if strict_quotes:
            raise RuntimeError(msg)
        if logger is not None:
            logger.error("%s（strict_quotes=false，将补空列）", msg)
        for c in missing:
            out[c] = None
    return out.reset_index(drop=True)


def fetch_quotes_eastmoney(
    codes: List[str],
    *,
    strict_quotes: bool = True,
    logger: Any = None,  # noqa: ANN401
) -> pd.DataFrame:
    if not codes:
        return normalize_quotes_columns(pd.DataFrame())

    base_url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
    fields = "f2,f3,f4,f5,f6,f12,f14,f15,f16,f17,f18"
    secids = [to_eastmoney_secid(c) for c in codes]

    batch_size = 80
    rows: List[Dict[str, Any]] = []
    for i in range(0, len(secids), batch_size):
        part = secids[i : i + batch_size]
        query = {
            "fltt": "2",
            "invt": "2",
            "fields": fields,
            "secids": ",".join(part),
        }
        url = f"{base_url}?{urllib.parse.urlencode(query)}"
        try:
            payload = urlopen_json_no_proxy(url, timeout=10, retries=2)
        except Exception as exc:  # noqa: BLE001
            if logger is not None:
                logger.error("Eastmoney 行情请求失败：%s", exc)
            continue

        data = (payload or {}).get("data") or {}
        diff = data.get("diff") or []
        if isinstance(diff, list):
            rows.extend([r for r in diff if isinstance(r, dict)])

    if not rows:
        return normalize_quotes_columns(pd.DataFrame())

    out_rows: List[Dict[str, Any]] = []
    mapping = {strip_baostock_prefix(c): c for c in codes}
    for r in rows:
        symbol = str(r.get("f12") or "").strip()
        name = str(r.get("f14") or "").strip()
        latest = _to_float(r.get("f2"))
        pct = _to_float(r.get("f3"))
        high = _to_float(r.get("f15"))
        low = _to_float(r.get("f16"))
        open_px = _to_float(r.get("f17"))
        prev_close = _to_float(r.get("f18"))
        # Eastmoney 成交量单位为“手”，统一转换为“股”口径
        volume = _to_float(r.get("f5"))
        volume = volume * 100 if volume is not None else None
        amount = _to_float(r.get("f6"))

        code_guess = to_baostock_code("auto", symbol)
        code = mapping.get(symbol, code_guess)

        out_rows.append(
            {
                "code": code,
                "symbol": symbol,
                "name": name,
                "open": open_px,
                "latest": latest,
                "prev_close": prev_close,
                "high": high,
                "low": low,
                "volume": volume,
                "amount": amount,
                "pct_change": pct,
            }
        )
