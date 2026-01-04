"""Akshare 数据访问封装，用于行为证据信号采集."""

from __future__ import annotations

from typing import Any, Iterable

try:
    import akshare as ak
except ImportError:  # pragma: no cover - 环境未安装 akshare 时延迟失败
    ak = None

import datetime as dt
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class AkshareDataFetcher:
    """封装常用的 Akshare 行为证据接口。"""

    def __init__(self) -> None:
        if ak is None:  # pragma: no cover - 运行时缺少依赖
            raise ImportError("akshare 未安装，无法初始化 AkshareDataFetcher")

    def _ensure_df(self, value: Any) -> pd.DataFrame:
        """把 AkShare 返回值尽量规整成 DataFrame；None / 不可转换 -> 空 DF。"""
        if value is None:
            return pd.DataFrame()
        if isinstance(value, pd.DataFrame):
            return value
        try:
            return pd.DataFrame(value)
        except Exception:  # noqa: BLE001
            return pd.DataFrame()

    def fetch_minute_data(self, code: str, trade_date: str | dt.date | None = None) -> pd.DataFrame:
        """拉取单只股票分时数据（包含时间、价格、成交量、均价线）。

        返回 DataFrame 列：[time, price, volume, avg_price]
        """
        # 清洗 symbol 格式
        # ak.stock_intraday_em (东财) 通常只需要纯数字 code，或者带前缀也能识别
        # 这里做简单的兼容处理
        clean_code = code.split(".")[-1] if "." in code else code
        sina_symbol = code.replace(".", "").lower() 
        if not (sina_symbol.startswith("sh") or sina_symbol.startswith("sz") or sina_symbol.startswith("bj")):
            # 简单猜测前缀用于新浪备用
            if code.startswith("6"): sina_symbol = "sh" + clean_code
            elif code.startswith("0") or code.startswith("3"): sina_symbol = "sz" + clean_code
            elif code.startswith("4") or code.startswith("8"): sina_symbol = "bj" + clean_code

        trade_date_val = None
        if trade_date is not None:
            try:
                trade_date_val = pd.to_datetime(trade_date, errors="coerce").date()
            except Exception:
                trade_date_val = None
        date_str = trade_date_val.isoformat() if trade_date_val else None

        try:
            # 1. 历史分时优先（仅当指定了历史日期）
            if date_str and trade_date_val and trade_date_val != dt.date.today():
                df = ak.stock_zh_a_hist_min_em(
                    symbol=clean_code,
                    start_date=date_str,
                    end_date=date_str,
                    period="1",
                    adjust="",
                )
                rename_map = {
                    "时间": "time",
                    "开盘": "open",
                    "收盘": "price",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                    "成交额": "amount",
                    "day": "time",
                    "close": "price",
                }
                df = df.rename(columns=rename_map)
            else:
                # 2. 当日分时走东财接口
                if date_str:
                    df = ak.stock_intraday_em(symbol=clean_code, date=date_str)
                else:
                    df = ak.stock_intraday_em(symbol=clean_code)

                # 东财列名映射兼容
                # 可能返回: ["时间", "成交价", "手数", "买卖盘", "均价", ...]
                rename_map = {
                    "时间": "time",
                    "成交价": "price",
                    "手数": "volume",
                    "均价": "avg_price",
                    "day": "time",
                    "close": "price",
                    "avg": "avg_price",
                }
                df = df.rename(columns=rename_map)

        except Exception as e:
            logger.debug(f"东财分时数据拉取失败({code}): {e}，尝试新浪接口...")
            if date_str and trade_date_val and trade_date_val != dt.date.today():
                logger.debug("非当日分时：跳过新浪接口兜底。")
                return pd.DataFrame()
            try:
                # 3. 备用新浪接口
                df = ak.stock_zh_a_minute_sina(symbol=sina_symbol, period="1")
                df = df.rename(columns={"day": "time", "close": "price", "volume": "volume"})
            except Exception as e2:
                logger.warning(f"分时数据全部拉取失败({code}): {e2}")
                return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        # 数据清洗与类型转换
        if "time" in df.columns:
            # 处理时间格式，如果是 "10:30" 这种由 string 补全日期
            sample = str(df["time"].iloc[0])
            if ":" in sample and len(sample) <= 8:
                base_date = trade_date_val or dt.date.today()
                df["time"] = pd.to_datetime(base_date.strftime("%Y-%m-%d") + " " + df["time"])
            else:
                df["time"] = pd.to_datetime(df["time"])
        
        for col in ["price", "volume", "avg_price"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # 补算均价线 (如果缺失)
        if "avg_price" not in df.columns and "price" in df.columns and "volume" in df.columns:
            try:
                cum_vol = df["volume"].cumsum()
                cum_amt = (df["price"] * df["volume"]).cumsum()
                df["avg_price"] = cum_amt / cum_vol.replace(0, 1)
            except Exception:
                pass

        required_cols = ["time", "price", "volume", "avg_price"]
        final_cols = [c for c in required_cols if c in df.columns]
        
        return df[final_cols].sort_values("time") if "time" in df.columns else df

    def fetch_today_minute_data(self, code: str) -> pd.DataFrame:
        """拉取单只股票当天分时数据（包含时间、价格、成交量、均价线）。"""
        return self.fetch_minute_data(code, trade_date=dt.date.today())

    def get_lhb_detail(self, trade_date: str) -> pd.DataFrame:
        """获取指定交易日的龙虎榜详情。"""
        normalized_date = str(trade_date).replace("-", "")
        raw = ak.stock_lhb_detail_em(start_date=normalized_date, end_date=normalized_date)
        return self._ensure_df(raw)

    def get_margin_detail(self, trade_date: str, exchange: str) -> pd.DataFrame:
        """获取沪深交易所的融资融券明细。"""
        exchange_map = {
            "sse": ak.stock_margin_detail_sse,
            "sh": ak.stock_margin_detail_sse,
            "shanghai": ak.stock_margin_detail_sse,
            "szse": ak.stock_margin_detail_szse,
            "sz": ak.stock_margin_detail_szse,
            "shenzhen": ak.stock_margin_detail_szse,
        }
        key = str(exchange).lower()
        if key not in exchange_map:
            raise ValueError(f"不支持的交易所标识: {exchange}")

        fetch_fn = exchange_map[key]
        raw = fetch_fn(date=str(trade_date).replace("-", ""))
        return self._ensure_df(raw)

    def get_shareholder_count(self, period: str) -> pd.DataFrame:
        """获取全市场股东户数汇总。"""
        raw = ak.stock_zh_a_gdhs(symbol=period)
        return self._ensure_df(raw)

    def get_shareholder_count_detail(self, symbol: str) -> pd.DataFrame:
        """获取单只股票的股东户数明细。"""
        raw = ak.stock_zh_a_gdhs_detail_em(symbol=symbol)
        return self._ensure_df(raw)

    def batch_get_shareholder_count_detail(
        self, symbols: Iterable[str]
    ) -> list[pd.DataFrame]:
        """批量获取股东户数明细，过滤掉空结果；单个失败不影响整体。"""
        frames: list[pd.DataFrame] = []
        for symbol in symbols:
            try:
                df = self.get_shareholder_count_detail(symbol)
            except Exception:  # noqa: BLE001
                # 外部逻辑只关心“能拿到多少”，这里不抛异常，直接跳过
                continue

            if df is not None and not df.empty:
                frames.append(df)

        return frames

    def get_board_industry_spot(self) -> pd.DataFrame:
        """东方财富行业板块当日表现快照。"""

        raw = ak.stock_board_industry_name_em()
        return self._ensure_df(raw)

    def get_board_industry_hist(
        self,
        board_name: str,
        start_date: str,
        end_date: str,
        adjust: str = "hfq",
        period: str = "日k",
    ) -> pd.DataFrame:
        """东方财富行业板块历史行情。"""

        normalized_start = str(start_date).replace("-", "")
        normalized_end = str(end_date).replace("-", "")
        raw = ak.stock_board_industry_hist_em(
            symbol=board_name,
            period=period,
            start_date=normalized_start,
            end_date=normalized_end,
            adjust=adjust,
        )
        return self._ensure_df(raw)

    def get_board_industry_constituents(self, board_name: str) -> pd.DataFrame:
        """东方财富行业板块成份股列表。"""

        raw = ak.stock_board_industry_cons_em(symbol=board_name)
        return self._ensure_df(raw)
