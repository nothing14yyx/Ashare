"""基于 Baostock 的示例脚本入口."""

from __future__ import annotations

import datetime as dt
import logging
import multiprocessing as mp
import os
import time
from pathlib import Path
from typing import Callable, Iterable, Tuple

import pandas as pd
from sqlalchemy import bindparam, inspect, text

from .akshare_fetcher import AkshareDataFetcher
from .baostock_core import BaostockDataFetcher
from .baostock_session import BaostockSession
from .config import ProxyConfig, get_section
from .db import DatabaseConfig, MySQLWriter
from .external_signal_manager import ExternalSignalManager
from .fundamental_manager import FundamentalDataManager
from .universe import AshareUniverseBuilder
from .utils import setup_logger


_worker_session: BaostockSession | None = None
_worker_fetcher: BaostockDataFetcher | None = None


def _init_kline_worker() -> None:
    """在子进程中初始化并复用 Baostock 会话。"""

    global _worker_session, _worker_fetcher
    _worker_session = BaostockSession()
    _worker_session.connect()
    _worker_fetcher = BaostockDataFetcher(_worker_session)


def _reset_worker_session() -> None:
    """在子进程内重新建立 Baostock 会话。"""

    global _worker_session, _worker_fetcher
    _worker_session = BaostockSession()
    _worker_session.connect()
    _worker_fetcher = BaostockDataFetcher(_worker_session)


def _fetch_kline_with_retry(
    fetcher: BaostockDataFetcher,
    session: BaostockSession,
    code: str,
    start_date: str,
    end_date: str,
    freq: str,
    adjustflag: str,
    max_retries: int,
    reset_callback: Callable[[], None] | None = None,
    logger: logging.Logger | None = None,
) -> tuple[str, pd.DataFrame | str]:
    """统一的 K 线重试逻辑，可在主进程与子进程复用。"""

    def _calc_backoff(attempt_index: int) -> float:
        base = 1.0
        max_backoff = 15.0
        return min(max_backoff, base * (2 ** (attempt_index - 1)))

    session.ensure_alive()
    for attempt in range(1, max_retries + 1):
        try:
            df = fetcher.get_kline(
                code=code,
                start_date=start_date,
                end_date=end_date,
                freq=freq,
                adjustflag=adjustflag,
            )
            return "ok", df
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_retries:
                return "error", str(exc)

            if logger is not None:
                logger.warning("%s 第 %s/%s 次拉取失败：%s", code, attempt, max_retries, exc)
            time.sleep(_calc_backoff(attempt))
            try:
                message = str(exc)
                force_refresh = attempt > 1 or "10054" in message
                session.ensure_alive(force_refresh=force_refresh)
            except Exception:  # noqa: BLE001
                if reset_callback is not None:
                    reset_callback()
                else:
                    session.reconnect()

    return "error", "达到最大重试次数"


def _worker_fetch_kline(args: tuple[str, str, str, str, str, int]) -> tuple[str, str, pd.DataFrame | str]:
    """子进程中拉取单只股票 K 线，复用会话减少登录开销。"""

    global _worker_session, _worker_fetcher
    code, start_date, end_date, freq, adjustflag, max_retries = args
    if _worker_session is None or _worker_fetcher is None:
        raise RuntimeError("Baostock 会话未初始化。")

    status, payload = _fetch_kline_with_retry(
        fetcher=_worker_fetcher,
        session=_worker_session,
        code=code,
        start_date=start_date,
        end_date=end_date,
        freq=freq,
        adjustflag=adjustflag,
        max_retries=max_retries,
        reset_callback=_reset_worker_session,
    )

    return status, code, payload


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
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 日志改为写到项目根目录的 ashare.log，不再跟 output 绑在一起
        self.logger = setup_logger()

        baostock_cfg = get_section("baostock")
        self.baostock_per_code_timeout = self._read_int_from_env(
            "ASHARE_BAOSTOCK_PER_CODE_TIMEOUT",
            baostock_cfg.get("per_code_timeout", 30),
        )
        self.baostock_max_retries = self._read_int_from_env(
            "ASHARE_BAOSTOCK_MAX_RETRIES", baostock_cfg.get("max_retries", 2)
        )
        worker_default = baostock_cfg.get("worker_processes", 1)
        worker_min = baostock_cfg.get("min_worker_processes", 1)
        worker_max = baostock_cfg.get("max_worker_processes", mp.cpu_count())
        network_cap = baostock_cfg.get("network_worker_cap", worker_default)
        self.worker_processes_default = self._read_int_from_env(
            "ASHARE_BAOSTOCK_WORKERS", worker_default
        )
        self.worker_processes_min = self._read_int_from_env(
            "ASHARE_BAOSTOCK_MIN_WORKERS", worker_min
        )
        self.worker_processes_max = self._read_int_from_env(
            "ASHARE_BAOSTOCK_MAX_WORKERS", worker_max
        )
        self.network_worker_cap = self._read_int_from_env(
            "ASHARE_BAOSTOCK_NETWORK_WORKER_CAP", network_cap
        )
        self.progress_log_every = self._read_int_from_env(
            "ASHARE_PROGRESS_LOG_EVERY", baostock_cfg.get("progress_log_every", 100)
        )
        self.resume_min_rows_per_code = self._read_int_from_env(
            "ASHARE_RESUME_MIN_ROWS_PER_CODE",
            baostock_cfg.get("resume_min_rows_per_code", 200),
        )
        self.history_flush_batch = self._read_int_from_env(
            "ASHARE_HISTORY_FLUSH_BATCH", baostock_cfg.get("history_flush_batch", 5)
        )
        self.write_chunksize = self._read_int_from_env(
            "ASHARE_HISTORY_WRITE_CHUNKSIZE",
            baostock_cfg.get("history_write_chunksize", 500),
        )
        cleanup_mode_raw = os.getenv(
            "ASHARE_HISTORY_CLEANUP_MODE", baostock_cfg.get("history_cleanup_mode", "window")
        )
        cleanup_mode = str(cleanup_mode_raw).strip().lower()
        if cleanup_mode not in {"window", "skip"}:
            self.logger.warning(
                "history_cleanup_mode=%s 无效，已自动回退为 window 模式。",
                cleanup_mode_raw,
            )
            cleanup_mode = "window"
        self.history_cleanup_mode = cleanup_mode
        self.history_retention_days = self._read_int_from_env(
            "ASHARE_HISTORY_RETENTION_DAYS", baostock_cfg.get("history_retention_days", 0)
        )
        if self.history_cleanup_mode == "skip" and self.history_retention_days > 0:
            self.logger.info(
                "已启用历史数据保留窗口 %s 天，即便追加模式也会定期清理过期数据。",
                self.history_retention_days,
            )
        if self.baostock_max_retries < 1:
            self.baostock_max_retries = 1
        if self.baostock_per_code_timeout <= 0:
            self.baostock_per_code_timeout = 30
        if self.progress_log_every < 1:
            self.progress_log_every = 100

        # 从 config.yaml 读取基础面刷新开关
        app_cfg = get_section("app")
        refresh_flag = app_cfg.get("refresh_fundamentals", False)
        # 下面的数值型参数也从 config.yaml 读取默认值（允许环境变量覆盖）
        if isinstance(refresh_flag, str):
            refresh_flag = refresh_flag.strip().lower() in {
                "1",
                "true",
                "yes",
                "y",
                "on",
            }
        self.refresh_fundamentals: bool = bool(refresh_flag)

        # config.yaml 的默认值（如果写成字符串也要能解析）
        cfg_history_days = app_cfg.get("history_days", 30)
        cfg_top_liquidity = app_cfg.get("top_liquidity_count", 100)
        cfg_min_listing = app_cfg.get("min_listing_days", 60)
        try:
            cfg_history_days = int(cfg_history_days)
        except Exception:
            cfg_history_days = 30
        try:
            cfg_top_liquidity = int(cfg_top_liquidity)
        except Exception:
            cfg_top_liquidity = 100
        try:
            cfg_min_listing = int(cfg_min_listing)
        except Exception:
            cfg_min_listing = 60

        # 日线K线拉取开关（config.yaml: app.fetch_daily_kline）
        # 可用环境变量 ASHARE_FETCH_DAILY_KLINE 覆盖（优先级更高）
        kline_flag = os.getenv("ASHARE_FETCH_DAILY_KLINE")
        if kline_flag is None:
            kline_flag = app_cfg.get("fetch_daily_kline", True)
        if isinstance(kline_flag, str):
            kline_flag = kline_flag.strip().lower() in {
                "1",
                "true",
                "yes",
                "y",
                "on",
            }
        self.fetch_daily_kline: bool = bool(kline_flag)

        self.history_days = (
            history_days
            if history_days is not None
            else self._read_int_from_env("ASHARE_HISTORY_DAYS", cfg_history_days)
        )
        resolved_top_liquidity = (
            top_liquidity_count
            if top_liquidity_count is not None
            else self._read_int_from_env("ASHARE_TOP_LIQUIDITY_COUNT", cfg_top_liquidity)
        )
        resolved_min_listing_days = (
            min_listing_days
            if min_listing_days is not None
            else self._read_int_from_env("ASHARE_MIN_LISTING_DAYS", cfg_min_listing)
        )
        self.logger.info(
            "参数配置：history_days=%s, top_liquidity_count=%s, min_listing_days=%s, fetch_daily_kline=%s",
            self.history_days,
            resolved_top_liquidity,
            resolved_min_listing_days,
            self.fetch_daily_kline,
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

    def _choose_worker_processes(self, total_codes: int) -> int:
        """根据配置、CPU 与任务规模动态确定子进程数量。"""

        bounded_max = max(1, min(self.worker_processes_max, mp.cpu_count()))
        network_cap = self.network_worker_cap
        if network_cap <= 0:
            hard_cap = bounded_max
        else:
            hard_cap = max(1, min(network_cap, bounded_max))

        bounded_min = max(1, min(self.worker_processes_min, hard_cap))
        base_default = max(bounded_min, min(self.worker_processes_default, hard_cap))
        adaptive = min(base_default, hard_cap, total_codes or 1)
        if total_codes > 1000 and adaptive < min(hard_cap, bounded_max):
            adaptive = min(hard_cap, bounded_max, adaptive + 1)

        chosen = max(bounded_min, min(hard_cap, adaptive))
        if hard_cap == 1 and chosen != 1:
            chosen = 1
        if hard_cap == 1:
            self.logger.info("Baostock 并发已限制为单进程模式以降低封禁风险。")

        return chosen

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

    def _compute_resume_threshold(self, end_day: dt.date, window_days: int) -> int:
        trading_days = self._get_recent_trading_days(end_day, window_days)
        trading_count = max(1, len(trading_days))
        dynamic_threshold = int(trading_count * 0.8)
        if dynamic_threshold <= 0:
            dynamic_threshold = trading_count
        return max(1, min(self.resume_min_rows_per_code, dynamic_threshold))

    def _table_exists(self, table_name: str) -> bool:
        inspector = inspect(self.db_writer.engine)
        return inspector.has_table(table_name)

    def _load_completed_codes(self, table_name: str, min_rows: int) -> set[str]:
        query = text(
            "SELECT `code` FROM `{table}` GROUP BY `code` HAVING COUNT(*) >= :threshold".format(
                table=table_name
            )
        )
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(query, conn, params={"threshold": min_rows})
        except Exception:  # noqa: BLE001
            return set()

        codes = {str(code) for code in df.get("code", []) if pd.notna(code)}
        return codes

    def _flush_history_batch(
        self, batch: list[pd.DataFrame], table_name: str, if_exists: str = "append"
    ) -> None:
        if not batch:
            return

        combined = pd.concat(batch, ignore_index=True)
        if {"code", "date"}.issubset(combined.columns):
            combined = combined.drop_duplicates(subset=["code", "date"])

        codes = (
            combined.get("code", pd.Series(dtype=str))
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        min_date = None
        max_date = None
        if "date" in combined.columns:
            date_series = pd.to_datetime(combined["date"], errors="coerce")
            min_date_raw = date_series.min()
            max_date_raw = date_series.max()
            if pd.notna(min_date_raw) and pd.notna(max_date_raw):
                min_date = min_date_raw.date().isoformat()
                max_date = max_date_raw.date().isoformat()

        if codes and min_date and max_date:
            if self.history_cleanup_mode == "skip":
                self.logger.info("清理模式为 skip，直接追加 %s 条记录。", len(combined))
            elif not self._table_exists(table_name):
                self.logger.info("表 %s 不存在，跳过旧数据删除。", table_name)
            else:
                delete_stmt = (
                    text(
                        "DELETE FROM `{table}` WHERE `code` IN :codes AND `date` BETWEEN :start_date AND :end_date".format(
                            table=table_name
                        )
                    ).bindparams(bindparam("codes", expanding=True))
                )
                try:
                    with self.db_writer.engine.begin() as conn:
                        conn.execute(
                            delete_stmt,
                            {
                                "codes": codes,
                                "start_date": min_date,
                                "end_date": max_date,
                            },
                        )
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("删除旧数据失败，将直接追加：%s", exc)

        self.db_writer.write_dataframe(
            combined,
            table_name,
            if_exists=if_exists,
            chunksize=self.write_chunksize,
            method="multi",
        )

        self._log_memory_usage("历史批量写入")

    def _log_memory_usage(self, prefix: str) -> None:
        try:
            import resource

            usage = resource.getrusage(resource.RUSAGE_SELF)
            memory_mb = usage.ru_maxrss / 1024
            self.logger.debug("%s后内存峰值约 %.2f MB", prefix, memory_mb)
        except Exception:
            return

    def _purge_old_history(self, table_name: str, reference_date: str) -> None:
        if self.history_retention_days <= 0:
            return
        if not self._table_exists(table_name):
            return

        try:
            end_day = dt.datetime.strptime(reference_date, "%Y-%m-%d").date()
        except ValueError:
            return

        cutoff = (end_day - dt.timedelta(days=self.history_retention_days)).isoformat()
        purge_stmt = text(
            "DELETE FROM `{table}` WHERE `date` < :cutoff".format(table=table_name)
        )
        try:
            with self.db_writer.engine.begin() as conn:
                conn.execute(purge_stmt, {"cutoff": cutoff})
            self.logger.info(
                "已按照保留窗口 %s 天清理 %s 之前的历史日线。", self.history_retention_days, cutoff
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("历史保留窗口清理失败：%s", exc)

    def _write_failed_codes(self, failed_codes: list[str]) -> None:
        if not failed_codes:
            return

        failed_path = self.output_dir / "failed_codes.txt"
        existing: set[str] = set()
        if failed_path.exists():
            existing = {line.strip() for line in failed_path.read_text().splitlines() if line.strip()}

        merged = sorted(existing.union(failed_codes))
        failed_path.write_text("\n".join(merged))

    def _fetch_and_store_history(
        self,
        stock_df: pd.DataFrame,
        start_day: str,
        end_date: str,
        base_table: str,
        adjustflag: str = "1",
        resume_threshold: int | None = None,
    ) -> tuple[int, list[str], list[str], int]:
        history_frames: list[pd.DataFrame] = []
        success_count = 0
        empty_codes: list[str] = []
        failed_codes: list[str] = []
        skipped = 0
        done_codes: set[str] = set()
        if resume_threshold is not None and resume_threshold > 0:
            done_codes = self._load_completed_codes(base_table, resume_threshold)

        codes = [str(code) for code in stock_df.get("code", []) if pd.notna(code)]
        codes_to_fetch = [code for code in codes if code not in done_codes]
        skipped = len(codes) - len(codes_to_fetch)
        if not codes_to_fetch:
            self.logger.info("历史日线已经完成，跳过拉取。")
            return success_count, empty_codes, failed_codes, skipped

        ctx = mp.get_context("spawn")
        worker_processes = self._choose_worker_processes(len(codes_to_fetch))
        worker_args = [
            (code, start_day, end_date, "d", adjustflag, self.baostock_max_retries)
            for code in codes_to_fetch
        ]

        self.logger.info(
            "本次历史日线拉取将使用 %s 个并发子进程（候选 %s 支股票）。",
            worker_processes,
            len(codes_to_fetch),
        )

        def _handle_result(
            status: str, code_value: str, payload: pd.DataFrame | str
        ) -> None:
            nonlocal success_count

            if status == "ok" and isinstance(payload, pd.DataFrame):
                if payload.empty:
                    empty_codes.append(code_value)
                else:
                    history_frames.append(payload)
                    success_count += 1
            else:
                failed_codes.append(code_value)

        if worker_processes == 1:
            self.session.ensure_alive()
            for idx, code in enumerate(codes_to_fetch, start=1):
                status, payload = _fetch_kline_with_retry(
                    fetcher=self.fetcher,
                    session=self.session,
                    code=code,
                    start_date=start_day,
                    end_date=end_date,
                    freq="d",
                    adjustflag=adjustflag,
                    max_retries=self.baostock_max_retries,
                    reset_callback=self.session.reconnect,
                    logger=self.logger,
                )
                _handle_result(status, code, payload)

                if history_frames and (
                    len(history_frames) >= self.history_flush_batch
                    or idx == len(codes_to_fetch)
                ):
                    self._flush_history_batch(
                        history_frames, base_table, if_exists="append"
                    )
                    history_frames.clear()

                if idx % self.progress_log_every == 0 or idx == len(codes_to_fetch):
                    self.logger.info(
                        "已完成 %s/%s 支股票的拉取，最近处理 %s",
                        idx,
                        len(codes_to_fetch),
                        code,
                    )
        else:
            with ctx.Pool(
                processes=worker_processes,
                initializer=_init_kline_worker,
                maxtasksperchild=200,
            ) as pool:
                for idx, result in enumerate(
                    pool.imap_unordered(_worker_fetch_kline, worker_args), start=1
                ):
                    status, code, payload = result
                    _handle_result(status, code, payload)

                    if history_frames and (
                        len(history_frames) >= self.history_flush_batch
                        or idx == len(codes_to_fetch)
                    ):
                        self._flush_history_batch(
                            history_frames, base_table, if_exists="append"
                        )
                        history_frames.clear()

                    if idx % self.progress_log_every == 0 or idx == len(codes_to_fetch):
                        self.logger.info(
                            "已完成 %s/%s 支股票的拉取，最近处理 %s",
                            idx,
                            len(codes_to_fetch),
                            code,
                        )

        if history_frames:
            self._flush_history_batch(history_frames, base_table, if_exists="append")

        if failed_codes:
            self._write_failed_codes(failed_codes)

        self.logger.info(
            "拉取结束：成功 %s/%s，空数据 %s，失败 %s，跳过 %s",
            success_count,
            len(codes_to_fetch),
            len(empty_codes),
            len(failed_codes),
            skipped,
        )

        if empty_codes:
            self.logger.debug("完全未返回数据的股票：%s", ", ".join(sorted(empty_codes)))
        if failed_codes:
            self.logger.debug("请求失败的股票：%s", ", ".join(sorted(failed_codes)))

        self._purge_old_history(base_table, end_date)

        return success_count, empty_codes, failed_codes, skipped

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
        self,
        stock_df: pd.DataFrame,
        end_date: str,
        days: int = 30,
        base_table: str = "history_daily_kline",
    ) -> Tuple[pd.DataFrame, str]:
        if stock_df.empty or "code" not in stock_df.columns:
            raise RuntimeError("导出历史日线失败：股票列表为空或缺少 code 列。")

        end_day = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
        recent_trading_days = self._get_recent_trading_days(end_day, days)
        resume_threshold = self._compute_resume_threshold(end_day, days)
        start_day = recent_trading_days[0].isoformat()

        self.logger.info(
            "开始导出 %s 只股票的最近 %s 个交易日历史数据，窗口 [%s, %s]",
            len(stock_df),
            days,
            start_day,
            end_date,
        )

        success_count, empty_codes, failed_codes, skipped = self._fetch_and_store_history(
            stock_df,
            start_day=start_day,
            end_date=end_date,
            base_table=base_table,
            adjustflag="1",
            resume_threshold=resume_threshold,
        )

        if success_count == 0 and skipped == 0:
            raise RuntimeError("导出历史日线失败：全部股票均未返回数据。")

        recent_df = self._slice_recent_window(base_table, end_day, days)
        if not recent_df.empty:
            self.logger.info(
                "已写入表 %s，当前样本行数 %s，空数据 %s，失败 %s，跳过 %s",
                base_table,
                len(recent_df),
                len(empty_codes),
                len(failed_codes),
                skipped,
            )
        recent_table = f"history_recent_{days}_days"
        self._save_sample(recent_df, recent_table)
        return recent_df, recent_table

    def _slice_recent_window(
        self, base_table: str, end_day: dt.date, window_days: int
    ) -> pd.DataFrame:
        window_trading_days = self._get_recent_trading_days(end_day, window_days)
        window_start = window_trading_days[0].isoformat()
        query = text(
            "SELECT * FROM `{table}` WHERE `date` >= :window_start".format(
                table=base_table
            )
        )

        with self.db_writer.engine.begin() as conn:
            recent_df = pd.read_sql_query(query, conn, params={"window_start": window_start})

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
        recent_df = recent_df[recent_df["date"].dt.date.isin(window_day_set)].copy()

        if recent_df.empty:
            raise RuntimeError(
                "从表 {table} 中切出最近 {days} 天数据失败：结果为空。".format(
                    table=base_table,
                    days=window_days,
                )
            )

        return recent_df

    def _export_daily_history_incremental(
        self,
        stock_df: pd.DataFrame,
        end_date: str,
        base_table: str = "history_daily_kline",
        window_days: int = 30,
        fetch_enabled: bool = True,
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

        resume_threshold = self._compute_resume_threshold(end_day, window_days)
        done_codes: set[str] = set()

        # 开关关闭：禁止调用 Baostock 日线K线接口，仅从数据库表切片读取
        if not fetch_enabled:
            self.logger.info(
                "日线K线拉取开关已关闭：跳过 Baostock 拉取，仅从表 %s 切片读取最近 %s 个交易日数据。",
                base_table,
                window_days,
            )
            if last_date_value is None:
                raise RuntimeError(
                    f"日线K线拉取已关闭，但表 {base_table} 不存在或为空，无法从数据库读取。"
                )
            if last_date_value < end_day:
                self.logger.warning(
                    "日线K线拉取已关闭：表 %s 仅包含截至 %s 的数据（目标截止 %s），将按数据库最新日期继续。",
                    base_table,
                    last_date_value.isoformat(),
                    end_day.isoformat(),
                )
                end_day = last_date_value
                end_date = end_day.isoformat()

        if fetch_enabled and last_date_value is None:
            self.logger.info(
                "历史表 %s 不存在或为空，执行冷启动：拉取最近 %s 天日线。",
                base_table,
                window_days,
            )
            recent_df, _ = self._export_recent_daily_history(
                stock_df, end_date, days=window_days, base_table=base_table
            )
            recent_table = f"history_recent_{window_days}_days"
            self._save_sample(recent_df, recent_table)
            return recent_df, recent_table
        elif fetch_enabled and last_date_value >= end_day:
            done_codes = self._load_completed_codes(base_table, resume_threshold)
            if len(done_codes) < len(stock_df):
                self.logger.info(
                    "历史表 %s 已存在但未覆盖全部股票，继续补齐缺口。", base_table
                )
                recent_df, _ = self._export_recent_daily_history(
                    stock_df, end_date, days=window_days, base_table=base_table
                )
                recent_table = f"history_recent_{window_days}_days"
                self._save_sample(recent_df, recent_table)
                return recent_df, recent_table
            self.logger.info(
                "历史日线表 %s 已包含截至 %s 的数据，跳过增量拉取。",
                base_table,
                end_date,
            )
            recent_df = self._slice_recent_window(base_table, end_day, window_days)
            recent_table = f"history_recent_{window_days}_days"
            self._save_sample(recent_df, recent_table)
            return recent_df, recent_table
        elif fetch_enabled:
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

            if start_day is not None:
                self._fetch_and_store_history(
                    stock_df,
                    start_day=start_day,
                    end_date=end_date,
                    base_table=base_table,
                    adjustflag="1",
                    resume_threshold=None,
                )
            else:
                self.logger.info("本次没有任何新的日线数据可写入。")

        recent_df = self._slice_recent_window(base_table, end_day, window_days)
        recent_table = f"history_recent_{window_days}_days"
        self._save_sample(recent_df, recent_table)
        return recent_df, recent_table

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
                    fetch_enabled=self.fetch_daily_kline,
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
