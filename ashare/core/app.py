"""基于 Baostock 的示例脚本入口."""

from __future__ import annotations

import contextlib
import datetime as dt
import logging
import re
import multiprocessing as mp
import os
import time
from pathlib import Path
from typing import Callable, Iterable, Tuple

import pandas as pd
from sqlalchemy import bindparam, inspect, text

from ashare.core.app_services import (
    DataIngestService,
    FundamentalService,
    HistoryKlineService,
    UniverseService,
)
from ashare.data.akshare_fetcher import AkshareDataFetcher
from ashare.data.baostock_core import ADJUSTFLAG_NONE, BaostockDataFetcher
from ashare.data.baostock_session import BaostockSession
from ashare.core.config import ProxyConfig, get_section
from ashare.core.db import DatabaseConfig, MySQLWriter
from ashare.data.external_signal_manager import ExternalSignalManager
from ashare.data.fundamental_manager import FundamentalDataManager
from ashare.core.schema_manager import SchemaManager
from ashare.data.universe import AshareUniverseBuilder
from ashare.utils import setup_logger


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
    if _worker_session is not None:
        with contextlib.suppress(Exception):
            _worker_session.logout()
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

    # 关键优化：不要对“每只股票”都 force_check 探活。
    # force_check=True 会触发一次额外的网络探针（_probe_alive -> query_sz50_stocks），
    # 全市场拉取会把请求量翻倍，显著拖慢速度；并发时更容易触发超时/限流。
    # 这里仅保证已登录即可；会话健康检查与自愈交给 BaostockDataFetcher/_call_with_retry 的节流逻辑。
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
                session.ensure_alive(
                    force_refresh=force_refresh, force_check=not force_refresh
                )
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
        cfg_history_view_days = app_cfg.get("history_view_days", 0)
        cfg_top_liquidity = app_cfg.get("top_liquidity_count", 100)
        cfg_min_listing = app_cfg.get("min_listing_days", 60)
        try:
            cfg_history_days = int(cfg_history_days)
        except Exception:
            cfg_history_days = 30
        try:
            cfg_history_view_days = int(cfg_history_view_days)
        except Exception:
            cfg_history_view_days = 0
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

        meta_flag = os.getenv("ASHARE_FETCH_STOCK_META")
        if meta_flag is None:
            meta_flag = app_cfg.get("fetch_stock_meta", True)
        if isinstance(meta_flag, str):
            meta_flag = meta_flag.strip().lower() in {
                "1",
                "true",
                "yes",
                "y",
                "on",
            }
        self.fetch_stock_meta: bool = bool(meta_flag)

        index_flag = os.getenv("ASHARE_FETCH_INDEX_KLINE")
        if index_flag is None:
            index_flag = app_cfg.get("fetch_index_kline", True)
        if isinstance(index_flag, str):
            index_flag = index_flag.strip().lower() in {
                "1",
                "true",
                "yes",
                "y",
                "on",
            }
        self.fetch_index_kline: bool = bool(index_flag)

        build_dim_flag = os.getenv("ASHARE_BUILD_STOCK_INDUSTRY_DIM")
        if build_dim_flag is None:
            build_dim_flag = app_cfg.get("build_stock_industry_dim", True)
        if isinstance(build_dim_flag, str):
            build_dim_flag = build_dim_flag.strip().lower() in {
                "1",
                "true",
                "yes",
                "y",
                "on",
            }
        self.build_stock_industry_dim: bool = bool(build_dim_flag)

        self.history_days = (
            history_days
            if history_days is not None
            else self._read_int_from_env("ASHARE_HISTORY_DAYS", cfg_history_days)
        )
        # 单独控制“近期自然日视图”窗口（便于你查近期数据）
        self.history_view_days = self._read_int_from_env(
            "ASHARE_HISTORY_VIEW_DAYS", cfg_history_view_days
        )
        if self.history_view_days < 0:
            self.history_view_days = 0

        index_codes_cfg = app_cfg.get("index_codes", [])
        if not isinstance(index_codes_cfg, (list, tuple)):
            index_codes_cfg = []
        self.index_codes = [str(c).strip() for c in index_codes_cfg if str(c).strip()]
        self.index_history_days = self._read_int_from_env(
            "ASHARE_INDEX_HISTORY_DAYS", app_cfg.get("index_history_days", 400)
        )
        if self.index_history_days <= 0:
            self.index_history_days = 400
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
            "参数配置：history_days=%s, history_view_days=%s, top_liquidity_count=%s, min_listing_days=%s, fetch_daily_kline=%s",
            self.history_days,
            self.history_view_days,
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
        board_cfg = akshare_cfg.get("board_industry", {}) if isinstance(akshare_cfg, dict) else {}
        if not isinstance(board_cfg, dict):
            board_cfg = {}
        self.board_industry_enabled = bool(board_cfg.get("enabled", False))
        self.board_spot_enabled = bool(board_cfg.get("spot_enabled", True))
        self.board_hist_enabled = bool(board_cfg.get("hist_enabled", False))
        self.board_constituent_enabled = bool(board_cfg.get("build_stock_board_dim", True))
        self.board_history_days = self._read_int_from_env(
            "ASHARE_BOARD_HISTORY_DAYS", board_cfg.get("history_days", 200)
        )
        self.board_adjust = str(board_cfg.get("adjust", "hfq") or "hfq")
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

        self.use_baostock: bool = bool(
            self.fetch_daily_kline
            or self.fetch_stock_meta
            or self.refresh_fundamentals
            or self.fetch_index_kline
        )

        # 登录一次会话，后续流程保持复用，减少频繁连接开销
        if self.use_baostock:
            self.session.ensure_alive()
            self.logger.info("Baostock 会话已建立，后续任务将持续复用当前连接。")
        else:
            self.logger.info("已关闭所有 Baostock 拉取开关，本次将以数据库为准执行离线模式。")

    def _save_sample(self, df: pd.DataFrame, table_name: str) -> str:
        self.db_writer.write_dataframe(df, table_name)
        return table_name

    def _create_recent_history_calendar_view(
        self,
        base_table: str,
        window_days: int,
        end_day: dt.date,
        view_name: str | None = None,
    ) -> str:
        """创建一个“最近 N 个自然日”的便捷视图，用于手动查近期数据。"""

        def _is_safe_ident(name: str) -> bool:
            return bool(re.fullmatch(r"[A-Za-z0-9_]+", name))

        view = view_name or f"history_recent_{window_days}_days"
        sanitized_days = max(1, int(window_days))
        if not _is_safe_ident(view) or not _is_safe_ident(base_table):
            raise RuntimeError(f"非法表/视图名：base_table={base_table}, view={view}")

        start_date = (end_day - dt.timedelta(days=sanitized_days - 1)).isoformat()
        end_exclusive = (end_day + dt.timedelta(days=1)).isoformat()

        drop_view_sql = text(f"DROP VIEW IF EXISTS `{view}`")
        drop_table_sql = text(f"DROP TABLE IF EXISTS `{view}`")
        create_view_sql = text(
            """
            CREATE OR REPLACE VIEW `{view}` AS
            SELECT *
            FROM `{base}`
            WHERE `date` >= '{start}'
              AND `date` < '{end_exclusive}'
            """.format(
                view=view,
                base=base_table,
                start=start_date,
                end_exclusive=end_exclusive,
            )
        )

        with self.db_writer.engine.begin() as conn:
            conn.execute(drop_view_sql)
            conn.execute(drop_table_sql)
            conn.execute(create_view_sql)

        self.logger.info(
            "已创建近期自然日视图 %s：[%s, %s)，共 %s 天。",
            view,
            start_date,
            end_exclusive,
            sanitized_days,
        )
        return view

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

    def _probe_daily_kline_availability(
        self, target_date: str, sample_codes: Iterable[str] | None = None
    ) -> bool:
        """探测指定交易日的日线是否已生成，避免无效的全量拉取。"""

        samples = list(sample_codes or ["sh.600000", "sz.000001", "sh.000001"])
        normalized = [self._normalize_stock_code(code) for code in samples if code]
        if not normalized:
            return True

        has_error = False
        self.session.ensure_alive(force_check=True)

        for code in normalized:
            try:
                df = self.fetcher.get_kline(
                    code=code,
                    start_date=target_date,
                    end_date=target_date,
                    freq="d",
                    adjustflag=ADJUSTFLAG_NONE,
                )
            except Exception as exc:  # noqa: BLE001
                has_error = True
                self.logger.debug("日线可用性探针异常（%s）：%s", code, exc)
                continue

            if df.empty:
                self.logger.debug("日线可用性探针：%s %s 返回空。", code, target_date)
                continue

            self.logger.debug("日线可用性探针：%s %s 已返回数据。", code, target_date)
            return True

        if has_error:
            self.logger.warning("日线可用性探针出现异常，继续尝试全量拉取。")
            return True

        return False

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

    def _get_recent_trading_days(
        self,
        end_day: dt.date,
        days: int,
        base_table: str = "history_daily_kline",
    ) -> list[dt.date]:
        if not self.use_baostock:
            return self._get_recent_trading_days_from_db(end_day, days, base_table)

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

    def _get_recent_trading_days_from_db(
        self, end_day: dt.date, days: int, base_table: str
    ) -> list[dt.date]:
        if not self._table_exists(base_table):
            raise RuntimeError(
                f"离线模式下未找到基础行情表 {base_table}，无法推导交易日。"
            )

        max_days = max(1, days)
        end_date = end_day.isoformat()
        query = text(
            """
            SELECT DISTINCT `date`
            FROM `{table}`
            WHERE `date` <= :end_date
            ORDER BY `date` DESC
            LIMIT {limit}
            """.format(table=base_table, limit=max_days * 2)
        )

        with self.db_writer.engine.begin() as conn:
            df = pd.read_sql_query(query, conn, params={"end_date": end_date})

        if df.empty:
            raise RuntimeError(
                f"离线模式下未能从 {base_table} 推导交易日，查询结果为空。"
            )

        parsed = (
            pd.to_datetime(df["date"], errors="coerce")
            .dropna()
            .dt.date.unique()
            .tolist()
        )
        parsed.sort()

        if len(parsed) < days:
            raise RuntimeError(
                f"离线模式下 {base_table} 仅包含 {len(parsed)} 个有效交易日，"
                f"不足以切出最近 {days} 天窗口。"
            )

        return parsed[-days:]

    def _compute_resume_threshold(
        self,
        end_day: dt.date,
        window_days: int,
        base_table: str = "history_daily_kline",
    ) -> int:
        trading_days = self._get_recent_trading_days(
            end_day, window_days, base_table=base_table
        )
        trading_count = max(1, len(trading_days))
        dynamic_threshold = int(trading_count * 0.8)
        if dynamic_threshold <= 0:
            dynamic_threshold = trading_count
        return max(1, min(self.resume_min_rows_per_code, dynamic_threshold))

    def _table_exists(self, table_name: str) -> bool:
        try:
            inspector = inspect(self.db_writer.engine)
            return inspector.has_table(table_name)
        except Exception:  # noqa: BLE001
            return False

    def _column_exists(self, table_name: str, column: str) -> bool:
        if not table_name or not column:
            return False
        stmt = text(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table AND column_name = :column
            """
        )
        try:
            with self.db_writer.engine.begin() as conn:
                row = conn.execute(
                    stmt,
                    {
                        "schema": self.db_writer.config.db_name,
                        "table": table_name,
                        "column": column,
                    },
                ).mappings().first()
        except Exception:  # noqa: BLE001
            return False
        return bool(row and row.get("cnt"))

    def _load_table(self, table_name: str) -> pd.DataFrame:
        """从数据库读取整表；表不存在则返回空 DataFrame。"""

        if not self._table_exists(table_name):
            return pd.DataFrame()

        query = text("SELECT * FROM `{table}`".format(table=table_name))
        with self.db_writer.engine.begin() as conn:
            return pd.read_sql_query(query, conn)

    def _infer_latest_trade_day_from_db(self, base_table: str) -> str:
        """在离线模式下，从历史表推断最近交易日。"""

        if not self._table_exists(base_table):
            return dt.date.today().isoformat()

        query = text(
            "SELECT MAX(`date`) AS max_date FROM `{table}`".format(table=base_table)
        )
        with self.db_writer.engine.begin() as conn:
            df = pd.read_sql_query(query, conn)

        if df.empty or "max_date" not in df.columns:
            return dt.date.today().isoformat()

        max_date = df.loc[0, "max_date"]
        if pd.isna(max_date):
            return dt.date.today().isoformat()

        parsed = pd.to_datetime(max_date, errors="coerce")
        if pd.isna(parsed):
            return dt.date.today().isoformat()

        return parsed.date().isoformat()

    def _build_stock_list_from_history(
        self, base_table: str, trade_day: str
    ) -> pd.DataFrame:
        """在没有 a_share_stock_list 的情况下，用历史行情表兜底生成股票列表。"""

        if not self._table_exists(base_table):
            return pd.DataFrame()

        query = text(
            "SELECT DISTINCT `code` FROM `{table}` WHERE `date` = :trade_day".format(
                table=base_table
            )
        )
        with self.db_writer.engine.begin() as conn:
            df = pd.read_sql_query(query, conn, params={"trade_day": trade_day})

        if df.empty:
            query_all = text(
                "SELECT DISTINCT `code` FROM `{table}`".format(table=base_table)
            )
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(query_all, conn)

        if df.empty:
            return df

        df["code"] = df["code"].astype(str)
        if "code_name" not in df.columns:
            df["code_name"] = ""
        if "tradeStatus" not in df.columns:
            df["tradeStatus"] = "1"
        return df[["code", "code_name", "tradeStatus"]]

    def _load_index_membership_from_db(self) -> dict[str, set[str]]:
        """从数据库读取指数成分股表，构造 {index_name: set(code)}。"""

        index_membership: dict[str, set[str]] = {}
        for index_name in ("hs300", "zz500", "sz50"):
            table = f"index_{index_name}_members"
            df = self._load_table(table)
            if df.empty or "code" not in df.columns:
                continue
            index_membership[index_name] = set(df["code"].dropna().astype(str))
        return index_membership

    def _load_completed_codes(
        self, table_name: str, min_rows: int, required_end_date: str | None = None
    ) -> set[str]:
        having_conditions = ["COUNT(*) >= :threshold"]
        params: dict[str, object] = {"threshold": min_rows}
        if required_end_date:
            having_conditions.append("MAX(`date`) >= :required_end_date")
            params["required_end_date"] = required_end_date

        having_clause = " AND ".join(having_conditions)
        query = text(
            "SELECT `code` FROM `{table}` GROUP BY `code` HAVING {clause}".format(
                table=table_name, clause=having_clause
            )
        )
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(query, conn, params=params)
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
            combined["trade_date"] = pd.to_datetime(
                combined["date"], errors="coerce"
            ).dt.date

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
                date_column = (
                    "trade_date" if self._column_exists(table_name, "trade_date") else "date"
                )
                delete_stmt = (
                    text(
                        "DELETE FROM `{table}` WHERE `code` IN :codes AND `{date_col}` BETWEEN :start_date AND :end_date".format(
                            table=table_name,
                            date_col=date_column,
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

    def _cleanup_session(self) -> None:
        """统一释放 Baostock 会话，避免遗留连接。"""

        try:
            self.session.logout()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Baostock 会话登出时发生异常：%s", exc)

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
        date_column = (
            "trade_date" if self._column_exists(table_name, "trade_date") else "date"
        )
        purge_stmt = text(
            "DELETE FROM `{table}` WHERE `{date_col}` < :cutoff".format(
                table=table_name,
                date_col=date_column,
            )
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
        adjustflag: str = ADJUSTFLAG_NONE,
        resume_threshold: int | None = None,
        probe_date: str | None = None,
        latest_existing_date: str | None = None,
    ) -> tuple[int, list[str], list[str], int]:
        history_frames: list[pd.DataFrame] = []
        success_count = 0
        empty_codes: list[str] = []
        failed_codes: list[str] = []
        skipped = 0
        done_codes: set[str] = set()
        if resume_threshold is not None and resume_threshold > 0:
            done_codes = self._load_completed_codes(
                base_table, resume_threshold, required_end_date=end_date
            )

        codes = [str(code) for code in stock_df.get("code", []) if pd.notna(code)]
        codes_to_fetch = [code for code in codes if code not in done_codes]
        total_attempted = len(codes_to_fetch)
        skipped = len(codes) - len(codes_to_fetch)
        if not codes_to_fetch:
            self.logger.info("历史日线已经完成，跳过拉取。")
            return success_count, empty_codes, failed_codes, skipped

        if (
            probe_date
            and start_day == end_date
            and not self._probe_daily_kline_availability(probe_date)
        ):
            db_hint = (
                f"；数据库仍截至 {latest_existing_date}" if latest_existing_date else ""
            )
            self.logger.info(
                "Baostock 尚未更新到 %s（日线返回全空），本次跳过增量拉取%s",
                probe_date,
                db_hint or "。",
            )
            return success_count, empty_codes, failed_codes, skipped + total_attempted

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
        # feat: 按累计行数阈值批量写库，减少频繁 flush 导致的 I/O 开销
        flush_rows_target = max(5000, int(self.write_chunksize) * 10)
        flush_rows_target = min(flush_rows_target, 20000)
        flush_frames_cap = max(self.history_flush_batch, 50)
        buffer_rows = 0


        def _handle_result(
            status: str, code_value: str, payload: pd.DataFrame | str
        ) -> None:
            nonlocal success_count
            nonlocal buffer_rows

            if status == "ok" and isinstance(payload, pd.DataFrame):
                if payload.empty:
                    empty_codes.append(code_value)
                else:
                    history_frames.append(payload)
                    buffer_rows += len(payload)
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
                    buffer_rows >= flush_rows_target
                    or len(history_frames) >= flush_frames_cap
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
                        buffer_rows >= flush_rows_target
                        or len(history_frames) >= flush_frames_cap
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

        all_empty = (
            success_count == 0
            and len(empty_codes) == total_attempted
            and not failed_codes
            and total_attempted > 0
        )

        if all_empty:
            db_hint = (
                f"；数据库仍截至 {latest_existing_date}" if latest_existing_date else ""
            )
            self.logger.info(
                "Baostock 尚未更新到 %s（日线返回全空），本次跳过增量拉取%s",
                end_date,
                db_hint or "。",
            )
        else:
            self.logger.info(
                "拉取结束：成功 %s/%s，空数据 %s，失败 %s，跳过 %s",
                success_count,
                total_attempted,
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
        if self.build_stock_industry_dim:
            self._save_stock_industry_dimension(industry_df)
        return industry_df

    def _save_stock_industry_dimension(self, industry_df: pd.DataFrame) -> None:
        if industry_df.empty:
            return

        dim = industry_df.copy()
        rename_map = {
            "updateDate": "update_date",
            "code_name": "code_name",
            "industry": "industry",
            "industryClassification": "industry_classification",
        }
        dim = dim.rename(columns=rename_map)
        for col in ["update_date"]:
            if col in dim.columns:
                dim[col] = pd.to_datetime(dim[col], errors="coerce")
        dim["source"] = "baostock"
        dim["updated_at"] = dt.datetime.now()

        cols = [
            "update_date",
            "code",
            "code_name",
            "industry",
            "industry_classification",
            "source",
            "updated_at",
        ]
        dim = dim[[c for c in cols if c in dim.columns]]

        with self.db_writer.engine.begin() as conn:
            dim.to_sql(
                "dim_stock_industry",
                conn,
                if_exists="replace",
                index=False,
                chunksize=self.write_chunksize,
            )
        self.logger.info("股票-行业维表 dim_stock_industry 已刷新，共 %s 条", len(dim))

    def _export_index_daily_history(self, end_date: str) -> pd.DataFrame:
        if not self.fetch_index_kline:
            self.logger.info("已关闭指数日线拉取，跳过 history_index_daily_kline。")
            return pd.DataFrame()
        if not self.index_codes:
            self.logger.warning("index_codes 为空，跳过指数日线拉取。")
            return pd.DataFrame()

        end_day = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
        start_day = end_day - dt.timedelta(days=max(1, int(self.index_history_days)))
        frames: list[pd.DataFrame] = []
        for code in self.index_codes:
            try:
                df = self.fetcher.get_kline(
                    code=code,
                    start_date=start_day.isoformat(),
                    end_date=end_day.isoformat(),
                    freq="d",
                    adjustflag=ADJUSTFLAG_NONE,
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("拉取指数 %s 失败：%s", code, exc)
                continue

            if df.empty:
                continue

            df["source"] = "baostock"
            df["created_at"] = dt.datetime.now()
            frames.append(df)

        if not frames:
            self.logger.warning("指数日线拉取结果为空。")
            return pd.DataFrame()

        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(subset=["date", "code"], keep="last")
        if "date" in merged.columns:
            merged["trade_date"] = pd.to_datetime(merged["date"], errors="coerce").dt.date
        with self.db_writer.engine.begin() as conn:
            if self._table_exists("history_index_daily_kline"):
                date_column = (
                    "trade_date"
                    if self._column_exists("history_index_daily_kline", "trade_date")
                    else "date"
                )
                conn.execute(
                    text(
                        "DELETE FROM history_index_daily_kline WHERE `{date_col}` >= :start_date AND `{date_col}` <= :end_date".format(
                            date_col=date_column
                        )
                    ),
                    {"start_date": start_day.isoformat(), "end_date": end_day.isoformat()},
                )
            merged.to_sql(
                "history_index_daily_kline",
                conn,
                if_exists="append",
                index=False,
                chunksize=self.write_chunksize,
            )
        self.logger.info(
            "指数日线已写入 history_index_daily_kline：%s 条（窗口 %s - %s）",
            len(merged),
            start_day,
            end_day,
        )
        return merged

    @staticmethod
    def _normalize_stock_code(symbol: str) -> str:
        code = str(symbol).strip()
        if not code:
            return code
        if code.startswith(("sh.", "sz.")):
            return code
        if code.startswith("6"):
            return f"sh.{code}"
        if code.startswith(("0", "3")):
            return f"sz.{code}"
        return code

    def _export_board_industry_spot(self) -> pd.DataFrame:
        if not (self.akshare_enabled and self.board_industry_enabled and self.board_spot_enabled):
            self.logger.info("板块快照拉取未开启，跳过 board_industry_spot。")
            return pd.DataFrame()

        try:
            fetcher = AkshareDataFetcher()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("AkShare 初始化失败，无法拉取板块快照：%s", exc)
            return pd.DataFrame()

        try:
            spot = fetcher.get_board_industry_spot()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("拉取行业板块快照失败：%s", exc)
            return pd.DataFrame()

        if spot.empty:
            self.logger.warning("行业板块快照为空。")
            return pd.DataFrame()

        normalized = spot.copy()
        rename_map = {}
        for col in normalized.columns:
            if "代码" in col:
                rename_map[col] = "board_code"
            if "板块" in col and "名称" in col:
                rename_map[col] = "board_name"
            if col in {"涨跌幅", "涨跌幅(%)", "change_rate", "pct_chg"}:
                rename_map[col] = "chg_pct"
            if col in {"成交额", "amount"}:
                rename_map[col] = "amount"
            if "领涨" in col and "涨跌幅" in col:
                rename_map[col] = "leader_pct"
            elif "领涨" in col:
                rename_map[col] = "leader_stock"

        normalized = normalized.rename(columns=rename_map)
        if "board_code" in normalized.columns:
            normalized["board_code"] = normalized["board_code"].astype(str)
        if "chg_pct" in normalized.columns:
            normalized["chg_pct"] = pd.to_numeric(normalized["chg_pct"], errors="coerce")
        normalized["ts"] = dt.datetime.now()

        with self.db_writer.engine.begin() as conn:
            normalized.to_sql(
                "board_industry_spot",
                conn,
                if_exists="append",
                index=False,
                chunksize=self.write_chunksize,
            )
        self.logger.info("行业板块快照已写入 board_industry_spot：%s 条", len(normalized))
        return normalized

    def _export_board_industry_constituents(
        self, spot: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        if not (
            self.akshare_enabled
            and self.board_industry_enabled
            and self.board_constituent_enabled
            and self.board_spot_enabled
        ):
            return pd.DataFrame()

        if spot is None or spot.empty:
            spot = self._export_board_industry_spot()

        if spot.empty:
            self.logger.info("缺少板块快照，跳过成份股维表构建。")
            return pd.DataFrame()

        try:
            fetcher = AkshareDataFetcher()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("AkShare 初始化失败，无法拉取板块成份股：%s", exc)
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        for _, row in spot.iterrows():
            board_name = str(row.get("board_name") or "").strip()
            board_code = str(row.get("board_code") or "").strip()
            if not board_name:
                continue
            try:
                cons = fetcher.get_board_industry_constituents(board_name)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("拉取板块 %s 成份股失败：%s", board_name, exc)
                continue

            if cons.empty:
                continue

            cons = cons.copy()
            rename_map = {}
            for col in cons.columns:
                if col in {"代码", "股票代码", "symbol"}:
                    rename_map[col] = "code"
                elif col in {"名称", "股票名称", "股票简称"}:
                    rename_map[col] = "code_name"
            cons = cons.rename(columns=rename_map)
            if "code" not in cons.columns:
                continue
            cons["code"] = cons["code"].astype(str).apply(self._normalize_stock_code)
            cons["board_name"] = board_name
            cons["board_code"] = board_code
            cons["source"] = "akshare"
            cons["updated_at"] = dt.datetime.now()
            frames.append(cons[["code", "board_name", "board_code", "source", "updated_at"]])

        if not frames:
            self.logger.info("未获取到任何板块成份股数据。")
            return pd.DataFrame()

        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(subset=["code", "board_name"], keep="last")
        merged = merged.sort_values(by=["code", "board_name"])
        merged = merged.dropna(subset=["code", "board_name"])
        merged["code"] = merged["code"].astype(str)

        with self.db_writer.engine.begin() as conn:
            merged.to_sql(
                "dim_stock_board_industry",
                conn,
                if_exists="replace",
                index=False,
                chunksize=self.write_chunksize,
            )
        self.logger.info("板块成份股维表已写入 dim_stock_board_industry：%s 条", len(merged))
        return merged

    def _export_board_industry_history(
        self, end_date: str, spot: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        if not (self.akshare_enabled and self.board_industry_enabled and self.board_hist_enabled):
            self.logger.info("板块历史拉取未开启，跳过 board_industry_hist_daily。")
            return pd.DataFrame()

        try:
            fetcher = AkshareDataFetcher()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("AkShare 初始化失败，无法拉取板块历史：%s", exc)
            return pd.DataFrame()

        if spot is None or spot.empty:
            spot = self._export_board_industry_spot()
        if spot.empty:
            self.logger.warning("没有有效的板块列表，跳过历史行情拉取。")
            return pd.DataFrame()

        start_day = (
            dt.datetime.strptime(end_date, "%Y-%m-%d").date()
            - dt.timedelta(days=max(1, int(self.board_history_days)))
        )
        boards: pd.Series | None = None
        if "board_name" in spot.columns:
            boards = spot["board_name"]
        elif "板块名称" in spot.columns:
            boards = spot["板块名称"]
        if boards is None:
            self.logger.warning("板块快照中缺少 board_name 列，跳过历史拉取。")
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        for name in boards.dropna().unique():
            try:
                hist = fetcher.get_board_industry_hist(
                    board_name=str(name),
                    start_date=start_day.isoformat(),
                    end_date=end_date,
                    adjust=self.board_adjust,
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("拉取板块 %s 历史失败：%s", name, exc)
                continue

            if hist.empty:
                continue

            hist = hist.copy()
            hist["board_name"] = name
            hist["source"] = "akshare"
            hist["created_at"] = dt.datetime.now()
            frames.append(hist)

        if not frames:
            self.logger.warning("板块历史拉取结果为空。")
            return pd.DataFrame()

        merged = pd.concat(frames, ignore_index=True)
        merged = merged.rename(columns={"日期": "date"}) if "日期" in merged.columns else merged
        merged = merged.drop_duplicates(subset=["date", "board_name"], keep="last")
        with self.db_writer.engine.begin() as conn:
            if self._table_exists("board_industry_hist_daily"):
                conn.execute(
                    text(
                        "DELETE FROM board_industry_hist_daily WHERE `date` >= :start_date AND `date` <= :end_date"
                    ),
                    {"start_date": start_day.isoformat(), "end_date": end_date},
                )
            merged.to_sql(
                "board_industry_hist_daily",
                conn,
                if_exists="append",
                index=False,
                chunksize=self.write_chunksize,
            )
        self.logger.info(
            "板块历史行情已写入 board_industry_hist_daily：%s 条（窗口 %s - %s）",
            len(merged),
            start_day,
            end_date,
        )
        return merged

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
        recent_trading_days = self._get_recent_trading_days(
            end_day, days, base_table=base_table
        )
        resume_threshold = self._compute_resume_threshold(
            end_day, days, base_table=base_table
        )
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
            adjustflag=ADJUSTFLAG_NONE,
            resume_threshold=resume_threshold,
        )

        if success_count == 0 and skipped == 0:
            raise RuntimeError("导出历史日线失败：全部股票均未返回数据。")

        recent_df, recent_table = self._slice_recent_window(base_table, end_day, days)
        if not recent_df.empty:
            self.logger.info(
                "已读取最近 %s 个交易日窗口（base=%s），当前样本行数 %s，空数据 %s，失败 %s，跳过 %s",
                days,
                recent_table,
                len(recent_df),
                len(empty_codes),
                len(failed_codes),
                skipped,
            )
        return recent_df, recent_table

    def _slice_recent_window(
        self,
        base_table: str,
        end_day: dt.date,
        window_days: int,
        view_name: str | None = None,
    ) -> Tuple[pd.DataFrame, str]:
        """从基础表切片读取最近 window_days 个交易日的数据（不再创建 history_recent_xxx_days 视图）。"""

        sanitized_days = max(1, int(window_days))

        # 防注入：表名仅允许字母数字下划线
        if not base_table or any((not (ch.isalnum() or ch == "_")) for ch in base_table):
            raise RuntimeError(f"非法表名：{base_table}")

        # 左闭右开：兼容 date 字段为 TEXT 且可能包含时间的情况
        end_exclusive = (end_day + dt.timedelta(days=1)).isoformat()

        start_date_sql = text(
            """
            SELECT MIN(`date`) AS start_date
            FROM (
                SELECT DISTINCT `date`
                FROM `{base}`
                WHERE `date` < :end_exclusive
                ORDER BY `date` DESC
                LIMIT :window_days
            ) AS d
            """.format(base=base_table)
        )
        slice_sql = text(
            """
            SELECT *
            FROM `{base}`
            WHERE `date` >= :start_date
              AND `date` < :end_exclusive
            ORDER BY `code`, `date`
            """.format(base=base_table)
        )

        with self.db_writer.engine.begin() as conn:
            row = conn.execute(
                start_date_sql,
                {"end_exclusive": end_exclusive, "window_days": sanitized_days},
            ).mappings().first()
            start_date = (row or {}).get("start_date")
            if start_date is None:
                raise RuntimeError(
                    f"从表 {base_table} 解析最近 {sanitized_days} 个交易日失败：start_date 为空（表为空或 date 异常）。"
                )
            recent_df = pd.read_sql_query(
                slice_sql,
                conn,
                params={"start_date": str(start_date), "end_exclusive": end_exclusive},
            )

        if recent_df.empty:
            raise RuntimeError(
                f"从表 {base_table} 切片读取最近 {sanitized_days} 个交易日数据失败：结果为空。"
            )

        if "date" not in recent_df.columns:
            raise RuntimeError(
                f"表 {base_table} 缺少 date 列，无法进行时间窗口切片。"
            )

        recent_df["date"] = pd.to_datetime(recent_df["date"])
        self.logger.debug(
            "已从表 %s 切片读取最近 %s 个交易日数据（start=%s, end<%s），不再创建 history_recent_%s_days 视图。",
            base_table,
            sanitized_days,
            str(start_date),
            end_exclusive,
            sanitized_days,
        )
        return recent_df, base_table


    def _refresh_history_calendar_view(
        self,
        base_table: str,
        end_day: dt.date,
        slice_window_days: int | None = None,
    ) -> str | None:
        """按自然日刷新近期查询视图（用于手动 SQL 查询，不影响回测/指标计算口径）。

        - 使用 app.history_view_days 控制自然日窗口（例如 45）。
        - 默认视图名为 history_recent_{N}_days；若与交易日切片视图同名，则自动改为
          history_recent_calendar_{N}_days 以避免覆盖。
        """

        view_days_raw = getattr(self, "history_view_days", 0) or 0
        try:
            view_days = int(view_days_raw)
        except (TypeError, ValueError):  # noqa: PERF203
            view_days = 0

        if view_days <= 0:
            self.logger.info(
                "history_view_days=%s，近期自然日视图未开启，跳过创建。",
                view_days_raw,
            )
            self._last_history_calendar_view = None
            return None

        view_name = f"history_recent_{view_days}_days"
        if slice_window_days is not None and int(slice_window_days) == view_days:
            view_name = f"history_recent_calendar_{view_days}_days"

        try:
            view = self._create_recent_history_calendar_view(
                base_table,
                view_days,
                end_day=end_day,
                view_name=view_name,
            )
            self.logger.debug(
                "近期自然日便捷视图已更新：%s（最近 %s 天）",
                view,
                view_days,
            )
            self._last_history_calendar_view = view
            return view
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "创建近期自然日视图失败（base=%s, view=%s, end=%s, days=%s）: %s",
                base_table,
                view_name,
                end_day.isoformat(),
                view_days,
                exc,
            )
            self._last_history_calendar_view = None
            return None

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

        resume_threshold = self._compute_resume_threshold(
            end_day, window_days, base_table=base_table
        )
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
            recent_df, recent_table = self._export_recent_daily_history(
                stock_df, end_date, days=window_days, base_table=base_table
            )
            self._refresh_history_calendar_view(base_table, end_day, slice_window_days=window_days)
            return recent_df, recent_table
        elif fetch_enabled and last_date_value >= end_day:
            done_codes = self._load_completed_codes(
                base_table, resume_threshold, required_end_date=end_date
            )
            if len(done_codes) < len(stock_df):
                self.logger.info(
                    "历史表 %s 已存在但未覆盖全部股票，继续补齐缺口。", base_table
                )
                recent_df, recent_table = self._export_recent_daily_history(
                    stock_df, end_date, days=window_days, base_table=base_table
                )
                self._refresh_history_calendar_view(base_table, end_day, slice_window_days=window_days)
                return recent_df, recent_table
            self.logger.info(
                "历史日线表 %s 已包含截至 %s 的数据，跳过增量拉取。",
                base_table,
                end_date,
            )
            recent_df, recent_table = self._slice_recent_window(base_table, end_day, window_days)
            self._refresh_history_calendar_view(base_table, end_day, slice_window_days=window_days)
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
                    adjustflag=ADJUSTFLAG_NONE,
                    resume_threshold=None,
                    probe_date=end_date,
                    latest_existing_date=last_date_value.isoformat(),
                )
            else:
                self.logger.info("本次没有任何新的日线数据可写入。")

        recent_df, recent_table = self._slice_recent_window(base_table, end_day, window_days)
        self._refresh_history_calendar_view(base_table, end_day, slice_window_days=window_days)
        return recent_df, recent_table
    def _print_preview(self, interfaces: Iterable[str]) -> None:
        preview = list(interfaces)
        self.logger.info("已发现 %s 个项目组件，前 10 个预览：", len(preview))
        for name in preview[:10]:
            self.logger.info(" - %s", name)

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

            SchemaManager(self.db_writer.engine, db_name=self.db_writer.config.db_name).ensure_all()

            ingest = DataIngestService(self)
            fundamental = FundamentalService(self)
            history = HistoryKlineService(self)
            universe = UniverseService(self)

            # 2) 获取最近交易日并导出股票列表/元数据（允许离线）
            latest_trade_day = ingest.resolve_latest_trade_day()
            self.logger.info("最近交易日：%s", latest_trade_day)

            ingest.export_index_daily_history(latest_trade_day)

            # 股票列表（至少要拿到 code）
            try:
                stock_df = ingest.load_stock_list(latest_trade_day)
            except RuntimeError:
                return

            stock_basic_df = ingest.load_stock_basic()

            industry_df = ingest.load_stock_industry()

            ingest.refresh_board_industry(latest_trade_day)
            index_membership = ingest.load_index_membership(latest_trade_day)
            fundamentals_wide = fundamental.load_fundamentals(
                latest_trade_day, index_membership, stock_df
            )

            # 3) 导出最近 N 日历史日线（增量模式）
            try:
                history_df, history_table = history.export_daily_history(
                    stock_df, latest_trade_day
                )
            except RuntimeError as exc:
                self.logger.error(
                    "导出最近 %s 个交易日的日线数据失败: %s",
                    self.history_days,
                    exc,
                )
                return

            # 3.1) 单独创建“最近 N 个自然日”的便捷视图（用于你手动查近期数据）

            # 4) 构建候选池并挑选成交额前 N 名
            try:
                universe_df = universe.build_universe(
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
                universe_df = universe.apply_fundamental_filters(
                    universe_df, fundamentals_wide
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("基本面过滤阶段出现异常，保留未过滤结果: %s", exc)

            if not universe.persist_universe_snapshot(universe_df):
                return

            try:
                top_liquidity = universe.pick_top_liquidity(universe_df)
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

            universe.sync_external_signals(latest_trade_day, top_liquidity)

            # 5) 提示历史日线路径
            self.logger.debug(
                "历史日线窗口数据来源：%s（切片最近 %s 个交易日）", history_table, self.history_days
            )
            recent_view = getattr(self, "_last_history_calendar_view", None)
            if recent_view:
                self.logger.debug(
                    "近期自然日便捷视图已更新：%s（最近 %s 天）",
                    recent_view,
                    self.history_view_days,
                )
        finally:
            self._cleanup_session()
            self.db_writer.dispose()

if __name__ == "__main__":
    AshareApp().run()
