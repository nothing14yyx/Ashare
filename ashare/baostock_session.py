"""Baostock 会话管理封装。

该模块提供 `BaostockSession` 类，用于管理 Baostock 的登录与登出，
避免在进程结束时忘记释放会话。
"""

from __future__ import annotations

import atexit
import contextlib
import io
import logging
import os
import socket
import time
from typing import Any

import baostock as bs

from .config import get_section


class BaostockSession:
    """管理 Baostock 登录状态的简单封装。"""

    retry: int = 3
    retry_sleep: float = 3.0
    logged_in: bool = False
    socket_timeout: float | None = None
    alive_check_interval: float = 60.0

    def __init__(self, retry: int | None = None, retry_sleep: float | None = None) -> None:
        """
        初始化会话参数并注册退出时的登出钩子。

        参数允许被覆盖，以便在特殊场景下调整重试策略。
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        cfg = get_section("baostock")

        if retry is None:
            retry = cfg.get("retry", self.retry)
        if retry_sleep is None:
            retry_sleep = cfg.get("retry_sleep", self.retry_sleep)
        socket_timeout_raw = os.getenv(
            "ASHARE_BAOSTOCK_SOCKET_TIMEOUT", cfg.get("socket_timeout")
        )
        alive_interval_raw = os.getenv(
            "ASHARE_BAOSTOCK_KEEPALIVE_INTERVAL",
            cfg.get("keepalive_interval", self.alive_check_interval),
        )

        try:
            self.retry = int(retry)
        except (TypeError, ValueError):
            self.retry = 3

        try:
            self.retry_sleep = float(retry_sleep)
        except (TypeError, ValueError):
            self.retry_sleep = 3.0

        try:
            timeout_value = float(socket_timeout_raw) if socket_timeout_raw else None
        except (TypeError, ValueError):
            timeout_value = None
        self.socket_timeout = timeout_value
        try:
            alive_interval = float(alive_interval_raw)
        except (TypeError, ValueError):
            alive_interval = self.alive_check_interval
        self.alive_check_interval = max(60.0, alive_interval)
        self._last_alive_ts: float = 0.0
        if self.socket_timeout and self.socket_timeout > 0:
            socket.setdefaulttimeout(self.socket_timeout)

        atexit.register(self.logout)

    def connect(self) -> None:
        """
        登录 Baostock，带有限次重试。

        - 如果已经登录则直接返回；
        - 登录失败会按配置的次数与间隔重试；
        - 全部失败后抛出带详细信息的 RuntimeError。
        """
        if self.logged_in:
            return

        last_error_msg = ""
        for attempt in range(1, self.retry + 1):
            with contextlib.redirect_stdout(io.StringIO()) as buf:
                result = bs.login()
            out = buf.getvalue().strip()
            if out:
                self.logger.debug("baostock: %s", out)
            if result.error_code == "0":
                self.logged_in = True
                self._last_alive_ts = time.time()
                return

            last_error_msg = result.error_msg
            if attempt < self.retry:
                time.sleep(self.retry_sleep)

        raise RuntimeError(
            "Baostock 登录失败，已重试 {count} 次，最后一次错误：{msg}".format(
                count=self.retry,
                msg=last_error_msg or "未知错误",
            )
        )

    def logout(self) -> None:
        """登出 Baostock 并重置状态。"""
        try:
            if self.logged_in:
                with contextlib.redirect_stdout(io.StringIO()) as buf:
                    bs.logout()
                out = buf.getvalue().strip()
                if out:
                    self.logger.debug("baostock: %s", out)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("登出失败：%s", exc)
        finally:
            # baostock 的 logout 在部分版本/异常路径下可能不会彻底关闭底层 socket，
            # 在 -Wd 下会看到 ResourceWarning: unclosed <socket.socket ...>。
            # 这里做一次“尽力而为”的反射式清理：只在 baostock 模块对象树里查找 socket 并关闭。
            self._force_close_baostock_sockets()
            # 无论登出是否成功，都要清理状态，避免遗留连接或阻塞退出
            self.logged_in = False
            self._last_alive_ts = 0.0

    def _force_close_baostock_sockets(self) -> None:
        """尽最大努力关闭 baostock 库内部遗留的 socket，避免解释器退出时报 ResourceWarning。

        注意：这里不会扫描全局所有对象（避免误伤 DB/HTTP 等连接），仅遍历 baostock 模块可达对象。
        """
        try:
            root = bs
        except Exception:  # noqa: BLE001
            return

        visited: set[int] = set()
        closed_count = 0

        def _close_sock(sock_obj: socket.socket) -> None:
            nonlocal closed_count
            try:
                # 已关闭的 socket.close() 再调用一般是安全的，但仍做 try/except 兜底
                sock_obj.close()
                closed_count += 1
            except Exception:  # noqa: BLE001
                return

        def _walk(obj: Any, depth: int) -> None:
            if obj is None:
                return
            if depth > 4:
                return

            oid = id(obj)
            if oid in visited:
                return
            visited.add(oid)

            # 直接命中 socket
            if isinstance(obj, socket.socket):
                _close_sock(obj)
                return

            # 基础类型不展开
            if isinstance(obj, (str, bytes, int, float, bool)):
                return

            # 常见容器展开
            if isinstance(obj, (list, tuple, set, frozenset)):
                for item in obj:
                    _walk(item, depth + 1)
                return
            if isinstance(obj, dict):
                for item in obj.values():
                    _walk(item, depth + 1)
                return

            # 常见“连接属性名”优先探测（避免全量遍历 __dict__ 太重）
            for attr in (
                "sock",
                "socket",
                "_sock",
                "_socket",
                "conn",
                "connection",
                "client",
                "_client",
                "tcpCliSock",
                "tcp_socket",
            ):
                try:
                    if hasattr(obj, attr):
                        _walk(getattr(obj, attr), depth + 1)
                except Exception:  # noqa: BLE001
                    continue

            # 最后再尽力遍历对象字典（限制深度 + visited 防爆）
            try:
                obj_dict = getattr(obj, "__dict__", None)
                if isinstance(obj_dict, dict):
                    for v in obj_dict.values():
                        _walk(v, depth + 1)
            except Exception:  # noqa: BLE001
                return

        try:
            _walk(root, 0)
        except Exception:  # noqa: BLE001
            return

        if closed_count:
            self.logger.debug("已强制关闭 baostock 遗留 socket：%s 个", closed_count)

    def ensure_alive(self, force_refresh: bool = False, force_check: bool = False) -> None:
        """确保会话可用，必要时重新登录或主动探测。

        - `force_refresh` 为 ``True`` 时直接重新登录；
        - `force_check` 为 ``True`` 时跳过探测节流，立即执行一次有效性检查。
        """

        if force_refresh:
            self.logger.info("强制刷新会话，重新登录。")
            self.reconnect()
            return

        if not self.logged_in:
            self.logger.info("会话未登录，执行登录。")
            self.connect()
            return

        now = time.time()
        if not force_check and now - self._last_alive_ts < self.alive_check_interval:
            return

        try:
            self._probe_alive()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("会话检查失败: %s", exc)
            self.reconnect()
        else:
            self._last_alive_ts = time.time()

    def _probe_alive(self) -> None:
        """通过轻量查询验证会话可用性。"""

        try:
            rs = bs.query_sz50_stocks()
            if getattr(rs, "error_code", None) != "0":
                raise RuntimeError(
                    f"Baostock 会话失效，错误代码：{getattr(rs, 'error_code', '未知')}，"
                    f"错误信息：{getattr(rs, 'error_msg', '未知')}"
                )
        except Exception as exc:
            self.logger.error("会话验证失败: %s", exc)
            raise RuntimeError(f"Baostock 会话失效，错误详情：{exc}") from exc

    def reconnect(self, max_retries: int = 3) -> None:
        """重新建立 Baostock 连接，并限制最大重试次数。"""

        retries = 0
        while retries < max_retries:
            try:
                self.logger.info("正在尝试重连 Baostock，会话重试次数：%s", retries + 1)
                self.logout()
                self.connect()
                self.logger.info("Baostock 会话重连成功。")
                return
            except Exception as exc:
                retries += 1
                self.logged_in = False
                self._last_alive_ts = 0.0
                self.logger.warning("Baostock 会话重连失败（第 %s 次）：%s", retries, exc)
                if retries < max_retries:
                    time.sleep(2)

        raise RuntimeError("Baostock 会话重连失败，已达到最大重试次数。")


def _demo() -> None:
    """简单示例：登录后查询全部股票并打印数量。"""
    session = BaostockSession()
    session.connect()

    rs = bs.query_all_stock()
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())

    print(f"[demo] 全部股票数量：{len(rows)}")


if __name__ == "__main__":
    _demo()
