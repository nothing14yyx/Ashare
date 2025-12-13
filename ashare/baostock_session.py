"""Baostock 会话管理封装。

该模块提供 `BaostockSession` 类，用于管理 Baostock 的登录与登出，
避免在进程结束时忘记释放会话。
"""

from __future__ import annotations

import atexit
import os
import socket
import time

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
        self.alive_check_interval = max(5.0, alive_interval)
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
            result = bs.login()
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
        if not self.logged_in:
            return

        try:
            bs.logout()
        except Exception:
            # 退出阶段网络抖动/服务端无响应时，避免抛异常导致 socket 未关闭警告
            pass
        finally:
            self.logged_in = False
            self._last_alive_ts = 0.0

    def ensure_alive(self, force_refresh: bool = False, force_check: bool = False) -> None:
        """确保会话可用，必要时重新登录或主动探测。

        - `force_refresh` 为 ``True`` 时直接重新登录；
        - `force_check` 为 ``True`` 时跳过探测节流，立即执行一次有效性检查，
          但不会主动登出再登录，避免频繁重置会话。
        """

        if force_refresh:
            self.reconnect()
            return

        if not self.logged_in:
            self.connect()
            return

        now = time.time()
        if not force_check and now - self._last_alive_ts < self.alive_check_interval:
            return

        try:
            self._probe_alive()
        except Exception:
            self.reconnect()
        else:
            self._last_alive_ts = time.time()

    def _probe_alive(self) -> None:
        """通过轻量查询验证会话可用性。"""

        rs = bs.query_sz50_stocks()
        if getattr(rs, "error_code", None) != "0":
            raise RuntimeError("Baostock 会话失效，需要重新登录。")

    def reconnect(self) -> None:
        """重新建立 Baostock 连接。"""

        try:
            self.logout()
        finally:
            self.logged_in = False
            self._last_alive_ts = 0.0
        self.connect()


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
