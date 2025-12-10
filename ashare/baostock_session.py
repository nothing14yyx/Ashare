"""Baostock 会话管理封装。

该模块提供 `BaostockSession` 类，用于管理 Baostock 的登录与登出，
避免在进程结束时忘记释放会话。
"""

from __future__ import annotations

import atexit
import time

import baostock as bs


class BaostockSession:
    """管理 Baostock 登录状态的简单封装。"""

    retry: int = 3
    retry_sleep: float = 3.0
    logged_in: bool = False

    def __init__(self, retry: int | None = None, retry_sleep: float | None = None) -> None:
        """
        初始化会话参数并注册退出时的登出钩子。

        参数允许被覆盖，以便在特殊场景下调整重试策略。
        """
        if retry is not None:
            self.retry = retry
        if retry_sleep is not None:
            self.retry_sleep = retry_sleep

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

        bs.logout()
        self.logged_in = False


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
