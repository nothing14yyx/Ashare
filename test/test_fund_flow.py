"""资金流接口可用性测试。"""

from __future__ import annotations

from datetime import date, timedelta
import unittest

from src.akshare_client import AKShareClient


class FundFlowTestCase(unittest.TestCase):
    """验证个股与板块资金流接口是否可正常返回数据。"""

    @classmethod
    def setUpClass(cls) -> None:  # noqa: D401
        cls.client = AKShareClient(use_proxies=False)

    def test_stock_fund_flow(self) -> None:
        """检查单只股票资金流是否可访问。"""

        end_date = date.today()
        start_date = end_date - timedelta(days=10)
        try:
            flows = self.client.fetch_stock_fund_flow(
                code="000001",
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )
        except Exception as exc:  # pragma: no cover - 外部依赖
            self.skipTest(f"暂缓使用：个股资金流接口不可用，原因：{exc}")
            return

        self.assertFalse(flows.empty, "个股资金流应返回数据")
        self.assertIn("net_main_inflow", flows.columns)
        self.assertIn("net_large_order", flows.columns)

    def test_board_fund_flow(self) -> None:
        """检查行业/概念资金流是否可访问。"""

        try:
            flows = self.client.fetch_board_fund_flow(board_type="industry", indicator="今日")
        except Exception as exc:  # pragma: no cover - 外部依赖
            self.skipTest(f"暂缓使用：板块资金流接口不可用，原因：{exc}")
            return

        self.assertFalse(flows.empty, "板块资金流应返回数据")
        self.assertIn("board_name", flows.columns)
        self.assertIn("net_main_inflow", flows.columns)


if __name__ == "__main__":
    unittest.main()
