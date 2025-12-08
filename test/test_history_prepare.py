"""历史行情预处理与工具函数的健壮性测试。"""

from __future__ import annotations

import unittest

import pandas as pd

from src.akshare_client import AKShareClient


class HistoryPrepareTestCase(unittest.TestCase):
    """验证内部预处理逻辑在无网络依赖时的正确性。"""

    @classmethod
    def setUpClass(cls) -> None:  # noqa: D401
        cls.client = AKShareClient(use_proxies=False)

    def test_ensure_float_columns_converts_numeric_strings(self) -> None:
        """字符串数字应被转换为浮点数，非数值转换为 NaN。"""

        data = pd.DataFrame(
            {
                "a": ["1.1", "invalid"],
                "b": ["2", "3"],
            }
        )

        self.client._ensure_float_columns(data, ["a", "b", "c"])

        self.assertTrue(pd.api.types.is_float_dtype(data["a"]))
        self.assertTrue(pd.api.types.is_numeric_dtype(data["b"]))
        self.assertTrue(pd.isna(data.loc[1, "a"]))

    def test_prepare_history_generates_standard_columns(self) -> None:
        """历史行情预处理应补齐标准列并正确计算衍生指标。"""

        source = pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-03"],
                "open": ["10", "11"],
                "close": ["11", "10.5"],
                "high": ["11", "12"],
                "low": ["9", "10"],
                "volume": ["1000", "2000"],
                "amount": ["5000", "6000"],
                "turnover_rate": ["1.1", "1.2"],
                "pct_chg": ["10", "-4.55"],
                "change": ["1", "-0.5"],
                "amplitude": ["5", "3"],
            }
        )

        prepared = self.client._prepare_history(source, "000001")

        expected_columns = {"代码", "日期", "开盘", "收盘", "昨收", "涨跌额", "涨跌幅", "振幅"}
        self.assertTrue(expected_columns.issubset(prepared.columns))
        self.assertEqual(prepared.iloc[0]["代码"], "000001")
        self.assertAlmostEqual(prepared.iloc[1]["涨跌额"], -0.5)
        self.assertAlmostEqual(prepared.iloc[1]["振幅"], 3.0, places=3)


if __name__ == "__main__":
    unittest.main()
