"""测试 core_fetcher 模块的功能."""
import pandas as pd
from unittest.mock import Mock, patch
import pytest

from ashare.core_fetcher import AshareCoreFetcher


class TestAshareCoreFetcher:
    """测试 AshareCoreFetcher 类."""
    
    def test_initialization(self):
        """测试初始化."""
        fetcher = AshareCoreFetcher()
        # 简单确认对象创建成功
        assert isinstance(fetcher, AshareCoreFetcher)
    
    @patch('ashare.core_fetcher.ak.stock_zh_a_spot')
    def test_get_realtime_all_a_success(self, mock_stock_func):
        """测试获取全市场A股实时行情成功."""
        mock_df = pd.DataFrame({'symbol': ['000001', '000002'], 'name': ['平安银行', '万科A']})
        mock_stock_func.return_value = mock_df
        
        fetcher = AshareCoreFetcher()
        result = fetcher.get_realtime_all_a()
        
        assert result.equals(mock_df)
        mock_stock_func.assert_called_once()
    
    @patch('ashare.core_fetcher.ak.stock_zh_a_spot')
    def test_get_realtime_all_a_with_exception(self, mock_stock_func):
        """测试获取全市场A股实时行情时发生异常."""
        mock_stock_func.side_effect = Exception("Network error")
        
        fetcher = AshareCoreFetcher()
        result = fetcher.get_realtime_all_a()
        
        # 应该返回空DataFrame而不是抛出异常
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    @patch('ashare.core_fetcher.ak.stock_zh_a_spot')
    def test_get_realtime_all_a_with_none_result(self, mock_stock_func):
        """测试获取全市场A股实时行情返回None."""
        mock_stock_func.return_value = None
        
        fetcher = AshareCoreFetcher()
        result = fetcher.get_realtime_all_a()
        
        # 应该返回空DataFrame
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    @patch('ashare.core_fetcher.ak.stock_zh_kcb_spot')
    def test_get_realtime_kcb_success(self, mock_stock_func):
        """测试获取科创板实时行情成功."""
        mock_df = pd.DataFrame({'symbol': ['688001', '688002'], 'name': ['华兴源创', '睿创微纳']})
        mock_stock_func.return_value = mock_df
        
        fetcher = AshareCoreFetcher()
        result = fetcher.get_realtime_kcb()
        
        assert result.equals(mock_df)
        mock_stock_func.assert_called_once()
    
    @patch('ashare.core_fetcher.ak.stock_zh_ah_spot')
    def test_get_realtime_ah_success(self, mock_stock_func):
        """测试获取A+H股实时行情成功."""
        mock_df = pd.DataFrame({'symbol': ['000001', 'HK00001'], 'name': ['平安银行', '平安银行']})
        mock_stock_func.return_value = mock_df
        
        fetcher = AshareCoreFetcher()
        result = fetcher.get_realtime_ah()
        
        assert result.equals(mock_df)
        mock_stock_func.assert_called_once()
    
    @patch('ashare.core_fetcher.ak.stock_zh_a_daily')
    def test_get_daily_a_sina_success(self, mock_stock_func):
        """测试获取A股历史日线成功."""
        mock_df = pd.DataFrame({'date': ['2023-01-01'], 'close': [10.0]})
        mock_stock_func.return_value = mock_df
        
        fetcher = AshareCoreFetcher()
        result = fetcher.get_daily_a_sina(symbol='000001')
        
        assert result.equals(mock_df)
        mock_stock_func.assert_called_once_with(symbol='000001')
    
    @patch('ashare.core_fetcher.ak.stock_zh_a_daily')
    def test_get_daily_a_sina_with_kwargs(self, mock_stock_func):
        """测试获取A股历史日线并传递参数."""
        mock_df = pd.DataFrame({'date': ['2023-01-01'], 'close': [10.0]})
        mock_stock_func.return_value = mock_df
        
        fetcher = AshareCoreFetcher()
        result = fetcher.get_daily_a_sina(symbol='000001', start_date='20230101')
        
        assert result.equals(mock_df)
        mock_stock_func.assert_called_once_with(symbol='000001', start_date='20230101')
    
    def test_safe_call_df_with_non_dataframe_result(self):
        """测试 _safe_call_df 方法处理非DataFrame结果."""
        fetcher = AshareCoreFetcher()
        
        # 模拟一个返回非DataFrame的函数
        def mock_func():
            return [{'symbol': '000001', 'name': '平安银行'}]  # 返回列表而不是DataFrame
        
        result = fetcher._safe_call_df(mock_func, "test_func")
        
        # 应该尝试将结果转换为DataFrame
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result) == 1


class TestSafeCallDfMethod:
    """专门测试 _safe_call_df 方法."""
    
    def test_safe_call_df_success(self):
        """测试安全调用成功."""
        fetcher = AshareCoreFetcher()
        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        
        def success_func():
            return mock_df
        
        result = fetcher._safe_call_df(success_func, "test_func")
        
        assert result.equals(mock_df)
    
    def test_safe_call_df_with_exception(self):
        """测试安全调用时发生异常."""
        fetcher = AshareCoreFetcher()
        
        def error_func():
            raise ValueError("Test error")
        
        result = fetcher._safe_call_df(error_func, "test_func")
        
        # 应该返回空DataFrame而不是抛出异常
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_safe_call_df_with_none_result(self):
        """测试安全调用返回None."""
        fetcher = AshareCoreFetcher()
        
        def none_func():
            return None
        
        result = fetcher._safe_call_df(none_func, "test_func")
        
        # 应该返回空DataFrame
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_safe_call_df_with_non_dataframe_result(self):
        """测试安全调用返回非DataFrame结果."""
        fetcher = AshareCoreFetcher()
        
        def list_func():
            return [{'a': 1}, {'a': 2}]
        
        result = fetcher._safe_call_df(list_func, "test_func")
        
        # 应该尝试转换为DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2