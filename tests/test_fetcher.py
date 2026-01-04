"""测试 fetcher 模块的功能."""
from unittest.mock import Mock, patch
import pytest
import akshare as ak

from ashare.fetcher import AshareDataFetcher
from ashare.dictionary import DataDictionaryFetcher
from ashare.config import ProxyConfig


class TestAshareDataFetcher:
    """测试 AshareDataFetcher 类."""
    
    def test_initialization_with_defaults(self):
        """测试使用默认值初始化."""
        with patch('ashare.config.ProxyConfig.from_env') as mock_from_env:
            mock_proxy_config = Mock(spec=ProxyConfig)
            mock_from_env.return_value = mock_proxy_config
            
            fetcher = AshareDataFetcher()
            
            # 检查代理配置是否正确设置
            assert fetcher.proxy_config == mock_proxy_config
            mock_proxy_config.apply_to_environment.assert_called_once()
            
            # 检查字典获取器是否正确初始化
            assert isinstance(fetcher.dictionary_fetcher, DataDictionaryFetcher)
    
    def test_initialization_with_custom_proxy_config(self):
        """测试使用自定义代理配置初始化."""
        custom_proxy_config = Mock(spec=ProxyConfig)
        
        with patch.object(DataDictionaryFetcher, '__init__', return_value=None):
            fetcher = AshareDataFetcher(proxy_config=custom_proxy_config)
            
            assert fetcher.proxy_config == custom_proxy_config
            custom_proxy_config.apply_to_environment.assert_called_once()
    
    def test_initialization_with_custom_dictionary_fetcher(self):
        """测试使用自定义字典获取器初始化."""
        custom_proxy_config = Mock(spec=ProxyConfig)
        custom_dict_fetcher = Mock(spec=DataDictionaryFetcher)
        
        fetcher = AshareDataFetcher(
            dictionary_fetcher=custom_dict_fetcher,
            proxy_config=custom_proxy_config
        )
        
        assert fetcher.dictionary_fetcher == custom_dict_fetcher
        assert fetcher.proxy_config == custom_proxy_config
    
    @patch.object(DataDictionaryFetcher, 'list_a_share_endpoints')
    def test_available_interfaces_basic(self, mock_list_endpoints):
        """测试获取可用接口的基本功能."""
        # 模拟从数据字典获取的候选接口
        mock_list_endpoints.return_value = [
            'stock_zh_a_spot',
            'stock_zh_a_daily',
            'nonexistent_function',  # 这个在akshare中不存在
        ]
        
        # 确保hasattr能正确工作
        def mock_hasattr(obj, name):
            return name in ['stock_zh_a_spot', 'stock_zh_a_daily']
        
        with patch('ashare.fetcher.hasattr', side_effect=mock_hasattr):
            fetcher = AshareDataFetcher(dictionary_fetcher=Mock(spec=DataDictionaryFetcher))
            interfaces = fetcher.available_interfaces()
            
            # 验证只返回akshare中存在的接口
            expected = ['stock_zh_a_spot', 'stock_zh_a_daily']
            assert sorted(interfaces) == sorted(expected)
    
    @patch.object(DataDictionaryFetcher, 'list_a_share_endpoints')
    def test_available_interfaces_filters_variables(self, mock_list_endpoints):
        """测试过滤掉变量名风格的接口."""
        # 模拟包含变量名风格的接口
        mock_list_endpoints.return_value = [
            'stock_zh_a_spot',      # 正常接口
            'stock_zh_a_spot_df',   # 变量名，应该被过滤
            'stock_zh_a_daily',     # 正常接口
            'stock_zh_a_qfq_df',    # 变量名，应该被过滤
            'stock_zh_a_hfq_df',    # 变量名，应该被过滤
            'stock_zh_kcb_spot',    # 正常接口
        ]
        
        # 确保hasattr能正确工作
        def mock_hasattr(obj, name):
            return name in mock_list_endpoints.return_value  # 所有接口都存在
            
        with patch('ashare.fetcher.hasattr', side_effect=mock_hasattr):
            fetcher = AshareDataFetcher(dictionary_fetcher=Mock(spec=DataDictionaryFetcher))
            interfaces = fetcher.available_interfaces()
            
            # 验证只返回正常的接口，过滤掉变量名风格的接口
            expected = ['stock_zh_a_spot', 'stock_zh_a_daily', 'stock_zh_kcb_spot']
            assert sorted(interfaces) == sorted(expected)
    
    @patch.object(DataDictionaryFetcher, 'list_a_share_endpoints')
    def test_available_interfaces_empty_result(self, mock_list_endpoints):
        """测试空结果."""
        mock_list_endpoints.return_value = []

        # 确保hasattr能正确工作
        def mock_hasattr(obj, name):
            return False  # 没有接口存在
            
        with patch('ashare.fetcher.hasattr', side_effect=mock_hasattr):
            fetcher = AshareDataFetcher(dictionary_fetcher=Mock(spec=DataDictionaryFetcher))
            interfaces = fetcher.available_interfaces()
            
            assert interfaces == []
    
    @patch.object(DataDictionaryFetcher, 'list_a_share_endpoints')
    def test_available_interfaces_no_existing_functions(self, mock_list_endpoints):
        """测试所有候选接口在akshare中都不存在."""
        mock_list_endpoints.return_value = [
            'nonexistent_function1',
            'nonexistent_function2',
        ]
        
        # 确保hasattr返回False
        def mock_hasattr(obj, name):
            return name == 'stock_zh_a_spot'  # 只有一个不相关的函数存在
            
        with patch('ashare.fetcher.hasattr', side_effect=mock_hasattr):
            fetcher = AshareDataFetcher(dictionary_fetcher=Mock(spec=DataDictionaryFetcher))
            interfaces = fetcher.available_interfaces()
            
            # 所有候选函数都不存在于akshare中，所以返回空列表
            assert interfaces == []