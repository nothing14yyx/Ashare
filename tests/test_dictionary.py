"""测试 dictionary 模块的功能."""
import re
from unittest.mock import Mock, patch
import pytest
import requests
from requests.exceptions import RequestException

from ashare.dictionary import DataDictionaryFetcher, A_SHARE_PATTERN


class TestASharePattern:
    """测试A股接口正则表达式模式."""
    
    def test_pattern_matches_valid_interfaces(self):
        """测试正则表达式能正确匹配有效的A股接口."""
        valid_interfaces = [
            "stock_zh_a_spot",
            "stock_zh_a_daily",
            "stock_zh_kcb_spot",
            "stock_zh_ah_spot",
            "stock_zh_a_hist_tx",
            "stock_zh_a_gdhs",
            "stock_zh_a_gbjg_em",
            "stock_zh_growth_comparison_em",
            "stock_zh_valuation_baidu",
        ]
        
        for interface in valid_interfaces:
            assert A_SHARE_PATTERN.match(interface), f"接口 {interface} 应该被匹配"
    
    def test_pattern_does_not_match_invalid_interfaces(self):
        """测试正则表达式不会匹配无效的接口."""
        invalid_interfaces = [
            "fund_open_fund_info",
            "bond_zh_hs_daily",
            "option_finance_board",
            "stock_us_daily",
            "currency_cnb_sina",
        ]
        
        for interface in invalid_interfaces:
            assert not A_SHARE_PATTERN.match(interface), f"接口 {interface} 不应该被匹配"
    
    def test_pattern_handles_edge_cases(self):
        """测试边界情况."""
        assert not A_SHARE_PATTERN.match("stock_zh")  # 太短
        assert not A_SHARE_PATTERN.match("stock_us_a_spot")  # 不是zh开头
        assert A_SHARE_PATTERN.match("stock_zh_a123_spot")  # 包含数字


class TestDataDictionaryFetcher:
    """测试 DataDictionaryFetcher 类."""
    
    def test_initialization_defaults(self):
        """测试初始化默认值."""
        fetcher = DataDictionaryFetcher()
        
        assert fetcher.stock_doc_url == "https://akshare.akfamily.xyz/data/stock/stock.html"
        assert fetcher.verify_ssl is False
        assert fetcher.timeout == 10
        assert fetcher._cached_html is None
        assert fetcher._cached_endpoints is None
        assert fetcher.proxies is None
    
    def test_initialization_with_custom_values(self):
        """测试使用自定义值初始化."""
        custom_url = "https://example.com/data"
        custom_proxies = {"http": "http://proxy:8080"}
        
        fetcher = DataDictionaryFetcher(
            stock_doc_url=custom_url,
            verify_ssl=True,
            timeout=30,
            proxies=custom_proxies
        )
        
        assert fetcher.stock_doc_url == custom_url
        assert fetcher.verify_ssl is True
        assert fetcher.timeout == 30
        assert fetcher.proxies == custom_proxies
    
    @patch('ashare.dictionary.requests.get')
    def test_fetch_raw_html_success(self, mock_get):
        """测试成功获取HTML."""
        mock_response = Mock()
        mock_response.text = "<html>test content</html>"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        fetcher = DataDictionaryFetcher()
        result = fetcher.fetch_raw_html()
        
        assert result == "<html>test content</html>"
        assert fetcher._cached_html == "<html>test content</html>"
        mock_get.assert_called_once()
    
    @patch('ashare.dictionary.requests.get')
    def test_fetch_raw_html_failure(self, mock_get):
        """测试获取HTML失败."""
        mock_get.side_effect = RequestException("Network error")
        
        fetcher = DataDictionaryFetcher()
        
        with pytest.raises(RuntimeError, match="无法获取 AKShare 数据字典页面"):
            fetcher.fetch_raw_html()
    
    @patch('ashare.dictionary.requests.get')
    def test_fetch_raw_html_cached(self, mock_get):
        """测试缓存功能."""
        fetcher = DataDictionaryFetcher()
        fetcher._cached_html = "<html>cached content</html>"
        
        result = fetcher.fetch_raw_html()
        
        assert result == "<html>cached content</html>"
        mock_get.assert_not_called()
    
    def test_extract_a_share_endpoints(self):
        """测试从HTML中提取A股接口."""
        html_content = """
        <html>
        <body>
            <script>
            stock_zh_a_spot
            stock_zh_a_daily
            stock_zh_kcb_spot
            fund_open_fund_info  # 不应该匹配
            stock_zh_ah_spot
            stock_us_daily       # 不应该匹配
            </script>
        </body>
        </html>
        """
        
        fetcher = DataDictionaryFetcher()
        endpoints = fetcher.extract_a_share_endpoints(html_content)
        
        expected = {"stock_zh_a_spot", "stock_zh_a_daily", "stock_zh_kcb_spot", "stock_zh_ah_spot"}
        assert endpoints == expected
    
    def test_extract_a_share_endpoints_empty_html(self):
        """测试从空HTML中提取接口."""
        fetcher = DataDictionaryFetcher()
        endpoints = fetcher.extract_a_share_endpoints("")
        
        assert endpoints == set()
    
    @patch.object(DataDictionaryFetcher, 'fetch_raw_html')
    def test_list_a_share_endpoints(self, mock_fetch_raw_html):
        """测试列出A股接口."""
        mock_fetch_raw_html.return_value = """
        <html>
        <body>
            <script>
            stock_zh_c_spot
            stock_zh_a_spot
            stock_zh_b_spot
            </script>
        </body>
        </html>
        """
        
        fetcher = DataDictionaryFetcher()
        endpoints = fetcher.list_a_share_endpoints()
        
        # 检查是否已缓存结果
        assert fetcher._cached_endpoints == endpoints
        # 检查是否按字母顺序排序
        assert endpoints == ["stock_zh_a_spot", "stock_zh_b_spot", "stock_zh_c_spot"]
    
    @patch.object(DataDictionaryFetcher, 'list_a_share_endpoints')
    def test_is_supported(self, mock_list_endpoints):
        """测试接口支持检查."""
        mock_list_endpoints.return_value = ["stock_zh_a_spot", "stock_zh_a_daily"]
        
        fetcher = DataDictionaryFetcher()
        
        assert fetcher.is_supported("stock_zh_a_spot") is True
        assert fetcher.is_supported("stock_zh_a_daily") is True
        assert fetcher.is_supported("stock_zh_kcb_spot") is False