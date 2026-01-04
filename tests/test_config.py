"""测试 config 模块的功能."""
import os
from unittest.mock import patch
import pytest

from ashare.config import ProxyConfig


class TestProxyConfig:
    """测试 ProxyConfig 类."""
    
    def test_initialization_default(self):
        """测试默认初始化."""
        config = ProxyConfig()
        
        assert config.http is None
        assert config.https is None
    
    def test_initialization_with_values(self):
        """测试使用自定义值初始化."""
        config = ProxyConfig(http="http://proxy:8080", https="https://proxy:8080")
        
        assert config.http == "http://proxy:8080"
        assert config.https == "https://proxy:8080"
    
    @patch.dict(os.environ, {}, clear=True)
    def test_from_env_empty(self):
        """测试从空环境变量加载配置."""
        config = ProxyConfig.from_env()
        
        assert config.http is None
        assert config.https is None
    
    @patch.dict(os.environ, {
        'ASHARE_HTTP_PROXY': 'http://custom_http:8080',
        'ASHARE_HTTPS_PROXY': 'https://custom_https:8080'
    })
    def test_from_env_ashare_vars(self):
        """测试从ASHARE_*环境变量加载配置."""
        config = ProxyConfig.from_env()
        
        assert config.http == 'http://custom_http:8080'
        assert config.https == 'https://custom_https:8080'
    
    @patch.dict(os.environ, {
        'HTTP_PROXY': 'http://http_proxy:8080',
        'HTTPS_PROXY': 'https://https_proxy:8080',
        'http_proxy': 'http://lowercase_http:8080',
        'https_proxy': 'https://lowercase_https:8080'
    })
    def test_from_env_standard_vars(self):
        """测试从标准环境变量加载配置."""
        config = ProxyConfig.from_env()
        
        # 根据代码逻辑，优先级是 ASHARE_* > *_PROXY > *_proxy
        assert config.http == 'http://http_proxy:8080'
        assert config.https == 'https://https_proxy:8080'
    
    @patch.dict(os.environ, {
        'ASHARE_HTTP_PROXY': 'http://custom_http:8080',
        'HTTPS_PROXY': 'https://https_proxy:8080'
    })
    def test_from_env_mixed_vars(self):
        """测试混合环境变量优先级."""
        config = ProxyConfig.from_env()
        
        # ASHARE_HTTP_PROXY 优先于 HTTPS_PROXY
        assert config.http == 'http://custom_http:8080'
        assert config.https == 'https://https_proxy:8080'
    
    @patch.dict(os.environ, {
        'http_proxy': 'http://lowercase_http:8080',
        'https_proxy': 'https://lowercase_https:8080'
    })
    def test_from_env_lowercase_vars(self):
        """测试从小写环境变量加载配置."""
        config = ProxyConfig.from_env()
        
        assert config.http == 'http://lowercase_http:8080'
        assert config.https == 'https://lowercase_https:8080'
    
    def test_as_requests_proxies_empty(self):
        """测试空配置生成requests代理格式."""
        config = ProxyConfig()
        proxies = config.as_requests_proxies()
        
        assert proxies == {}
    
    def test_as_requests_proxies_http_only(self):
        """测试仅HTTP代理配置."""
        config = ProxyConfig(http="http://proxy:8080")
        proxies = config.as_requests_proxies()
        
        assert proxies == {"http": "http://proxy:8080"}
    
    def test_as_requests_proxies_https_only(self):
        """测试仅HTTPS代理配置."""
        config = ProxyConfig(https="https://proxy:8080")
        proxies = config.as_requests_proxies()
        
        assert proxies == {"https": "https://proxy:8080"}
    
    def test_as_requests_proxies_both(self):
        """测试HTTP和HTTPS代理配置."""
        config = ProxyConfig(
            http="http://http_proxy:8080",
            https="https://https_proxy:8080"
        )
        proxies = config.as_requests_proxies()
        
        expected = {
            "http": "http://http_proxy:8080",
            "https": "https://https_proxy:8080"
        }
        assert proxies == expected
    
    @patch.dict(os.environ, {}, clear=True)
    def test_apply_to_environment_empty(self):
        """测试应用空配置到环境变量."""
        config = ProxyConfig()
        
        # 保存原始环境变量
        original_http_proxy = os.environ.get('HTTP_PROXY')
        original_https_proxy = os.environ.get('HTTPS_PROXY')
        original_http_proxy_lower = os.environ.get('http_proxy')
        original_https_proxy_lower = os.environ.get('https_proxy')
        
        config.apply_to_environment()
        
        # 验证环境变量没有被设置
        assert 'HTTP_PROXY' not in os.environ
        assert 'HTTPS_PROXY' not in os.environ
        assert 'http_proxy' not in os.environ
        assert 'https_proxy' not in os.environ
        
        # 恢复原始环境变量
        if original_http_proxy is not None:
            os.environ['HTTP_PROXY'] = original_http_proxy
        if original_https_proxy is not None:
            os.environ['HTTPS_PROXY'] = original_https_proxy
        if original_http_proxy_lower is not None:
            os.environ['http_proxy'] = original_http_proxy_lower
        if original_https_proxy_lower is not None:
            os.environ['https_proxy'] = original_https_proxy_lower
    
    @patch.dict(os.environ, {}, clear=True)
    def test_apply_to_environment_with_values(self):
        """测试应用配置到环境变量."""
        config = ProxyConfig(
            http="http://new_http_proxy:8080",
            https="https://new_https_proxy:8080"
        )
        
        config.apply_to_environment()
        
        assert os.environ.get('HTTP_PROXY') == "http://new_http_proxy:8080"
        assert os.environ.get('HTTPS_PROXY') == "https://new_https_proxy:8080"
        assert os.environ.get('http_proxy') == "http://new_http_proxy:8080"
        assert os.environ.get('https_proxy') == "https://new_https_proxy:8080"
    
    @patch.dict(os.environ, {
        'HTTP_PROXY': 'http://old_proxy:8080',
        'HTTPS_PROXY': 'https://old_proxy:8080'
    })
    def test_apply_to_environment_overwrites(self):
        """测试应用配置覆盖现有环境变量."""
        config = ProxyConfig(
            http="http://new_http_proxy:8080",
            https="https://new_https_proxy:8080"
        )
        
        # 验证初始值
        assert os.environ.get('HTTP_PROXY') == 'http://old_proxy:8080'
        assert os.environ.get('HTTPS_PROXY') == 'https://old_proxy:8080'
        
        config.apply_to_environment()
        
        # 验证被新值覆盖
        assert os.environ.get('HTTP_PROXY') == "http://new_http_proxy:8080"
        assert os.environ.get('HTTPS_PROXY') == "https://new_https_proxy:8080"
        assert os.environ.get('http_proxy') == "http://new_http_proxy:8080"
        assert os.environ.get('https_proxy') == "https://new_https_proxy:8080"
    
    @patch.dict(os.environ, {
        'HTTP_PROXY': 'http://old_proxy:8080'
    })
    def test_apply_to_environment_partial(self):
        """测试部分配置应用到环境变量."""
        config = ProxyConfig(https="https://new_https_proxy:8080")
        
        # 验证初始值
        assert os.environ.get('HTTP_PROXY') == 'http://old_proxy:8080'
        assert os.environ.get('HTTPS_PROXY') is None
        
        config.apply_to_environment()
        
        # 验证只有https代理被设置，http代理保持不变
        assert os.environ.get('HTTP_PROXY') == 'http://old_proxy:8080'  # 保持不变
        assert os.environ.get('HTTPS_PROXY') == "https://new_https_proxy:8080"
        assert os.environ.get('https_proxy') == "https://new_https_proxy:8080"
        # 由于config.http为None，不会更新http_proxy
        assert os.environ.get('http_proxy') is None  # 因为没有设置config.http，所以http_proxy不会被设置