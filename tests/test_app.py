"""测试 app 模块的功能."""
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import pytest

from ashare.app import AshareApp
from ashare.config import ProxyConfig


class TestAshareApp:
    """测试 AshareApp 类."""
    
    def test_initialization_default(self, tmp_path):
        """测试默认初始化."""
        output_dir = tmp_path / "output"
        
        app = AshareApp(output_dir=output_dir)
        
        # 验证输出目录被创建
        assert output_dir.exists()
        assert output_dir.is_dir()
        
        # 验证组件被正确初始化
        assert app.output_dir == output_dir
        assert app.core_fetcher is not None
        assert app.fetcher is not None
        assert app.universe_builder is not None
    
    def test_initialization_with_custom_params(self, tmp_path):
        """测试使用自定义参数初始化."""
        output_dir = tmp_path / "custom_output"
        proxy_config = Mock(spec=ProxyConfig)
        top_liquidity_count = 50
        
        app = AshareApp(
            output_dir=output_dir,
            proxy_config=proxy_config,
            top_liquidity_count=top_liquidity_count
        )
        
        assert app.output_dir == output_dir
        assert app.fetcher.proxy_config == proxy_config
        assert app.universe_builder.top_liquidity_count == top_liquidity_count
    
    def test_print_interfaces(self, capsys):
        """测试打印接口列表功能."""
        app = AshareApp()
        
        # 创建一个有15个接口的模拟列表
        interfaces = [f"interface_{i}" for i in range(15)]
        
        app._print_interfaces(interfaces)
        
        captured = capsys.readouterr()
        output = captured.out
        
        # 验证输出包含总数和前10个接口
        assert "数据字典共发现 15 个 A 股接口, 前 10 个预览:" in output
        for i in range(10):  # 前10个接口应该被打印
            assert f" - interface_{i}" in output
    
    @patch('pandas.DataFrame.to_csv')
    def test_save_interfaces(self, mock_to_csv, tmp_path):
        """测试保存接口列表到CSV."""
        output_dir = tmp_path / "output"
        app = AshareApp(output_dir=output_dir)
        
        interfaces = ["interface_1", "interface_2", "interface_3"]
        result_path = app._save_interfaces(interfaces)
        
        # 验证返回的路径
        assert result_path.name == "a_share_interfaces.csv"
        assert result_path.parent == output_dir
        
        # 验证to_csv被正确调用
        mock_to_csv.assert_called_once()
    
    @patch('pandas.DataFrame.to_csv')
    def test_save_sample(self, mock_to_csv, tmp_path):
        """测试保存样本数据."""
        output_dir = tmp_path / "output"
        app = AshareApp(output_dir=output_dir)
        
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        filename = "test_file.csv"
        
        result_path = app._save_sample(df, filename)
        
        # 验证返回的路径
        assert result_path.name == filename
        assert result_path.parent == output_dir
        
        # 验证to_csv被正确调用
        mock_to_csv.assert_called_once()
    
    @patch('builtins.print')
    @patch.object(AshareApp, '_save_interfaces')
    @patch.object(AshareApp, '_print_interfaces')
    @patch.object(AshareApp, 'fetcher', create=True)
    def test_run_success(self, mock_fetcher, mock_print_interfaces, 
                         mock_save_interfaces, mock_print, tmp_path):
        """测试运行成功的情况."""
        output_dir = tmp_path / "output"
        app = AshareApp(output_dir=output_dir)
        
        # 模拟获取接口列表成功
        mock_fetcher.available_interfaces.return_value = ["interface_1", "interface_2"]
        mock_save_interfaces.return_value = output_dir / "a_share_interfaces.csv"
        
        # 模拟universe builder的方法
        with patch.object(app.universe_builder, 'build_universe') as mock_build_universe, \
             patch.object(app.universe_builder, 'pick_top_liquidity') as mock_pick_top_liquidity, \
             patch.object(app, '_save_sample') as mock_save_sample:
            
            mock_build_universe.return_value = pd.DataFrame({"symbol": ["000001"]})
            mock_pick_top_liquidity.return_value = pd.DataFrame({"symbol": ["000001"], "volume": [1000000]})
            mock_save_sample.return_value = output_dir / "test.csv"
            
            app.run()
            
            # 验证各个方法被调用
            mock_fetcher.available_interfaces.assert_called_once()
            mock_print_interfaces.assert_called_once()
            mock_save_interfaces.assert_called_once()
            mock_build_universe.assert_called_once()
            mock_pick_top_liquidity.assert_called_once()
    
    @patch('builtins.print')
    @patch.object(AshareApp, 'fetcher', create=True)
    def test_run_fetcher_error(self, mock_fetcher, mock_print, tmp_path):
        """测试获取接口列表失败的情况."""
        output_dir = tmp_path / "output"
        app = AshareApp(output_dir=output_dir)
        
        # 模拟获取接口列表失败
        mock_fetcher.available_interfaces.side_effect = RuntimeError("Network error")
        
        app.run()
        
        # 验证打印了错误信息，并且后续步骤没有执行
        error_calls = [call for call in mock_print.call_args_list 
                      if "加载数据字典失败" in str(call)]
        assert len(error_calls) > 0
    
    @patch('builtins.print')
    @patch.object(AshareApp, '_save_interfaces')
    @patch.object(AshareApp, 'fetcher', create=True)
    def test_run_universe_builder_error(self, mock_fetcher, mock_save_interfaces, 
                                       mock_print, tmp_path):
        """测试构建股票池失败的情况."""
        output_dir = tmp_path / "output"
        app = AshareApp(output_dir=output_dir)
        
        # 模拟获取接口列表成功
        mock_fetcher.available_interfaces.return_value = ["interface_1", "interface_2"]
        mock_save_interfaces.return_value = output_dir / "a_share_interfaces.csv"
        
        # 模拟universe builder失败
        with patch.object(app.universe_builder, 'build_universe') as mock_build_universe:
            mock_build_universe.side_effect = RuntimeError("Universe build failed")
            
            app.run()
            
            # 验证接口列表仍然被处理了
            mock_fetcher.available_interfaces.assert_called_once()
            mock_save_interfaces.assert_called_once()
            
            # 但构建股票池失败，应该打印错误
            error_calls = [call for call in mock_print.call_args_list 
                          if "生成当日候选池失败" in str(call)]
            assert len(error_calls) > 0
    
    @patch('builtins.print')
    @patch.object(AshareApp, '_save_interfaces')
    @patch.object(AshareApp, 'fetcher', create=True)
    def test_run_top_liquidity_error(self, mock_fetcher, mock_save_interfaces, 
                                    mock_print, tmp_path):
        """测试挑选高流动性股票失败的情况."""
        output_dir = tmp_path / "output"
        app = AshareApp(output_dir=output_dir)
        
        # 模拟获取接口列表成功
        mock_fetcher.available_interfaces.return_value = ["interface_1", "interface_2"]
        mock_save_interfaces.return_value = output_dir / "a_share_interfaces.csv"
        
        # 模拟universe builder成功，但挑选高流动性股票失败
        with patch.object(app.universe_builder, 'build_universe') as mock_build_universe, \
             patch.object(app.universe_builder, 'pick_top_liquidity') as mock_pick_top_liquidity:
            
            mock_build_universe.return_value = pd.DataFrame({"symbol": ["000001"]})
            mock_pick_top_liquidity.side_effect = RuntimeError("Pick top liquidity failed")
            
            app.run()
            
            # 验证前面的步骤成功执行
            mock_fetcher.available_interfaces.assert_called_once()
            mock_build_universe.assert_called_once()
            
            # 但挑选高流动性股票失败，应该打印错误
            error_calls = [call for call in mock_print.call_args_list 
                          if "挑选成交额前" in str(call) and "失败" in str(call)]
            assert len(error_calls) > 0