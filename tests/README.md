# 测试说明

本项目包含对 ashare 模块的全面单元测试，覆盖以下功能模块：

## 测试模块

- `test_dictionary.py`: 测试数据字典解析功能
- `test_fetcher.py`: 测试A股接口获取功能
- `test_core_fetcher.py`: 测试核心数据获取功能
- `test_config.py`: 测试配置管理功能
- `test_app.py`: 测试应用程序入口功能

## 运行测试

### 安装依赖

```bash
pip install pytest pandas akshare requests
```

### 运行所有测试

```bash
# 在项目根目录运行
pytest

# 或者在tests目录运行
python -m pytest tests/
```

### 运行特定测试文件

```bash
pytest tests/test_dictionary.py
pytest tests/test_fetcher.py
```

### 运行测试并显示覆盖率

```bash
pip install pytest-cov
pytest --cov=ashare --cov-report=html
```

## 测试内容

### DataDictionaryFetcher 测试
- 验证A股接口正则表达式模式的准确性
- 测试数据字典获取器的初始化
- 测试HTML内容获取和缓存功能
- 测试接口名称提取功能
- 测试接口支持检查功能

### AshareDataFetcher 测试
- 测试初始化逻辑
- 测试可用接口列表获取功能
- 验证接口过滤逻辑（过滤掉变量名风格的接口）

### AshareCoreFetcher 测试
- 测试各种A股数据获取方法
- 验证异常处理和安全调用机制
- 测试返回空DataFrame而不是抛出异常的功能

### ProxyConfig 测试
- 测试代理配置的初始化
- 测试从环境变量加载配置的功能
- 测试生成requests代理格式的功能
- 测试应用配置到环境变量的功能

### AshareApp 测试
- 测试应用程序初始化
- 测试接口列表打印和保存功能
- 测试完整运行流程
- 验证错误处理机制