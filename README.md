# A 股接口清单导出工具

基于 [AKShare](https://akshare.akfamily.xyz/) 数据字典构建的简单 A 股接口清单导出示例, 只专注于接口发现与保存。

## 功能
- 通过解析 AKShare 股票数据字典(`data/stock/stock.html`) 自动识别全部 A 股相关接口。
- 自动将全部 A 股接口清单保存到 `output/a_share_interfaces.csv` 便于后续查阅。
- 提供脚本化入口 `start.py`, 直接运行后会输出接口清单并保存到 `output/` 目录。

## 使用方式
1. 确保已安装依赖 (`pip install -r requirements.txt`)。
2. 直接运行启动脚本:
   ```bash
   python start.py
   ```
3. 输出文件默认保存在仓库根目录下的 `output/` 文件夹。

### 使用代理网络
如需通过代理访问网络资源 (例如本地代理端口为 `7890`), 可以在运行前设置环境变量:

```bash
export ASHARE_HTTP_PROXY="http://127.0.0.1:7890"
export ASHARE_HTTPS_PROXY="http://127.0.0.1:7890"
python start.py
```

脚本启动时会自动把上述配置应用到数据字典抓取流程中。

## 说明
- 由于部分环境的证书链不完整, 数据字典解析默认关闭证书校验; 如需开启, 可在 `DataDictionaryFetcher` 中把 `verify_ssl` 设置为 `True`。
- 接口调用均基于数据字典校验, 若 AKShare 版本变更导致接口缺失会抛出友好的异常提示。
