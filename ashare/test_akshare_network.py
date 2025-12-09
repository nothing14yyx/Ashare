import sys
import os
import traceback

import akshare as ak

try:
    import requests
except ImportError:
    requests = None


def print_env_info():
    print("===== 环境信息 =====")
    print("Python 版本:", sys.version.replace("\n", " "))
    print("AkShare 版本:", getattr(ak, "__version__", "unknown"))
    print("HTTP_PROXY :", os.environ.get("HTTP_PROXY"))
    print("HTTPS_PROXY:", os.environ.get("HTTPS_PROXY"))
    print("====================\n")


def test_akshare_spot():
    print(">>> 测试 akshare.stock_zh_a_spot() 实时行情接口（新浪）")
    try:
        df = ak.stock_hk_spot()
        print("调用成功！DataFrame 形状:", df.shape)
        print("前 5 行预览：")
        print(df.head())
    except Exception as e:
        print("调用失败！异常类型:", type(e).__name__)
        print("异常内容:", repr(e))
        print("详细堆栈：")
        traceback.print_exc()
    print("-" * 60 + "\n")


def _test_url(url: str):
    """简单测试某个 URL 能不能访问。"""
    if requests is None:
        print(f"无法测试 {url}：requests 未安装（pip install requests 后再试）")
        return

    print(f">>> 测试直连 URL: {url}")
    try:
        resp = requests.get(url, timeout=8)
        print("状态码:", resp.status_code)
        text = resp.text[:200].replace("\n", " ").replace("\r", " ")
        print("返回内容前 200 字符预览:")
        print(text)
    except Exception as e:
        print("请求失败！异常类型:", type(e).__name__)
        print("异常内容:", repr(e))
        print("详细堆栈：")
        traceback.print_exc()
    print("-" * 60 + "\n")


def main():
    print_env_info()

    # 1) 先测 akshare 自己的接口
    test_akshare_spot()

    # 2) 再测几个关键网站是否能访问
    print(">>> 开始测试几个关键网站是否能访问（纯网络连通性排查）\n")

    # 新浪实时行情底层常用域名之一
    _test_url("https://hq.sinajs.cn/list=sh000001")

    # 新浪财经站点
    _test_url("https://vip.stock.finance.sina.com.cn")

    # 测一个百度，确认能不能访问国内站点
    _test_url("https://www.baidu.com")

    # 如有需要，也可以顺便测下东方财富（只是网络测试，不会影响你项目不用 em）
    _test_url("https://push2.eastmoney.com/api/qt/stock/get?secid=1.600000&fields=f58,f43")


if __name__ == "__main__":
    main()
