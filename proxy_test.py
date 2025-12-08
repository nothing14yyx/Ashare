import os
import requests
import akshare as ak

# ========= 1. 配置你的代理地址 =========
# 如果你的代理端口不是 7890，在这里改
PROXY_URL = "http://127.0.0.1:7890"

# ========= 2. 设置环境变量，让 Python/akshare 都走这个代理 =========
os.environ["HTTP_PROXY"] = PROXY_URL
os.environ["HTTPS_PROXY"] = PROXY_URL
os.environ["ALL_PROXY"] = PROXY_URL
os.environ["http_proxy"] = PROXY_URL
os.environ["https_proxy"] = PROXY_URL
os.environ["all_proxy"] = PROXY_URL

print("当前进程代理相关环境变量：")
for k in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
          "http_proxy", "https_proxy", "all_proxy"]:
    print(f"  {k} = {os.environ.get(k)}")

# ========= 3. 先用代理测一个简单网站，确认代理本身没问题 =========

def test_proxy_basic():
    print("\n=== [1] 代理连通性测试：访问 https://www.baidu.com ===")
    try:
        r = requests.get("https://www.baidu.com", timeout=10)
        print("status:", r.status_code)
        print("前 100 字符:", r.text[:100])
    except Exception as e:
        print("[ERROR] 通过代理访问百度失败：", repr(e))

# ========= 4. 用代理访问 EM 原始 URL（和浏览器一样） =========

def test_proxy_em_raw():
    print("\n=== [2] 代理 + requests 测试：EM 原始 URL ===")

    url = (
        "https://82.push2.eastmoney.com/api/qt/clist/get"
        "?pn=1&pz=50&po=1&np=1"
        "&ut=bd1d9ddb04089700cf9c27f6f7426281"
        "&fltt=2&invt=2&fid=f12"
        "&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
        "&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,"
        "f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,"
        "f22,f11,f62,f128,f136,f115,f152"
    )

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        r = requests.get(url, headers=headers, timeout=10)
        print("status:", r.status_code)
        print("前 200 字符:", r.text[:200])
    except Exception as e:
        print("[ERROR] 通过代理访问 EM clist 失败：", repr(e))

# ========= 5. 用代理测试 ak.stock_zh_a_spot_em =========

def test_proxy_ak_spot_em():
    print("\n=== [3] 代理 + akshare 测试：ak.stock_zh_a_spot_em ===")
    try:
        df = ak.stock_zh_a_spot_em()
        print("返回行数:", len(df))
        print(df.head())
    except Exception as e:
        print("[ERROR] ak.stock_zh_a_spot_em 通过代理访问失败：", repr(e))


def main():
    test_proxy_basic()
    test_proxy_em_raw()
    test_proxy_ak_spot_em()


if __name__ == "__main__":
    main()
