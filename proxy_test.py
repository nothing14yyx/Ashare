import os
import requests

PROXY = "http://127.0.0.1:7890"  # 用你 tesst.py 里测过的那个

os.environ["HTTP_PROXY"] = PROXY
os.environ["HTTPS_PROXY"] = PROXY

print("当前代理:", PROXY)

url = (
    "https://82.push2.eastmoney.com/api/qt/clist/get"
    "?pn=1&pz=1&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281"
    "&fltt=2&invt=2&fid=f12&fs=m:0+t:6&fields=f1,f2"
)

resp = requests.get(url, timeout=10)
print("status:", resp.status_code)
print("text:", resp.text[:100])
