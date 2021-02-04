import json

import requests

symbols = "CTR;FPT;BMP;VHC;VTP;NLG;CTG"
res = requests.get(
    "https://solieu3.mediacdn.vn/ProxyHandler.ashx?RequestName=StockSymbolSlide&RequestType=json&sym=%s" % symbols
).text
substring = json.loads('{' + res[res.find('({') + len('({'):res.find('})')] + '}')["Symbols"]
print(json.dumps(substring))
