# tushare_test.py
import tushare as ts
import yaml

# 加载 token
with open("../Config/config.yaml", "r") as f:
    config = yaml.safe_load(f)
    ts.set_token(config["tushare_token"])
    pro = ts.pro_api()

# 快速测试 trade_cal 接口
df = pro.trade_cal(exchange='SSE', start_date='20240101', end_date='20240110')
print(df.head())
