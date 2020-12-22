import tushare as ts #记得安装tushare包 pip install tushare
ts.set_token('626f578561a8ac48966a9fb33e7987e83750e57ee244f7f0309a9624')#设置token
pro = ts.pro_api()
df = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')
print(df.head())