import pandas as pd
import tushare as ts #记得安装tushare包 pip install tushare
import pymysql
pymysql.install_as_MySQLdb() #为了兼容mysqldb
from sqlalchemy import create_engine
import os,datetime,re
ts.set_token('626f578561a8ac48966a9fb33e7987e83750e57ee244f7f0309a9624')#设置token
pro = ts.pro_api()
account=os.getenv("mysql_user")
passw=os.getenv("mysql_password")
ip=os.getenv("mysql_server")
port='3306'
db='stock'
print("mysql://{0}:{1}@{2}:{3}/{4}?charset=gbk".format(account,passw,ip,port,db))
conn=create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=gbk".format(account,passw,ip,port,db))
	#conn='mysql+pymysql://%s:%s@%s:3306/zkzx?charset=utf8mb4'%(account,passw,ip,port,db)
	# Connect to the database
	# Connect to the database
connection = pymysql.connect(host=ip,	user=account,	password=passw,	db=db,	charset='gbk',	cursorclass=pymysql.cursors.DictCursor)
cursor=connection.cursor()
	#create_basic()

# 股票每日交易数据入库
def daily_to_mysql():
    ts.set_token('626f578561a8ac48966a9fb33e7987e83750e57ee244f7f0309a9624')#设置token
    pro = ts.pro_api()
    df = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')
    df.to_sql('stock_daily_basic',con=conn,if_exists='append',index=False)
    
def daily_to_msyql_code(ts_code,start,end):
	if isnull(start):start=datetime.date.today()
	if  isnull(end):end=start
	df = pro.daily(ts_code=ts_code, start_date=start, end_date=end)
	cursor=connection.cursor()
	for row in df:
		ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg,vol,amount =row.values[0]
		sql="INSERT INTO stock_daily_basic (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount) VALUES (%(ts_code)s, %(trade_date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(pre_close)s, %(change)s, %(pct_chg)s, %(vol)s, %(amount)s)"
		cursor.execute(sql)
	connection.commit()      

def put_basic():
	data = pro.query('stock_basic',
		exchange='', list_status='L', 
		fields='ts_code,symbol,name,area,industry')
	for row in data:
		
	
	
	
if __name__ == "__main__":
	

	
	connection.close()