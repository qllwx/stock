import pandas as pd
import tushare as ts #记得安装tushare包 pip install tushare
import os,datetime,re,time
import sqlite3
from datetime import  date,timedelta

ts.set_token('626f578561a8ac48966a9fb33e7987e83750e57ee244f7f0309a9624')#设置token
pro = ts.pro_api()
conn=sqlite3.connect("stock.db")
cursor=conn.cursor()
today=datetime.date.today().strftime('%Y%m%d') 


def create_basic():
	print("没有基础信息表，添加中...")
	data = pro.query('stock_basic', exchange='', list_status='L', 
		fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
		)
	data.to_sql('basic'+today,conn,if_exists='replace')

create_basic()

reslut=conn.execute("SELECT * FROM basic").fetchall()
if not reslut:
	print("没有基础信息表，添加中...")
	data = pro.query('stock_basic', exchange='', list_status='L', 
		fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
		)
	data.to_sql('basic'+today,conn,if_exists='replace')

#每日行情
reslut=conn.execute("SELECT trade_date FROM daily group by trade_date").fetchall()
have_date=[]
for row in reslut:
	have_date.append(row[0])
print(have_date)
e=pd.bdate_range(date(1991,1,1),date.today())
for d in e:
	if int(d.strftime('%w'))>5:
		print("%s是周未，跳过"%d.strftime('%Y%m%d'))
		continue
	if  (d.strftime('%Y%m%d')  in have_date):
		print(".",end='')
		continue
	d=d.strftime('%Y%m%d')
	df=pro.daily(trade_date=d)
	if df.empty:
		print("%s没有数据"%d)
		continue
	else:
	#df.set_index(['ts_code','trade_date'])
		try:
			df.to_sql('daily',conn,if_exists='append')
			print("增加%s,%s条"%(d,len(df)))
		except:
			print("执行查找后，跳过",d)
			pass
	

#新股列表
start_date=(date.today()+timedelta(days=-700)).strftime('%Y%m%d')
end_date=date.today().strftime('%Y%m%d')
df = pro.new_share(start_date=start_date, end_date=end_date)

df.to_sql('new_share',conn,if_exists='append')

#cursor.execute('create unique index idx_ts_code on new_share(ts_code) ')
conn.commit()

conn.close()