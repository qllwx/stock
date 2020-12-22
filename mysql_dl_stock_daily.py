#coding=utf-8
import pandas as pd
import tushare as ts #记得安装tushare包 pip install tushare
import os,datetime,re,time
from datetime import  date,timedelta
from sqlalchemy import create_engine
import pymysql,os

account=os.getenv("mysql_user")
passw=os.getenv("mysql_password")
ip=os.getenv("mysql_server")
port=3306
db='stock'
conn=pymysql.connect(host=ip,
				user=account,
				password=passw,
				database=db,
				port=port,
				charset="utf8")
cursor=conn.cursor()
connect_info = 'mysql+pymysql://'+account+':'+passw+'@syzx:3306/stock?charset=utf8'
engine = create_engine(connect_info) #use sqlalchemy to build link-engine
ts.set_token('626f578561a8ac48966a9fb33e7987e83750e57ee244f7f0309a9624')#设置token
pro = ts.pro_api()


today=datetime.date.today().strftime('%Y%m%d') 
cursor.execute("SELECT * FROM basic")
reslut=cursor.fetchall()
if not reslut:
	print("没有基础信息表，添加中...")
	data = pro.query('stock_basic', exchange='', list_status='L', 
		fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
		)
	data.to_sql('basic'+today,con=engine,if_exists='replace')

#每日行情
cursor.execute("SELECT trade_date FROM daily group by trade_date")
reslut=cursor.fetchall()
have_date=[]
for row in reslut:
	have_date.append(row[0])
if len(have_date)>0:
	
	start_date=datetime.datetime.strptime(max(reslut)[0],'%Y%m%d')
	e=pd.bdate_range(start_date,date.today())
else:
	e=pd.bdate_range(date(1991,1,1),date.today())

for d in e:
	if int(d.strftime('%w'))>5 | int(d.strftime('%w'))==0:
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
			df.to_sql('daily',con=engine,if_exists='append')
			print("增加%s,%s条"%(d,len(df)))
		except:
			print("执行查找后，跳过",d)
			pass
	

#新股列表
start_date=(date.today()+timedelta(days=-700)).strftime('%Y%m%d')
end_date=date.today().strftime('%Y%m%d')
df = pro.new_share(start_date=start_date, end_date=end_date)
try:
	df.to_sql('new_share',con=engine,if_exists='append')
except:
	pass

#cursor.execute('create unique index idx_ts_code on new_share(ts_code) ')
#cursor.execute("create index idx_td on daily(trade_date)")
conn.commit()
cursor.close()
conn.close()