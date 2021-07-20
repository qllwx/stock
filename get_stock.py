import pandas as pd
import os,datetime,re,time
import sqlite3
import tushare as ts 
from datetime import date,timedelta
from urllib.request import urlretrieve
import connect
os.makedirs('./image/',exist_ok=True)
os.makedirs('./image/min/',exist_ok=True)
os.makedirs('./image/daily',exist_ok=True)
os.makedirs('./image/weekly',exist_ok=True)
os.makedirs('./image/monthly',exist_ok=True)
os.makedirs('./image/sina/',exist_ok=True)

ts.set_token('626f578561a8ac48966a9fb33e7987e83750e57ee244f7f0309a9624')#设置token
pro = ts.pro_api()

if datetime.datetime.now().hour<15:
	today=(date.today()+timedelta(days=-1)).strftime('%Y%m%d')
else:
	today=datetime.date.today().strftime('%Y%m%d') 
os.makedirs('./'+today,exist_ok=True)

df = pro.query('trade_cal', start_date='20200101', end_date=today)
cal_date=list(df[df.is_open==1]['cal_date'])

conn=sqlite3.connect("stock.db")
#conn=connect.connect()
cursor=conn.cursor()

def lastdays(days=7):
	day_list=[]
	for i in range(days):
		day_list.append(cal_date[-i-1])
	return day_list
	
def  change_rise(days):
	df_result=pd.DataFrame()
	list_day=['ts_code']
	for day in lastdays(days):
		#print(day)
		#df=pro.daily(trade_date=day)
		sql="SELECT ts_code,change FROM daily where trade_date=? and change>0"
		df=pd.read_sql(sql,conn,params=(day,))
		print(day,df.count())
		
		if  not df.empty:
			if df_result.empty:
				df_result=df
				list_day.append(day)
				continue
			df_result=pd.merge(df_result,df,on=['ts_code'])
			#print("result",df_result)
			list_day.append(day)
	df_result.columns=list_day
	return df_result
def  vol_rise(days):
	df_result=pd.DataFrame()
	list_day=['ts_code']
	for day in lastdays(days):
		#print(day)
		#df=pro.daily(trade_date=day)
		sql="SELECT ts_code,vol FROM daily where trade_date=? "
		df=pd.read_sql(sql,conn,params=(day,))
		print(day,df.count())
		
		if  not df.empty:
			if df_result.empty:
				df_result=df
				list_day.append('d_'+day)
				continue
			df_result=pd.merge(df_result,df,on=['ts_code'])
			#print("result",df_result)
			list_day.append('d_'+day)
	df_result.columns=list_day
	return df_result
def is_exists_table(name):
	sql="select name from sqlite_master where type='table' and name=?"
	result=pd.read_sql(sql,conn,params=(name,))
	return not result.empty
	
def get_ljqs(date=today,days=5):
	df_name='df_change_'+today
	if is_exists_table(df_name):
		sql='select * from %s;'%df_name
		df_change=pd.read_sql(sql,conn)
	else:
		df_change=change_rise(days)
		df_change.to_sql(df_name,conn,if_exists='replace')
	#print("change列表",change_rise(3))
	df_name='df_vol_'+today
	if is_exists_table(df_name):
		sql='select * from %s;'%df_name
		df=pd.read_sql(sql,conn)
	else:
		df=vol_rise(days)
	df_name='df_ljqs_'+today
	if is_exists_table(df_name):
		sql='select * from %s;'%df_name
		df_ljqs=pd.read_sql(sql,conn)
		
	else:
		tj=[]
		volist=list(df.columns)
		ii=volist.index('ts_code')+1
		for i in range(ii,len(volist)-1):
			tj.append(volist[i]+ ' > ' + volist[i+1])
		print(tj)	
		tj_str=''
		for row in tj: 
			tj_str=tj_str+ row +' and '
		tj_str=tj_str[:-5]
		print(tj_str)
		df_vol=df.query(tj_str)
		df_vol.to_sql(df_name,conn,if_exists='replace')
		df_ljqs=pd.merge(df_change,df_vol,on=['ts_code'])
		df_ljqs.to_sql('df_ljqs_'+today,conn,if_exists='replace')
		print("量价齐升列表",df_ljqs)
	for row in list(df_ljqs['ts_code']):
		ts_code=row.split('.')[1].lower()+row.split('.')[0]
		get_sina(ts_code=ts_code)
	return df_ljqs
	
def get_sina(ts_code='sh000001'):
	type_lists=['min','daily','weekly','monthly']
	for type in type_lists:
		img_url='http://image.sinajs.cn/newchart/'+type+'/n/'+ts_code+'.gif'
		img_save='./image/'+type+'/'+ts_code+'.gif'
		urlretrieve(img_url, img_save)
	
get_ljqs()
	
