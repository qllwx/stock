import pandas as pd
import os,datetime,re,time
import sqlite3
import tushare as ts 
from datetime import date,timedelta
from urllib.request import urlretrieve
import time 
from tqdm import tqdm,trange
from sqlalchemy import create_engine
import pymysql
import os 
import matplotlib.pyplot as plt
plt.close("all")

year=date.today().strftime('%Y')
account=os.getenv("mysql_user")
passw=os.getenv("mysql_password")
ip=os.getenv("mysql_server")
port=3306
db='stock'
connect_info = 'mysql+pymysql://'+account+':'+passw+'@syzx:3306/stock?charset=utf8'
engine = create_engine(connect_info) #use sqlalchemy to build link-engine
conn=engine
ts.set_token('626f578561a8ac48966a9fb33e7987e83750e57ee244f7f0309a9624')#设置token
pro = ts.pro_api()

#connect=sqlite3.connect("stock.db")
#cursor=connect.cursor()

def get_connect():
	connect=pymysql.connect(host=ip,
				user=account,
				password=passw,
				database=db,
				port=port,
				charset="utf8")
	return connect
def get_cursor():
	con=get_connect()
	cursor=con.cursor()
	return cursor

cursor=get_cursor()
def get_tables():
	sql="show tables;"
	cursor=get_cursor()
	cursor.execute(sql)
	tables=[]
	for t in  list(cursor.fetchall()):
		tables.append(list(t)[0])
	return tables
tables=get_tables()
def is_exists_tab(tn):	
	return tn  in get_tables()

def mk_subdir():
	os.makedirs('./image/',exist_ok=True)
	os.makedirs('./image/min/',exist_ok=True)
	os.makedirs('./image/daily',exist_ok=True)
	os.makedirs('./image/weekly',exist_ok=True)
	os.makedirs('./image/monthly',exist_ok=True)
	os.makedirs('./image/sina/',exist_ok=True)


def get_today():
	if datetime.datetime.now().hour<15:
		today=(date.today()+timedelta(days=-1)).strftime('%Y%m%d')
	else:
		today=datetime.date.today().strftime('%Y%m%d') 
	os.makedirs('./'+today,exist_ok=True)
	return today


today=get_today()
def   have_daily(date=today):
	cur=get_cursor()
	sql="select * from daily where trade_date = '%s';"%date
	result=cur.execute(sql)
	if result==0:
		get_daily(date)
	cur.close()	
def get_basic():
	if  not is_exists_tab('basic'):
		print("没有基础信息表，添加中...")
		data = pro.query('stock_basic', exchange='', list_status='L', 
			fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
			)
		data.to_sql('basic',con=engine,if_exists='replace')
		sql="alter table basic add column id int primary key not null auto_increment ;"
		cursor.execute(sql)
	
def get_daily(date=today):
	df=pro.daily(trade_date=date)
	if df.empty:
		print("%s没有数据"%date)
		
	else:
		try:
			df.to_sql('daily',conn,if_exists='append')
			print("增加%s,%s条"%(date,len(df)))
		except:
			print("执行查找后，跳过",date)
			pass
		
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
		sql="SELECT ts_code,close-pre_close as 'change' FROM daily where trade_date='%s' and close-pre_close>0"%day
		df=pd.read_sql(sql,conn)
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
		sql="SELECT ts_code,vol FROM daily where trade_date='%s' "%day
		df=pd.read_sql(sql,conn)
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

	
def get_ljqs(date=today,days=5):
	df_name='df_change_'+today
	if df_name in tables:
		sql='select * from %s;'%df_name
		df_change=pd.read_sql(sql,conn)
	else:
		df_change=change_rise(days)
		df_change.to_sql(df_name,conn,if_exists='replace')
	#print("change列表",change_rise(3))
	df_name='df_vol_'+today
	if df_name in get_tables():
		sql='select * from %s;'%df_name
		df=pd.read_sql(sql,conn)
	else:
		df=vol_rise(days)
	df_name='df_ljqs_'+today
	if df_name in get_tables():
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
	
def get_change(year='2020'):
	start_date=year+'0101'
	end_date=year+'1231'
	df = pro.query('trade_cal', start_date=start_date, end_date=end_date)
	cal_date=list(df[df.is_open==1]['cal_date'])
	df_result=pd.DataFrame()
	list_day=['ts_code']
	print('get_%s_change'%year)
	for day  in tqdm(cal_date):
		
		sql="SELECT ts_code,close-pre_close as 'td_change' FROM daily where trade_date='%s' ;"%day
		df=pd.read_sql(sql,conn)
		
		if  not df.empty:
			if df_result.empty:
				df_result=df
				list_day.append(day)
				continue
			df_result=pd.merge(df_result,df,on=['ts_code'],how='outer')
			#print("result",df_result)
			list_day.append(day)
	df_result.columns=list_day
	return df_result
	

	
def  tabTdf(table):
	sql="select * from  %s"%table
	df_result=pd.read_sql(sql,conn)
	df_t=df_result.T
	df=df_t.iloc[1:,]
	df.columns=df_t.iloc[0,]
	return df
def tab2df(table):
	sql="select * from  %s"%table
	df_result=pd.read_sql(sql,conn)
	return df_result
	
def get_columns(table):
	if table in get_tables():
		sql="SELECT * FROM "+ table
		df=pd.read_sql(sql,conn)
		return df.columns
	else:
		return []
def drop_tables(strlike):
	cursor=get_cursor()
	sql="show tables;"
	cursor.execute(sql)
	tables=[]
	for t in  list(cursor.fetchall()):
		if strlike in t[0]:
			tables.append(t[0])
			
	for t in tables:
		print(t)
		sql="drop table %s;"%t
		cursor.execute(sql)
	cursor.close()
	return tables
	

def get_cal_date(year='2020'):
	start_date=year+'0101'
	end_date=year+'1231'
	df = pro.query('trade_cal', start_date=start_date, end_date=end_date)
	cal_date=list(df[df.is_open==1]['cal_date'])
	return cal_date
def   column_to_df(year,col_):
	cal_date=get_cal_date(year=year)
	df_result=pd.DataFrame()
	list_day=['ts_code']
	print('get_%s_%s'%(year,col_))
	for day  in tqdm(cal_date):
		sql="SELECT ts_code,%s FROM daily where trade_date='%s';"%(col_,day)
		df=pd.read_sql(sql,conn)
		
		if  not df.empty:
			if df_result.empty:
				df_result=df
				list_day.append(day)
				continue
			df_result=pd.merge(df_result,df,on=['ts_code'],how='outer')
			#print("result",df_result)
			list_day.append(day)
	df_result.columns=list_day
	return df_result

def add_column2table(year,col):
	cur=get_cursor()
	tab_name=col+"_"+str(year)
	sql="desc %s;"%tab_name
	cur.execute(sql)
	date_list=list(cur.fetchall())
	col_last_date=date_list[-1][0]
	sql="select max(trade_date) from daily;"
	cur.execute(sql)
	date_list=list(cur.fetchall())
	last_date=date_list[-1][0]
	if col_last_date<last_date:
		sql="select ts_code,%s from  daily where trade_date= '%s';"%(col,last_date)
		df_col=pd.read_sql(sql,conn)
		if not df_col.empty:
			df_result=pd.merge(df_result,df_col,on=['ts_code'],how='outer')
	
	
#drop_tables('vol_')
#drop_tables('change_')
def get_all_vol_change():
	begin_time=time.time()
	for y in  range(1991,2021):
		year=str(y)
		table_name1='vol_'+year
		if  not table_name1 in get_tables():
			df_vol=get_vol(year)
			df_vol.to_sql(table_name1,conn,if_exists='replace',index=False)
			print(df_vol)
		else:
			print(table_name1,'共计交易天数',len(get_columns(table_name1)))
		table_name2='change_'+year	
		if  not table_name2 in get_tables():
			df_change=get_change(year)
			df_change.to_sql(table_name2,conn,if_exists='replace',index=False)
			print(df_change)
		else:
			print(table_name2,"共计交易天数",len(get_columns(table_name2)))
		
		print('累计用时%s秒'%int(time.time()-begin_time))

def get_col(year,col):
	begin_time=time.time()
	year=str(year)
	table_name=col+"_"+year
	if  not table_name in get_tables():
		df_col=column_to_df(year,col)
		df_col.to_sql(table_name,conn,if_exists='replace',index=False)
		print(df_col)
	else:
		print(table_name,'共计交易天数',len(get_columns(table_name)))
		
	print('累计用时%s秒'%int(time.time()-begin_time))


def draw(s1,s2,s3,s4):
	fig=plt.figure(num=1,figsize=(4,4))
	ax1=fig.add_subplot(221)
	ax1.plot(s1)
	ax2=fig.add_subplot(222)
	ax2.plot(s2)
	ax3=fig.add_subplot(223)
	ax3.plot(s3)
	ax4=fig.add_subplot(224)
	ax4.plot(s4)
	plt.show()


def get_continue(df,days=3,end_date=today):
	tscode=[]
	a=df.index.tolist()
	print(len(a),a.index(end_date))
	Move_forward=a.index(end_date)-len(a)
	print("len=",len(df.columns.tolist()))	
	for col in trange(3,len(df.columns)):
		begin_row=Move_forward-days-1
		f=df.iloc[begin_row,col]
		isget=True
		tsc=[]
		tsc.append(f)
		print("begin_row=",begin_row)
		for row in range(begin_row+1,Move_forward+1):
			
			tmp=df.iloc[row,col]
			#print("row=%s,col=%s,tmp=%s"%(row,col,tmp))
			if f<tmp:
				f=tmp
				tsc.append(f)
			else:
				isget=False
		if isget:
			tscode.append(df.columns[col])
			print(tsc)
	print('continues:',len(tscode))
	return tscode

def list_jj(a,b):
	tmp=list(set(a).intersection(set(b)))
	return tmp

def ljqs_code(date=today,days=3,year='2020'):
	df_vol=tabTdf('vol_'+year)
	#df_cha=tabTdf('change_'+year)
	df_ope=tabTdf('open_'+year)
	jj=list_jj(get_continue(df=df_vol,days=days,end_date=date),get_continue(df=df_ope,days=days,end_date=date))
	return jj
	
	

def get_ljqs_year(year='2020'):
	cal_date=get_cal_date(year)
	cursor=get_cursor()
	if not is_exists_tab('ljqs'):
		sql='''CREATE TABLE  IF NOT EXISTS ljqs_(
		id smallint unsigned not null auto_increment,
		trade_date char(8) not null,
		basic_id int not null references basic(id) ,
		primary key (id))'''
		cursor.execute(sql)
		print("create table ljqs_")
	for d in cal_date:
		code_list=ljqs_code(date=d,days=3,year=year)
		for c in code_list:
			sql="select id from basic where ts_code='%s';"%c
			result=cursor.execute(sql)
			if result>0:
				rr=cursor.fetchall()
				r=int(rr[0][0])
				sql="insert into ljqs_(trade_date,basic_id) values('%s',%d);"%(d,r)
				print(sql)
				cursor.execute(sql)
				
	cursor.close()
	con.close()
def get_code_id(codelist):
	pass
def get_all_col(year='2020'):
	columns=['open','high','pct_chg','amount']
	for r in columns:
		get_col(year,r)
	

def get_all_df(year):
	df_vol=tabTdf('vol_'+year)
	df_cha=tabTdf('change_'+year)
	df_ope=tabTdf('open_'+year)
	df_pct=tabTdf('pct_chg_'+year)
	df_amo=tabTdf('amount_'+year)
	vol=df_vol.iloc[:,1]
	cha=df_cha.iloc[:,1]
	ope=df_ope.iloc[:,1]
	pct=df_pct.iloc[:,1]
	amo=df_amo.iloc[:,1]
	
	
	ope.plot()
	plt.show()
	
	draw(vol,cha,ope,amo)

###############
if __name__ == '__main__':
	drop_tables('ljqs')	
	#get_basic()
	have_daily()
	#get_ljqs_year()
	get_ljqs_year()