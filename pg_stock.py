import pandas as pd
import numpy as np 
import os,datetime,re,time
import sqlite3
import tushare as ts 
from datetime import date,timedelta
from urllib.request import urlretrieve
import time 
from tqdm import tqdm,trange
from sqlalchemy import create_engine
import psycopg2 
import os 
import matplotlib.pyplot as plt
from   connect  import connect
plt.close("all")

year=date.today().strftime('%Y')

user=os.getenv("pg_user")
password=os.getenv("pg_password")
host=os.getenv("pg_host")
port=os.getenv("pg_port")
db=os.getenv("pg_db")



connect_info = 'postgresql+psycopg2://'+user+':'+password+'@'+host+':5432/'+db # +'?charset=utf8'
engine = create_engine(connect_info) #use sqlalchemy to build link-engine
conn=engine.connect()
ts.set_token('626f578561a8ac48966a9fb33e7987e83750e57ee244f7f0309a9624')#设置token
pro = ts.pro_api()

#connect=sqlite3.connect("stock.db")
#cursor=connect.cursor()
def get_engine():
	connect_info = 'postgresql+psycopg2://'+user+':'+password+'@'+host+':5432/'+db # +'?charset=utf8'
    engine = create_engine(connect_info) #use sqlalchemy to build link-engine
	return engine

def get_connect():
	connect=psycopg2.connect(host=host,
				user=user,
				password=password,
				database=db,
				port=port,
				)
	return connect
def get_cursor():
	con=get_connect()
	cursor=con.cursor()
	return cursor

cursor=get_cursor()
def get_tables():
	sql="SELECT * \
		FROM pg_catalog.pg_tables \
		WHERE schemaname != 'pg_catalog' AND \
		schemaname != 'information_schema';"
	cursor=get_cursor()
	cursor.execute(sql)
	tables=[]
	for t in  list(cursor.fetchall()):
		tables.append(list(t)[1])
	return tables
def get_tables_like(str):
	sql="SELECT * \
		FROM pg_catalog.pg_tables \
		WHERE schemaname != 'pg_catalog' AND \
		schemaname != 'information_schema' AND \
		tablename like '%s%%';"%str
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
	if not is_exists_tab('daily'):
		print("没有每日股票信息表，添加中...")
		for getdate in  get_cal_date():
			get_daily(date=getdate)
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
		#sql="alter table basic add column id SERIAL ;"
		#cursor.execute(sql)
	
def get_dailies():
	for date in get_cal_date():
		get_daily(date=date)

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
	cal_date=get_cal_date()
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
	
def get_change(year=year):
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
def get_max_col(table,col):
	if table in get_tables():
		sql="select max(%s) from %s"%(col,table)
		df=pd.read_sql(sql,conn)
		return df.values[0][0]
	else:
		return None
def drop_tables(strlike):
	tables=get_tables_like(strlike)
	for t in tables:
		print(t)
		sql="drop table %s;"%t
		cursor.execute(sql)
	cursor.close()
	return tables
	

def get_cal_date(year=year):
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
			df_vol=get_col(year,'vol')
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
		id serial not null,
		trade_date char(8) not null,
		basic_id int not null references basic(index) ,
		primary key (id))'''
		cursor.execute(sql)
		print("create table ljqs_")
	for d in cal_date:
		code_list=ljqs_code(date=d,days=3,year=year)
		for c in code_list:
			sql="select index from basic where ts_code='%s';"%c
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
def get_data():
	"""
	helper function to return stock data from last 7 days
	"""
	df = pd.read_sql("""
	SELECT * FROM daily
	WHERE trade_date::date >= (NOW() - '7 day'::INTERVAL)::date
	""", conn)

	# convert to absolute time in seconds
	#df['trade_date'] = df['trade_date'].apply(lambda x: (x-datetime.datetime(1970,1,1)).total_seconds())

	grouped = df.groupby('ts_code')
	unique_names = df.ts_code.unique()
	ys = [grouped.get_group(stock)['pct_chg'] for stock in unique_names]
	xs = [grouped.get_group(stock)['trade_date'] for stock in unique_names]
	vs = [grouped.get_group(stock)['vol'] for stock in unique_names]
	
	return (xs, ys, max_ys, unique_names)
def save_today_all():
	if  is_exists_tab('ts_daily_all'):
		lastday=get_max_col('ts_daily_all','trade_date')
		if lastday<today:
			df=ts.get_today_all()
			df['trade_date']=today
			df.drop_duplicates(['code','trade_date','name'],inplace=True)
			df.to_sql('ts_daily_all',conn,if_exists="append")
	
def create_daily_tmp():
	sql='''
	create table daily_tmp as
	select * from daily 
	where trade_date in'%s' and change>0;
	'''%lastdays()
	df = pd.read_sql(sql, conn)
def pg_get_ljqs():	
	sql='''
begin;
drop table if exists daily_tmp;
drop table if exists daily_tmp_1;
drop table if exists daily_tmp_2;
drop table if exists daily_tmp_3;
drop table if exists today_ljqs;
create or replace function f_get_day(day date) 
returns setof int 
as 
$$
select CURRENT_DATE - day;
$$
language 'sql';
select * from f_get_day('2021212'::date);
select max(trade_date),min(trade_date) from daily;

create table daily_tmp as 
   select *  from daily 
       where change >0 and 
trade_date in(select   trade_date from daily group by trade_date order by trade_date desc limit 4)
;

create table today_ljqs as 
(select  distinct a.ts_code,c.trade_date
     from daily_tmp as a ,daily_tmp as b ,daily_tmp as c
     where 
      a.ts_code= b.ts_code and  a.ts_code=c.ts_code and b.ts_code=c.ts_code and 
      a.trade_date::date =  (b.trade_date::date - '1 day'::INTERVAL)::date   and
     a.trade_date::date = (c.trade_date::date - '2 day'::INTERVAL)::date    and 
     a.vol<b.vol  and b.vol< c.vol
);

select * from ts_daily_all 
where 
trade_date=(select max(trade_date) from daily_tmp) and
code in (select left(ts_code,6) from today_ljqs)
;
commit;
	'''
	cur=get_cursor()
	result=cur.execute(sql)
###############
if __name__ == '__main__':
	get_dailies()
	#save_today_all()
	#get_daily()
	#have_daily()
	#pg_get_ljqs()