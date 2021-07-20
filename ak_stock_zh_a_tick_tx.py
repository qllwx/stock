import akshare as ak 
import pandas as pd
import numpy as np 
import os,datetime,re,time
import sqlite3
import tushare as ts 
from datetime import date,timedelta
from urllib.request import urlretrieve
import time 
from tqdm import tqdm,trange
import sqlalchemy 
import psycopg2 
import os 
import matplotlib.pyplot as plt



def get_engine():
    connect_info = 'postgresql+psycopg2://'+user+':'+password+'@'+host+':5432/'+db # +'?charset=utf8'
    engine = sqlalchemy.create_engine(connect_info) #use sqlalchemy to build link-engine
    return engine

def get_tscode():
    data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code')
    return data

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

def create_index(code,con):
    sql='create index date_time on  %s (日期,成交时间)'%code
    con.execute(sql)
def delete_blank_table(code,con,today):
    df=pd.read_sql_table(code,con)
    if df.count()[0]<1:
       con.execute("drop table %s"%code)
    else:
        con.execute("delete from  %s where 日期 like '%s'"%(code,today))
if __name__ == '__main__':
    user=os.getenv("pg_user")
    password=os.getenv("pg_password")
    host=os.getenv("pg_host")
    port=os.getenv("pg_port")
    db=os.getenv("pg_db")
    connect_info = 'postgresql+psycopg2://'+user+':'+password+'@'+host+':5432/'+db
    # +'?charset=utf8'
    engine = sqlalchemy.create_engine(connect_info) #use sqlalchemy to build link-engine
    conn=engine.connect()
    res=conn.execute('show server_version;')
    print(res.fetchall())
    ts.set_token('626f578561a8ac48966a9fb33e7987e83750e57ee244f7f0309a9624')#设置token
    pro = ts.pro_api()
    codes=get_tscode()
    print(codes)
    


    code_array=[]
    #codes=ak.stock_zh_ah_name()
    for code in codes.ts_code:
        today=datetime.date.today().strftime('%Y%m%d')
        code_split=code.split('.')
        code=code_split[1].lower()+code_split[0]
        print(code)
        code_array.append(code)
        df=ak.stock_zh_a_tick_tx(code=code)
        if df.count()[0]<1:
            continue

        delete_blank_table(code,engine,today)   
        df["日期"]=today
        df.set_index(["日期","成交时间"])
        print(df.head)
        df.to_sql(code,con=engine,if_exists="append")
        