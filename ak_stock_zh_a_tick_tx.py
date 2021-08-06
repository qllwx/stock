import akshare as ak 
import pandas as pd
import numpy as np 
import os,re
import sqlite3
import tushare as ts 
from datetime import date,timedelta,datetime
from urllib.request import urlretrieve
import time 
from tqdm import tqdm,trange
import sqlalchemy 
import psycopg2 
import os 
import matplotlib.pyplot as plt
from multiprocessing import Pool
import multiprocessing


def get_engine():
    connect_info = 'postgresql+psycopg2://'+user+':'+password+'@'+host+':5432/'+db # +'?charset=utf8'
    engine = sqlalchemy.create_engine(connect_info) #use sqlalchemy to build link-engine
    return engine

def get_tscode():
    data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code')
    return data

def get_connect():
    user=os.getenv("pg_user")
    password=os.getenv("pg_password")
    host=os.getenv("pg_host")
    port=os.getenv("pg_port")
    db=os.getenv("pg_db")
    connect=psycopg2.connect(host=host,user=user,password=password,database=db,port=port,)
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

def get_tick_codes(today,conn):
    sql="select count(*) from %s where 日期 like '%s' "%(code, today)
    res=conn.execute(sql)
    if list(res)[0]>0:
        return code
def is_tick_have_download(conn,date,code):
    sql="select count(*) from %s where 日期 like '%s' "%(code, date)
    res=conn.execute(sql).fetchone()
    if list(res)[0]>0:
        return list(res)[0]  
    else:
        return False

def get_code_from_tables():
    sql="select tablename  from pg_tables where tablename like 's%' and length(tablename)=8;"
    res=conn.execute(sql)
    return res.fetchall()

def put_table(code):
        #code=codes.pop()
        
        today=date.today()
        if datetime.now()<datetime(today.year,today.month,today.day,15,20):
             today=(today+timedelta(days=-1)).strftime('%Y%m%d')
        
        conn=get_conn()
        res=conn.execute("select count(*) from pg_class where relname = '%s'"%code).fetchone()
        #print(code,list(res)[0])
        if list(res)[0]>0:
           res=conn.execute("select count(*) from %s"%code).fetchone()
           if list(res)[0]==0: 
                print("找到空表{}，删除重建{}".format(code,res))
                conn.execute("drop table %s"%code)
                conn.execute("CREATE TABLE %s AS TABLE sz000001 WITH NO DATA;"%code)
           try:
                res=conn.execute("select count(*) from %s where 日期 like '%s'"%(code, today))
           except:
                print(code,today,"没有下载",multiprocessing.current_process().name)
                pass #continue
           #print(code,"have table continue.....")
        else:
            conn.execute("CREATE TABLE %s AS TABLE sz000001 WITH NO DATA;"%code)
            pass
        res=is_tick_have_download(conn,today,code)
        if res:
            print("%s已经下载(%s)，跳过"%(code,res))
            return #continue
        else:
            print("{1}--数据库中不存在{0}的{1}数据".format(code,today))

        df=ak.stock_zh_a_tick_tx(code=code)
        if df.count()[0]<1:
            print("%s没有取到数据 "%code )
            conn.close()
            return #continue
            try:
                df=ak.stock_zh_a_tick_tx_js(code=code)
            except:
                return  #continue
            if df.count()[0]<1:
                print("换一种方法也是一样%s没有取到数据 "%code )
                # continue
        engine=get_engine()
        #delete_blank_table(code,engine,today)   
        df["日期"]=today
        df.set_index(["日期","成交时间"])
        print(df.head)
       
        df.to_sql(code,con=engine,if_exists="append")  
        print(code,"保存入库",df.count()[0])
        engine.dispose()
        conn.close()
        

def ceate_stock_table(code):
    pass
        

def get_engine():
    user=os.getenv("pg_user")
    password=os.getenv("pg_password")
    host=os.getenv("pg_host")
    port=os.getenv("pg_port")
    db=os.getenv("pg_db")
    connect_info = 'postgresql+psycopg2://'+user+':'+password+'@'+host+':5432/'+db
    # +'?charset=utf8'
    engine = sqlalchemy.create_engine(connect_info) #use sqlalchemy to build link-engine
    return engine 

def get_conn():
    conn=get_engine().connect()
    return conn

if __name__ == '__main__':
    engine=get_engine()
    conn=engine.connect()
    res=get_conn().execute('show server_version;')
    print(res.fetchone())
    ts.set_token('626f578561a8ac48966a9fb33e7987e83750e57ee244f7f0309a9624')#设置token
    pro = ts.pro_api()
    codes=get_tscode()
    print(codes)
    today=date.today()
    if datetime.now()<datetime(today.year,today.month,today.day,15,20):
        today=(today+timedelta(days=-1)).strftime('%Y%m%d')
        try:
            res=conn.execute("update %s ")
        except:
            pass


        print("要每天15：20后才能下载当天的数据,现在下载的是上一交易日的数据")
        
    else:
        today=date.today().strftime('%Y%m%d')
        

    code_array=[]
    for code in codes.ts_code:
        code_split=code.split('.')
        code=code_split[1].lower()+code_split[0]
        #put_table(code)
        code_array.append(code)
    print("code_array count is :",len(code_array))
    #codes=ak.stock_zh_ah_name()
    max_worker=multiprocessing.cpu_count()
    
    p=Pool(max_worker)
    try:
        p.map(put_table, code_array)
    except Exception as e:
        
        print(e.args,p,e)
    
    #for code in codes.ts_code:
    #    put_table(code,conn,engine)





"""
        
        code_split=code.split('.')
        code=code_split[1].lower()+code_split[0]
        print(code)
        code_array.append(code)
        try:
            res=conn.execute("select count(*) from %s where 日期 like '%s'"%(code, today))
        except:
            
            continue

        print()
        if list(res.fetchone())[0]>0:
            print("%s已经下载，跳过"%code)
            continue
        df=ak.stock_zh_a_tick_tx(code=code)
        if df.count()[0]<1:
            print("%s没有取到数据 "%code )
            continue
            try:
                df=ak.stock_zh_a_tick_tx_js(code=code)
            except:
                continue
            if df.count()[0]<1:
                print("换一种方法也是一样%s没有取到数据 "%code )
                continue

        delete_blank_table(code,engine,today)   
        df["日期"]=today
        df.set_index(["日期","成交时间"])
        print(df.head)
        df.to_sql(code,con=engine,if_exists="append")
"""        