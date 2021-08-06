#!/usr/bin/python
import psycopg2
from config import config
import sqlalchemy
import os



def get_engine():
    user=os.getenv("pg_user")
    password=os.getenv("pg_password")
    host=os.getenv("pg_host")
    port=os.getenv("pg_port")
    db=os.getenv("pg_db")
    connect_info = 'postgresql+psycopg2://'+user+':'+password+'@'+host+':5432/'+db # +'?charset=utf8'
    engine = sqlalchemy.create_engine(connect_info) #use sqlalchemy to build link-engine
    return engine.connect()

def connect(filename='database.conf',display=True ):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config(filename=filename)

        # connect to the PostgreSQL server
        if display: print('Connecting to the PostgreSQL database...',filename)
        conn = psycopg2.connect(**params)		
    except (Exception, psycopg2.DatabaseError) as error:
        if display: print(error)
        exit()
    finally:
        if conn is not None:
            if display: print("Database connected")
            return conn
            
def connect_test():
    """ Connect to the PostgreSQL database server """
    conn = connect()
    # create a cursor
    cur = conn.cursor()
      
    # execute a statement
    print('PostgreSQL database version:')
    cur.execute('SELECT version()')
    # display the PostgreSQL database server version
    db_version = cur.fetchone()
    print(db_version)
       
    # close the communication with the PostgreSQL
    cur.close()
    conn.close()
    print('Database connection closed.')

def create_tables(commands):

    execute(commands)

def executes(sql):
    # create table one by one
    for command in commands:
        execute(command)
    # close communication with the PostgreSQL database server
    
def execute(sql,display=False):
    commands=sql
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except Exception as e:
        if display: print(e)
        pass
    # close communication with the PostgreSQL database server
    cur.close()
    # commit the changes
    conn.commit()
    conn.close()

def fetchall(sql):
    conn=connect()
    cur=conn.cursor()
    cur.execute(sql)
    res=cur.fetchall()
    cur.close()
    conn.close()
    return res

if __name__ == '__main__':
    connect_test()