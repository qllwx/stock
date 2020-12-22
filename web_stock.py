from flask import Flask,escape,url_for,request
from flask import render_template
import os,datetime
import sqlite3
import pandas as pd
from flask import g
from datetime import date,timedelta
app=Flask(__name__)
DATABASE = './stock.db'

def connect_db():
    return sqlite3.connect(DATABASE)

@app.before_request
def before_request():
    g.db = connect_db()

@app.teardown_request
def teardown_request(exception):
    if hasattr(g, 'db'):
        g.db.close()
def get_connection():
    db = getattr(g, '_db', None)
    if db is None:
        db = g._db = connect_db()
    return db
    
def query_db(query, args=(), one=False):
    cur = g.db.execute(query, args)
    rv = [dict((cur.description[idx][0], value)
               for idx, value in enumerate(row)) for row in cur.fetchall()]
    return (rv[0] if rv else None) if one else rv   
    



if datetime.datetime.now().hour<15:
	today=(date.today()+timedelta(days=-1)).strftime('%Y%m%d')
else:
	today=datetime.date.today().strftime('%Y%m%d') 

@app.route('/')
def index():
    return 'index'

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        return do_the_login()
    else:
        return show_the_login_form()

@app.route('/user/<username>')
def profile(username):
    return '{}\'s profile'.format(escape(username))

with app.test_request_context():
    print(url_for('index'))
    print(url_for('login'))
    print(url_for('login', next='/'))
    print(url_for('profile', username='John Doe'))
@app.route('/post/<int:post_id>')
def show_post(post_id):
    # show the post with the given id, the id is an integer
    return 'Post %d' % post_id

@app.route('/path/<path:subpath>')
def show_subpath(subpath):
    # show the subpath after /path/
    return 'Subpath %s' % escape(subpath)

@app.route('/projects/')
def projects():
    return 'The project page'

@app.route('/about')
def about():
    return 'The about page'
    
    
@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
    return render_template('hello.html', name=name)

@app.route('/stock/')
@app.route('/stock/<name>')
def stock(name=None):
    con=sqlite3.connect('stock.db')
    con.row_factory=sqlite3.Row
    cur=con.cursor()
    ts_codes=[]
    df_name='df_ljqs_'+today
    sql='select ts_code from %s;'%df_name
    cur.execute(sql)
    rows=cur.fetchall()
    for row in rows:
         ts_codes.append(row['ts_code'].split('.')[1].lower()+row['ts_code'].split('.')[0])
    images=[]
    for img in ts_codes:
        img_url='http://image.sinajs.cn/newchart/daily/n/'+img+'.gif'
        images.append(img_url)
    return render_template('stock.html', name=images)