#!usr/bin/python

from flask import Flask
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # allows db creation, deletion
import re
import state
from pathlib import Path
import sys

from datetime import datetime
import os

import query_create as q

## define some basics
def path_to_file(path_to_dir,filename):
    if path_to_dir[-1] == '/':
        out = path_to_dir
    else:
        out = path_to_dir+'/'
    return(out+filename)

def establish_connection(db_name='postgres'):
    host_name = 'db'
    user_name = 'postgres'
    password = 'notverysecure'

    # the connect() function returns a new instance of connection
    conn = psycopg2.connect(host = host_name, user = user_name, password = password, database = db_name)
    return conn

def create_cursor(connection):
    # create a new cursor with the connection object.
    cur = connection.cursor()
    return cur

def check_args(s,f,t):
    if not isinstance(s,state.State):
        return('Error: '+s+' is not a known state.')
        sys.exit()
    mypath=Path(f)
    if not mypath.is_file():
        return('Error: File '+f+' does not exist.')
        sys.exit()
    # *** check t for whitespace
    return (s,f,t)




app = Flask(__name__)

@app.route('/build')
def build():
# initialize report
    report=[]
# set global log file
    now=datetime.now()
    now_str=now.strftime('%Y%b%d%H%M')
    logfilepath = 'local_logs/hello'+now_str+'.log'
    with open(logfilepath,'a') as sys.stdout:

    # create the state of NC
        nc = state.create_instance('NC')

    # *** hard-code arguments for now
        s = nc
        f = 'local_data/NC/meta/mod_layout_results_pct.txt'
        t = 'results'   # name of table
        check_args(s,f,t)
        [drop_query,create_query] = q.create_table(t,f,'psql',s)
  
  
        # connect and create db for the state
        conn = establish_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        query = 'DROP DATABASE IF EXISTS '+s.db_name
        cur.execute(query)
        report.append(query)
        
        query = 'CREATE DATABASE '+s.db_name
        cur.execute(query)
        report.append(query)
        if cur:
            cur.close()
        if conn:
            conn.close()

        # connect to the state db
        report.append('Connect to database '+s.db_name)
        conn = establish_connection(s.db_name)
        cur = conn.cursor()

        cur.execute(drop_query)
        report.append(drop_query)
        cur.execute(create_query)
        report.append(create_query)

        conn.commit()
        # close connection
        if cur:
            cur.close()
        if conn:
            conn.close()
        return("<p>"+"</p><p>  ".join(report))


@app.route('/fill')
def fill():
    
    # create the state of NC *** is it really necessary to do this in each @app.route?
    nc = state.create_instance('NC')
    
    # connect to the state db
    conn = establish_connection(nc.db_name)
    cur = conn.cursor()

    # create the state of NC *** is it really necessary to do this in each @app.route?
    nc = state.create_instance('NC')
    # load the data
    load_query = q.load_data('results')
    with open(path_to_file(nc.path_to_data,'trunc_results_pct_20181106.txt'),'r') as f:
        cur.copy_expert(load_query,f)

    # close connection
    if cur:
        cur.close()
    if conn:
        conn.close()
    return load_query

