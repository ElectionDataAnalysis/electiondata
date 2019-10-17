#!usr/bin/python

from flask import Flask
import psycopg2
import re
import state
from pathlib import Path
import sys
	
from datetime import datetime
import os

import query_create as q

host_name = 'db'
user_name = 'postgres'
password = 'notverysecure'
database = 'tmp'

def establish_connection():
    print ("establishing connection to database...")
    # the connect() function returns a new instance of connection
    conn = psycopg2.connect(host = host_name, user = user_name, password = password, dbname = database)
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

@app.route('/')
def hello():
# set global log file
	now=datetime.now()
	now_str=now.strftime('%Y%b%d%H%M')
	logfilepath = 'hello'+now_str+'.log'
	with open(logfilepath,'a') as sys.stdout:

	# define the state of NC and how to parse its metadata files
		nc_meta_p= re.compile(r"""
		(?P<field>.*\S+\b)        # capture field
		\s\s+                    # skip all more-than-one whitespace
		(?P<type>[a-z]+)       # capture type, including number in parens if there
		(?P<number>\(\d+\))?
		\s+                     # skip all whitespace
		(?P<comment>.*)        # capture remaining part of the line, not including end-of-line
		""",re.VERBOSE)
		nc_type_map = {'number':'INT', 'text':'varchar', 'char':'varchar'}
		nc = state.State("NC","North Carolina",nc_meta_p,nc_type_map)

	# *** hard-code arguments for now
		s = nc
		f = 'local_data/NC/meta/mod_layout_results_pct.txt'
		t = 'results'   # name of table
		check_args(s,f,t)
		[drop_query,create_query] = q.create_table(t,f,'psql',s)
		return(" ".join([drop_query,create_query]))
		

	
	
	
	   # db_connection = establish_connection()
		#connection_cursor = create_cursor(db_connection)

		#db_connection.commit()

		#if connection_cursor:
		#    connection_cursor.close()
		#if db_connection:
		#    db_connection.close()
