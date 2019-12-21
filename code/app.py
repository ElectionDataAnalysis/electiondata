#!usr/bin/python3

### next four lines are necessary to install numpy and pandas packages for some reason...
import os
os.system("pip install --upgrade pip")
os.system("pip install pandas")
os.system("pip install numpy")

import numpy as np
import pandas as pd
from flask import Flask
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # allows db creation, deletion
from psycopg2 import sql
import re
import states_and_files as sf
from pathlib import Path
import sys
# do we need numpy? If not, remove from requirements

from datetime import datetime

import db_routines as dbr
import clean as cl
import context

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
    if not isinstance(s,sf.State):
        return('Error: '+s+' is not a known state.')
        sys.exit()
    mypath=Path(f)
    if not mypath.is_file():
        return('Error: File '+f+' does not exist.')
        sys.exit()
    # *** check t for whitespace
    return (s,f,t)

def create_schema(s):
    # connect and create schema for the state
    conn = establish_connection()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    cur.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(s.schema_name)))
    cur.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(s.schema_name)))
    
    if cur:
        cur.close()
    if conn:
        conn.close()

##########################################

app = Flask(__name__)

@app.route('/raw_data')
def raw_data():
# initialize rs for logging
    rs=[str(datetime.now())]
#    tables = [['results_pct','utf8'],['absentee','utf16']] # varies by state *** name and encoding of metadata file
        # note: 'absentee' needs better data cleaning
#    fmeta = {'results_pct':'layout_results_pct.txt','absentee':'sfs_by_hand_layout_absentee.txt'}  # name of metadata file; varies by state ***
#    fdata = {'results_pct':'results_pct_20181106.txt','absentee':'absentee_20181106.csv'} # name of data file, varies by state and election ***
# set global log file
    now=datetime.now()
    now_str=now.strftime('%Y%b%d%H%M')
    logfilepath = 'local_logs/hello'+now_str+'.log'
    with open(logfilepath,'a') as sys.stdout:

    # instantiate state of NC
        s = sf.create_state('NC','local_data/NC')
    # instantiate the NC datafiles
        datafiles = [sf.create_datafile(s,'General Election 2018-11-06','results_pct_20181106.txt',{}), sf.create_datafile(s,'General Election 2018-11-06','absentee_20181106.csv',{})]

    # create the schema for the state
        create_schema(s)

    # connect to the state schema
        rs.append('Connect to database')
        conn = establish_connection()
        cur = conn.cursor()
        
        for df in datafiles:
            t = df.table_name   # name of table
            e = df.metafile_encoding
            fpath = 'local_data/NC/meta/'+df.metafile_name   # this path is outside the docker container.
            check_args(s,fpath,t)   # checking s is redundant
        
        # clean the metadata file
            fpath = cl.extract_first_col_defs(fpath,'local_data/tmp/',e)

        # create table and commit
 
            cur.execute(sql.SQL('DROP TABLE IF EXISTS {}.{}').format(sql.Identifier(s.schema_name),sql.Identifier(t)))
            [query,strs,sql_ids] = dbr.create_table(df)
            format_args = [sql.Identifier(x) for x in sql_ids]
            cur.execute(sql.SQL(query).format( *format_args ),strs)
            diagnostic = sql.SQL(query).format( *format_args ).as_string(conn)
            conn.commit()

        # correct any errors due to foibles of particular datafile and commit
            for query in df.correction_query_list:
                cur.execute(sql.SQL(query).format(sql.Identifier(s.schema_name), sql.Identifier(df.table_name)))
                rs.append(query)
                conn.commit()

    # load data into tables
            dbr.load_data(conn,cur,s,df)
            rs.append('Data from file '+df.file_name+' loaded into table '+s.schema_name+'.'+df.table_name)
    
    # close connection
        if cur:
            cur.close()
        if conn:
            conn.close()
        return("<p>"+"</p><p>  ".join(rs))
        
        
@app.route('/file_to_context')
def file_to_context():
# initialize report for logging
    rs=[str(datetime.now())]
# instantiate state of NC
    s = sf.create_state('NC','local_data/NC')
# instantiate the NC pct_result datafile
    df = sf.create_datafile(s,'General Election 2018-11-06','results_pct_20181106.txt',{})
# instantiate the 'nc_export1' munger
    m = sf.create_munger('local_data/mungers/nc_export1.txt')
    
# connect to db
    conn = establish_connection()
    cur = conn.cursor()
    
    [munger_d,munger_inverse_d] = context.build_munger_d(df.state,m)

    a = context.raw_to_context(df,m,munger_d,conn,cur)
    rs.append(str(a))

    return("<p>"+"</p><p>  ".join(rs))

@app.route('/create_cdf')
def create_cdf():
    from db_routines import Create_CDF_db as CDF
    rs =[str(datetime.now())]
    conn = establish_connection()
    cur = conn.cursor()
    rs.append('Connected to database')
    a = CDF.create_common_data_format_schema(conn,cur,'cdf2')
    rs.append(str(a))
    
    if cur:
        cur.close()
    if conn:
        conn.close()
    return("<p>"+"</p><p>  ".join(rs))

# close connection
    rs.append('Close connection')
    if cur:
        cur.close()
    if conn:
        conn.close()
    return("<p>"+"</p><p>  ".join(rs))

@app.route('/load_cdf')
def load_cdf():
    rs=[str(datetime.now())]
    # instantiate state of NC, munger
    rs.append('Create NC')
    s = sf.create_state('NC','local_data/NC')
    rs.append('Create munger')
    m = sf.create_munger('local_data/mungers/nc_export1.txt')
    
    rs.append('Connect to db')
    con = establish_connection()
    cur = con.cursor()

    rs.append('Load information from context dictionary for '+s.name)
    #ids = sf.context_to_cdf(s,conn,cur,rs)  # *** find better code, maybe in context module?
    #rs.append('ids are '+str(ids))
    context_to_db_d = context.context_to_cdf(s,'cdf2',con,cur)  # {'ReportingUnit':{'North Carolina':59, 'North Carolina;Alamance County':61} ... }
    con.commit()
    

    ## load data from state raw schema
    rs.append('NOT YET CODED: Load info from records in schema '+s.schema_name+' into CDF schema')
    ## *** to do

# close connection
    rs.append('Close connection')
    if cur:
        cur.close()
    if con:
        con.close()
    return("<p>"+"</p><p>  ".join(rs))


@app.route('/analyze')
def analyze():
    rs=[""]
# instantiate state of NC
    s = sf.create_state('NC','local_data/NC') # do we need this?
    conn = establish_connection()
    cur = conn.cursor()
    
# hard code table for now *** need to modify build() to track source file, separate build() and load()
    table_name = 'results_pct'
    contest_field = 'contest_name'
    county_field = 'county'
    vote_field = 'absentee_by_mail'
    party_field = 'choice_party'
    tolerance = 2  ## number of standard deviations from the mean that we're calling outliers
    
    
    q_abs = "SELECT "+contest_field+", "+county_field+","+party_field+", sum("+vote_field+") FROM "+table_name+"  GROUP BY "+contest_field+", "+county_field+","+party_field+" ORDER BY "+contest_field+", "+county_field+","+party_field
    cur.execute(q_abs)
    votes = pd.DataFrame(cur.fetchall(),columns=['contest','county','party','votes'])
    contests = votes['contest'].unique().tolist()     # list of contests

# loop through contests

    for c in contests:
    # for given contest_name, calculate DEM votes and total votes on absentee ballots by county
        if c:
            rs.append(c)
            c_votes=votes[votes.contest==c]
            if 'DEM' in c_votes['party'].values:
                table = pd.pivot_table(c_votes, values='votes', index=['county'], columns=['party'], aggfunc=np.sum).fillna(0)
                table['total']= table.DEM + table.REP #  + table.CST + table.LIB + table.GRE *** how to sum NaN? How to automate this list?
                table['pct_DEM'] = table.DEM/table.total
            # find outliers
                mean = table['pct_DEM'].mean()
                std = table['pct_DEM'].std()
                outliers = table[np.absolute(table.pct_DEM-mean)> tolerance*std]
                # report.append(str(table['pct_DEM']))
                if outliers.empty:
                    rs.append("No outliers more than "+str(tolerance)+" standard deviations from mean")
                else:
                    rs.append("Outliers are:"+str(outliers))
            else:
                rs.append("No DEM votes in contest "+c)

    # look for outlier in DEM percentage
    
    if cur:
        cur.close()
    if conn:
        conn.close()


    
    return("<p>"+"</p><p>  ".join(rs))

#### diagnostic below *** can delete

from munge_routines import  format_type_for_insert

@app.route('/test')
def gui():
    from munge_routines import nc_export1
    from munge_routines import upsert
    rs=[str(datetime.now())]
    con = establish_connection()
    cur = con.cursor()
    # instantiate state of NC
    s = sf.create_state('NC','local_data/NC')
    # instantiate the 'nc_export1' munger
    m = sf.create_munger('local_data/mungers/nc_export1.txt')
        # instantiate the NC pct_result datafile
    df = sf.create_datafile(s,'General Election 2018-11-06','results_pct_20181106.txt',m)

    nc_export1.rtcdf(df,'cdf2',con,cur)
    #context_to_db_d = context.context_to_cdf(s,'cdf2',con,cur)  # {'ReportingUnit':{'North Carolina':59, 'North Carolina;Alamance County':61} ... }
    #con.commit()
       
    rs.append(nc_export1.raw_to_cdf(df,'cdf2',con,cur))


    if cur:
        cur.close()
    if con:
        con.close()
    return("<p>"+"</p><p>  ".join(rs))

