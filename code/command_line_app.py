#!usr/bin/python3
import sys, os
import os.path
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))


import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # allows db creation, deletion
from psycopg2 import sql
import re

import db_routines as dbr
import states_and_files as sf
from pathlib import Path
import sys

from datetime import datetime

import clean as cl
import context
from db_routines import Create_CDF_db as CDF


## define some basics


def path_to_file(path_to_dir,filename):
    if path_to_dir[-1] == '/':
        out = path_to_dir
    else:
        out = path_to_dir+'/'
    return(out+filename)

def establish_connection(db_name='postgres'):
    host_name = 'localhost'
    user_name = 'postgres'
    password = 'notverysecure'

    # the connect() function returns a new instance of connection
    con = psycopg2.connect(host = host_name, user = user_name, password = password, database = db_name)
    return con

def create_cursor(con):
    # create a new cursor with the connection object.
    cur = con.cursor()
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
    con = establish_connection()
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    
    cur.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(s.schema_name)))
    cur.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(s.schema_name)))
    
    if cur:
        cur.close()
    if con:
        con.close()

def raw_data():
# initialize rs for logging
    print(str(datetime.now()))
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
        con = establish_conection()
        cur = con.cursor()
        
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
            diagnostic = sql.SQL(query).format( *format_args ).as_string(con)
            con.commit()

        # correct any errors due to foibles of particular datafile and commit
            for query in df.correction_query_list:
                cur.execute(sql.SQL(query).format(sql.Identifier(s.schema_name), sql.Identifier(df.table_name)))
                print(query)
                con.commit()

    # load data into tables
            dbr.load_data(con,cur,s,df)
            rs.append('Data from file '+df.file_name+' loaded into table '+s.schema_name+'.'+df.table_name)
    
    # close connection
        if cur:
            cur.close()
        if con:
            con.close()
        return("<p>"+"</p><p>  ".join(rs))
        
        
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
    con = establish_connection()
    cur = con.cursor()
    
    [munger_d,munger_inverse_d] = context.build_munger_d(df.state,m)

    a = context.raw_to_context(df,m,munger_d,con,cur)
    rs.append(str(a))

    return("<p>"+"</p><p>  ".join(rs))

def create_cdf():
    from db_routines import Create_CDF_db as CDF
    rs =[str(datetime.now())]
    con = establish_connection()
    cur = con.cursor()
    rs.append('Connected to database')
    a = CDF.create_common_data_format_schema(con,cur,'cdf2')
    rs.append(str(a))
    
    if cur:
        cur.close()
    if con:
        con.close()
    return("<p>"+"</p><p>  ".join(rs))

# close connection
    rs.append('Close connection')
    if cur:
        cur.close()
    if con:
        con.close()
    return("<p>"+"</p><p>  ".join(rs))

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



#### diagnostic below *** can delete

from munge_routines import  format_type_for_insert

def gui():
    from munge_routines import nc_export1
    from munge_routines import upsert
    rs=[str(datetime.now())]
    con = establish_connection()
    cur = con.cursor()
    # instantiate state of NC
    s = sf.create_state('NC','local_data/NC')
    # instantiate the 'nc_export1' munger

    # create cdf schema
    print('Creating CDF schema')
    CDF.create_common_data_format_schema(con, cur, 'cdf2')
    # load state context info into cdf schema
    print('Loading state context info into CDF schema')
    context.context_to_cdf(s,'cdf2',con,cur)
    con.commit()

    m = sf.create_munger('local_data/mungers/nc_export1.txt')
    # instantiate the NC pct_result datafile
    df = sf.create_datafile(s,'General Election 2018-11-06','results_pct_20181106.txt',m)

    # load data from df to CDF schema (assumes already loaded to schema s.schema_name)
    nc_export1.rtcdf(df,'cdf2',con,cur)



    if cur:
        cur.close()
    if con:
        con.close()
    return("<p>"+"</p><p>  ".join(rs))

if __name__ == '__main__':
    gui()