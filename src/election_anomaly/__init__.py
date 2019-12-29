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

def raw_data(df,con,cur):
    """ Loads the raw data from the df into the schema for the associated state *** needs work, including redesign of df class
    """

    s = df.state
    # create the schema for the state
    create_schema(s)

    t = df.table_name   # name of table
    e = df.metafile_encoding
    fpath = s.path_to_state_dir + 'meta/'+df.metafile_name
    check_args(s,fpath,t)   # checking s is redundant ***

    # clean the metadata file
    fpath = cl.extract_first_col_defs(fpath,'local_data/tmp/',e)

    # create table and commit

    cur.execute(sql.SQL('DROP TABLE IF EXISTS {}.{}').format(sql.Identifier(s.schema_name),sql.Identifier(t)))
    [query,strs,sql_ids] = dbr.create_table(df)
    format_args = [sql.Identifier(x) for x in sql_ids]
    cur.execute(sql.SQL(query).format( *format_args ),strs)
    con.commit()

    # correct any errors due to foibles of particular datafile and commit
    for query in df.correction_query_list:
        cur.execute(sql.SQL(query).format(sql.Identifier(s.schema_name), sql.Identifier(df.table_name)))
        print(query)
        con.commit()

# load data into tables
    dbr.load_data(con,cur,s,df)

    # close connection
    if cur:
        cur.close()
    if con:
        con.close()
    return

        
def file_to_context(s,df,m,con,cur):
    """ s is a state, df is a datafile, m is a munger """

    [munger_d,munger_inverse_d] = context.build_munger_d(df.state,m)
    a = context.raw_to_context(df,m,munger_d,con,cur)
    print(str(a))
    return

def load_cdf(s,cdf_schema_name,con,cur):
    print('Load information from context dictionary for '+s.name)
    context_to_db_d = context.context_to_cdf(s,cdf_schema_name,con,cur)  # {'ReportingUnit':{'North Carolina':59, 'North Carolina;Alamance County':61} ... }
    con.commit()

    # load data from df's table in state's schema to CDF schema. Note: data from df must already be loaded into df's state's raw-data schema.
    print('Loading data from df to CDF schema')
    nc_export1.raw_records_to_cdf(df,cdf_schema_name,con,cur)
    print('Done!')
    return



#### diagnostic below *** can delete

from munge_routines import  format_type_for_insert

def full_process(state_abbr,path_to_state_dir,cdf_schema_name,munger_path,df_election,df_name):
    """ state_abbr: e.g., 'NC'. path_to_state_dir: e.g., 'local_data/NC'
    munger_path: e.g., 'local_data/mungers/nc_export1.txt'
    df_election: e.g. 'General Election 2018-11-06','results_pct_20181106.txt',
    df_name: e.g. 'General Election 2018-11-06','results_pct_20181106.txt'
    *** need df_election to be a key in the state's Election.txt file. """
    from munge_routines import nc_export1
    from munge_routines import upsert
    con = establish_connection()
    cur = con.cursor()

    print('Instantiating the state of '+state_abbr)
    s = sf.create_state(state_abbr,path_to_state_dir)

    # create cdf schema
    print('Creating CDF schema '+cdf_schema_name)
    CDF.create_common_data_format_schema(con, cur, cdf_schema_name)

    print('Loading state context info into CDF schema '+cdf_schema_name) # *** takes a long time; why?
    context.context_to_cdf(s,cdf_schema_name,con,cur)
    con.commit()

    print('Creating munger instance from '+munger_path)
    m = sf.create_munger(munger_path)
    # instantiate the NC pct_result datafile

    print('Creating datafile instance')
    df = sf.create_datafile(s,df_election,df_name,m)

    print('Load raw data from datafile ' + df_name + ' into schema '+s.schema_name)
    raw_data(s, df)

    # load data from df to CDF schema (assumes already loaded to schema s.schema_name)
    print('Loading data from df to CDF schema '+cdf_schema_name)
    nc_export1.raw_records_to_cdf(df,cdf_schema_name,con,cur)
    print('Done!')



    if cur:
        cur.close()
    if con:
        con.close()
    return("<p>"+"</p><p>  ".join(rs))

if __name__ == '__main__':

    from munge_routines import nc_export1

    con = establish_connection()
    cur = con.cursor()
    # instantiate state of XX

    s = sf.create_state('XX','local_data/XX')
    print('Creating munger instance')
    m = sf.create_munger('local_data/mungers/nc_export1.txt')
    # instantiate the NC pct_result datafile

    # create cdf schema
    print('Creating CDF schema')
    CDF.create_common_data_format_schema(con, cur, 'cdf2')
    # load state context info into cdf schema
    print('Loading state context info into CDF schema') # *** takes a long time; why?
    context.context_to_cdf(s,'cdf2',con,cur)
    con.commit()

    print('Creating metafile instance')
    mf = sf.create_metafile(s,'layout_results_pct.txt')

    print('Creating datafile instance')
    df = sf.create_datafile(s,'General Election 2018-11-06','mini.txt',mf,m)

    # load data from df to CDF schema (assumes already loaded to schema s.schema_name)
    print('Loading data from df to CDF schema')
    nc_export1.raw_records_to_cdf(df,'cdf2',con,cur)
    print('Done!')
