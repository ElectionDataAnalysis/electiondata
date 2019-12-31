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

import db_routines as dbr
from db_routines import Create_CDF_db as CDF
import states_and_files as sf
import context


## define some basics


def path_to_file(path_to_dir,filename):
    if path_to_dir[-1] == '/':
        out = path_to_dir
    else:
        out = path_to_dir+'/'
    return(out+filename)

def establish_connection(db_name='postgres'):
    params = dbr.config()
    con = psycopg2.connect(**params)
    return con

def create_cursor(con):
    # create a new cursor with the connection object.
    cur = con.cursor()
    return cur

def create_schema(s):
    # connect and create schema for the state
    # TODO double check that user wants to drop the existing schema.
    con = establish_connection()
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    dbr.query('DROP SCHEMA IF EXISTS {} CASCADE',[s.schema_name],[],con,cur)
    dbr.query('CREATE SCHEMA {}',[s.schema_name],[],con,cur)

    if cur:
        cur.close()
    if con:
        con.close()
    return

def raw_data(df,con,cur):
    """ Loads the raw data from the df into the schema for the associated state *** needs work, including redesign of df class
    """

    s = df.state
    # create the schema for the state
    create_schema(s)

    t = df.table_name   # name of table
    e = df.metafile.encoding
    fpath = s.path_to_state_dir + 'meta/'+df.metafile.file_name


    # create table and commit

    dbr.query('DROP TABLE IF EXISTS {}.{}',[s.schema_name,t],[],con,cur)
    [q,strs,sql_ids] = dbr.create_table(df)
    dbr.query(q,sql_ids,strs,con,cur)

    # correct any errors due to foibles of particular datafile and commit
    for q in df.correction_query_list:
        dbr.query(q,[s.schema_name,df.table_name],[],con,cur)
        print(q)
        con.commit()

    # load data into tables
    dbr.load_data(con,cur,s,df)
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
    create_schema(s)

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
    raw_data(df,con,cur)

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

    # instantiate state of XX

    s = sf.create_state('XX','local_data/XX')
    create_schema(s)
    print('Creating munger instance')
    m = sf.create_munger('local_data/mungers/nc_export1.txt')

    con = establish_connection()
    cur = con.cursor()



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

    print('Load raw data from '+df.file_name)
    raw_data(df,con,cur)


    # load data from state's raw data schema to CDF schema
    print('Loading data from df to CDF schema')
    nc_export1.raw_records_to_cdf(df,'cdf2',con,cur)
    print('Done!')
