#!usr/bin/python3
import sys, os
import os.path
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))



import db_routines as dbr
import munge_routines as mr
from db_routines import Create_CDF_db as CDF
import states_and_files as sf
import context

def raw_data(df,con,cur):
    """ Loads the raw data from the df into the schema for the associated state
    Schema for the state should already exist
    """

    s = df.state

    t = df.table_name   # name of table
    # create table
    dbr.query('DROP TABLE IF EXISTS {}.{}',[s.schema_name,t],[],con,cur)
    [q,strs,sql_ids] = dbr.create_table(df)
    dbr.query(q,sql_ids,strs,con,cur)

    # correct any errors due to foibles of particular datafile and commit
    for q in df.correction_query_list:
        dbr.query(q,[s.schema_name,df.table_name],[],con,cur)
        print('\tCorrection to table definition necessary:\n\t\t'+q)

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

def full_process(state_abbr,path_to_state_dir,cdf_schema_name,munger_path,df_election,df_name):
    """ state_abbr: e.g., 'NC'. path_to_state_dir: e.g., '../local_data/NC'
    munger_path: e.g., '../local_data/mungers/nc_export1.txt'
    df_election: e.g. 'General Election 2018-11-06','results_pct_20181106.txt',
    df_name: e.g. 'General Election 2018-11-06','results_pct_20181106.txt'
    *** need df_election to be a key in the state's Election.txt file. """
    from munge_routines import nc_export1
    from munge_routines import id_from_select_or_insert
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
    default = 'NC'
    abbr = input('Enter two-character abbreviation for your state/district/territory (default is '+default+')\n') or default

    default = 'cdf_nc'
    cdf_schema=input('Enter name of CDF schema (default is '+default+')\n') or default

    default = 'nc_export1'
    munger_name = input('Enter name of desired munger (default is '+default+')\n') or default

    default = 'results_pct_20181106.txt'
    df_name = input('Enter name of datafile (default is '+default+')\n') or default


    s = sf.create_state(abbr,'../local_data/'+abbr)

    munger_path = '../local_data/mungers/'+munger_name+'.txt'
#    exec('from munge_routines import '+munger_name+ ' as mu')
    print('Creating munger instance from '+munger_path)
    m = sf.create_munger(munger_path)


    dbr.create_schema(s.schema_name)
    con = dbr.establish_connection()
    cur = con.cursor()




    # create cdf schema
    print('Creating CDF schema '+ cdf_schema)
    CDF.create_common_data_format_schema(con, cur, cdf_schema)

    # load state context info into cdf schema
    need_to_load_data = input('Load context data for '+abbr+' into schema '+cdf_schema+' (y/n)?')
    if need_to_load_data == 'y':
        print('Loading state context info into CDF schema') # *** takes a long time; why?
        context.context_to_cdf(s,cdf_schema,con,cur)

    print('Creating metafile instance')
    mf = sf.create_metafile(s,'layout_results_pct.txt')

    print('Creating datafile instance')
    df = sf.create_datafile(s,'General Election 2018-11-06',df_name,mf,m)

    need_to_load_data = input('Load raw data from '+df.file_name+' into schema '+s.schema_name+' (y/n)?')
    if need_to_load_data == 'y':
        print('Load raw data from '+df.file_name)
        raw_data(df,con,cur)


    print('Loading data from df table\n\tin schema '+ s.schema_name+ '\n\tto CDF schema '+cdf_schema+'\n\tusing munger '+munger_name)
    mr.raw_records_to_cdf(df,m,cdf_schema,con,cur)
    print('Done!')
