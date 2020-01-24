#!usr/bin/python3
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
import analyze as an
import os

import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import event
from sqlalchemy.schema import CreateSchema

def create_schema(session,name):
    if eng.dialect.has_schema(eng, name):
        recreate = input('WARNING: schema ' + name + ' already exists; erase and recreate (y/n)?\n')
        if recreate == 'y':
            session.bind.engine.execute(sqlalchemy.schema.DropSchema(name,cascade=True))
            session.bind.engine.execute(sqlalchemy.schema.CreateSchema(name))
            print('New schema created: ' + name)
        else:
            print('Schema preserved: '+ name)
    else:
        session.bind.engine.execute(sqlalchemy.schema.CreateSchema(name))
        print('New schema created: ' + name)
    session.commit()
    return

def raw_data(session,meta,df,con,cur):
    """ Loads the raw data from the datafile df into the schema for the associated state
    Schema for the state should already exist
    """
    s = df.state
    t = s.schema_name + '.' + df.table_name   # name of table, including schema

    #%% drop table in case it already exists
    meta.tables[t].drop()

    # create table
    #dbr.query('DROP TABLE IF EXISTS {}.{}',[s.schema_name,t],[],con,cur)
    [q,strs,sql_ids] = dbr.create_table(df)
    #dbr.query(q,sql_ids,strs,con,cur)

    # correct any errors due to foibles of particular datafile and commit
    for q in df.correction_query_list:
        dbr.query(q,[s.schema_name,df.table_name],[],con,cur)
        print('\tCorrection to table definition necessary:\n\t\t'+q)

    # load data into tables
    dbr.load_data(con,cur,s,df)
    return

if __name__ == '__main__':



    to_cdf = input('Load and process election data into a common-data-format database (y/n)?\n')
    if to_cdf == 'y':
        default = 'NC'
        abbr = input('Enter two-character abbreviation for your state/district/territory (default is '+default+')\n') or default

        default = 'cdf_nc_test'
        cdf_schema=input('Enter name of CDF schema (default is '+default+')\n') or default

        default = 'nc_export1'
        munger_name = input('Enter name of desired munger (default is '+default+')\n') or default

        default = 'results_pct_20181106.txt'
        df_name = input('Enter name of datafile (default is '+default+')\n') or default


        s = sf.create_state(abbr,'../local_data/'+abbr)

        munger_path = '../local_data/mungers/'+munger_name+'.txt'
        print('Creating munger instance from '+munger_path)
        m = sf.create_munger(munger_path)

        # %% Initiate db engine and create session
        eng, meta = dbr.sql_alchemy_connect()   # TODO this meta won't be used; remove
        Session = sessionmaker(bind=eng)
        session = Session()

        #%% create schema for state
        dbr.create_schema(session,s.schema_name)

        #%% create cdf schema
        print('Creating CDF schema '+ cdf_schema)

        enumeration_tables = CDF.enum_table_list()
        metadata = CDF.create_common_data_format_schema(session, cdf_schema, enumeration_tables)

        #%% fill enumeration tables
        print('\tFilling enumeration tables')
        CDF.fill_cdf_enum_tables(session,metadata,enumeration_tables)

        # load state context info into cdf schema
        need_to_load_data = input('Load context data for '+abbr+' into schema '+cdf_schema+' (y/n)?')
        if need_to_load_data == 'y':
            print('Loading state context info into CDF schema') # *** takes a long time; why?
            context.context_to_cdf(session,metadata,s,cdf_schema)

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

    find_anomalies = input('Find anomalies in an election (y/n)?\n')
    if find_anomalies == 'y':
        default = 'cdf_nc'
        cdf_schema = input('Enter name of cdf schema (default is '+default+')\n') or default

        default = '../local_data/database.ini'
        paramfile = input('Enter path to database parameter file (default is '+default+')\n') or default

        eng, meta, Session = dbr.sql_alchemy_connect(cdf_schema, paramfile)
        session = Session()

        election_dframe = dbr.election_list(session,meta,cdf_schema)
        print('Available elections in schema '+cdf_schema)
        for index,row in election_dframe.iterrows():
            print(row['Name']+' (Id is '+str(row['Id'])+')')

        default = '15834'
        election_id = input('Enter Id of the election you wish to analyze (default is '+default+')\n') or default
        election_id = int(election_id)

        default = 'nc_2018'
        election_short_name = input('Enter short name for the election (alphanumeric with underscore, no spaces -- default is '+default+')\n') or default

        default = 'precinct'
        atomic_ru_type = input('Enter the \'atomic\' Reporting Unit Type on which you wish to base your rolled-up counts (default is '+default+')\n') or default

        default = 'county'
        roll_up_to_ru_type = input('Enter the (larger) Reporting Unit Type whose counts you want to analyze (default is '+default+')\n') or default

        default = '../local_data/pickles/'+election_short_name+'/'
        pickle_dir = input('Enter the directory for storing pickled dataframes (default is '+default+')\n') or default

        try:
            assert os.path.isdir(pickle_dir)
        except AssertionError as e:
            print(e)
            dummy = input('Create the directory and hit enter to continue.')

        e = an.create_election(cdf_schema,election_id,roll_up_to_ru_type,atomic_ru_type,pickle_dir,paramfile)
        e.anomaly_scores(eng,meta,cdf_schema)
        #%%
        e.worst_bar_for_each_contest(eng,meta,2)

    print('Done!')


