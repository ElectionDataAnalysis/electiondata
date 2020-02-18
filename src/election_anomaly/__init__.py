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

from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Table,MetaData
from alembic.migration import MigrationContext
from alembic.operations import Operations
import pandas as pd

try:
    import cPickle as pickle
except:
    import pickle


def raw_data(session,meta,df):
    """ Loads the raw data from the datafile df into the schema for the associated state
    Schema for the state should already exist
    """
    s = df.state
    t = s.schema_name + '.' + df.table_name   # name of table, including schema

    #%% drop table in case it already exists
    if t in meta.tables.keys():
        meta.tables[t].drop()


    # create table
    col_list = [Column(field,typ,comment=comment) for [field, typ, comment] in df.column_metadata]
    Table(df.table_name,meta,*col_list,schema=s.schema_name)
    meta.create_all()

    #%% correct any type errors in metafile (e.g., some values in file are longer than the metafile thinks possible) # TODO use raw sql query from dbr
    ctx = MigrationContext.configure(eng)
    op = Operations(ctx)
    for d in df.type_correction_list:
        r = dict(d)
        del r['column']
        op.alter_column(df.table_name,d['column'],schema=s.schema_name,**r)
        print('\tCorrection to table definition necessary:\n\t\t'+df.table_name+';'+str(d))

    # load raw data into tables
    dbr.load_raw_data(session, meta,s.schema_name, df)
    return

if __name__ == '__main__':
    # initialize state and create database for it (if not already exists)
    # TODO error handling: what if db already exists?
    default = 'NC'
    abbr = input(
        'Enter short name (only alphanumeric and underscore, no spaces) for your state/district/territory (default is ' + default + ')\n'
    ) or default
    s = sf.State(abbr,'../local_data/')
    create_db = input('Make database and schemas for '+abbr+' (y/n)?\n')
    if create_db == 'y':
        s.create_db_and_schemas()

    # initialize main session for connecting to db
    eng, meta_generic = dbr.sql_alchemy_connect(db_name=s.short_name)
    Session = sessionmaker(bind=eng)
    session = Session()

    if create_db == 'y':
        # create build tables in cdf schema
        print('Creating common data format tables in schema `cdf` in database '+s.short_name)
        enumeration_tables = CDF.enum_table_list()
        meta_cdf = CDF.create_common_data_format_schema(session,'cdf',enumeration_tables,delete_existing=True)
        session.commit()

        # load data from context directory into context schema
        # TODO make it possible to update the context schema
        print('Loading context data from '+s.short_name+'/context directory into `context` schema in database '+s.short_name)
        # for file in context folder, create table in context schema.
        context = {}
        for f in os.listdir(s.path_to_state_dir+'/context/'):
            if f[0] == '.': continue
            table_name = f.split('.')[0]

            context[table_name] = pd.read_csv(s.path_to_state_dir+'/context/'+f,sep='\t')
            context[table_name].to_sql(table_name,session.bind,'context',if_exists='fail')

        # %% fill enumeration tables
        print('\tFilling enumeration tables')
        CDF.fill_cdf_enum_tables(session,meta_cdf,'cdf',enumeration_tables)

        need_to_load_data = input('Load raw data (y/n)?\n')
        if need_to_load_data == 'y':
            # user picks election
            election_list = [f for f in os.listdir(s.path_to_state_dir + 'data/') if os.path.isdir(s.path_to_state_dir + 'data/'+f)]
            assert election_list != [], 'No elections available for in directory '+s.short_name
            default = election_list[0]
            need_election = True
            while need_election:
                print('Available elections are:')
                for e in election_list: print(e)
                election_name = input('Enter short name of election (default is ' + default + ')\n') or default
                if election_name in election_list: need_election = False
                else: print('Election not available; try again.')

            # user picks munger
            munger_list = [f for f in os.listdir(s.path_to_state_dir + 'data/'+election_name+'/') if os.path.isdir(s.path_to_state_dir + 'data/'+election_name+'/'+f)]
            assert munger_list != [], 'No mungers available for in directory '+s.short_name +'/'+election_name
            default = munger_list[0]
            need_munger = True
            while need_munger:
                print('Available mungers are:')
                for e in munger_list: print(e)
                munger_name = input('Enter short name of munger (default is ' + default + ')\n') or default
                if munger_name in munger_list: need_munger = False
                else: print('Election not available; try again.')

            munger_path = '../mungers/'+munger_name+'/'
            print('Creating munger instance from '+munger_path)
            mu = sf.Munger(munger_path)

            for datafile in os.listdir(s.path_to_state_dir + 'data/'+election_name+'/'+mu.name+'/'):
                # TODO process datafile
                pass


        print('Loading state context info into CDF schema') # *** takes a long time; why?
        # TODO -- or maybe this will be obsolete?
        context.context_to_cdf_PANDAS(session,meta_cdf,s,cdf_schema,enumeration_tables)
        session.commit()
    else:
        meta_cdf = MetaData(bind=session.bind,schema='cdf')

    election_id, election_type, election_name = an.get_election_id_type_name(session,meta_generic,cdf_schema,default=3219)



        print('Creating metafile instance')
        mf = sf.create_metafile(s,'layout_results_pct.txt')

        print('Creating datafile instance')
        df = sf.create_datafile(s, election_name, df_name, mf, m)
        print('Load raw data from '+df.file_name)
        if df.separator == 'tab': delimiter = '\t'
        elif df.separator == 'comma': delimiter = ','
        else:
            print('Separator in file unknown, assumed to be tab')
            delimiter = '\t'
        raw_data_dframe = pd.read_csv(s.path_to_state_dir+'data/' + df.file_name,sep=delimiter)
        try:
            raw_data_dframe.to_sql(df.table_name,con=session.bind,schema=s.schema_name,index=False,if_exists='fail')
            session.flush()
        except:
            replace = input('Raw data table ' +df.table_name + ' already exists in database. Replace (y/n)?\n')
            if replace == 'y':
                raw_data_dframe.to_sql(df.table_name, con=session.bind, schema=s.schema_name, index=False,if_exists='replace')
                session.flush()
            else:
                print('Continuing with existing file')

        print('Loading data from df table\n\tin schema '+ s.schema_name+ '\n\tto CDF schema '+cdf_schema+'\n\tusing munger '+munger_name)
        mr.raw_records_to_cdf(session,meta_cdf,df,m,cdf_schema,s.schema_name,election_type)
        session.commit()
        print('Done loading raw records from '+ df_name+ ' into schema ' + cdf_schema +'.')

    e = an.get_anomaly_scores(session,meta_cdf,cdf_schema,election_id,election_name)

    default = 3
    n = input('Draw how many most-anomalous plots?\n') or default
    try:
        n = int(n)
        e.draw_most_anomalous(session,meta_cdf,n=n,mode='pct')
        e.draw_most_anomalous(session,meta_cdf,n=n,mode='raw')

    except:
        print('Input was not an integer; skipping most-anomalous plots')

    draw_all = input('Plot worst bar chart for all contests? (y/n)?\n')
    if draw_all == 'y':
        e.worst_bar_for_each_contest(session,meta_generic)

    e.worst_bar_for_selected_contests(session,meta_generic)

    eng.dispose()
    print('Done!')
    exit()


