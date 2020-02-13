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
    # %% Initiate db engine and create session
    eng, meta_generic = dbr.sql_alchemy_connect()
    Session = sessionmaker(bind=eng)
    session = Session()

    default = 'NC'
    abbr = input(
        'Enter two-character abbreviation for your state/district/territory (default is ' + default + ')\n'
                ) or default
    s = sf.create_state(abbr, '../local_data/' + abbr)

    default = 'cdf_nc2018p'
    cdf_schema = input(
        'Enter name of CDF schema (default is ' + default + ')\n'
                ) or default

    context_to_cdf = input('Load and process context data into a common-data-format database (y/n)?\n')
    if context_to_cdf == 'y':
        # create cdf schema
        print('Creating CDF schema ' + cdf_schema)
        enumeration_tables = CDF.enum_table_list()
        meta_cdf_schema = CDF.create_common_data_format_schema(session,cdf_schema,enumeration_tables,
                                                               delete_existing=True)
        session.commit()

        # load state context info into cdf schema
        # need_to_load_data = input('Load enumeration & context data for '+abbr+' into schema '+cdf_schema+' (y/n)?')
        need_to_load_context_data = 'y'
        if need_to_load_context_data == 'y':

            # %% fill enumeration tables
            print('\tFilling enumeration tables')
            CDF.fill_cdf_enum_tables(session, meta_cdf_schema, cdf_schema,enumeration_tables)

            print('Loading state context info into CDF schema') # *** takes a long time; why?
            context.context_to_cdf_PANDAS(session, meta_cdf_schema, s, cdf_schema,enumeration_tables)
            session.commit()
    else:
        meta_cdf_schema = MetaData(bind=session.bind,schema=cdf_schema)

    election_id, election_type, election_name = an.get_election_id_type_name(session,meta_generic,cdf_schema,default=3219)

    # need_to_load_data = 'y'
    need_to_load_data = input('Load raw data (y/n)?\n')
    if need_to_load_data == 'y':
        default = 'nc_primary'
        munger_name = input('Enter name of desired munger (default is '+default+')\n') or default

        munger_path = '../mungers/'+munger_name+'/'
        print('Creating munger instance from '+munger_path)
        m = sf.create_munger(munger_path)

        # default = 'filtered_results_pct_20181106.txt'
        default = 'results_pct_20180508.txt'
        # default = 'filtered_yancey2018.txt'
        #default = 'alamance.txt'
        df_name = input('Enter name of datafile (default is '+default+')\n') or default

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
        mr.raw_records_to_cdf(session,meta_cdf_schema,df,m,cdf_schema,s.schema_name,election_type)
        session.commit()
        print('Done loading raw records from '+ df_name+ ' into schema ' + cdf_schema +'.')

    e = an.get_anomaly_scores(session,meta_cdf_schema,cdf_schema,election_id,election_name)

    default = 3
    n = input('Draw how many most-anomalous plots?\n') or default
    n = int(n)

    e.draw_most_anomalous(session,meta_cdf_schema,n=n,mode='pct')
    e.draw_most_anomalous(session,meta_cdf_schema,n=n,mode='raw')

    draw_all = input('Plot worst bar chart for all contests? (y/n)?\n')
    if draw_all == 'y':
        e.worst_bar_for_each_contest(session,meta_generic)

    e.worst_bar_for_selected_contests(session,meta_generic)

    eng.dispose()
    print('Done!')
    exit()


