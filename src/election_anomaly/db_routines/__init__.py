#!/usr/bin/python3
# db_routines/__init__.py

import psycopg2
import sqlalchemy
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
import sqlalchemy as db
import user_interface as ui
from configparser import ConfigParser
import pandas as pd
import tkinter as tk
import os


def create_database(con,cur,db_name):
    sure = input('If the db exists, it will be deleted and data will be lost. Are you absolutely sure (y/n)?\n')
    if sure == 'y':
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        q = "DROP DATABASE IF EXISTS {0}"
        sql_ids = [db_name]
        out1 = query(q,sql_ids,[],con,cur)

        q = "CREATE DATABASE {0}"
        out2 = query(q,sql_ids,[],con,cur)
        return out1,out2
    else:
        return None,None


def create_raw_schema(con,cur,schema):
    q = "CREATE SCHEMA {0}"
    sql_ids = [schema]
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    out1 = query(q,sql_ids,[],con,cur)
    return out1


def fill_composing_reporting_unit_join(session):
    print('Filling ComposingReportingUnitJoin table, i.e., recording nesting relations of ReportingUnits')
    ru_dframe = pd.read_sql_table('ReportingUnit',session.bind,'cdf',index_col=None)
    cruj_dframe = append_to_composing_reporting_unit_join(session,ru_dframe)
    return cruj_dframe


def append_to_composing_reporting_unit_join(session,ru):
    """<ru> is a dframe of reporting units, with cdf internal name in column 'Name'.
    cdf internal name indicates nesting via semicolons.
    This routine calculates the nesting relationships from the Names and uploads to db.
    Returns the *all* CRUJ data from the db."""
    ru['split'] = ru['Name'].apply(lambda x:x.split(';'))
    ru['length'] = ru['split'].apply(len)
    
    # pull ReportingUnit to get ids matched to names
    ru_cdf = pd.read_sql_table('ReportingUnit',session.bind,index_col=None)
    ru_static = ru.copy()
    # get Id of the child reporting unit, if it's not already there
    if 'Id' not in ru.columns:
        ru_static = ru_static.merge(ru_cdf[['Name','Id']],on='Name',how='left')
    cruj_dframe_list = []
    for i in range(ru['length'].max() - 1):
        # check that all components of all Reporting Units are themselves ReportingUnits
        ru_for_cruj = ru_static.copy()  # start fresh, without detritus from previous i

        # get name of ith ancestor
        ru_for_cruj['ancestor_{}'.format(i)] = ru_static['split'].apply(lambda x:';'.join(x[:-i - 1]))
        # get Id of ith ancestor
        ru_for_cruj = ru_for_cruj.merge(ru_cdf,left_on='ancestor_{}'.format(i),right_on='Name',
                                        suffixes=['','_' + str(i)])
        cruj_dframe_list.append(ru_for_cruj[['Id','Id_{}'.format(i)]].rename(
            columns={'Id':'ChildReportingUnit_Id','Id_{}'.format(i):'ParentReportingUnit_Id'}))
    if cruj_dframe_list:
        cruj_dframe = pd.concat(cruj_dframe_list)
        cruj_dframe = dframe_to_sql(cruj_dframe,session,None,'ComposingReportingUnitJoin')
    else:
        cruj_dframe = pd.read_sql_table('ComposingReportingUnitJoin',session.bind)
    session.flush()
    return cruj_dframe


def get_path_to_db_paramfile():
    current_dir = os.getcwd()
    path_to_src = current_dir.split('/election_anomaly/')[0]
    fpath='{}/jurisdictions/database.ini'.format(path_to_src)
    return fpath


def establish_connection(paramfile = '../jurisdictions/database.ini',db_name='postgres'):
    params = config(paramfile)
    if db_name != 'postgres': params['dbname']=db_name
    con = psycopg2.connect(**params)
    return con


def sql_alchemy_connect(
        schema=None,
        paramfile='/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/database.ini',
        db_name='postgres'):
    """Returns an engine and a metadata object"""

    params = config(paramfile)
    if db_name != 'postgres': params['dbname'] = db_name
    # We connect with the help of the PostgreSQL URL
    url = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    url = url.format(**params)


    # The return value of create_engine() is our connection object
    engine = db.create_engine(url, client_encoding='utf8')


    # We then bind the connection to MetaData()
    meta = db.MetaData(bind=engine, reflect=True,schema=schema)

    return engine, meta


def config(filename=None, section='postgresql'):
    """
    Creates the parameter dictionary needed to log into our db
    using info in <filename>
    """
    if not filename:
        # if parameter file is not provided, ask for it
        # initialize root widget for tkinter
        tk_root = tk.Tk()
        project_root=ui.get_project_root()
        ui.pick_paramfile(tk_root,project_root=project_root)

    # create a parser
    parser = ConfigParser()
    # read config file

    if not os.path.isfile(filename): # if <filename> doesn't exist, look in a canonical place
        filename=get_path_to_db_paramfile()
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db


def query(q,sql_ids,strs,con,cur):  # needed for some raw queries, e.g., to create db and schemas
    format_args = [sql.Identifier(a) for a in sql_ids]
    cur.execute(sql.SQL(q).format(*format_args),strs)
    con.commit()
    if cur.description:
        return cur.fetchall()
    else:
        return None


def raw_query_via_SQLALCHEMY(session,q,sql_ids,strs):
    connection = session.bind.connect()
    con = connection.connection
    cur = con.cursor()
    format_args = [sql.Identifier(a) for a in sql_ids]
    cur.execute(sql.SQL(q).format(*format_args),strs)
    con.commit()
    if cur.description:
        return_item = cur.fetchall()
    else:
        return_item = None
    cur.close()
    con.close()
    return return_item


def dframe_to_sql(dframe,session,schema,table,index_col='Id',flush=True,raw_to_votecount=False):
    """
    Given a dframe and an existing cdf db table name, clean the dframe
    (i.e., drop any columns that are not in the table, add null columns to match any missing columns)
    append records any new records to the corresponding table in the db (and commit!)
    Return the updated dframe, including all rows from the db and all from the dframe.
    """
    # pull copy of existing table
    target = pd.read_sql_table(table,session.bind,schema=schema,index_col=index_col)
    # VoteCount table gets added columns during raw data upload, needs special treatment
    if raw_to_votecount:
        # join with SECVCJ
        secvcj = pd.read_sql_table('SelectionElectionContestVoteCountJoin',session.bind,schema=schema,index_col=None)
        # drop columns that don't belong, but were temporarily created in order to get VoteCount_Id correctly into SECVCJ
        target=target.drop(['Election_Id','Contest_Id','Selection_Id'],axis=1)
        target=target.merge(secvcj,left_on='Id',right_on='VoteCount_Id')
        target=target.drop(['Id','VoteCount_Id'],axis=1)
    df_to_db = dframe.drop_duplicates().copy()
    # TODO there should be no  duplicates in dframe in the first place. MD votecount had some. Why?
    if 'Count' in df_to_db.columns:
        # TODO bug: catch anything not an integer (e.g., in MD 2018g upload)
        df_to_db.loc[:,'Count']=df_to_db['Count'].astype('int64',errors='ignore')
    #df_to_db=df_to_db.astype(str)
    #target=target.astype(str)

    # remove columns that don't exist in target table
    for c in dframe.columns:
        if c not in target.columns:
            df_to_db = df_to_db.drop(c, axis=1)
    # add columns that exist in target table but are missing from original dframe
    for c in target.columns:
        if c not in dframe.columns:
            df_to_db[c] = None
    appendable = pd.concat([target,target,df_to_db],sort=False).drop_duplicates(keep=False)
    # note: two copies of target ensures none of the original rows will be appended.

    # drop the Id column
    if 'Id' in appendable.columns:
        appendable = appendable.drop('Id',axis=1)

    appendable.to_sql(table, session.bind, schema=schema, if_exists='append', index=False)
    if table == 'ReportingUnit' and not appendable.empty:
        append_to_composing_reporting_unit_join(session,appendable)
    up_to_date_dframe = pd.read_sql_table(table,session.bind,schema=schema)
    if raw_to_votecount:
        # need to drop rows that were read originally from target -- these will have null Election_Id
        up_to_date_dframe=up_to_date_dframe[up_to_date_dframe['Election_Id'].notnull()]
    if flush:
        session.flush()
    return up_to_date_dframe


if __name__ == '__main__':

    print('Done')

