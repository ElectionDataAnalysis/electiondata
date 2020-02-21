#!/usr/bin/python3
# db_routines/__init__.py

import psycopg2
import sqlalchemy
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
import sqlalchemy as db
from sqlalchemy.engine import reflection
from configparser import ConfigParser
import pandas as pd

def create_database(con,cur,db_name):
    sure = input('If the db exists, it will be deleted and data will be lost. Are you absolutely sure (y/n)?\n')
    if sure == 'y':
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        q = "DROP DATABASE IF EXISTS {0}"
        sql_ids = [db_name]
        out1 = query(q,sql_ids,[],con,cur)
        print(out1)

        q = "CREATE DATABASE {0}"
        out2 = query(q,sql_ids,[],con,cur)
        print(out2)  # TODO diagnostic
    return out1,out2

def create_raw_schema(con,cur,schema):
    q = "CREATE SCHEMA {0}"
    sql_ids = [schema]
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    out1 = query(q,sql_ids,[],con,cur)
    return out1


def establish_connection(paramfile = '../local_data/database.ini',db_name='postgres'):
    params = config(paramfile)
    if db_name != 'postgres': params['dbname']=db_name
    con = psycopg2.connect(**params)
    return con

def sql_alchemy_connect(schema=None,paramfile = '../local_data/database.ini',db_name='postgres'):
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

def config(filename='../local_data/database.ini', section='postgresql'):
    """
    Creates the parameter dictionary needed to log into our db
    """
    # create a parser
    parser = ConfigParser()
    # read config file
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

def create_schema(session,name,delete_existing=False):
    eng = session.bind
    if eng.dialect.has_schema(eng, name):
        if delete_existing:
            recreate = 'y'
        else:
            recreate = input('WARNING: schema ' + name + ' already exists; erase and recreate (y/n)?\n')
        if recreate == 'y':
            session.bind.engine.execute(sqlalchemy.schema.DropSchema(name,cascade=True))
            session.bind.engine.execute(sqlalchemy.schema.CreateSchema(name))
            print('New schema created: ' + name)
            new_schema_created = True
        else:
            print('Schema preserved: '+ name)
            new_schema_created = False
            insp = reflection.Inspector.from_engine(eng)    # TODO got warning about deprecation of reflection.
            tablenames = insp.get_table_names(schema=name)
            viewnames = insp.get_view_names(schema=name)
            if tablenames:
                print('WARNING: Some tables exist: \n\t'+'\n\t'.join([name + '.' + t for t in tablenames]))
            if viewnames:
                print('WARNING: Some views exist: \n\t' + '\n\t'.join([name + '.' + t for t in viewnames]))

    else:
        session.bind.engine.execute(sqlalchemy.schema.CreateSchema(name))
        print('New schema created: ' + name)
        new_schema_created = True
    session.flush()
    return new_schema_created

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
        # TODO join with SECVCJ, should this be right join? What if a VC_Id has multiple entries in secvcj?
        secvcj = pd.read_sql_table('SelectionElectionContestVoteCountJoin',session.bind,schema=schema,index_col=None)
        target0=target.copy() # TODO diagnostic
        target1=target.drop(['Election_Id','Contest_Id','Selection_Id'],axis=1)
        target2=target1.merge(secvcj,left_on='Id',right_on='VoteCount_Id')
        target=target2.drop(['Id','VoteCount_Id'],axis=1)
        # target.rename(columns={'VoteCount_Id':'Id'},inplace=True)
    df_to_db = dframe.copy()

    # remove columns that don't exist in target table
    for c in dframe.columns:
        if c not in target.columns:
            df_to_db = df_to_db.drop(c, axis=1)
    # add columns that exist in target table but are mission from original dframe
    for c in target.columns:
        if c not in dframe.columns:
            df_to_db[c] = None  # TODO why doesn't this throw an error? Column is not equal to a scalar...
    appendable = pd.concat([target,target,df_to_db],sort=False).drop_duplicates(keep=False)
    # note: two copies of target ensures none of the original rows will be appended.

    # drop the Id column # TODO inefficient? Why not drop it before? Might even add it above, only to be dropped?
    if 'Id' in appendable.columns:
        appendable = appendable.drop('Id',axis=1)

    appendable.to_sql(table, session.bind, schema=schema, if_exists='append', index=False)
    up_to_date_dframe = pd.read_sql_table(table,session.bind,schema=schema)
    if raw_to_votecount:
        # need to drop rows that were read originally from target -- these will have null Election_Id
        up_to_date_dframe=up_to_date_dframe[up_to_date_dframe['Election_Id'].notnull()]
    if flush:
        session.flush()
    return up_to_date_dframe

if __name__ == '__main__':

    print('Done')

