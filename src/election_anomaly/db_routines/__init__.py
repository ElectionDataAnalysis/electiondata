#!/usr/bin/python3
# db_routines/__init__.py

import sys
import re
import psycopg2
import sqlalchemy
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
import sqlalchemy as db
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.engine import reflection
from sqlalchemy.orm import sessionmaker
from configparser import ConfigParser
import pandas as pd

import clean as cl

def establish_connection(paramfile = '../local_data/database.ini',db_name='postgres'):
    params = config(paramfile)
    con = psycopg2.connect(**params)
    return con

def sql_alchemy_connect(schema=None,paramfile = '../local_data/database.ini',db_name='postgres'):
    """Returns an engine and a metadata object"""

    params = config(paramfile)
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgresql://{user}:{password}@{host}:{port}/{database}'
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

def read_field_value(con,cur,schema,tup):
    """ need to write function to take a tuple of (table, id, field)
    and return the corresponding field value read from the db.
    """
    (table,id,field) = tup
    q = 'SELECT {2} FROM {0}.{1} WHERE "Id" = %s'
    sql_ids = [schema,table,field]
    strs = (id,)
    a = query(q,sql_ids,strs,con,cur)[0]
    return a[0]

def read_single_value_from_id(con,meta,schema,table,field,id):
    """Takes an engine connection con return the corresponding field value
    read from the record with the given id from the given table of the given schema.
    """
    t = db.Table(table,meta,autoload=True, autoload_with=con,schema=schema)
    q = db.select([eval('t.columns.'+field)]).where(t.columns.Id == str(id))
    ResultProxy = con.execute(q)
    ResultSet = ResultProxy.fetchall()
    if ResultSet:
        return ResultSet[0][0]
    else:
        print('No record in '+schema+'.'+table+' with Id '+str(id))
        return

def read_all_value_from_id(con,meta,schema,table,field):
    """Takes an engine connection con return an {id:field_value} dictionary
    read from the given table of the given schema.
    """
    t = db.Table(table,meta,autoload=True, autoload_with=con,schema=schema)
    q = db.select([ t.columns.Id,eval('t.columns.'+ field)])
    ResultProxy = con.execute(q)
    ResultSet = ResultProxy.fetchall()
    return dict(ResultSet)

def read_some_value_from_id(con,meta,schema,table,field,id_list):
    """Takes an engine connection con return an {id:field_value} dictionary
    read from the given table of the given schema.
    """
    t = db.Table(table,meta,autoload=True, autoload_with=con,schema=schema)
    if id_list:
        id_df = pd.DataFrame(id_list)
        id_list_clean = [x[0] for x in id_df.to_records(column_dtypes={0:db.INTEGER},index=False)]
        q = db.select([ t.columns.Id,eval('t.columns.'+ field)]).where(t.columns.Id.in_(id_list_clean))
        ResultProxy = con.execute(q)
        ResultSet = ResultProxy.fetchall()
        return dict(ResultSet)
    else:
        return {}

def election_list(session,meta,cdf_schema):
    #t = db.Table('Election',meta,autoload=True, autoload_with=session,schema=cdf_schema)
    # q = db.select([t.columns.Id,t.columns.Name])
    Election = meta.tables[cdf_schema+'.Election']
    result_list = [[instance.Id,instance.Name] for instance in session.query(Election)]
    result_dframe = pd.DataFrame(result_list,columns=['Id','Name'])
    return result_dframe

def contest_ids_from_election_id(con,meta,schema,Election_Id):
    """ given an election id, return list of all contest ids """
    #%%
    t = db.Table('ElectionContestJoin',meta,autoload=True, autoload_with=con,schema=schema)
    q = db.select([t.columns.Contest_Id]).where(t.columns.Election_Id == Election_Id)
    ResultProxy = con.execute(q)
    ResultSet = ResultProxy.fetchall()
    return [x[0] for x in ResultSet]

def contest_type_from_contest_id(con,meta,schema,Contest_Id):
    """ given an contest id, return CandidateContest or BallotMeasureContest """
    t = db.Table('BallotMeasureContest',meta,autoload=True, autoload_with=con,schema=schema)
    q = db.select([t.columns.Id]).where(t.columns.Id == str(Contest_Id))
    ResultProxy = con.execute(q)
    ResultSet = ResultProxy.first()
    if ResultSet is None:
        t = db.Table('CandidateContest', meta, autoload=True, autoload_with=con, schema=schema)
        q = db.select([t.columns.Id]).where(t.columns.Id == str(Contest_Id))
        ResultProxy = con.execute(q)
        ResultSet = ResultProxy.first()
        if ResultSet is None:
            return
        else:
            return "Candidate"
    else:
        return "BallotMeasure"

    return [x[0] for x in ResultSet]

def read_id_from_enum(con,meta,schema,table,txt):
    """ given the Txt value of an enumeration table entry, return the corresponding Id"""
    t = db.Table(table,meta,autoload=True, autoload_with=con,schema=schema)
    # TODO assert that table is an enumeration?
    q = db.select([t.columns.Id]).where(t.columns.Txt == txt)
    ResultProxy = con.execute(q)
    ResultSet = ResultProxy.fetchall()
    if ResultSet:
        return ResultSet[0][0]
    else:
        print('No record in '+schema+'.'+table+' with Txt '+txt)
        return

    return

def query_as_string(q,sql_ids,strs,con,cur):
    format_args = [sql.Identifier(a) for a in sql_ids]
    return cur.mogrify(sql.SQL(q).format(*format_args),strs).decode("utf-8")

def query(q,sql_ids,strs,con,cur):
    format_args = [sql.Identifier(a) for a in sql_ids]
    cur.execute(sql.SQL(q).format(*format_args),strs)
    con.commit()
    if cur.description:
        return cur.fetchall()
    else:
        return None

def query_SQLALCHEMY(session,q):
    # TODO
    return

def file_to_sql_statement_list(fpath):
    with open(fpath,'r') as f:
        fstring = f.read()
    p = re.compile('-- .*$',re.MULTILINE)
    clean_string = re.sub(p,' ',fstring)            # get rid of comments
    clean_string = re.sub('\n|\r',' ',clean_string)    # get rid of newlines
    query_list = clean_string.split(';')
    query_list = [q.strip() for q in query_list]    # strip leading and trailing whitespace
    return query_list

def fill_enum_table(schema,table,filepath,con,cur):
    """takes lines of text from file and inserts each line into the txt field of the enumeration table"""

    with open(filepath, 'r') as f:
        entries = f.read().splitlines()
    for entry in entries:
        q = 'INSERT INTO {0}.{1} ("Txt") VALUES (%s)'
        sql_ids = [schema,table]
        strs = [entry,]
        query(q,sql_ids,strs,con,cur)
    return

def create_table(df):   # *** modularize and use df.column_metadata
## clean the metadata file
    create_query = 'CREATE TABLE {}.{} ('
    sql_ids_create = [df.state.schema_name,df.table_name]
    sql_ids_comment = []
    strs_create = []
    strs_comment = []
    comments = []
    var_defs = []
    for [field,type,comment] in df.column_metadata:
        if len(comment):
            comments.append('comment on column {}.{}.{} is %s;')
            sql_ids_comment += [df.state.schema_name,df.table_name,field]
            strs_comment.append(comment)
    ## check that type var is clean before inserting it
        p = re.compile('^[\w\d()]+$')       # type should contain only alphanumerics and parentheses
        if p.match(type):
            var_defs.append('{} '+ type)    # not safest way to pass the type, but not sure how else to do it ***
            sql_ids_create.append(field)
        else:
            print('Corrupted type: '+type)
            var_defs.append('corrupted type')
    create_query = create_query + ','.join(var_defs) + ');' +  ' '.join(comments)

    return create_query, strs_create + strs_comment, sql_ids_create + sql_ids_comment
        
def load_data(conn,cursor,state,df):
# write raw data to db
    ext = df.file_name.split('.')[-1]    # extension, determines format
    if ext == 'txt':
        q = "COPY {}.{} FROM STDIN DELIMITER E'\\t' QUOTE '\"' CSV HEADER"
    elif ext == 'csv':
        q = "COPY {}.{} FROM STDIN DELIMITER ',' CSV HEADER"

    
    clean_file=cl.remove_null_bytes(state.path_to_state_dir+'data/'+df.file_name,'../local_data/tmp/')
    with open(clean_file,mode='r',encoding=df.encoding,errors='ignore') as f:
        cursor.copy_expert(sql.SQL(q).format(sql.Identifier(state.schema_name),sql.Identifier(df.table_name)),f)
    conn.commit()
    return

  
def clean_meta_file(infile,outdir,s):       ## update or remove ***
    ''' create in outdir a metadata file based on infile, with all unnecessaries stripped, for the given state'''
    if s.abbreviation == 'NC':
        return "hello"  # need to election_anomaly this ***
    else:
        return "clean_meta_file: error, state not recognized"
        sys.exit()



def create_schema(session,name):    # TODO move to db_routines
    eng = session.bind
    if eng.dialect.has_schema(eng, name):
        recreate = input('WARNING: schema ' + name + ' already exists; erase and recreate (y/n)?\n')
        if recreate == 'y':
            session.bind.engine.execute(sqlalchemy.schema.DropSchema(name,cascade=True))
            session.bind.engine.execute(sqlalchemy.schema.CreateSchema(name))
            print('New schema created: ' + name)
            new_schema_created = True
        else:
            print('Schema preserved: '+ name)
            new_schema_created = False
            insp = reflection.Inspector.from_engine(eng)
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
    session.commit()
    return new_schema_created

if __name__ == '__main__':
    schema = 'cdf_nc'
    con, meta ,Session = sql_alchemy_connect(schema=schema,paramfile='../../local_data/database.ini')
    table = 'ReportingUnit'
    field = 'Name'
    id = 50
    a = read_single_value_from_id(con,meta,schema,table,field,id)
    d = read_all_value_from_id(con,meta,schema,'ReportingUnitType','Txt')
    print('Done')
