#!/usr/bin/python3
# db_routines/__init__.py
# under construction
# Creates a table with columns specified in arg3 file, assuming format from state arg1.
# arg 1: a two-letter state election_anomaly
# arg 2: table_name
# arg 3: (a path to) a file containing metadata from that state

import sys
import re
from psycopg2 import sql
from configparser import ConfigParser


import clean as cl



def config(filename='local_data/database.ini', section='postgresql'):
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


def query(q,sql_ids,strs,con,cur):
    format_args = [sql.Identifier(a) for a in sql_ids]
    cur.execute(sql.SQL(q).format(*format_args),strs)
    con.commit()
    if cur.pgresult_ptr:    # TODO is this the right attribute?
        return cur.fetchall()
    else:
        return None


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
        q = 'INSERT INTO {0}.{1} ("txt") VALUES (%s)'
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
        
def load_data(conn,cursor,state,df):      ## does this belong in app.py? *** might not need psycopg2 here then
# write raw data to db
    ext = df.file_name.split('.')[-1]    # extension, determines format
    if ext == 'txt':
        q = "COPY {}.{} FROM STDIN DELIMITER E'\\t' QUOTE '\"' CSV HEADER"
    elif ext == 'csv':
        q = "COPY {}.{} FROM STDIN DELIMITER ',' CSV HEADER"

    
    clean_file=cl.remove_null_bytes(state.path_to_state_dir+'data/'+df.file_name,'local_data/tmp/')
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

