#!/usr/bin/python3
# under construction
# Creates a table with columns specified in arg3 file, assuming format from state arg1.
# arg 1: a two-letter state code
# arg 2: table_name
# arg 3: (a path to) a file containing metadata from that state

import sys
from pathlib import Path
import re
import psycopg2
from psycopg2 import sql

import clean as cl

def file_to_sql_statement_list(fpath):
    query_list = []
    with open(fpath,'r') as f:
        fstring = f.read()
    p = re.compile('-- .*$',re.MULTILINE)
    clean_string = re.sub(p,' ',fstring)            # get rid of comments
    clean_string = re.sub('\n|\r',' ',clean_string)    # get rid of newlines
    query_list = clean_string.split(';')
    query_list = [q.strip() for q in query_list]    # strip leading and trailing whitespace
    return(query_list)



    
def parse_line(s,line):
    '''parse_line takes a state and a line of (metadata) text and parses it, including changing the type in the file to the type required by psql, according to the state's type-map dictionary'''
    d=s.type_map
    p=s.meta_parser
    m = p.search(line)
    field = (m.group('field')).replace(' ','_')
    type = d[m.group('type')]
    number = m.group('number')
    if number:
        type=type+(number)
    try:
        comment = m.group('comment')
    except:
        comment = ''
    return(field,type,comment)

def create_table(df):
## clean the metadata file
    fpath = cl.extract_first_col_defs(df.state.path_to_state_dir+'meta/'+df.metafile_name,df.state.path_to_state_dir+'tmp/',df.metafile_encoding)
    create_query = 'CREATE TABLE {}.{} ('
    sql_ids_create = [df.state.schema_name,df.table_name]
    sql_ids_comment = []
    strs_create = []
    strs_comment = []
    comments = []
    var_defs = []
    with open(fpath,'r',encoding=df.metafile_encoding) as f:
        lines = f.readlines()
    for line in lines:
        if line.find('"')>0:
            print('create_table:Line has double quote, will not be processed:\n'+line)
        else:
            try:
                [field,type,comment] = parse_line(df.state,line)
            except:
                print('create_table:Quoted line cannot be parsed, will not be processed: \n"'+line+'"')
                [field,type,comment] = ['parse_error','parse_error','parse_error']
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
                var_defs.append('corrupted type')
    create_query = create_query + ','.join(var_defs) + ');' +  ' '.join(comments)

    return(create_query,strs_create+strs_comment,sql_ids_create+sql_ids_comment)
        
def load_data(conn,cursor,state,datafile):      ## does this belong in app.py? *** might not need psycopg2 here then
# write raw data to db
    ext = datafile.file_name.split('.')[-1]    # extension, determines format
    if ext == 'txt':
        q = "COPY {}.{} FROM STDIN DELIMITER E'\\t' QUOTE '\"' CSV HEADER"
    elif ext == 'csv':
        q = "COPY {}.{} FROM STDIN DELIMITER ',' CSV HEADER"

    
    clean_file=cl.remove_null_bytes(state.path_to_state_dir+'data/'+datafile.file_name,'local_data/tmp/')
    with open(clean_file,mode='r',encoding=datafile.encoding,errors='ignore') as f:
        cursor.copy_expert(sql.SQL(q).format(sql.Identifier(state.schema_name),sql.Identifier(datafile.table_name)),f)
    conn.commit()
# update values to obey convention *** need to take this out, but first make sure /analysis will work
    fup = []
    for field in datafile.value_convention.keys():
        condition = []
        for value in datafile.value_convention[field].keys():
            condition.append(" WHEN "+field+" = '"+value+"' THEN '"+datafile.value_convention[field][value]+"' ")
        fup.append(field + " =  CASE "+   " ".join(condition)+" END ")
    if len(fup) > 0:
        qu = "UPDATE "+state.schema_name+"."+datafile.table_name+" SET "+",".join(fup)
        cursor.execute(qu)
        conn.commit()
    return

  
def clean_meta_file(infile,outdir,s):
    ''' create in outdir a metadata file based on infile, with all unnecessaries stripped, for the given state'''
    if s.abbreviation == 'NC':
        return("hello") # need to code this *** 
    else:
        return("clean_meta_file: error, state not recognized")
        sys.exit()

