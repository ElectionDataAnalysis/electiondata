#!/usr/bin/python3
# under construction
# Creates a table with columns specified in arg3 file, assuming format from state arg1.
# arg 1: a two-letter state code
# arg 2: table_name
# arg 3: (a path to) a file containing metadata from that state

import sys
from pathlib import Path
import re
import clean as cl
import psycopg2
from psycopg2 import sql

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

def var_def(field,type):
    v = field+' '+type
    return v
    
def comment_q(table,field,comment):
    ''' args are table name, field name, comment text'''
    c = 'comment on column '+table+'.'+field+' is \''+comment+'\';'
    return c
    
def create_table(table_name,var_def_file,s,enc='utf8'):
        create_query =  'CREATE TABLE '+s.schema_name+'.'+table_name+' ('
        with open(var_def_file,'r',encoding=enc) as f:
            var_def_list=[]
            comment_list=[]
            for line in f.readlines():
                if line.find('"')>0:
                    print('create_table:Line has double quote, will not be processed:\n'+line)
                else:
                    try:
                        [field,type,comment] = parse_line(s,line)
                    except:
                        print('create_table:Quoted line cannot be parsed, will not be processed: \n"'+line+'"')
                        [field,type,comment] = ['parse_error','parse_error','parse_error']
                    try:
                        if len(comment):
                            comment_list.append(comment_q(s.schema_name+'.'+table_name,field,comment))
                        var_def_list.append(var_def(field,type))
                    except:
                    	print("create_table: no comment found in "+";".join(comment,field,type))
        create_query = create_query + ','.join(var_def_list) + ');' +  ' '.join(comment_list)
        return(create_query)
        
def old_load_data(table_name,ext):
    if ext == 'txt':
        delimit = " DELIMITER E'\\t' QUOTE '\"' "
    elif ext == 'csv':
        delimit = " DELIMITER ',' "
    q = "COPY "+table_name+" FROM STDIN "+delimit+" CSV HEADER"
    return q
    
def load_data(conn,cursor,state,datafile):
# write raw data to db
    ext = datafile.file_name.split('.')[-1]    # extension, determines format
    if ext == 'txt':
        delimit = " DELIMITER E'\\t' QUOTE '\"' "
    elif ext == 'csv':
        delimit = " DELIMITER ',' "
    q = "COPY "+state.schema_name+"."+datafile.table_name+" FROM STDIN "+delimit+" CSV HEADER"
    clean_file=cl.remove_null_bytes(state.path_to_state_dir+'/data/'+datafile.file_name,'local_data/tmp/')
    with open(clean_file,mode='r',encoding=datafile.encoding,errors='ignore') as f:
        cursor.copy_expert(q,f)
    conn.commit()
# update values to obey convention
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

