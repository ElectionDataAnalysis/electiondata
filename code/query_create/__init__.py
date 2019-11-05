#!/usr/bin/python3
# under construction
# Creates a table with columns specified in arg3 file, assuming format from state arg1.
# arg 1: a two-letter state code
# arg 2: table_name
# arg 3: (a path to) a file containing metadata from that state

import sys
from pathlib import Path
import re



    
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
    
def create_table(table_name,var_def_file,s):
        drop_query = 'DROP TABLE IF EXISTS '+table_name+';'
        create_query =  'CREATE TABLE '+table_name+' ('
        with open(var_def_file,'r') as f:
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
                    try:
                        if len(comment):
                            comment_list.append(comment_q(table_name,field,comment))
                        var_def_list.append(var_def(field,type))
                    except:
                    	print("create_table:error with "+";".join(comment,field,type))
        create_query = create_query + ','.join(var_def_list) + ');' +  ' '.join(comment_list)
        return(drop_query,create_query)
        
def load_data(table_name):
    q = "COPY "+table_name+" FROM STDIN DELIMITER E'\\t' CSV HEADER"  # E'\\t' is the tab character
    return q

  
def clean_meta_file(infile,outdir,s):
    ''' create in outdir a metadata file based on infile, with all unnecessaries stripped, for the given state'''
    if s.abbreviation == 'NC':
        return("hello") # need to code this *** 
    else:
        return("clean_meta_file: error, state not recognized")
        sys.exit()

