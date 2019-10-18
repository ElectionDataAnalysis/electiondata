#!/usr/bin/python3
# under construction
# Creates a table with columns specified in arg3 file, assuming format from state arg1.
# arg 1: a two-letter state code
# arg 2: table_name
# arg 3: (a path to) a file containing metadata from that state

import sys
from pathlib import Path
import re

## define some basics
    
    
def parse_line(s,line):
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
    print("parse_line: "+ " ".join([field,type,comment]))
    return(field,type,comment)

def var_def(field,type):
    v = field+' '+type
    return v
    
def comment_q(table,field,comment,flavor):
    ''' args are table name, field name, comment text and flavor (e.g., 'psql' or 'mysql')'''
    if flavor=='psql':
        c = 'comment on column '+table+'.'+field+' is \''+comment+'\';'
    else:
        print('Flavor not recognized: '+flavor)
        sys.exit()
    return c
    
def create_table(table_name,var_def_file,flavor,s):
        drop_query = 'DROP TABLE IF EXISTS '+table_name+';'
        create_query =  'CREATE TABLE '+table_name+' ('
        with open(var_def_file,'r') as f:
            var_def_list=[]
            comment_list=[]
            for line in f.readlines():
                print('line: '+line)
                print('var_def_list: '+" ".join(var_def_list))
                if line.find('"')>0:
                    print('Line has double quote, will not be processed:\n'+line)
                else:
                    try:
                        [field,type,comment] = parse_line(s,line)
                    except:
                        print('Quoted line cannot be parsed, will not be processed: \n"'+line+'"')
                    try:
                        if flavor == 'psql':
                            if len(comment):
                                comment_list.append(comment_q(table_name,field,comment,'psql'))
                            var_def_list.append(var_def(field,type))
                        elif flavor == 'mysql':
                            if len(comment):
                                comment_text = ' COMMENT "'+comment+'"'
                            else:
                                comment_text = ''
                            var_def_list.append(var_def(field,type)+comment_text)
                        else:
                            print('Flavor not recognized: '+flavor)
                            sys.exit()
                    except:
                    	print("error with "+";".join(flavor,comment,field,type))
        create_query = create_query + ','.join(var_def_list) + ');' +  ' '.join(comment_list)
        return(drop_query,create_query)
        
def load_data(table_name,data_file,flavor,s):   # *** needs testing
    if flavor == 'psql':
        q = 'COPY '+table_name+' FROM '+data_file

if __name__ == "__main__":
    [state,filepath] = check_args(sys.argv[1],sys.argv[3])
    table_name = sys.argv[2]
    [drop_query,create_query] = create_table(table_name,sys.argv[3],'mysql')
    print(drop_query)
    print(create_query)

    

