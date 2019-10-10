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
def statelist():
    '''List of 2-char state codes that this code can deal with'''
    return ['NC']
    
def type_map():
    d={'number':'INT', 'text':'varchar', 'char':'varchar'}
    return d

def check_args(s,f):
    if s not in statelist():
        print('Error: Either "'+s+'" is not a two-character state/territory/district abbreviation, or this program doesn\'t know how to parse its metadata yet.')
        sys.exit()
    mypath=Path(f)
    if not mypath.is_file():
        print('Error: File '+f+' does not exist.')
        sys.exit()
    return (s,f)
    
def parse_line(s,line):
    if s == 'NC':
        d=type_map()
        p= re.compile(r"""
            \A                      # start at beginning of line
            (?P<field>\S+\b)        # capture field
            \s+                     # skip all whitespace
            (?P<type>\S+)       # capture type, including number in parens if there
            \s+                     # skip all whitespace
            (?P<comment>.*)        # capture remaining part of the line, not including end-of-line
            """,re.VERBOSE)
        m = p.search(line)
        field = m.group('field')
        type = m.group('type')
        try:
            comment = m.group('comment')
        except:
            comment = ''
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
    
def create_table_q(table_name,var_def_file,flavor):
        drop_query = 'DROP TABLE IF EXISTS '+table_name+';'
        create_query =  'CREATE TABLE '+table_name+' ('
        with open(filepath,'r') as f:
            var_def_list=[]
            comment_list=[]
            for line in f.readlines():
                if line.find('"')>0:
                    print('Line has double quote, will not be processed:\n'+line)
                else:
                    try:
                        [field,type,comment] = parse_line('NC',line)
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
                        print('Quoted line cannot be parsed, will not be processed: \n"'+line+'"')
        create_query = create_query + ','.join(var_def_list) + ');' +  ' '.join(comment_list)
        return(drop_query,create_query)

if __name__ == "__main__":
    [state,filepath] = check_args(sys.argv[1],sys.argv[3])
    table_name = sys.argv[2]
    [drop_query,create_query] = create_table_q(table_name,sys.argv[3],'mysql')
    print(drop_query)
    print(create_query)

    

