#!/usr/bin/python3
# under construction
# argument 1: a two-letter state code
# argument 2: (a path to) a file containing metadata from that state

import sys
import os.path
from pathlib import Path
import re

def statelist():
    '''List of 2-char state codes that this code can deal with'''
    return ['NC']

def check_args(s,f):
    if s not in statelist():
        print('Error: Either "'+s+'" is not a two-character state/territory/district abbreviation, or this program doesn\'t know how to parse its metadata yet.')
        sys.exit()
    mypath=Path(f)
    if not mypath.is_file():
        print('Error: File '+f+' does not exist.')
        sys.exit()
    return
    
def parse_line(s,line):
    if s == 'NC':
        p= re.compile(r"""
            \A                      # start at beginning of line
            (?P<field>\S+\b)        # capture field
            \s+                     # skip all whitespace
            (?P<type>\b\S+\b)       # capture type
            \s+                     # skip all whitespace
            (?P<comment>.+)        # capture remaining part of the line, not including end-of-line
            """,re.VERBOSE)
        m = p.search(line)
        field = m.group('field')
        type = m.group('type')
        comment = m.group('comment')
    return(field,type,comment)
    
def psql(field,type,comment):
    v = field+' '+type
    if len(comment):
        v = v + ' COMMENT "' + comment +'"'
    return v
        
if __name__ == "__main__":
    check_args(sys.argv[1],sys.argv[2])
    line = 'id          INT         the identifying number'
    [field,type,comment] = parse_line('NC',line)
    print(psql(field,type,comment))
    

    

