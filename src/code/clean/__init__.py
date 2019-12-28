#!usr/bin/python3

import re
import sys

def extract_first_col_defs(filepath,directory,enc='utf8'):
    # get filename from the filepath
    filename = filepath.split('/')[-1]
    # add slash to end of directory if necessary
    if directory[-1]!= '/':
        directory.append('/')
        
    p=re.compile(r"""
        ((?:[^\n\t]+\t+[^\n\t]+\t+[^\n\t]+\n)+) # finds packet of lines looking like column definitions. This assumes all columns have name, type and comment, none null
        """,re.VERBOSE)
    
    with open(filepath,mode='r',encoding=enc, errors='ignore') as fin:
        data=fin.read()
    a=re.search(p,data)     # finds first instance of pattern, ignoring later
    outpath = directory+'first_col_defs_'+filename
    with open(outpath,'w',encoding=enc) as fout:
        fout.write(a.group())
    return outpath
    
def remove_null_bytes(filepath,directory):
    # get filename from the filepath
    filename = filepath.split('/')[-1]
    # add slash to end of directory if necessary
    if directory[-1]!= '/':
        directory.append('/')
    with open(filepath,'rb') as fin:
        data=fin.read()
    outpath=directory+'nn_'+filename
    with open(outpath,'wb') as fout:
        fout.write(data.replace(b'\000',b''))
    return outpath
