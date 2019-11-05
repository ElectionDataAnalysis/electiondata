#!usr/bin/python
import re
import sys

def extract_first_col_defs(filepath,directory):
    # get filename from the filepath
    filename = filepath.split('/')[-1]
    # add slash to end of directory if necessary
    if directory[-1]!= '/':
        directory.append('/')
        
    p=re.compile(r"""
    # note:     ((?:[^\n\t]+\t+[^\n\t]+\t+[^\n\t]+\n)+) # finds packet of lines looking like column definitions. This assumes all columns have name, type and comment, none null
    """,re.VERBOSE)
    
    with open(filepath,'r') as fin:
        data=fin.read()
    a=re.search(p,data)     # finds first instance of pattern, ignoring later
    outpath = directory+'first_col_defs'+filename
    with open(outpath,'w') as fout:
        fout.write(a.group())
    return outpath
    
def remove_null_bytes(filepath,directory):
    # get filename from the filepath
    filename = filepath.split('/')[-1]
    # add slash to end of directory if necessary
    if directory[-1]!= '/':
        directory.append('/')
    with open(filepath,'r') as fin:
        data=fin.read()
    outpath=directory+'nn_'+filename,'w'
    with open(outpath,'w') as fout:
        fout.write(data.replace(b'\000',b''))
    return outpath

if __name__='__main__':
    print('hello')
