#!usr/bin/python3

# TODO  create tmp directory programmatically

import re

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
