#!usr/bin/python
import re
import sys

class State:
    def __init__(self,abbr,name,meta_parser,type_map,db_name,path_to_data,correction_query_list):
        self.abbr = abbr
        self.name = name
        self.meta_parser=meta_parser
        self.type_map=type_map
        self.db_name=db_name
        self.path_to_data=path_to_data
        self.correction_query_list=correction_query_list    # fix any known metadata errors
    
    
def create_instance(abbr):
    if abbr == 'NC':
        nc_meta_p= re.compile(r"""
        (?P<field>.*\S+\b)        # capture field
        \s\s+                    # skip all more-than-one whitespace
        (?P<type>[a-z]+)       # capture type, including number in parens if there
        (?P<number>\(\d+\))?
        \s+                     # skip all whitespace
        (?P<comment>.*)        # capture remaining part of the line, not including end-of-line
        """,re.VERBOSE)
        nc_type_map = {'number':'INT', 'text':'varchar', 'char':'varchar'}
        nc_path_to_data = "local_data/NC/data"
        nc_correction_query_list = ['ALTER TABLE results ALTER COLUMN precinct SET DATA TYPE varchar(23)']  #metadata says precinct field has at most 12 characters but 'ABSENTEE BY MAIL 71-106' has 13
        return State("NC","North Carolina",nc_meta_p,nc_type_map,"nc",nc_path_to_data,nc_correction_query_list)
    else:
        return('Error: "'+abbr+'" is not a state abbreviation recognized by the code.')
        sys.exit()


