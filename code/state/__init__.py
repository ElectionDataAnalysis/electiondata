#!usr/bin/python
import re

class State:
    def __init__(self,abbr,name,meta_parser,type_map,db_name,path_to_data):
        self.abbr = abbr
        self.name = name
        self.meta_parser=meta_parser
        self.type_map=type_map
        self.db_name=db_name
        self.path_to_data=path_to_data
    
    
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
        return State("NC","North Carolina",nc_meta_p,nc_type_map,"nc",nc_path_to_data)


