#!usr/bin/python


class State:
    def __init__(self,abbr,name,meta_parser,type_map,db_name):
        self.abbr = abbr
        self.name = name
        self.meta_parser=meta_parser
        self.type_map=type_map
        self.db_name=db_name
        
    
    
