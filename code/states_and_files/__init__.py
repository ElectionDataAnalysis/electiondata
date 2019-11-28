#!usr/bin/python3
import re
import sys
import os.path
from os import path

class State:
    def __init__(self,abbr,name,fips,meta_parser,type_map,schema_name,path_to_data):
        self.abbr = abbr
        self.name = name
        self.name = fips
        self.meta_parser=meta_parser
        self.type_map=type_map
        self.schema_name=schema_name
        self.path_to_data=path_to_data      # should end in /
    
    
def create_state(abbr,path_to_parent_dir):
    string_attributes = ['name','schema_name','parser_string','fips']
    object_attributes = ['type_map']
    if not os.path.isdir(path_to_parent_dir):
        print('Error: No directory '+path_to_parent_dir)
        return('Error: No directory '+path_to_parent_dir)
        sys.exit()
    for attr in string_attributes + object_attributes:
        if not os.path.isfile(path_to_parent_dir+abbr+'/external/'+attr+'.txt'):
            print('Error: No file '+path_to_parent_dir+abbr+'/external/'+attr+'.txt')
            return('Error: No file '+path_to_parent_dir+abbr+'/external/'+attr+'.txt')
            sys.exit()
    
    if path_to_parent_dir[-1] != '/':
        path_to_parent_dir += '/'
    d = {} # dictionary to hold attributes
    for attr in string_attributes:     # strings
        with open(path_to_parent_dir+abbr+'/external/'+attr+'.txt') as f:
            d[attr]=f.readline().strip()
    for attr in object_attributes:     # python objects
        with open(path_to_parent_dir+abbr+'/external/'+attr+'.txt') as f:
            d[attr]=eval(f.readline().strip())
    path_to_data = path_to_parent_dir+abbr+'/data/'
    meta_p=re.compile(d['parser_string'])
    return State(abbr,d['name'],d['fips'],meta_p,d['type_map'],d['schema_name'],path_to_data)


class Datafile:
    def __init__(self,state, table_name, file_name, encoding,metafile_name,metafile_encoding,value_convention,source,correction_query_list):
        self.state=state
        self.table_name=table_name
        self.file_name=file_name
        self.encoding=encoding
        self.metafile_name=metafile_name
        self.metafile_encoding=metafile_encoding
        self.value_convention=value_convention
        self.source=source
        self.correction_query_list=correction_query_list    # fix any known metadata errors



def create_datafile(s,file_name):
    if s.abbr=='NC':
        if file_name=='results_pct_20181106.txt':
            table_name='results_pct'
            encoding='utf8'
            metafile='layout_results_pct.txt'
            metafile_encoding='utf8'
            source='NC State Board of Elections; file dated 11/16/2018; downloaded 10/2019 from state website *** need dates for metafile too'
            correction_query_list =  ['ALTER TABLE '+ s.schema_name +'.'+table_name+' ALTER COLUMN precinct SET DATA TYPE varchar(23)']  #metadata says precinct field has at most 12 characters but 'ABSENTEE BY MAIL 71-106' has 13

            value_convention={}         # dictionary of dictionaries, each for a field. Key is the name of the field. For each subdictionary, key is the sourcefile's convention and value is the db convention
            ## create dictionary for contest_name
            value_convention['contest_name']={}
            for n in ['01','02','03','04','05','06','07','08','09','10','11','12','13']:
                value_convention['contest_name']['US HOUSE OF REPRESENTATIVES DISTRICT '+n]='NC_USC_'+n+'_2018'
            return Datafile(s,table_name, file_name, encoding, metafile, metafile_encoding, value_convention,source,correction_query_list)
        elif file_name == 'absentee_20181106.csv':
            table_name='absentee'
            encoding='utf8'
            metafile='sfs_by_hand_layout_absentee.txt'
            metafile_encoding='utf16'
            source='NC State Board of Elections; file dated 11/16/2018; downloaded 10/2019 from state website'
            correction_query_list = []
            value_convention={}         # dictionary of dictionaries, each for a field. Key is the name of the field. For each subdictionary, key is the sourcefile's convention and value is the db convention
            ## create dictionary for contest_name
            value_convention['cong_dist_desc']={}
            for n in range(1,14):
                 value_convention['cong_dist_desc']['CONGRESSIONAL DISTRICT '+str(n)]='NC_USC_'+str(n).zfill(2)+'_2018'
            return Datafile(s,table_name, file_name, encoding, metafile, metafile_encoding, value_convention,source,correction_query_list)
        else:
            return('Error: state, file_name pair not recognized')
    else:
        return('Error: state not recognized')


