#!usr/bin/python3
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
    
    
def create_state(abbr):
    if abbr == 'NC':
        nc_meta_p=re.compile(r"""
        (?P<field>[^\n\t]+)
        \t+
        (?P<type>[A-z]+)
        (?P<number>\(\d+\))?
        \t+(?P<comment>[^\n\t]+)
        \n""",re.VERBOSE)
        nc_type_map = {'number':'INT', 'text':'varchar', 'char':'varchar'}
        nc_path_to_data = "local_data/NC/data"
        nc_correction_query_list = ['ALTER TABLE results_pct ALTER COLUMN precinct SET DATA TYPE varchar(23)']  #metadata says precinct field has at most 12 characters but 'ABSENTEE BY MAIL 71-106' has 13
        return State("NC","North Carolina",nc_meta_p,nc_type_map,"nc",nc_path_to_data,nc_correction_query_list)
    else:
        return('Error: "'+abbr+'" is not a state abbreviation recognized by the code.')
        sys.exit()


class Datafile:
    def __init__(self,state_abbr, table_name, file_name, encoding,metafile_name,metafile_encoding,value_convention,source):
        self.state_abbr=state_abbr
        self.table_name=table_name
        self.file_name=file_name
        self.encoding=encoding
        self.metafile_name=metafile_name
        self.metafile_encoding=metafile_encoding
        self.value_convention=value_convention
        self.source=source
        
def create_datafile(state_abbr,file_name):
    if state_abbr=='NC':
        if file_name=='results_pct_20181106.txt':
            table_name='results_pct'
            encoding='utf8'
            metafile='layout_results_pct.txt'
            metafile_encoding='utf8'
            source='NC State Board of Elections; file dated 11/16/2018; downloaded 10/2019 from state website *** need dates for metafile too'
            value_convention={}         # dictionary of dictionaries, each for a field. Key is the name of the field. For each subdictionary, key is the sourcefile's convention and value is the db convention
            ## create dictionary for contest_name
            value_convention['contest_name']={}
            for n in ['01','02','03','04','05','06','07','08','09','10','11','12','13']:
                value_convention['contest_name']['US HOUSE OF REPRESENTATIVES DISTRICT '+n]='NC_USC_'+n+'_2018'
            return Datafile(state_abbr,table_name, file_name, encoding, metafile, metafile_encoding, value_convention,source)
        elif file_name == 'absentee_20181106.csv':
            table_name='absentee'
            encoding='utf8'
            metafile='sfs_by_hand_layout_absentee.txt'
            metafile_encoding='utf16'
            source='NC State Board of Elections; file dated 11/16/2018; downloaded 10/2019 from state website'
            value_convention={}         # dictionary of dictionaries, each for a field. Key is the name of the field. For each subdictionary, key is the sourcefile's convention and value is the db convention
            ## create dictionary for contest_name
            value_convention['cong_dist_desc']={}
        return Datafile(state_abbr,table_name, file_name, encoding, metafile, metafile_encoding, value_convention,source)
        for n in range(1,14):
             value_convention['cong_dist_desc']['CONGRESSIONAL DISTRICT '+str(n)]='NC_USC_'+str(n).zfill(2)+'_2018'
        else:
            return('Error: state, file_name pair not recognized')
    else:
        return('Error: state not recognized')
