#!usr/bin/python3
import re
import sys
import os.path
import clean as cl
import pandas as pd
import db_routines as dbr
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
class State:
    def create_db_and_schemas(self):
        # create db
        con = dbr.establish_connection()
        cur = con.cursor()
        dbr.create_database(con,cur,self.short_name)
        cur.close()
        con.close()
        # connect to  and create the three schemas
        con = dbr.establish_connection(db_name=self.short_name)
        cur = con.cursor()
        q = "CREATE SCHEMA raw;"
        dbr.query(q,[],[],con,cur)
        q = "CREATE SCHEMA context;"
        dbr.query(q,[],[],con,cur)
        q = "CREATE SCHEMA cdf;"
        dbr.query(q,[],[],con,cur)
        cur.close()
        con.close()
        return

    def __init__(self,short_name,path_to_parent_dir):        # reporting_units,elections,parties,offices):
        """ short_name is the name of the directory containing the state info, including data,
         and is used other places as well.
         path_to_parent_dir is the parent directory of dir_name
        """
        self.short_name = short_name
        if path_to_parent_dir[-1] != '/':     # should end in /
            path_to_parent_dir += '/'
        self.path_to_state_dir= path_to_parent_dir + short_name+'/'
        assert os.path.isdir(self.path_to_state_dir), 'Error: No directory '+ self.path_to_state_dir
        assert os.path.isdir(self.path_to_state_dir+'context/'), 'Error: No directory '+ self.path_to_state_dir+'context/'
        # Check that context directory is missing no files
        context_file_list = ['BallotMeasureSelection.txt','datafile.txt','Election.txt','ExternalIdentifier.txt','metafile.txt','name.txt','Office.txt','remark.txt','ReportingUnit.txt'] # TODO remove schema_name.txt and alter README.txt
        file_missing_list = [f for f in context_file_list if not os.path.isfile(self.path_to_state_dir+'context/'+f)]
        assert file_missing_list == [], 'Error: Missing files in '+ self.path_to_state_dir+'context/'+f +':\n'+ str(file_missing_list)


class Munger:
    def __init__(self,name,path_to_munger_dir):
        self.name=name      # 'nc_export1'
        self.path_to_munger_dir=path_to_munger_dir

class FileFromState:
    def __init__(self,state,file_name,encoding,source_url,file_date,download_date,note):
        self.state=state
        self.file_name=file_name
        self.encoding=encoding
        self.source_url=source_url
        self.file_date=file_date
        self.download_date=download_date
        self.note=note

class Metafile(FileFromState):
    def __init__(self,state,file_name,encoding,source_url,file_date,download_date,note,column_block_parser,type_map,line_parser):
        FileFromState.__init__(self,state,file_name,encoding,source_url,file_date,download_date,note)
        self.column_block_parser=column_block_parser      # regex for extracting the meaningful rows and ignoring other rows
        self.type_map = type_map    # maps field types from metafile to postgresql field types
        self.line_parser=line_parser

class Datafile(FileFromState):
    def __init__(self, state, file_name, encoding, source_url, file_date, download_date, note, separator, election, table_name, munger, type_correction_list, metafile, column_metadata=[]):
        FileFromState.__init__(self,state,file_name,encoding,source_url,file_date,download_date,note)
        self.separator=separator
        self.election=election
        self.table_name=table_name
        self.munger=munger
        self.type_correction_list=type_correction_list    # fix any known metadata errors; might be unnecessary if we are loading data via python directly into CDF rather than loading data into a raw SQL db. ***
        self.metafile=metafile
        self.column_metadata=column_metadata    # list of triples [column_name, postgres datatype, text description]. Order should match order of columns in original file

def extract_column_metadata_block(mf):
    """ mf is a metafile; """
    p = re.compile(mf.column_block_parser)

    metadata_file_text = cl.get_text_from_file(mf.state.path_to_state_dir + 'meta/' + mf.file_name,mf.encoding)

    a = re.search(p, metadata_file_text)  # finds first instance of pattern, ignoring later
    column_metadata_block = a.group().split('\n')
    return column_metadata_block    # list of lines, each describing a column

def parse_line(mf,line):
    '''parse_line takes a metafile and a line of (metadata) text and parses it,
    including changing the type in the file to the type required by sqlalchemy,
    according to the metafile's type-map dictionary'''
    d=mf.type_map
    p=mf.line_parser
    m = p.search(line)
    try:
        field = (m.group('field')).replace(' ','_')
        typ = d[m.group('type')]
        if m.group('number'):
            number = int(m.group('number')[1:-1])
            typ = typ(number)
    except:
        field = None
        typ = None
        comment = None
        return [field, typ, comment]

    try:
        comment = m.group('comment')
    except:
        comment = ''
    return [field,typ,comment]

def create_munger(dir_path):   # director should contain all munger info in tab-separated txt files
    assert os.path.isdir(dir_path), 'Not a directory: '+dir_path
    assert os.path.isfile(dir_path + 'cdf_tables.txt') and os.path.isfile(dir_path + 'count_columns.txt') and os.path.isfile(dir_path + 'name.txt') and os.path.isfile(dir_path + 'raw_columns.txt'), 'Directory '+dir_path+ ' must contain files cdf_tables.txt, count_columns.txt, name.txt and raw_columns.txt'
    with open(dir_path + 'name.txt') as f:
        name = f.readline()
    return(Munger(name,dir_path))

def tab_sep_to_dict(s,fpath,k):
    full_fpath = fpath+k+'.txt'
 #   with open(full_fpath,'r') as f:

    lines = pd.read_csv(full_fpath)

    #%% BallotMeasureSelection.txt
    if k == 'BallotMeasureSelection':
        v = set(lines.Selection.values)
    else:
        v = None
    return v

def dframe_to_dict(dframe,index_column = 'Name'):
    col_list = list(dframe.columns)
    col_list.remove(index_column)
    out_d = {}
    for index, row in dframe.iterrows():
        row_d = {}
        for c in col_list:
            row_d[c] = row[c]
        out_d[row[index_column]] = row_d
    return out_d

def create_metafile(s,name):
    # meta_parser = re.compile(string_d['parser_string'])

    # read metafile info from context folder
    dframe = pd.read_csv(s.path_to_state_dir+'context/metafile.txt',sep='\t')
    d_all = dframe_to_dict(dframe,'name')
    d = d_all[name]
#    with open(s.path_to_state_dir+'context/metafile.txt','r') as f:

#        d_all = eval(f.read())
#        d = d_all[name]

    # get parser objects from regex strings
    column_block_parser = re.compile(d['column_block_parser_string'])
    line_parser = re.compile(d['line_parser_string'])

    #%% convert string (read from tab-separated file) to a dictionary
    d['type_map'] = eval (d['type_map'])

    return Metafile(s,name,d['encoding'],d['source_url'],d['file_date'],d['download_date'],d['note'],column_block_parser,d['type_map'],line_parser)

def create_datafile(s,election,data_file_name,mf,munger):
    """ given state s, metafile mf, munger, plus strings for election and filename, create datafile object.
    """
    # check election is in the state's context dictionary
    if not in_context_dictionary(s,'Election',election):
        print('No such election ('+election+') defined for state '+s.name)
        return False
    
    # read datafile info from context folder
    dframe = pd.read_csv(s.path_to_state_dir+'context/datafile.txt',sep='\t')
    d_all = dframe_to_dict(dframe,index_column='name')
    d = d_all[election+';'+data_file_name]

    #%% convert string (read from tab-separated file) to list
    d['type_correction_list'] = eval(d['type_correction_list'])

    # create tablename for the raw data
    table_name=re.sub(r'\W+', '', election+data_file_name)

    #%% create column metadata
    col_block = extract_column_metadata_block(mf)
    column_metadata = []
    for line in col_block:
        if line.find('"') > 0:
            print('create_table:Line has double quote, will not be processed:\n' + line)
        elif not re.search(r'\t',line):
            print('line has no tabs, will not be processed:\n' + line)
        else:
            pl = parse_line(mf,line)
            if pl == [None,None,None]:
                print('line not parsable, will not be processed:\n' + line)
            else:
                column_metadata.append(pl)
    return Datafile(s,data_file_name,d['encoding'],d['source_url'],d['file_date'],d['download_date'],d['note'],d['separator'],election,table_name, munger,d['type_correction_list'],mf,column_metadata)

def in_context_dictionary(state,context_item,value):
    if value in state.context_dictionary[context_item].keys():
        return True
    else:
        return False

if __name__ == '__main__':
    s = create_state('NC','../../local_data/NC/')
    fpath = '../../local_data/NC/context_new/'
    tab_sep_to_dict(s,fpath,'BallotMeasureSelection')

    print('Done')