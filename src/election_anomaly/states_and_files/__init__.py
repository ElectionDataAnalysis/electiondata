#!usr/bin/python3
import re
import sys
import os.path
import clean as cl

class State:
    def __init__(self,abbr,name,schema_name,path_to_state_dir,context_dictionary):        # reporting_units,elections,parties,offices):
        self.abbr = abbr
        self.name = name
        self.schema_name=schema_name
        if path_to_state_dir[-1] != '/':     # should end in /
            path_to_state_dir += '/'
        self.path_to_state_dir=path_to_state_dir    #  include 'NC' in path.
        self.context_dictionary=context_dictionary

class Munger:
    def __init__(self,name,content_dictionary):
        self.name=name      # 'nc_export1'
        self.content_dictionary= content_dictionary    # dictionary of queries of the db of raw data; each querymust have exactly two slots for state.schema and datafile.table_name

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
    def __init__(self,state,file_name,encoding,source_url,file_date,download_date,note,election, table_name, munger,correction_query_list,metafile,column_metadata=[]):
        FileFromState.__init__(self,state,file_name,encoding,source_url,file_date,download_date,note)
        self.election=election
        self.table_name=table_name
        self.munger=munger
        self.correction_query_list=correction_query_list    # fix any known metadata errors; might be unnecessary if we are loading data via python directly into CDF rather than loading data into a raw SQL db. ***
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
    '''parse_line takes a metafile and a line of (metadata) text and parses it, including changing the type in the file to the type required by psql, according to the metafile's type-map dictionary'''
    d=mf.type_map
    p=mf.line_parser
    m = p.search(line)
    try:
        field = (m.group('field')).replace(' ','_')
        type = d[m.group('type')]
        number = m.group('number')
    except:
        field = 'line not parsable'
        type = 'line not parsable'
        comment = 'line not parsable'
        print('Line not parsable: \n'+ line)
        return ([field, type, comment])

    if number:
        type=type+(number)
    try:
        comment = m.group('comment')
    except:
        comment = ''
    return([field,type,comment])

def create_munger(file_path):   # file should contain all munger info in a dictionary
    with open(file_path,'r') as f:
        d = eval(f.read())
    return(Munger(d['name'],d['content_dictionary']))

def create_state(abbr,path_to_state_dir):
    '''abbr is the capitalized two-letter postal election_anomaly for the state, district or territory'''
    string_attributes = ['name','schema_name']
    context_d_keys = ['ReportingUnit','Election','Party','Office','BallotMeasureSelection']    # what consistency checks do we need?
    if path_to_state_dir[-1] != '/':
        path_to_state_dir += '/'
    if not os.path.isdir(path_to_state_dir):
        return('Error: No directory '+path_to_state_dir)
        sys.exit()
    for attr in string_attributes + context_d_keys:
        if not os.path.isfile(path_to_state_dir+'context/'+attr+'.txt'):
            return('Error: No file '+path_to_state_dir+'context/'+attr+'.txt')
            sys.exit()
    string_d = {} # dictionary to hold string attributes
    for attr in string_attributes:     # strings
        with open(path_to_state_dir+'context/'+attr+'.txt') as f:
            string_d[attr]=f.readline().strip()
    context_d = {}
    for attr in context_d_keys:     # python objects
        with open(path_to_state_dir+'context/'+attr+'.txt') as f:
            context_d[attr]=eval(f.read())
    return State(abbr,string_d['name'],string_d['schema_name'],path_to_state_dir,context_d)

def create_metafile(s,name):
    # meta_parser = re.compile(string_d['parser_string'])

    # read metafile info from context folder
    with open(s.path_to_state_dir+'context/metafile.txt','r') as f:
        d_all = eval(f.read())
        d = d_all[name]

    # get parser objects from regex strings
    column_block_parser = re.compile(d['column_block_parser_string'])
    line_parser = re.compile(d['line_parser_string'])

    return Metafile(s,name,d['encoding'],d['source_url'],d['file_date'],d['download_date'],d['note'],column_block_parser,d['type_map'],line_parser)

def create_datafile(s,election,data_file_name,mf,munger):
    """ given state s, metafile mf, munger, plus strings for election and filename, create datafile object.
    """
    # check election is in the state's context dictionary
    if not in_context_dictionary(s,'Election',election):
        print('No such election ('+election+') defined for state '+s.name)
        return False
    
    # read datafile info from context folder
    with open(s.path_to_state_dir+'context/datafile.txt','r') as f:
        d_all = eval(f.read())
        d = d_all[election+';'+data_file_name]

    # create tablename for the raw data
    table_name=re.sub(r'\W+', '', election+data_file_name)

    # create column metadata
    col_block = extract_column_metadata_block(mf)
    column_metadata = []
    for line in col_block:
        if line.find('"') > 0:
            print('create_table:Line has double quote, will not be processed:\n' + line)
        elif not re.search(r'\t',line):
            print('line has no tabs, will not be processed:\n' + line)
        else:
            column_metadata.append(parse_line(mf,line))
    return Datafile(s,data_file_name,d['encoding'],d['source_url'],d['file_date'],d['download_date'],d['note'],election,table_name, munger,d['correction_query_list'],mf,column_metadata)

def in_context_dictionary(state,context_item,value):
    if value in state.context_dictionary[context_item].keys():
        return True
    else:
        return False

