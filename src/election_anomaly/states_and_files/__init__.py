#!usr/bin/python3
import re
import sys
import os.path
import clean as cl
from os import path
import psycopg2
from psycopg2 import sql

class State:
    def __init__(self,abbr,name,schema_name,path_to_state_dir,main_reporting_unit_type,context_dictionary):        # reporting_units,elections,parties,offices):
        self.abbr = abbr
        self.name = name
        self.schema_name=schema_name
        if path_to_state_dir[-1] != '/':     # should end in /
            path_to_state_dir += '/'
        self.path_to_state_dir=path_to_state_dir    #  include 'NC' in path.
        self.main_reporting_unit_type=main_reporting_unit_type  # *** is this used?
        self.context_dictionary=context_dictionary

class Munger:
    def __init__(self,name,query_from_raw):
        self.name=name      # 'nc_export1'
        self.query_from_raw= query_from_raw    # dictionary of queries of the db of raw data; each querymust have exactly two slots for state.schema and datafile.table_name

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
    def __init__(self,metadata_extraction_parser,type_map,meta_parser):
        self.column_metadata_extraction_parser=metadata_extraction_parser      # regex for extracting the meaningful rows and ignoring other rows
        self.type_map = type_map
        self.meta_parser=meta_parser

class Datafile(FileFromState):
    def __init__(self,election, table_name, munger,correction_query_list,metafile,column_metadata):
        self.election=election
        self.table_name=table_name
        self.munger=munger
        self.correction_query_list=correction_query_list    # fix any known metadata errors; might be unnecessary if we are loading data via python directly into CDF rather than loading data into a raw SQL db. ***
        self.metafile=metafile
        self.column_metadata=column_metadata    # list of triples [column_name, postgres datatype, text description]. Order should match order of columns in original file

    def update_column_metadata_from_metafile(self):
        """ under construction
        """
        if self.column_metadata:
            print('Warning: overwriting existing column_metadata:\n'+ str(self.column_metadata))
        column_metadata_block = extract_column_metadata_block(self.metafile) #  list
        self.column_metadata = []
        for col_line in column_metadata_block:
            self.column_metadata.append(parse_line(self.metafile,col_line))
        return

## Initialize classes # *** fix to reflect changes in class definitions
def create_munger(file_path):   # file should contain all munger info in a dictionary
    with open(file_path,'r') as f:
        d = eval(f.read())
    return(Munger(d['name'],d['query_from_raw']))

def create_state(abbr,path_to_state_dir):
    '''abbr is the capitalized two-letter postal election_anomaly for the state, district or territory'''
    string_attributes = ['name','schema_name','parser_string','main_reporting_unit_type']
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
    with open(path_to_state_dir+'context/type_map.txt') as f:
        type_map= eval(f.read().strip())
    string_d = {} # dictionary to hold string attributes
    for attr in string_attributes:     # strings
        with open(path_to_state_dir+'context/'+attr+'.txt') as f:
            string_d[attr]=f.readline().strip()
    meta_p=re.compile(string_d['parser_string'])
    context_d = {}
    for attr in context_d_keys:     # python objects
        with open(path_to_state_dir+'context/'+attr+'.txt') as f:
            context_d[attr]=eval(f.read())
    return State(abbr,string_d['name'],meta_p,string_d['schema_name'],path_to_state_dir,string_d['main_reporting_unit_type'],type_map,context_d)

def create_datafile(s,election,data_file_name,munger):
    # check that election is compatible with state
    
    if election not in s.context_dictionary['Election'].keys():
        return('No such election ('+election+') associated with state '+s.name)
        # sys.exit()
    
    # read datafile info from context folder *** should be more efficient
    with open(s.path_to_state_dir+'context/datafile.txt','r') as f:
        d_all = eval(f.read())
        d = d_all[election+';'+data_file_name]
    table_name=re.sub(r'\W+', '', election+data_file_name)
    return(Datafile(s,election, table_name,data_file_name,d['data_file_encoding'],d['meta_file'], d['meta_file_encoding'],munger,d['source_url'],d['file_date'],d['download_date'],d['note'],d['correction_query_list']))
    

    
########################################
def parse_line(mf,line):
    '''parse_line takes a state and a line of (metadata) text and parses it, including changing the type in the file to the type required by psql, according to the state's type-map dictionary'''
    d=mf.type_map
    p=mf.meta_parser
    m = p.search(line)
    field = (m.group('field')).replace(' ','_')
    type = d[m.group('type')]
    number = m.group('number')
    if number:
        type=type+(number)
    try:
        comment = m.group('comment')
    except:
        comment = ''
    return([field,type,comment])

def extract_column_metadata_block(mf):
    """ mf is a metafile; """
    p = re.compile(mf.column_metadata_extraction_parser)

    metadata_file_text = cl.get_text_from_file(mf.state.path_to_state_dir + 'meta/' + mf.file_name,
                                               self.metafile.encoding)

    a = re.search(p, metadata_file_text)  # finds first instance of pattern, ignoring later
    column_metadata_block = a.group().split('\n')
    return column_metadata_block    # list of lines, each describing a column


def external_identifiers_to_cdf(id,d,conn,cur):
    """ id is a primary key for an object in the database; d is a dictionary of external identifiers for that object (e.g., {'fips':'3700000000','nc_export1':'North Carolina'}); cur is a cursor on the db. The function alters the table cdf.externalidentifier appropriately. """
    for kk in d:
        cur.execute('SELECT id FROM cdf.identifiertype WHERE text = %s',[kk])
        a=cur.fetchone()
        if a:
            idtype_id = a[0]
            othertext=''
        else:
            idtype_id=otherid_id
            othertext=kk
        cur.execute('INSERT INTO cdf.externalidentifier (foreign_id,value,identifiertype_id,othertype) VALUES (%s,%s,%s,%s) ON CONFLICT (foreign_id,identifiertype_id,othertype) DO NOTHING',[id,d[k]['ExternalIdentifiers'][kk],idtype_id,othertext])
    conn.commit()
    return

def context_to_cdf(state, conn, cur,report):
    '''Loads information from context folder for the state into the common data format'''
# reporting units
    report.append('Entering ReportingUnit information')
    cur.execute("SELECT id FROM  cdf.identifiertype WHERE text = 'other'")
    otherid_id = cur.fetchone()[0]
    d = state.reporting_units
    for k in d.keys():
    # create corresponding gp_unit
        cur.execute('INSERT INTO cdf.gpunit (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name= %s RETURNING id',[k,k])   # *** OK as long as there aren't too many of these. Otherwise can bloat db
        id=cur.fetchone()[0]
        external_identifiers_to_cdf(id,d[k]['ExternalIdentifiers'],cur)
        conn.commit()

# elections
    report.append('Entering Elections information')
    cur.execute("SELECT id FROM  cdf.electiontype WHERE text = 'other'")
    otherelection_id = cur.fetchone()[0]
    d = state.elections
    for k in d.keys():
        if 'Type' in d[k].keys():
            cur.execute('SELECT id FROM cdf.electiontype WHERE text = %s',[d[k]['Type'],])
            a=cur.fetchone()
            if a:
                electiontype_id = a[0]
                othertext=''
            else:
                electiontype_id=otherelection_id
                othertext=d[k]['Type']
        startdate = d[k].get('StartDate',None)
        enddate = d[k].get('EndDate',None)
        cur.execute('INSERT INTO cdf.election (name,enddate,startdate,electiontype_id,othertype) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (name) DO UPDATE SET name= %s RETURNING id',[k,enddate,startdate,electiontype_id,othertext,k])   # *** OK as long as there aren't too many conflicts. Otherwise can bloat db
        id=cur.fetchone()[0]
        external_identifiers_to_cdf(id,d[k]['ExternalIdentifiers'],cur)
        conn.commit()

# parties
    report.append('Entering Parties information')
    d = state.parties
    for k in d.keys():
        cur.execute('INSERT INTO cdf.party (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name= %s RETURNING id',[k,k])   # *** OK as long as there aren't too many conflicts. Otherwise can bloat db
        id=cur.fetchone()[0]
        external_identifiers_to_cdf(id,d[k]['ExternalIdentifiers'],cur)

    return()

# offices
    report.append('Entering Offices information')
    d = state.offices
    for k in d.keys():
        cur.execute('INSERT INTO cdf.office (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name= %s RETURNING id',[k,k])   # *** OK as long as there aren't too many conflicts. Otherwise can bloat db
        id=cur.fetchone()[0]
        external_identifiers_to_cdf(id,d[k]['ExternalIdentifiers'],cur)
        description = d[k].get('Description',None)
        if description:
            cur.execute('UPDATE cdf.office SET description = CONCAT(description,";",%s) WHERE id = %s AND description != %s',[description,id,description])
        conn.commit()



######## obsolete below ***
