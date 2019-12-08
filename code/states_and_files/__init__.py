#!usr/bin/python3
import re
import sys
import os.path
from os import path
import psycopg2
from psycopg2 import sql

## Define classes

class State:
    def __init__(self,abbr,name,meta_parser,type_map,schema_name,path_to_state_dir,main_reporting_unit_type,reporting_units,elections,parties,offices):
        self.abbr = abbr
        self.name = name
        self.meta_parser=meta_parser
        self.type_map=type_map
        self.schema_name=schema_name
        if path_to_state_dir[-1] != '/':     # should end in /
            path_to_state_dir += '/'
        self.path_to_state_dir=path_to_state_dir    #  include 'NC' in path.
        self.main_reporting_unit_type=main_reporting_unit_type
        self.reporting_units=reporting_units  # dictionary, with external codes
        self.elections=elections
        self.parties=parties
        self.offices=offices

    
class Datafile:
    def __init__(self,state, election, table_name, file_name, encoding,metafile_name,metafile_encoding,value_convention,source_url,file_date,download_date,note,correction_query_list):
        self.state=state
        self.election=election
        self.table_name=table_name
        self.file_name=file_name
        self.encoding=encoding
        self.metafile_name=metafile_name
        self.metafile_encoding=metafile_encoding
        self.value_convention=value_convention
        self.source_url=source_url
        self.file_date=file_date
        self.download_date=download_date
        self.note=note
        self.correction_query_list=correction_query_list    # fix any known metadata errors; might be unnecessary if we are loading data via python directly into CDF rather than loading data into a raw SQL db. ***

class Munger:
    def __init__(self,name,election_query):
        self.name=name      # 'nc_export1'
        self.election_query=election_query       # 'SELECT DISTINCT election_date FROM {}.{}', must have exactly two slots for state.schema and datafile.table_name
        
def file_to_context(file_path,s,df,m,conn,cur):
    ''' s is a state; df is a datafile; m is a munger '''

    
# steps to extract context info from a precinct-results file and put it into the context folder ***:

###### add election (and/or external id per file) to election.txt if necessary
    cur.execute(sql.SQL(m.election_query).format(sql.Identifier(s.schema_name), sql.Identifier(df.table_name)))
    a = cur.fetchall()
###### add county external identifiers (per file) to reporting_units.txt if necessary
###### add geoprecincts (and/or external ids per file) to reporting_units.txt if necessary
###### add not-strictly-geo reporting units (and/or external ids per file) to reporting_units.txt if necessary
###### add parties (and/or external ids per file) to parties.txt if necessary
###### add offices (and/or external ids per file) to offices.txt if necessary
######

# steps to extract other info and put it into db
###### create records in cdf.CandidateContest and cdf.BallotMeasureContest, with joins
###### create records in cdf.CandidateSelection and cdf.BallotMeasureSelection, with joins
###### create records in cdf.VoteCount, with joins
    return(a)



## Initialize classes
def create_munger(file_path):   # file should contain all munger info in a dictionary
    with open(file_path,'r') as f:
        d = eval(f.read())
    return(Munger(d['name'],d['election_query']))

def create_state(abbr,path_to_state_dir):
    '''abbr is the capitalized two-letter postal code for the state, district or territory'''
    string_attributes = ['name','schema_name','parser_string','main_reporting_unit_type']
    object_attributes = ['type_map','reporting_units','elections','parties','offices']    # what consistency checks do we need? E.g., shouldn't all reporting units start with the state name?
    if path_to_state_dir[-1] != '/':
        path_to_state_dir += '/'
    if not os.path.isdir(path_to_state_dir):
        return('Error: No directory '+path_to_state_dir)
        sys.exit()
    for attr in string_attributes + object_attributes:
        if not os.path.isfile(path_to_state_dir+'context/'+attr+'.txt'):
            return('Error: No file '+path_to_state_dir+'context/'+attr+'.txt')
            sys.exit()

    d = {} # dictionary to hold attributes
    for attr in string_attributes:     # strings
        with open(path_to_state_dir+'context/'+attr+'.txt') as f:
            d[attr]=f.readline().strip()
    for attr in object_attributes:     # python objects
        with open(path_to_state_dir+'context/'+attr+'.txt') as f:
            d[attr]=eval(f.read())
    meta_p=re.compile(d['parser_string'])
    return State(abbr,d['name'],meta_p,d['type_map'],d['schema_name'],path_to_state_dir,d['main_reporting_unit_type'],d['reporting_units'],d['elections'],d['parties'],d['offices'])

def create_datafile(s,election,data_file_name,value_convention):
    # check that election is compatible with state
    # *** need to code the value_convention to pull the right identifiers, maybe column names too.
    if s.name +' '+ election not in s.elections.keys():
        return('No such election ('+election+') associated with state '+s.name)
        # sys.exit()
    
    # read datafile info from context folder *** should be more efficient
    with open(s.path_to_state_dir+'context/datafiles.txt','r') as f:
        d_all = eval(f.read())
        d = d_all[election+';'+data_file_name]
    table_name=re.sub(r'\W+', '', election+data_file_name)
    return(Datafile(s,election, table_name,data_file_name,d['data_file_encoding'],d['meta_file'], d['meta_file_encoding'],value_convention,d['source_url'],d['file_date'],d['download_date'],d['note'],d['correction_query_list']))
    
def build_value_convention(external_key):
    ''' given an external identifier key (e.g., 'nc_export1'), return a dictionary whose keys are fields and whose values are dictionaries of field values E.g.,
        {'nc_export1':{
            'reportingunit':{
                'county':'County',
                'precinct':'CONCATENATE(county,";",precinct)'}
            },
        }
        The purpose is to explicitly map the cdf.reportingunit.name to the info in the export file.
        *** problem: for reporting units, the format depends on the type (county vs. precinct vs. vote-center, etc.), while for other things (e.g., parties) the format does not depend on the type. how to handle this discrepancy?
        '''
    
########################################

def external_identifiers_to_cdf(id,d,conn,cur):
    ''' id is a primary key for an object in the database; d is a dictionary of external identifiers for that object (e.g., {'fips':'3700000000','nc_export1':'North Carolina'}); cur is a cursor on the db. The function alters the table cdf.externalidentifier appropriately. '''
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
def old_create_datafile(s,file_name):
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

