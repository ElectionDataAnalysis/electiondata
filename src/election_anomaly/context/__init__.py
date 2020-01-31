#!/usr/bin/python3
# context/__init__.py
# under construction
# utilities for extracting state context info and inserting it into the files in the context folder
import sys
import re
import os

from db_routines import dframe_to_sql
from munge_routines import format_type_for_insert_PANDAS, id_from_select_only_PANDAS, composing_from_reporting_unit_name_PANDAS
import db_routines as dbr
import pandas as pd


def table_and_name_to_foreign_id(dict,row):
    """
    row must be a Series object, and must have entries Table and Name.
    dict must be a dictionary of dataframes, keyed by table name.
    """
    id = id_from_select_only_PANDAS(dict[row['Table']], {'Name': row['Name']})
    if id == 0:
        raise Exception('No entry found for \n\tTable: '+ row['Table']+'\n\tName: '+row['Name'])
    else:
        return id

def ei_id_and_othertype(df,row,other_id):
    """
    row must be a Series object, and must have entry 'ExternalIdentifierType'
    df must be a dataframe with columns Id and Txt
    other_id must be the Id for the dataframe entry Txt = 'other'
    """

    return format_type_for_insert_PANDAS(df,row['ExternalIdentifierType'],other_id)



def context_to_cdf_PANDAS(session,meta,s,schema,enum_table_list,cdf_def_dirpath = 'CDF_schema_def_info/'):
    """Takes the info from the text files in the state's context files and inserts it into the db.
    Assumes enumeration tables are already filled.
    """
    context_cdframe = {}    # dictionary of dataframes from context info
    cdf_d = {}  # dict of dframes for CDF db tables

    #%% create and fill enum dframes and associated dictionaries
    enum_dframe = {}        # dict of dataframes of enumerations, taken from db
    other_id = {}       # dict of the Id for 'other' in the various enumeration tables
    enum_id_d = {}  # maps raw Type string to an Id
    enum_othertype_d = {}  # maps raw Type string to an othertype string
    for e in enum_table_list:
        enum_id_d[e] = {}  # maps raw Type string to an Id
        enum_othertype_d[e] = {}  # maps raw Type string to an othertype string

        # %% pull enumeration table into a DataFrame
        enum_dframe[e] = pd.read_sql_table(e, session.bind, schema=schema, index_col='Id')
        # %% find the id for 'other', if it exists
        try:
            other_id[e] = enum_dframe[e].index[enum_dframe[e]['Txt'] == 'other'].to_list()[0]
        except:  # CountItemStatus has no "other" field
            other_id[e] = None  # TODO how does this flow through?
        # %% create and (partially) fill the id/othertype dictionaries

    # pull list of tables in CDF
    if not cdf_def_dirpath[-1] == '/': cdf_def_dirpath += '/'
    with open(cdf_def_dirpath+'tables.txt','r') as f:
        table_def_list = eval(f.read())

    for table_def in table_def_list:      # iterating over tables in the common data format schema, need 'Office' after 'ReportingUnit'
        ## need to process 'Office' after 'ReportingUnit', as Offices may create ReportingUnits as election districts *** check for this

        t = table_def[0]      # e.g., cdf_table = 'ReportingUnit'

        # for each table # Note:s.context_dictionary.keys() doesn't include Candidate and other tables filled from datafile, not from context.

        # fill BallotMeasureSelection dframe, table, pickle


        if t in s.context_dictionary.keys(): # excludes 'ExternalIdentifier', as well as tables filled from datafile but not from context, such as Candidate
            print('\tProcessing ' + t + 's')
            # TODO read dataframe for t from pickle if available

            # create DataFrame with enough info to define db table eventually
            if t == 'BallotMeasureSelection':   # note: s.context_dictionary['BallotMeasureSelection'] is a set not a dict
                context_cdframe['BallotMeasureSelection'] = pd.DataFrame(list(s.context_dictionary['BallotMeasureSelection']),columns=['Selection'])
                # %% commit table to db
                dframe_to_sql(context_cdframe[t], session, schema, t)

            else:
                context_cdframe[t] = pd.read_csv(s.path_to_state_dir + 'context/'+ t + '.txt',sep = '\t')
                for e in table_def[1]['enumerations']:  # e.g., e = "ReportingUnitType"
                    #%% for every instance of the enumeration in the current table, add id and othertype columns to the dataframe
                    if e in context_cdframe[t].columns: # some enumerations (e.g., CountItemStatus for t = ReportingUnit) are not available from context.
                        for v in context_cdframe[t][e].unique(): # e.g., v = 'county' or v = 'precinct'
                            enum_id_d[e][v],enum_othertype_d[e][v] = format_type_for_insert_PANDAS(enum_dframe[e],v,other_id[e])
                        #%% create new id, othertype columns
                        context_cdframe[t][e+'_Id'] = context_cdframe[t][e].map(enum_id_d[e])
                        context_cdframe[t]['Other'+e] = context_cdframe[t][e].map(enum_othertype_d[e])

                # %% commit table to db
                dframe_to_sql(context_cdframe[t], session, schema, t)

                if t == 'Office':
                    # Check that all ElectionDistrictTypes are recognized
                    for edt in context_cdframe['Office']['ElectionDistrictType'].unique():
                        enum_id_d['ReportingUnitType'][edt], enum_othertype_d['ReportingUnitType'][edt] = format_type_for_insert_PANDAS(enum_dframe['ReportingUnitType'], edt, other_id['ReportingUnitType'])
                        if [enum_id_d['ReportingUnitType'][edt], enum_othertype_d['ReportingUnitType'][edt]] == [None,None]:
                            raise Exception('Office table has unrecognized ElectionDistrictType: ' + edt)

                    # insert corresponding ReportingUnits, that don't already exist in db ReportingUnit table.
                    cdf_d['ReportingUnit'] = pd.read_sql_table('ReportingUnit',session.bind,schema,index_col=None)
                    # note: db Id column is *not* the index for the dataframe cdf_d['ReportingUnit'].
                    new_ru = []
                    for index, context_row in context_cdframe['Office'].iterrows():   # TODO more pyhonic/pandic way?
                        if context_row['ElectionDistrict'] not in list(cdf_d['ReportingUnit']['Name']):
                            new_ru.append ( pd.Series({'Name':context_row['ElectionDistrict'],'ReportingUnitType_Id':enum_id_d['ReportingUnitType'][context_row['ElectionDistrictType']],'OtherReportingUnitType':enum_othertype_d['ReportingUnitType'][context_row['ElectionDistrictType']]}))
                    # send any new ReportingUnits into the db
                    new_ru_dframe = pd.DataFrame(new_ru)
                    cdf_d['ReportingUnit'] = dframe_to_sql(new_ru_dframe,session,schema,'ReportingUnit',index_col=None)

                    # create corresponding CandidateContest records, if they don't already exist
                    # TODO include VotesAllowed, NumberElected and NumberRunoff fields, probably from context
                    print('WARNING: Note assumption that VotesAllowed = 1 for all contests!!!!')
                    # TODO MAJOR FAULTY ASSUMPTION: assuming VotesAllowed = 1

                    cdf_d['CandidateContest'] = pd.read_sql_table('CandidateContest', session.bind, schema,index_col=None)
                    cdf_d['Office'] = pd.read_sql_table('Office', session.bind, schema,index_col=None)
                    new_cc = []
                    for index, context_row in context_cdframe['Office'].iterrows():
                        if context_row['ElectionDistrict'] not in cdf_d['CandidateContest']['Name'].to_list():
                            ru_id = id_from_select_only_PANDAS(cdf_d['ReportingUnit'],{'Name':context_row['ElectionDistrict']},dframe_Id_is_index=False)
                            office_id = id_from_select_only_PANDAS(cdf_d['Office'],{'Name':context_row['Name']},dframe_Id_is_index=False)
                            new_cc.append(pd.Series({'Name':context_row['ElectionDistrict'], 'Office_Id':office_id, 'ElectionDistrict_Id':ru_id,'VotesAllowed':1}))
                            # TODO MAJOR FAULTY ASSUMPTION: assuming VotesAllowed = 1
                    # send any new CandidateContests to the db
                    new_cc_dframe = pd.DataFrame(new_cc)
                    cdf_d['CandidateContest'] = dframe_to_sql(new_cc_dframe,session,schema,'CandidateContest')
                    session.flush()
    # TODO need to fill CandidateContest table


    #%% fill ExternalIdentifier table
    print('Fill ExternalIdentifier table')
    # TODO why does this step take so long?
    # get table from context directory with the tab-separated definitions of external identifiers
    context_cdframe['ExternalIdentifier'] = pd.read_csv(s.path_to_state_dir + 'context/ExternalIdentifier.txt',sep = '\t')
    ei_df = context_cdframe['ExternalIdentifier']   # for legibility

    # pull corresponding tables from the cdf db
    cdf = {}
    for t in ei_df['Table'].unique():
        cdf[t] = pd.read_sql_table(t,session.bind,schema,'Id')

    # add columns to dframe to match columns in CDF db
    ei_df['ForeignId'] = ei_df.apply(lambda row: table_and_name_to_foreign_id(cdf,row),axis=1)
    ei_df['Value'] = ei_df['ExternalIdentifierValue']
    ### apply(lambda ...) returns a column of 2-elt lists, so need to unpack.
    ei_df['id_othertype_pairs']= ei_df.apply(lambda row: ei_id_and_othertype(enum_dframe['IdentifierType'],row,other_id['IdentifierType']),axis=1)

    ei_df['IdentifierType_Id'] = ei_df.apply(lambda row: row['id_othertype_pairs'][0],axis=1)
    ei_df['OtherIdentifierType'] = ei_df.apply(lambda row: row['id_othertype_pairs'][1],axis=1)

    # insert appropriate dataframe columns into ExternalIdentifier table in the CDF db
    dframe_to_sql(ei_df, session, schema, 'ExternalIdentifier')
    session.flush()

    cdf_d['ComposingReportingUnitJoin'] = fill_composing_reporting_unit_join(session,schema,cdf_d,pickle_dir=s.path_to_state_dir+'pickles/')

    return

def fill_composing_reporting_unit_join(session,schema,cdf_d,pickle_dir='../local_data/pickles/'):
    if os.path.isfile(pickle_dir + 'ComposingReportingUnitJoin'):
        print('Filling ComposingReportingUnitJoin table from pickle in ' + pickle_dir)
        cruj_dframe = pd.read_pickle(pickle_dir + 'ComposingReportingUnitJoin')
    else:
        print('Filling ComposingReportingUnitJoin table, i.e., recording nesting relations of ReportingUnits')
        # TODO why does this take so long?
        cruj_dframe = pd.read_sql_table('ComposingReportingUnitJoin', session.bind, schema,index_col='Id')
        ru_dframe = pd.read_sql_table('ReportingUnit', session.bind, schema,index_col='Id')
        for index,context_row in ru_dframe.iterrows():
            cruj_dframe = composing_from_reporting_unit_name_PANDAS(session, schema, ru_dframe,cruj_dframe,context_row['Name'],index)
        cruj_dframe.to_pickle(pickle_dir + 'ComposingReportingUnitJoin')

    dframe_to_sql(cruj_dframe, session, schema, 'ComposingReportingUnitJoin')
    session.flush()
    return cruj_dframe



def build_munger_d(s,m):
    """given a state s and a munger m,
    use the state's context dictionaries to build some dictionaries restricted to the given munger.
    """
    munger_d = {}
    munger_inverse_d = {}
    key_list = ['Election','Party','ReportingUnit;precinct','Office']   # TODO should this be different for different mungers?
    for t in key_list:
        t_parts = t.split(';')
        context_key = t_parts[0]            # e.g., 'ReportingUnit', or 'Election'
        if len(t_parts) > 1:
            type = t_parts[1]               # e.g., 'precinct' or None
        else:
            type = None
        munger_d[t] = {}
        for k in s.context_dictionary[context_key].keys():  # e.g., k = 'North Carolina;General Assembly;House of Representatives;2019-2020;District 1'
            if 'ExternalIdentifiers' in s.context_dictionary[context_key][k].keys() and   m.name in s.context_dictionary[context_key][k]['ExternalIdentifiers'].keys() and (type == None or s.context_dictionary[context_key][k][context_key+'Type'] == type):
                    munger_d[t][k] = s.context_dictionary[context_key][k]['ExternalIdentifiers'][m.name]
        munger_inverse_d[t] = {}
        for k,v in munger_d[t].items():
            if v in munger_inverse_d[t].keys():
                return('Error: munger_d[\''+t+'\'] has duplicate keys with value '+ v)
            munger_inverse_d[v] = k
    return(munger_d,munger_inverse_d)

def raw_to_context(df,m,munger_d,con,cur):
    ''' Purely diagnostic -- reports what items in the datafile are missing from the context_dictionary (e.g., offices we don't wish to analyze)'''
    print('\'Missing\' below means \'Existing in the datafile, but missing from the munger dictionary, created from the state\'s context_dictionary, which was created from files in the context folder.')
    for t in m.query_from_raw.keys():
        t_parts = t.split(';')
        context_key = t_parts[0]
        if len(t_parts) > 1:
            type = t_parts[1]
        if context_key in df.state.context_dictionary.keys():   # why do we need this criterion? ***
            items_per_df = dbr.query(m.query_from_raw[t],[df.state.schema_name,df.table_name],[],con,cur) # TODO revise now that query_from_raw no longer works
            missing = []
            for e in items_per_df:
                if e[0] is not None and e[0] not in munger_d[t].values():
                    missing.append(e[0])
            if len(missing)>0:
                missing.sort()   #  and sort
            print('Sample data for '+t+': '+str( items_per_df[0:4]))
            print('For \''+m.name +'\', <b> missing '+t+' list is: </b>'+str(missing)+'. Add any missing '+t+' to the '+context_key+'.txt file and rerun')
    return



### supporting routines
def shorten_and_cap_county(normal):
    ''' takes a county name in normal form, strips "County" from the name, and capitalizes'''
    parts=normal.split(';')
    
    parser = re.compile('^(?P<short_name>[^\n\t]+)\ County')
    return(parser.search(parts[1]).group('short_name').upper())

def add_externalidentifier(dict,id_type):
    '''input is a dictionary whose keys are county names in normal form and values are dictionaries, including identifiertype-identifier pairs, and an identifiertype. Output is same dictionary, with the identifiers (short name, all caps) included, labeled by the given id_type.'''
    for k in dict.keys():
        if dict[k]['Type'] == 'county':
            print(k)
            dict[k]['ExternalIdentifiers'][id_type]=shorten_and_cap_county(k)
    return(dict)
        
def dict_insert(dict_file_path,input_d):
    '''Insert the objects in the dictionary (input_d) into the dictionary stored in the file (at dict_file_path), updating each ExternalIdentifiers dict and any new info, throwing error if the dictionaries conflict'''
    with open(dict_file_path,'r') as f:
        file_d = eval(f.read())
    for k in input_d.keys():
        if k in file_d.keys():
            for kk in input_d[k].keys():
                if kk == 'ExternalIdentifiers':  # update external identifiers, checking for conflict
                    for kkk in input_d[k]['ExternalIdentifiers'].keys():
                        if kkk in file_d[k]['ExternalIdentifiers'].keys():
                            if input_d[k]['ExternalIdentifiers'][kkk] != file_d[k]['ExternalIdentifiers'][kkk]:
                                print('Error: ExternalIdentifiers conflict on ' + kkk)
                                sys.exit()
                        else:
                             file_d[k]['ExternalIdentifiers'][kkk] = input_d[k]['ExternalIdentifiers'][kkk]
                else:   # for properties of the item other than External Idenifiers
                    if kk in file_d[k].keys():
                        if input_d[k][kk] != file_d[k][kk]:
                            print('Error: conflict on ' + kk)
                            sys.exit()
                    else:
                        file_d[k][kk]=input_d[k][kk]
        else:
            file_d[k] = input_d[k]    # put input_d info into file_d
    with open(dict_file_path,'w') as f:
            f.write(str(file_d))
    return(file_d)


def insert_reporting_unit(dict,reporting_unit_list,id_type):
    '''Insert the reporting units in reporting_unit_list (list of unit, type pairs) into dict, with correct type (e.g., precinct) and recording the name of the reporting unit also as an external identifier, unless the reporting unit is already in the dict, in which case do the right thing. '''
    for r in reporting_unit_list:
        k = r[0]    # Reporting unit
        t = r[1]    # Type
        if k not in dict.keys():    # if reporting unit not already in dictionary, add it
            dict[k]={'Type':t,'ExternalIdentifiers':{id_type:k}}
        elif dict[k]['Type'] != t: # if reporting type is in the dictionary, but has different 'Type'
            t_dict = dict[k]['Type']
            dict[r+' ('+  t_dict   +')'] = dict.pop(r) # rename existing key to include type (e.g., precinct)
            dict[r+' ('+  t   +')'] = {'Type':t,'ExternalIdentifiers':{id_type:r}}
            
def extract_precincts(s,df):
    ''' s is a state; df is a datafile with precincts (*** currently must be in the format of the nc_pct_results file; need to read info from metafile) '''
    if s != df.state:   # consistency check: file must belong to state
        print('Datafile ' +df+ ' belongs to state '+df.state.name+', not to '+s.name)
    rep_unit_list=[]
    with open(s.path_to_state_dir+'data/'+df.file_name,'r') as f:
        lines=f.readlines()
    for line in lines[1:]:
        fields = line.strip('\n\r').split('\t')
        real_precinct=fields[14]
        if real_precinct == 'Y':     # if row designated a "real precinct" in th nc file
            county = fields[0]
            precinct = fields[2]
            rep_key = s.name+';'+county.capitalize()+' County;Precinct '+precinct
            rep_unit_list.append([rep_key,'precinct'])  # return key and 'Type'
        elif real_precinct == 'N':
            county = fields[0]
            election = fields[1]
            precinct = fields[2]
            rep_key = s.name+';'+county.capitalize()+' County;'+election+';'+precinct
            rep_unit_list.append([rep_key,'other;'+rep_key])
    return(rep_unit_list)
    

    
def insert_offices(s,d):
    ''' s is a state; d is a dictionary giving the number of districts for standard offices within the state, e.g., {'General Assembly;House of Representatives':120,'General Assembly;Senate':50} for North Carolina. Returns dictionary of offices. '''
    state = s.name
    out_d = {}
    for k in d.keys():
        for i in range(d[k]):
            office_name = state + ';' + k +';District ' + str(i+1)
            out_d[office_name] = {'ElectionDistrict':office_name}
    out_d['North Carolina;US Congress;Senate']={'ElectionDistrict':'North Carolina'}
    dict_insert(s.path_to_state_dir + 'context/Office.txt',out_d)
    return(out_d)
    
if __name__ == '__main__':
    from sqlalchemy.orm import sessionmaker
    import states_and_files as sf
    import db_routines.Create_CDF_db as CDF

    schema='test'
    eng,meta = dbr.sql_alchemy_connect(schema=schema,paramfile='../../local_data/database.ini')
    Session = sessionmaker(bind=eng)
    session = Session()

    data = {'Foo':[1,2,3,2,1],'Selection_Id':[42,43,44,44,46],'Id':[2,3,4,5,6]}
    test_dframe = pd.DataFrame(data=data)
    new = dframe_to_sql(test_dframe, session, schema, 'SelectionElectionContestVoteCountJoin', index_col='Id')

    s = sf.create_state('NC','../../local_data/NC/')
    enumeration_table_list = CDF.enum_table_list(dirpath='../CDF_schema_def_info/')
    context_to_cdf_PANDAS(session,meta,s,schema,enumeration_table_list,cdf_def_dirpath='../CDF_schema_def_info/')
    print('Done!')


