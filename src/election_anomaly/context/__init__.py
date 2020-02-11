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
        ## need to process 'Office' after 'Party' so that primary contests will be entered

        t = table_def[0]      # e.g., cdf_table = 'ReportingUnit'



        if t in s.context_dictionary.keys(): # excludes 'ExternalIdentifier', as well as tables filled from datafile but not from context, such as Candidate
            print('\tProcessing ' + t + 's')
            # TODO read dataframe for t from pickle if available

            # create DataFrames of relevant context information
            if t == 'BallotMeasureSelection':   # note: s.context_dictionary['BallotMeasureSelection'] is a set not a dict
                context_cdframe['BallotMeasureSelection'] = pd.DataFrame(list(s.context_dictionary['BallotMeasureSelection']),columns=['Selection'])
                # %% commit table to db
                cdf_d[t] = dframe_to_sql(context_cdframe[t], session, schema, t)

            else:
                context_cdframe[t] = pd.read_csv(s.path_to_state_dir + 'context/'+ t + '.txt',sep = '\t')
                # save to state schema
                context_cdframe[t].to_sql(t, session.bind, schema=s.schema_name,
                             if_exists='replace')  # TODO better option than replacement?

                for e in table_def[1]['enumerations']:  # e.g., e = "ReportingUnitType"
                    #%% for every instance of the enumeration in the current table, add id and othertype columns to the dataframe
                    if e in context_cdframe[t].columns: # some enumerations (e.g., CountItemStatus for t = ReportingUnit) are not available from context.
                        for v in context_cdframe[t][e].unique(): # e.g., v = 'county' or v = 'precinct'
                            enum_id_d[e][v],enum_othertype_d[e][v] = format_type_for_insert_PANDAS(enum_dframe[e],v,other_id[e])
                        #%% create new id, othertype columns
                        context_cdframe[t][e+'_Id'] = context_cdframe[t][e].map(enum_id_d[e])
                        context_cdframe[t]['Other'+e] = context_cdframe[t][e].map(enum_othertype_d[e])

                # %% commit info in context_cdframe to corresponding cdf table to db
                cdf_d[t] = dframe_to_sql(context_cdframe[t], session, schema, t)

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

                    # create corresponding CandidateContest records for general election contests (and insert in cdf db if they don't already exist)
                    cc_data = context_cdframe['Office'].merge(cdf_d['Office'],left_on='Name',right_on='Name').merge(cdf_d['ReportingUnit'],left_on='Name',right_on='Name',suffixes=['','_ru'])
                    # restrict to the columns we need, and set order
                    cc_data = cc_data[['Name','VotesAllowed','NumberElected','NumberRunoff','Id','Id_ru','IsPartisan']]
                    # rename columns as necesssary
                    cc_data.rename(columns={'Id_ru':'ElectionDistrict_Id','Id':'Office_Id'},inplace=True)
                    # insert values for 'PrimaryParty_Id' column
                    cc_data['PrimaryParty_Id'] = [None]*cc_data.shape[0]
                    cc_d_gen = cc_data.copy()
                    for party_id in cdf_d['Party']['Id'].to_list():
                        pcc = cc_d_gen[cc_d_gen['IsPartisan']]    # non-partisan contests don't have party primaries, so omit them.
                        pcc['PrimaryParty_Id'] = party_id
                        pcc['Name'] = pcc['Name'] + ' Primary;' + cdf_d['Party'][cdf_d['Party']['Id'] == party_id].iloc[0]['Name']
                        cc_data = pd.concat([cc_data,pcc])


                    cdf_d['CandidateContest'] = dframe_to_sql(cc_data,session,schema,'CandidateContest')

                    # create corresponding CandidateContest records for primary contests (and insert in cdf db if they don't already exist)


    # load external identifiers from context

    cdf_d['ExternalIdentifier'] = fill_externalIdentifier_table(session,schema,s.schema_name,enum_dframe,other_id['IdentifierType'],s.path_to_state_dir + 'context/ExternalIdentifier.txt',pickle_dir=s.path_to_state_dir+'pickles/')
    # load CandidateContest external ids into cdf_d['ExternalIdentifier'] too
    ei_df = cdf_d['Office'].merge(cdf_d['ExternalIdentifier'],left_on='Id',right_on='ForeignId',suffixes=['_office','ei']).merge(cdf_d['CandidateContest'],left_on='Id',right_on='Office_Id',suffixes=['','_cc'])[['Id_cc','Value','IdentifierType_Id','OtherIdentifierType']]
    ei_df.columns = ['ForeignId','Value','IdentifierType_Id','OtherIdentifierType']
    cdf_d['ExternalIdentifier'] = dframe_to_sql(ei_df,session,schema,'ExternalIdentifier')

    # Fill the ComposingReportingUnitJoin table
    cdf_d['ComposingReportingUnitJoin'] = fill_composing_reporting_unit_join(session,schema,cdf_d,pickle_dir=s.path_to_state_dir+'pickles/') # TODO put pickle directory info into README.md
    session.flush()
    return

def fill_externalIdentifier_table(session,schema,context_schema,enum_dframe,id_other_id_type,fpath,pickle_dir='.'):
    """
    fpath is a path to the tab-separated context file holding the external identifier info
    s is the state
    """
    if os.path.isfile(pickle_dir + 'ExternalIdentifier'):
        print('Filling ExternalIdentifier table from pickle in ' + pickle_dir)
        ei_df = pd.read_pickle(pickle_dir + 'ExternalIdentifier')
    else:
        print('Fill ExternalIdentifier table')
        # TODO why does this step take so long?
        # get table from context directory with the tab-separated definitions of external identifiers
        ei_df = pd.read_csv(fpath,sep = '\t')

        # load context table into state schema for later reference
        # dframe_to_sql(ei_df,session,s.schema_name,'ExternalIdentifierContext')
        ei_df.to_sql('ExternalIdentifierContext',session.bind,schema=context_schema,if_exists='replace') # TODO better option than replacement?

        # pull corresponding tables from the cdf db
        cdf = {}
        for t in ei_df['Table'].unique():
            cdf[t] = pd.read_sql_table(t,session.bind,schema,'Id')

        # add columns to ei_dframe to match columns in CDF db
        # TODO use .merge(), etc., not .apply(), and take care of primaries


        ei_df['ForeignId'] = ei_df.apply(lambda row: table_and_name_to_foreign_id(cdf,row),axis=1) # TODO make this work for primaries, where office name is not the same as the contest name
        ei_df['Value'] = ei_df['ExternalIdentifierValue']
        ### apply(lambda ...) returns a column of 2-elt lists, so need to unpack.
        ei_df['id_othertype_pairs']= ei_df.apply(lambda row: ei_id_and_othertype(enum_dframe['IdentifierType'],row,id_other_id_type),axis=1)

        ei_df['IdentifierType_Id'] = ei_df.apply(lambda row: row['id_othertype_pairs'][0],axis=1)
        ei_df['OtherIdentifierType'] = ei_df.apply(lambda row: row['id_othertype_pairs'][1],axis=1)
        ei_df.to_pickle(pickle_dir + 'ExternalIdentifier')

    # insert appropriate dataframe columns into ExternalIdentifier table in the CDF db
    dframe_to_sql(ei_df, session, schema, 'ExternalIdentifier')
    session.flush()
    return ei_df

def fill_composing_reporting_unit_join(session,schema,cdf_d,pickle_dir='../local_data/pickles/'):
    if os.path.isfile(pickle_dir + 'ComposingReportingUnitJoin'):
        print('Filling ComposingReportingUnitJoin table from pickle in ' + pickle_dir)
        cruj_dframe = pd.read_pickle(pickle_dir + 'ComposingReportingUnitJoin')
    else:
        print('Filling ComposingReportingUnitJoin table, i.e., recording nesting relations of ReportingUnits')
        # TODO why does this take so long?

        # TODO speedup plan: 1. pull ru table; 2. check that all component rus are in the RU table; 3. don't make calls to db except for push at end
        cruj_dframe = pd.read_sql_table('ComposingReportingUnitJoin', session.bind, schema,index_col='Id')
        ru_dframe = pd.read_sql_table('ReportingUnit', session.bind, schema,index_col='Id')

        # check that all components of all Reporting Units are themselves ReportingUnits

        for index,context_row in ru_dframe.iterrows():
            cruj_dframe = composing_from_reporting_unit_name_PANDAS(session, schema, ru_dframe,cruj_dframe,context_row['Name'],index)
        cruj_dframe.to_pickle(pickle_dir + 'ComposingReportingUnitJoin')

    dframe_to_sql(cruj_dframe, session, schema, 'ComposingReportingUnitJoin')
    session.flush()
    return cruj_dframe


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


