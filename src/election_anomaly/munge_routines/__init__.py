#!/usr/bin/python3
# munge_routines/__init__.py
# under construction

import db_routines as dbr
import pandas as pd
import PySimpleGUI as sg
import sqlalchemy as db



def report_error(error_string):
    print('Munge error: '+error_string)

def id_and_name_from_external_PANDAS(ei_dframe, t_dframe,  external_name, identifiertype_id, otheridentifiertype, internal_name_field='Name',t_dframe_Id_is_index=True):
    ## find the internal db name and id from external identifier

    ei_value_d = {'Value':external_name,'IdentifierType_Id':identifiertype_id,'OtherIdentifierType':otheridentifiertype}
    ei_filtered = ei_dframe.loc[(ei_dframe[list(ei_value_d)] == pd.Series(ei_value_d)).all(axis=1)]
    if ei_filtered.shape[0] == 1:
        table_id = ei_filtered['ForeignId'].to_list()[0]
        if t_dframe_Id_is_index:
            name = t_dframe.loc[table_id][internal_name_field]
        else:
            name = t_dframe[t_dframe['Id'] == table_id][internal_name_field].tolist()[0]
        return table_id, name
    elif ei_filtered.shape[0] > 1:
        raise Exception('Unexpected duplicates found')
    else:
        return None, None

def id_from_select_only_PANDAS(dframe,value_d, mode='no_dupes',dframe_Id_is_index=True):
    """Returns the Id of the record in table with values given in the dictionary value_d.
    On error (nothing found, or more than one found) returns 0"""

    # filter the dframe by the relevant value_d conditions
    cdf_value_d = {}
    for k,v in value_d.items():
        if k in dframe.columns:
            cdf_value_d[k] = v
    filtered_dframe = dframe.loc[(dframe[list(cdf_value_d)] == pd.Series(cdf_value_d)).all(axis=1)]

    if filtered_dframe.shape[0] == 0: # if no rows meet value_d criteria
        return 0
    elif filtered_dframe.shape[0] >1 and mode == 'no_dupes':
        raise Exception('More than one record found for these values:\n\t'+str(value_d))
    else:
        if dframe_Id_is_index:
            return filtered_dframe.index.to_list()[0]
        else:
            return filtered_dframe['Id'].to_list()[0]

def id_from_select_or_insert_PANDAS(dframe, value_d, session, schema, db_table_name,mode='no_dupes',dframe_Id_is_index=True):
    """  value_d gives the values for the fields in the dataframe.
    If there is a corresponding record in the table, return the id (and the original dframe)
    If there is no corresponding record, insert one into the db and return the id and the updated dframe.
    (.e.g., value_d['Name'] = 'North Carolina;Alamance County');
    E.g., tables_d[table] = {'tablename':'ReportingUnit', 'fields':[{'fieldname':'Name','datatype':'TEXT'}],
    'enumerations':['ReportingUnitType','CountItemStatus'],'other_element_refs':[], 'unique_constraints':[['Name']],
    'not_null_fields':['ReportingUnitType_Id']
    modes with consequences: 'dupes_ok'
       } """
    # filter the cdf_value_d by the relevant value_d conditions # TODO This code repeats
    cdf_value_d = {}
    for k,v in value_d.items():
        if k in dframe.columns:
            cdf_value_d[k] = v

    # filter the dframe by the value_d conditions
    filtered_dframe = dframe.loc[(dframe[list(cdf_value_d)] == pd.Series(cdf_value_d)).all(axis=1)]

    if mode == 'no_dupes' and filtered_dframe.shape[0] > 1: # if there are dupes (and we care)
        raise Exception('Duplicate values found for ' + str(value_d))
    if filtered_dframe.shape[0] == 0:   # if no such row found
        filtered_dframe = filtered_dframe.append(value_d, ignore_index=True)
        dbr.dframe_to_sql(filtered_dframe, session, schema, db_table_name)
        if dframe_Id_is_index:
            index_col = 'Id'
        else:
            index_col = None
        dframe = pd.read_sql_table(db_table_name,session.bind,schema=schema,index_col=index_col)
        id = id_from_select_only_PANDAS(dframe,value_d,mode=mode,dframe_Id_is_index= dframe_Id_is_index)
    else:
        id = id_from_select_only_PANDAS(dframe,value_d,mode=mode,dframe_Id_is_index= dframe_Id_is_index)
    assert filtered_dframe.shape[0] == 1, 'filtered dataframe should have exactly one row'
    return id, dframe

def composing_from_reporting_unit_name_PANDAS(session,schema,ru_dframe,cruj_dframe,name,id=0):
    """inserts all ComposingReportingUnit joins that can be deduced from the internal db name of the ReportingUnit
    into the ComposingReportingUnitJoin dataframe; returns bigger dataframe.
    # Use the ; convention to identify all parents
    """
    if id == 0:
        child_id, ru_dframe = id_from_select_or_insert_PANDAS(ru_dframe, {'Name': name},session,schema,'ReportingUnit')
    else:
        child_id = id
    chain = name.split(';')
    if len(chain) > 1:
        for i in range(1,len(chain)):
            parent = ';'.join(chain[0:i])
            parent_id = id_from_select_only_PANDAS(ru_dframe, {'Name': parent})
            unused_id, cruj_dframe = id_from_select_or_insert_PANDAS(cruj_dframe, {'ParentReportingUnit_Id': parent_id, 'ChildReportingUnit_Id': child_id},session,schema,'ComposingReportingUnitJoin')
    return cruj_dframe

def format_type_for_insert_PANDAS(dframe,txt,id_type_other_id,t_dframe_Id_is_index=True):
    """This is designed for enumeration dframes, which must have an "Id" field and a "Txt" field.
    other_id is the id for 'other' IdentifierType
    This function returns a (type_id, othertype_text) pair; for types in the enumeration, returns (type_id for the given txt, ""),
    while for other types returns (type_id for "other",txt) """
    # TODO check that dframe columns are 'Id' and 'Txt'
    assert 'Txt' in dframe.columns, 'dframe must have a Txt column'
    if t_dframe_Id_is_index:
        id_list = dframe.index[dframe['Txt'] == txt].to_list()
    else:
        assert 'Id' in dframe.columns, 'When flag t_dframe_Id_is_index is false, there must be an Id column in dframe'
        id_list = dframe[dframe['Txt'] == txt].to_list()
    if len(id_list) == 1:   # TODO prevent multiple fillings of *Type tables, which yield rowcounts > 1
        return([id_list[0],''])
    elif len(id_list) == 0:
        return[id_type_other_id,txt]
    else:
         raise Exception('Dataframe has duplicate rows with value ' + txt + ' in Txt column')

def fill_cdf_table_from_raw(session, row, cdf_schema, mu, t, ei_dframe, foreign_key_d = {}, filters=[], id_type_other_id=0, index_col='Id'):
    """
    t is name of table in cdf
    mu is munger
    `row` is a dataframe of the raw data file; debugger may not recognize its use, hidden in eval()
    NB: the name `row` is essential and appears in def of munger as of 1/2020
    """
    # get munger info
    munger_fields_d = mu.content_dictionary['fields_dictionary']

    ei_dict = {} # to hold internal_name - internal_id pairs
    ids_d = foreign_key_d  # note: name `ids_d` is used in definition of munger, so can't be changed.

    # filter the row dataframe
    for f in filters:
        row = row[eval(f)]
        print('\tFilter: '+f)

    t_dframe = pd.read_sql_table(t, session.bind, cdf_schema, index_col=index_col)
    for item in munger_fields_d[t]:  # there should be only one of these
        # loop through unique values in the raw file
        raw_column = eval(item['ExternalIdentifier'])
        print('\tFor table '+t+', '+ str(len(raw_column.unique())) + ' items to process')
        for external_name in raw_column.unique():
            if external_name and external_name.strip() != '':   # treat only items with content
                # print('\t\tProcessing '+external_name)
                # get internal db name and id for ExternalIdentifier
                [cdf_id, cdf_name] = id_and_name_from_external_PANDAS(ei_dframe, t_dframe, external_name, id_type_other_id, mu.name, item['InternalNameField']) # TODO note new flag t_dframe_Id_is_index for id_and_name_from_external
                # ... or if no such is found in db, insert it!
                if [cdf_id, cdf_name] == [None, None]:
                    if external_name:
                        cdf_name = external_name.strip()
                    value_d ={**{item['InternalNameField']: cdf_name},**foreign_key_d}
                    for e in item['Enumerations'].keys():
                        [value_d[e + '_Id'], value_d['Other' + e]] = format_type_for_insert_PANDAS(t_dframe,item['Enumerations'][e])
                        # TODO note new flag t_dframe_Id_is_index for format_type_for_insert_PANDAS
                        # format_type_for_insert(session,meta.tables[meta.schema + '.' + e], item['Enumerations'][e])
                    for f in item['OtherFields'].keys():
                        value_d[f] = eval(item['OtherFields'][f])
                    cdf_id = id_from_select_only_PANDAS(t_dframe,value_d) # TODO note new flag t_dframe_Id_is_index for id_from_select_only_PANDAS
                    if cdf_id == 0:  # if nothing found
                        # TODO must insert. But with what Id?  What is we pull 'Id' as a plain column to t_dframe?
                        cdf_id, t_dframe = id_from_select_or_insert_PANDAS(t_dframe, value_d,session,cdf_schema,t)
                if cdf_name is not None:
                    ei_dict[cdf_name] = cdf_id
        # % commit table to the db
    t_dframe = dbr.dframe_to_sql(t_dframe,session,cdf_schema,t)
    print('\tTable loaded to database: '+cdf_schema+'.'+t)

    #%% pull the table back from the db, to get Ids right.
    t_dframe = pd.read_sql_table(t,session.bind,cdf_schema,index_col=index_col)
    return t_dframe, ei_dict

def bulk_elements_to_cdf(session,mu,row,cdf_schema,context_schema,election_id,id_type_other_id,state_id):
    """
    NOTE: Tables from context assumed to exist already in db
    (e.g., BallotMeasureSelection, Party, ExternalIdentifier, ComposingReportingUnitJoin, Election, ReportingUnit etc.)
    Create tables, which are repetitive,
    and don't come from context
    and whose db Ids are needed for other insertions.
    `row` is a dataframe of the raw data file
    Assumes table 'ExternalIdentifierContext' in the context schema
    """

    # NB: the name `row` in the code is essential and appears in def of munger as of 1/2020
    cdf_d = {}  # dataframe for each table
    for t in ['ExternalIdentifier','Party','BallotMeasureSelection','ReportingUnit','Office','CountItemType']:
        cdf_d[t] = pd.read_sql_table(t, session.bind, cdf_schema)   # note: keep 'Id as df column (not index) so we have access in merges below.
    context_ei = pd.read_sql_table('ExternalIdentifierContext',session.bind,context_schema)
    context_ei = context_ei[ (context_ei['ExternalIdentifierType']== mu.name)]  # limit to our munger

    row_copy = row.copy() # TODO delete


    munge = {}
    munge['Office'] = "row['Contest Name']"  # TODO munger dependent
    munge['Party'] = "row['Choice Party']"  # TODO munger dependent
    munge['Candidate'] = "row['Choice']"  # TODO munger dependent
    munge['ReportingUnit'] = "row['County'] + ';' + row['Precinct']" # TODO munger dependent

    # add columns for ids needed later
    row['Election_Id'] = [election_id] * row.shape[0]
    row['ReportingUnit_external'] = eval(munge['ReportingUnit'])

    cdf_d['ReportingUnit'] = pd.read_sql_table('ReportingUnit',session.bind,cdf_schema)
    # TODO ru_id
    row = row.merge(context_ei[context_ei['Table']=='ReportingUnit'],left_on='ReportingUnit_external',right_on='ExternalIdentifierValue',suffixes=['','_ReportingUnit']).drop(['ExternalIdentifierValue','Table'],axis=1)
    row.rename(columns={'Name':'ReportingUnit'},inplace=True)
    row = row.merge(cdf_d['ReportingUnit'],left_on='ReportingUnit',right_on='Name',suffixes=['','_ReportingUnit']).drop('Name',axis=1)
    row.rename(columns={'Id':'ReportingUnit_Id'},inplace=True)

    # TODO split row into a df for ballot measures and a df for contests
    bm_selections = cdf_d['BallotMeasureSelection']['Selection'].to_list()
    munge['BallotMeasureSelection'] = "row['Choice']"

    bm_row = row[eval(munge['BallotMeasureSelection']).isin(bm_selections)]
    cc_row = row[~(eval(munge['BallotMeasureSelection']).isin(bm_selections))]

# Process rows with ballot measures and selections
    munge['BallotMeasureContest'] = "row['Contest Name']"   # TODO munger dependent
    print('WARNING: all ballot measure contests assumed to have the whole state as their district')
    row = bm_row

    for munge_key in ['BallotMeasureContest','BallotMeasureSelection']:
        row[munge_key] = eval(munge[munge_key])

    # bm_df = row[row['BallotMeasureSelection'].isin(bm_selections)][['BallotMeasureContest', 'BallotMeasureSelection']].drop_duplicates()
    bm_df = row[['BallotMeasureContest', 'BallotMeasureSelection']].drop_duplicates()
    bm_df.columns = ['Name', 'Selection']  # internal db name for ballot measure contest matches name in file
    bm_df['ElectionDistrict_Id'] = [state_id] * bm_df.shape[0]  # append column for ElectionDistrict Id

    # Load BallotMeasureContest table
    cdf_d['BallotMeasureContest'] = dbr.dframe_to_sql(bm_df[['Name', 'ElectionDistrict_Id']].drop_duplicates(), session,
                                                      cdf_schema, 'BallotMeasureContest')

    # add Ballot Measure ids needed later
    row = row.merge(cdf_d['BallotMeasureSelection'],left_on='BallotMeasureSelection',right_on='Selection',suffixes=['','_Selection'])
    row.rename(columns={'Id':'Selection_Id'},inplace=True)
    row = row.merge(cdf_d['BallotMeasureContest'],left_on='BallotMeasureContest',right_on='Name',suffixes=['','_Contest'])
    row.rename(columns={'Id':'Contest_Id'},inplace=True)

    # Load BallotMeasureContestSelectionJoin table
    # TODO why was it empty?
    # to make sure all added columns get labeled well, make sure 'Name' and 'Id' are existing columns
    if 'Name' not in bm_df.columns:
        bm_df['Name'] = [None]*bm_df.shape[0]
    if 'Id' not in bm_df.columns:
        bm_df['Id']  = [None]*bm_df.shape[0]

    bm_df = bm_df.merge(cdf_d['BallotMeasureSelection'],left_on='Selection',right_on='Selection',suffixes=['','_Selection'])
    bm_df = bm_df.merge(cdf_d['BallotMeasureContest'],left_on='Name',right_on='Name',suffixes=['','_Contest'])
    bmcsj_df  = bm_df.drop(labels=['Name','Selection','ElectionDistrict_Id','Id','ElectionDistrict_Id_Contest'],axis=1)
    bmcsj_df.rename(columns={'Id_Selection':'BallotMeasureSelection_Id','Id_Contest':'BallotMeasureContest_Id'},inplace=True)
    cdf_d['BallotMeasureContestSelectionJoin'] = dbr.dframe_to_sql(bmcsj_df,session,cdf_schema,'BallotMeasureContestSelectionJoin')

    # Load ElectionContestJoin table (for ballot measures)
    ecj_df = cdf_d['BallotMeasureContest'].copy()
    ecj_df['Election_Id'] = [election_id] * ecj_df.shape[0]
    ecj_df.rename(columns={'Id': 'Contest_Id'}, inplace=True)
    cdf_d['ElectionContestJoin'] = dbr.dframe_to_sql(ecj_df,session,cdf_schema,'ElectionContestJoin')

    # TODO load vote counts and vote count join ???  !!!
    print('Create vote_counts dframe')
    vote_counts = row.drop(['County','Election Date','Precinct','Contest Group ID','Contest Type','Contest Name','Choice','Choice Party','Vote For','Real Precinct','ReportingUnit_external','ReportingUnit','index','ExternalIdentifierType','ReportingUnitType_Id','OtherReportingUnitType','CountItemStatus_Id','OtherCountItemStatus','BallotMeasureContest','Name','BallotMeasureSelection','Selection', 'ElectionDistrict_Id'],axis=1)
    vc_col_d = {k:v['CountItemType'] for k,v in mu.content_dictionary['counts_dictionary'].items()}
    vote_counts.rename(columns=vc_col_d,inplace=True)
    print('Reshape')
    vote_counts=vote_counts.melt(id_vars=['Election_Id','Contest_Id','Selection_Id','ReportingUnit_Id'],value_vars=['election-day', 'early', 'absentee-mail', 'provisional', 'total'],var_name='CountItemType',value_name='Count')
    print('Merge CountItemType')
    vote_counts = vote_counts.merge(cdf_d['CountItemType'],left_on='CountItemType',right_on='Txt')
    vote_counts.rename(columns={'Id':'CountItemType_Id'},inplace=True)

    # TODO need to get 'VoteCount_Id' attached to the correct row. Plan: temporariy add columns to VoteCount
    # add SelectionElectionContestJoin columns to VoteCount
    print('Add columns to cdf table')
    session.execute('ALTER TABLE ' + cdf_schema + '."VoteCount" ADD COLUMN "Election_Id" INTEGER, ADD COLUMN "Contest_Id" INTEGER,  ADD COLUMN "Selection_Id" INTEGER') # TODO don't use string concat!!
    session.flush()
    # dbr.add_int_columns(con,cdf_schema,'VoteCount',['Election_Id','Contest_Id','Selection_Id'])
    print('Upoad to VoteCount')
    vote_counts_fat = dbr.dframe_to_sql(vote_counts,session,cdf_schema,'VoteCount')
    vote_counts_fat.rename(columns={'Id':'VoteCount_Id'},inplace=True)
    session.flush()
    print('Upload to SelectionElectionContestVoteCountJoin')

    cdf_d['SelectionElectionContestVoteCountJoin'] = dbr.dframe_to_sql(vote_counts_fat,session,cdf_schema,'SelectionElectionContestVoteCountJoin')
    print('Drop columns from cdf table')
    session.execute('ALTER TABLE ' + cdf_schema + '."VoteCount" DROP COLUMN , DROP COLUMN "Contest_Id", DROP COLUMN "Selection_Id"') # TODO don't use string concat!!
    session.flush()


    cdf_d['VoteCounts'] = dbr.dframe_to_sql(vote_counts,session,cdf_schema,'VoteCount')
    # vote_counts = vote_counts.merge(cdf_d['VoteCounts'],left_on='')
    # vote_counts.rename({'Id':'VoteCount_Id'},inplace=True)
    # dbr.dframe_to_sql(vote_counts,session,cdf_schema,'SelectionElectionContestVoteCountJoin')

# process rows with candidate contests
    row = cc_row
    # to make sure all added columns get labeled well, make sure 'Name' and 'Id' are existing columns
    if 'Name' not in row.columns:
        row['Name'] = [None]*row.shape[0]
    if 'Id' not in row.columns:
        row['Id']  = [None]*row.shape[0]

    # create columns with good internal labels
    for munge_key in ['Office','Party','ReportingUnit','Candidate']:
        row[munge_key] = eval(munge[munge_key])
    # append columns with info from context tables of cdf db
    for t in ['Office','Party','ReportingUnit']:    # Office first is most efficient, as it filters out rows for offices not listed in Office.txt
        filtered_ei = context_ei[(context_ei['Table'] == t) & (context_ei['ExternalIdentifierType'] == mu.name)][['Name','ExternalIdentifierValue']]
        filtered_ei.columns = [t+'_Name','ExternalIdentifierValue']
        row = row.merge(filtered_ei,left_on=t,right_on='ExternalIdentifierValue',suffixes=['','_'+t]).drop(labels=['ExternalIdentifierValue'],axis=1)
        row = row.merge(cdf_d[t],left_on=t+'_Name',right_on='Name',suffixes=['','_'+t])

    # load Candidate table
    row['Candidate'] = eval(munge['Candidate'])
    c_df = row[['Candidate','Id_Party']].copy().drop_duplicates()
    c_df.rename(columns={'Candidate':'BallotName','Id_Party':'Party_Id'},inplace=True)
    c_df['Election_Id'] = [election_id] * c_df.shape[0]
    cdf_d['Candidate'] = dbr.dframe_to_sql(c_df,session,cdf_schema,'Candidate')

    # load CandidateSelection
    cs_df = cdf_d['Candidate'].copy()
    cs_df.rename(columns={'Id':'Candidate_Id'},inplace=True)
    cdf_d['CandidateSelection'] = dbr.dframe_to_sql(cs_df,session,cdf_schema,'CandidateSelection')

    # load CandidateContest
    office_context_df = pd.read_sql_table('Office',session.bind,schema=context_schema)
    cc_df = office_context_df.merge(row[['Name_Office','Id_Office']],left_on='Name',right_on='Name_Office',suffixes=['','_row'])[['Name','VotesAllowed','NumberElected','NumberRunoff','Id_Office','ElectionDistrict']].merge(cdf_d['ReportingUnit'],left_on='ElectionDistrict',right_on='Name',suffixes=['','_ReportingUnit'])
    cc_df.rename(columns={'Id_Office':'Office_Id','Id_ReportingUnit':'ElectionDistrict_Id'},inplace=True)
    cdf_d['CandidateContest'] = dbr.dframe_to_sql(cc_df,session,cdf_schema,'CandidateContest')

    # load ElectionContestJoin for Candidate Contests
    ecj_df = cdf_d['CandidateContest'].copy()
    ecj_df['Election_Id'] = [election_id] * ecj_df.shape[0]
    ecj_df.rename(columns={'Id': 'Contest_Id'}, inplace=True)
    cdf_d['ElectionContestJoin'] = dbr.dframe_to_sql(ecj_df,session,cdf_schema,'ElectionContestJoin')

    #  load CandidateContestSelectionJoin
    ccsj_df = row[['Candidate','Id_Office']].copy().drop_duplicates().\
        merge(cdf_d['CandidateContest'], left_on='Id_Office', right_on='Office_Id',suffixes=['','_CandidateContest']).\
        merge(cdf_d['Candidate'],left_on='Candidate',right_on='BallotName',suffixes=['','_Candidate']).\
        merge(cdf_d['CandidateSelection'],left_on='Id_Candidate',right_on='Candidate_Id',suffixes=['','_CandidateSelection'])
    ccsj_df.rename(columns={'Id_CandidateSelection':'CandidateSelection_Id','Id':'CandidateContest_Id'},inplace=True)
    # ccsj_df['Election_Id'] = [election_id] * ccsj_df.shape[0]
    cdf_d['CandidateContestSelectionJoin'] = dbr.dframe_to_sql(ccsj_df,session,cdf_schema,'CandidateContestSelectionJoin')

    session.flush()
    return

def bulk_elements_to_cdf_OLD(session, mu, cdf_schema, row, election_id, id_type_other_id,state_id):
    """
    Create tables, which are repetitive,
    don't come from context (hence not BallotMeasureSelection, Party)
    and whose db Ids are needed for other insertions.
    `row` is a dataframe of the raw data file
    NB: the name `row` is essential and appears in def of munger as of 1/2020
    """
    assert int(id_type_other_id) and id_type_other_id != 0,'id_type_other_id must be a nonzero integer'
    assert int(state_id) and state_id !=0, 'state_id must be a nonzero integer'

    cdf_d = {}  # dataframe for each table
    ei_d = {}   # external-internal name dictionary for each table

    # get external identifier info
    cdf_d['ExternalIdentifier'] = pd.read_sql_table('ExternalIdentifier', session.bind, cdf_schema, index_col='Id')
    cdf_ei = cdf_d['ExternalIdentifier']  # for legibility

    cdf_d['Party'] = pd.read_sql_table('Party',session.bind,cdf_schema,index_col='Id')

    # get BallotMeasureSelections to distinguish between BallotMeasure- and Candidate-Contests
    cdf_d['BallotMeasureSelection'] = pd.read_sql_table('BallotMeasureSelection',session.bind,cdf_schema,index_col='Id')
    bm_selections = cdf_d['BallotMeasureSelection']['Selection'].to_list()

    # load BallotMeasureContest
    foreign_key_d = {'state':state_id}
    raw_filters=[" | ".join(["(row['Choice'] == '" + i + "')" for i in bm_selections])] # TODO munger-dependent
    cdf_d['BallotMeasureContest'], ei_d['BallotMeasureContest'] = fill_cdf_table_from_raw(session, row, cdf_schema, mu, 'BallotMeasureContest', cdf_d['ExternalIdentifier'], filters=raw_filters, foreign_key_d=foreign_key_d, id_type_other_id=id_type_other_id)

    # load Contest-Election join table
    ecj_list = []
    for contest_type in ['Candidate','BallotMeasure']:
        cdf_d[contest_type + 'Contest'] = pd.read_sql_table(contest_type + 'Contest',session.bind,cdf_schema,index_col='Id')
        for idx in cdf_d[contest_type + 'Contest'].index:
            ecj_list.append([idx,election_id])
    ecj_df = pd.DataFrame(ecj_list,columns=['Contest_Id','Election_Id'])
    cdf_d['ElectionContestJoin'] = dbr.dframe_to_sql(ecj_df,session,cdf_schema,'ElectionContestJoin')

    #  process Candidates and CandidateContestSelectionJoin, looping through party-contest pairs
    party_ext_name = row['Choice Party']    # TODO munger-dependent
    contest_ext_name = row['Contest Name']  # TODO munger-dependent
    candidate_ext_name = row['Choice']  # TODO munger-dependent

    # TODO make frame of unique triples; then merge ids; then load to cdf db.
    triple_df = pd.concat([party_ext_name,contest_ext_name,candidate_ext_name],axis=1).drop_duplicates()
    triple_df.columns = ['Party','CandidateContest','Candidate']
    for index, triple in triple_df:
        filtered_row = row[(row['Choice Party'] == triple['Party']) & (row['Contest Name'] == triple['CandidateContest'])]
        party_id = cdf_d['ExternalIdentifier'][(cdf_d['ExternalIdentifier']['Table'] == 'Party') & (cdf_d['ExternalIdentifier']['Value'] == triple['Party'])  ]['ForeignId'].to_list()[0]
        foreign_key_d = {'Party_Id':party_id,'Election_Id':election_id}
# TODO filter first, group by candidate, then pass to fill_cdf function
        cdf_d['Candidate'], ei_d['Candidate'] = fill_cdf_table_from_raw(session, filtered_row, cdf_schema, mu, 'Candidate', cdf_d['ExternalIdentifier'], foreign_key_d=foreign_key_d, filters=[], id_type_other_id=id_type_other_id)


    # process CandidateSelection
    cdf_d['CandidateSelection'] = pd.DataFrame(cdf_d['Candidate'].index.values, columns=['Candidate_Id'])
    cdf_d['CandidateSelection'] = dbr.dframe_to_sql(cdf_d['CandidateSelection'], session, cdf_schema, 'CandidateSelection')

    # TODO load CandidateContest-CandidateSelection join table

    # TODO load BallotMeasureContest-Selection join table

    return cdf_d, id_type_other_id

def row_by_row_elements_to_cdf(session,mu,cdf_schema,raw_rows,cdf_d,election_id,id_type_other_id,contest_type='Candidate'):
    """
    mu is a munger. cdf_d is a dictionary of dataframes
    contest type must be either 'Candidate' or 'BallotMeasure'. Can't treat both at once.
    """

    ids_d = {}
    name_d = {}

    # TODO merge with externalidentifier table before going row by row
    for index, row in raw_rows.iterrows():
        # track progress
        sg.one_line_progress_meter('row-by-row progress', index + 1, raw_rows.shape[0], 'key')
        frequency_of_report = 500
        if index % frequency_of_report == 0:
            print('\t\tProcessing row ' + str(index) + ':\n' + str(row))

        if contest_type == 'Candidate':
            external_office_name = eval(mu.content_dictionary['fields_dictionary']['Office'][0]['ExternalIdentifier'])
            ids_d['Office'],internal_office_name = id_and_name_from_external_PANDAS(cdf_d['ExternalIdentifier'],cdf_d['Office'], external_office_name,id_type_other_id,mu.name)
            if ids_d['Office'] == 0: # skip rows for which office was not explicitly listed in context folder
                continue
            ids_d['contest_id'] = id_from_select_only_PANDAS(cdf_d['CandidateContest'], {'Office_Id': ids_d['Office']})
            assert ids_d['contest_id'] !=0 , 'contest_id cannot be zero'

        for t in ['ReportingUnit','Party','Office']:
            # TODO error handling? What if id not found?
            for item in mu.content_dictionary['fields_dictionary'][t]:
                ids_d[t],name_d[t] = id_and_name_from_external_PANDAS(cdf_d['ExternalIdentifier'], cdf_d[t], eval(item['ExternalIdentifier']),id_type_other_id,mu.name,item['InternalNameField'])

        # process Candidate and BallotMeasure elements
        if contest_type == 'BallotMeasure':
            selection = eval(mu.content_dictionary['fields_dictionary']['BallotMeasureSelection'][0]['ExternalIdentifier'])
            ids_d['selection_id'] = cdf_d['BallotMeasureSelection'][cdf_d['BallotMeasureSelection']['Selection'] == selection].index.to_list()[0]
            ids_d['contest_id'] = id_from_select_only_PANDAS(cdf_d['BallotMeasureContest'],{'Name':eval(mu.content_dictionary['fields_dictionary']['BallotMeasureContest'][0]['ExternalIdentifier'])})
        # fill BallotMeasureContestSelectionJoin
            value_d = {'BallotMeasureContest_Id': ids_d['contest_id'], 'BallotMeasureSelection_Id': ids_d['selection_id']}
            join_id,cdf_d['BallotMeasureContestSelectionJoin'] = id_from_select_or_insert_PANDAS(cdf_d['BallotMeasureContestSelectionJoin'],value_d,session,cdf_schema,'BallotMeasureContestSelectionJoin')
        else:
            ballot_name = eval(mu.content_dictionary['fields_dictionary']['Candidate'][0]['ExternalIdentifier'])
            ids_d['Candidate'] = id_from_select_only_PANDAS(cdf_d['Candidate'],{'BallotName':ballot_name})
            ids_d['selection_id'] = id_from_select_only_PANDAS(cdf_d['CandidateSelection'],{'Candidate_Id':ids_d['Candidate']})

        # fill CandidateContestSelectionJoin
            value_d = {'CandidateContest_Id': ids_d['contest_id'], 'CandidateSelection_Id': ids_d['selection_id'],'Election_Id':election_id}
            join_id,cdf_d['CandidateContestSelectionJoin'] = id_from_select_or_insert_PANDAS(cdf_d['CandidateContestSelectionJoin'], value_d, session, cdf_schema, 'CandidateContestSelectionJoin')

        # fill ElectionContestJoin
        # TODO doesn't need to be row-by-row, can be done in bulk.
        value_d = {'Election_Id': election_id, 'Contest_Id': ids_d['contest_id']}
        join_id, cdf_d['ElectionContestJoin'] = id_from_select_or_insert_PANDAS(cdf_d['ElectionContestJoin'], value_d, session, cdf_schema,'ElectionContestJoin')

        for ct,dic in mu.content_dictionary['counts_dictionary'].items():
        # fill VoteCount
            value_d = {'Count':row[ct],'ReportingUnit_Id':ids_d['ReportingUnit'],'CountItemType_Id': dic['CountItemType_Id'],'OtherCountItemType':dic['OtherCountItemType']}
            # TODO dupes are a problem only when contest & reporting unit are specified.
            ids_d['VoteCount'], cdf_d['VoteCount'] =id_from_select_or_insert_PANDAS(cdf_d['VoteCount'], value_d, session, cdf_schema, 'VoteCount',  'dupes_ok')

        # fill SelectionElectionContestVoteCountJoin
            value_d = {'Selection_Id':ids_d['selection_id'],'Contest_Id':ids_d['contest_id'],'Election_Id':election_id,'VoteCount_Id':ids_d['VoteCount']}
            join_id, cdf_d['SelectionElectionContestVoteCountJoin'] = id_from_select_or_insert_PANDAS(cdf_d['SelectionElectionContestVoteCountJoin'],  value_d, session, cdf_schema,'SelectionElectionContestVoteCountJoin')
        if index % frequency_of_report == 0:
            print('\t\tPushing to db ')
            for t in ['BallotMeasureContestSelectionJoin', 'CandidateContestSelectionJoin', 'ElectionContestJoin',
                      'VoteCount', 'SelectionElectionContestVoteCountJoin']:
                print ('Pushing to database table '+ t)
                dbr.dframe_to_sql(cdf_d[t],session,cdf_schema,t)
            session.flush()

    for t in ['BallotMeasureContestSelectionJoin','CandidateContestSelectionJoin','ElectionContestJoin','VoteCount','SelectionElectionContestVoteCountJoin']:
        dbr.dframe_to_sql(cdf_d[t],session,cdf_schema,t)
    session.flush()
    return

def raw_records_to_cdf(session,meta,df,mu,cdf_schema,context_schema,state_id = 0,id_type_other_id = 0,cdf_table_filepath='CDF_schema_def_info/tables.txt'):
    """ munger-agnostic raw-to-cdf script; ***
    df is datafile, mu is munger """
    cdf_d = {}  # to hold various dataframes from cdf db tables


    # get id for IdentifierType 'other' if it was not passed as parameter
    if id_type_other_id == 0:
        cdf_d['IdentifierType'] = pd.read_sql_table('IdentifierType', session.bind, cdf_schema, index_col='Id')
        id_type_other_id = cdf_d['IdentifierType'].index[cdf_d['IdentifierType']['Txt'] == 'other'].to_list()[0]
        if not id_type_other_id:
            raise Exception('No Id found for IdentifierType \'other\'; fix IdentifierType table and rerun.')

    with open(cdf_table_filepath, 'r') as f:
        table_def_list = eval(f.read())
    tables_d = {}
    for table_def in table_def_list:
        tables_d[table_def[0]] = table_def[1]

    # get dataframes needed before bulk processing
    for t in ['ElectionType', 'Election','ReportingUnitType','ReportingUnit','CountItemType']:
        cdf_d[t] = pd.read_sql_table(t, session.bind, cdf_schema, index_col='Id')


    # get id for  election
    [electiontype_id, otherelectiontype] = format_type_for_insert_PANDAS(cdf_d['ElectionType'],df.state.context_dictionary['Election'][df.election]['ElectionType'],id_type_other_id)
    value_d = {'Name': df.election, 'EndDate': df.state.context_dictionary['Election'][df.election]['EndDate'],
               'StartDate': df.state.context_dictionary['Election'][df.election]['StartDate'],
               'OtherElectionType': otherelectiontype, 'ElectionType_Id': electiontype_id}
    election_id, cdf_d['Election'] = id_from_select_or_insert_PANDAS(cdf_d['Election'], value_d,session,cdf_schema,'Election')

    # if state_id is not passed as parameter, select-or-insert state, get id (default Reporting Unit for ballot questions)
    if state_id == 0:
        [reportingunittype_id, otherreportingunittype] = format_type_for_insert_PANDAS(cdf_d['ReportingUnitType'], 'state',id_type_other_id)
        value_d = {'Name': df.state.name, 'ReportingUnitType_Id': reportingunittype_id,
                   'OtherReportingUnitType': otherreportingunittype}
        state_id, cdf_d['ReportingUnit'] = id_from_select_or_insert_PANDAS(cdf_d['ReportingUnit'], value_d, session, cdf_schema,'ReportingUnit')

    # store state_id and election_id for later use
    ids_d = {'state': state_id, 'Election_Id': election_id}  # to hold ids of found items for later reference

    munger_raw_cols = mu.content_dictionary['raw_cols'] # TODO is this used?
    # create dictionaries for processing data from rows. Not all CDF elements are included. E.g., 'Election' element is not filled from df rows, but from df.election

    munger_counts_d = mu.content_dictionary['counts_dictionary'] # TODO is this used?
    # look up id,type pairs for each kind of count, add info to counts dictionary
    for ct,dic in munger_counts_d.items():
        text = dic['CountItemType']
        [dic['CountItemType_Id'], dic['OtherCountItemType']] = format_type_for_insert_PANDAS(cdf_d['CountItemType'], text,id_type_other_id)
    munger_fields_d = mu.content_dictionary['fields_dictionary']
    # TODO is the above used?
    #%% read raw data rows from db
    raw_rows = pd.read_sql_table(df.table_name,session.bind,schema=df.state.schema_name)

    bulk_items_already_loaded = input('Are bulk items (Candidate, etc.) already loaded (y/n)?\n')
    if bulk_items_already_loaded != 'y':
        bulk_elements_to_cdf(session, mu,raw_rows, cdf_schema, context_schema, election_id, id_type_other_id,ids_d['state'])

    # get all dataframes needed for processing row-by-row
    for t in ['Party', 'Office','CandidateContest','Candidate','CandidateSelection','BallotMeasureContest','VoteCount','SelectionElectionContestVoteCountJoin','BallotMeasureContestSelectionJoin','ElectionContestJoin','CandidateContestSelectionJoin','ComposingReportingUnitJoin','ExternalIdentifier','BallotMeasureSelection']:
        cdf_d[t] = pd.read_sql_table(t, session.bind, cdf_schema, index_col='Id')

    process_ballot_measures = input('Process ballot measures (y/n)?\n')
    if process_ballot_measures == 'y':
        print('\tFiltering for desired rows of raw file')
        selection_list = list(cdf_d['BallotMeasureSelection']['Selection'].unique())
        ballot_measure_rows = raw_rows['Choice'].isin(selection_list) # TODO Munger-dependent
        print('\tStart row-by-row processing')
        row_by_row_elements_to_cdf(session, mu, cdf_schema, ballot_measure_rows, cdf_d, election_id, id_type_other_id,contest_type='BallotMeasure')


    process_candidate_contests = input('Process candidate contests [whose offices are listed in Office.txt] (y/n)?\n')
    if process_candidate_contests == 'y':
        print('\tFiltering for desired rows of raw file')
        cdf_office_list = list(cdf_d['Office'].index.unique())
        raw_office_list = cdf_d['ExternalIdentifier'][cdf_d['ExternalIdentifier']['ForeignId'].isin(cdf_office_list)]['Value'].to_list()
        bool_rows = raw_rows['Contest Name'].isin(raw_office_list)    # TODO munger dependent, will need dataframe to have name from munger (as of 1/2020, 'row')
        cc_rows = raw_rows.loc[bool_rows]
        print('\tStart row-by-row processing')
        row_by_row_elements_to_cdf(session,mu,cdf_schema,cc_rows,cdf_d,election_id,id_type_other_id,contest_type='Candidate')

    return str(ids_d)

def is_ballot_measure(row,selection_list,mu):
    """
    row is a pd.Series; cdf is a dictionary of dataframes,
    one of which must be 'BallotMeasureSelection' with index Id
    another  of which must be 'ExternalIdentifier'
    mu is a munger # TODO fix description
    """
    bm_filter = " | ".join(["(row['Choice'] == '" + i + "')" for i in selection_list])  # TODO munger-dependent
    is_bm_row = eval(bm_filter)
    return is_bm_row

if __name__ == '__main__':
    import db_routines.Create_CDF_db as CDF
    import states_and_files as sf
    from sqlalchemy.orm import sessionmaker

    cdf_schema='cdf_nc_test2'
    eng,meta = dbr.sql_alchemy_connect(schema=cdf_schema,paramfile='../../local_data/database.ini')
    Session = sessionmaker(bind=eng)
    session = Session()

    s = sf.create_state('NC', '../../local_data/NC')

    munger_path = '../../local_data/mungers/nc_export1.txt'
    print('Creating munger instance from ' + munger_path)
    mu = sf.create_munger(munger_path)

    print('Creating metafile instance')
    mf = sf.create_metafile(s, 'layout_results_pct.txt')

    print('Creating datafile instance')
    df = sf.create_datafile(s, 'General Election 2018-11-06', 'filtered_results_pct_20181106.txt', mf, mu)

    row = pd.read_sql_table('GeneralElection20181106filtered_results_pct_20181106txt',session.bind,s.schema_name)

    election_id = 3218
    id_type_other_id = 35
    state_id = 59
    bulk_elements_to_cdf(session, mu, row, cdf_schema, s.schema_name, election_id, id_type_other_id, state_id)


    raw_records_to_cdf(session,meta,df,mu,cdf_schema,s.schema_name,0,0,'../CDF_schema_def_info/tables.txt')
    print('Done!')

