#!/usr/bin/python3
# munge_routines/__init__.py
# under construction

import db_routines as dbr
import pandas as pd
import time
import analyze as an
import re

def add_munged_column(row_df,munge_dictionary,munge_key,new_col_name):
    """Alters dataframe <row_df> (in place), adding or redefining <new_col_name>
    via the string corresponding to <munge_key>, per <munge_dictionary>"""
    if row_df.empty:
        return
    else:
        # use regex to turn value string in munge dictionary into the corresponding commands (e.g., decode '<County>;<Precinct>'
        p = re.compile('(?P<text>[^<>]*)<(?P<field>[^<>]+)>')   # pattern to find text,field pairs
        q = re.compile('(?<=>)[^<]*$')                          # pattern to find text following last pair
        text_field_list = re.findall(p,munge_dictionary[munge_key])
        last_text = re.findall(q,munge_dictionary[munge_key])

        if last_text:
            row_df.loc[:,new_col_name] = last_text[0]
        else:
            row_df.loc[:,new_col_name] = ''

        text_field_list.reverse()
        for t,f in text_field_list:
            row_df.loc[:,new_col_name] = t+row_df.loc[:,f]+row_df.loc[:,new_col_name]

        return

def get_internal_ids_from_context(row_df,ctxt_ei_df,table_df,table_name,internal_name_column,drop_unmatched=False):
    """replace columns in <df> with external identifier values by columns with internal names
    Note: this requires using the ExternalIdentifier table from the context schema, which has the info about which table the element is in. """
    if drop_unmatched:
        how='inner'
    else:
        how='left'
    row_df = row_df.merge(ctxt_ei_df[ctxt_ei_df['Table'] == table_name],how=how,left_on=table_name + '_external',right_on='ExternalIdentifierValue',suffixes=['','_' + table_name]).drop(['ExternalIdentifierValue','Table','ExternalIdentifierType','index',table_name+'_external'],axis=1)
    if 'Name_'+table_name in row_df.columns:
        row_df.rename(columns={'Name_'+table_name:table_name},inplace=True)
    else:
        row_df.rename(columns={'Name':table_name},inplace=True)
    row_df = row_df.merge(table_df[['Id',internal_name_column]],how='left',left_on=table_name,right_on=internal_name_column,suffixes=['','_' + table_name])
    row_df=row_df.drop([internal_name_column],axis=1)
    row_df.rename(columns={'Id':table_name + '_Id','Name':table_name},inplace=True)

    return row_df

def enum_col_to_id_othertext(df,type_col,enum_df):
    """Returns a copy of dataframe <df>, replacing a <type> column (e.g., 'CountItemType') with
    the corresponding two id and othertext columns (e.g., 'CountItemType_Id' and 'OtherCountItemType
    using the enumeration given in <enum_df>"""
    assert type_col in df.columns
    assert 'Txt' in enum_df.columns and 'Id' in enum_df.columns,'Enumeration dataframe should have columns \'Id\' and \'Txt\''
    for c in ['Id','Txt']:
        if c in df.columns:
            # avoid conflict by temporarily renaming the column in the main dataframe
            assert c*3 not in df.colums, 'Column name '+c*3+' conflicts with variable used in code'
            df.rename(columns={c:c*3},inplace=True)
    df = df.merge(enum_df,how='left',left_on=type_col,right_on='Txt')
    df.rename(columns={'Id':type_col+'_Id'},inplace=True)
    df.loc[:,'Other'+type_col]=''

    other_id_df = enum_df[enum_df['Txt']=='other']
    if not other_id_df.empty:
        other_id = other_id_df.iloc[0]['Id']
        df[type_col+'_Id'] = df[type_col+'_Id'].fillna(other_id)
        df.loc[df[type_col+'_Id'] == other_id,'Other'+type_col] = df.loc[df[type_col+'_Id'] == other_id,type_col]
    df = df.drop(['Txt',type_col],axis=1)
    for c in ['Id','Txt']:
        if c*3 in df.columns:
            # avoid restore name renaming the column in the main dataframe
            df.rename(columns={c*3:c},inplace=True)
    return df

def bulk_elements_to_cdf(session,mu,row,cdf_schema,context_schema,election_id,election_type,state_id):
    """
    NOTE: Tables from context assumed to exist already in db
    (e.g., BallotMeasureSelection, Party, ExternalIdentifier, ComposingReportingUnitJoin, Election, ReportingUnit etc.)
    Create tables, which are repetitive,
    and don't come from context
    and whose db Ids are needed for other insertions.
    `row` is a dataframe of the raw data file
    Assumes table 'ExternalIdentifier' in the context schema
    """

    # NB: the name `row` in the code is essential and appears in def of munger as of 1/2020
    cdf_d = {}  # dataframe for each table
    for t in ['ExternalIdentifier','Party','BallotMeasureSelection','ReportingUnit','Office','CountItemType','CandidateContest']:
        cdf_d[t] = pd.read_sql_table(t, session.bind, cdf_schema)   # note: keep 'Id as df column (not index) so we have access in merges below.
    context_ei = pd.read_sql_table('ExternalIdentifier',session.bind,context_schema)
    context_ei = context_ei[ (context_ei['ExternalIdentifierType']== mu.name)]  # limit to our munger

    # get vote count column mapping for our munger
    fpath = mu.path_to_munger_dir
    vc_col_d = pd.read_csv(fpath + 'count_columns.txt',sep='\t',index_col='RawName').to_dict()['CountItemType']
    col_list = list(vc_col_d.values()) + ['Election_Id','ReportingUnit_Id',
                                          'ReportingUnitType_Id', 'OtherReportingUnitType', 'CountItemStatus_Id',
                                          'OtherCountItemStatus','Selection_Id','Contest_Id']    # is ElectionDistrict_Id necessary?
    munge = pd.read_csv(fpath + 'cdf_tables.txt',sep='\t',index_col='CDFTable').to_dict()['ExternalIdentifier']

    row.rename(columns=vc_col_d,inplace=True)  # standardize vote-count column names

    # add columns corresponding to cdf fields
    row.loc[:,'Election_Id'] = election_id

    for t in ['Party','ReportingUnit']:  # tables that were filled from context
        #  merge works for all columns, even if, say, Party is null, because of how='left' (left join) in add_munged_column function
        add_munged_column(row,munge,t,t+'_external')
        row=get_internal_ids_from_context(row,context_ei,cdf_d[t],t,"Name")

    # some columns are their own internal names (no external identifier map needed)
    for t in ['BallotMeasureSelection','BallotMeasureContest']:
        add_munged_column(row,munge,t,t)
    # NOTE: this will put, e.g., candidate names into the BallotMeasureSelection column; beware!
    # some columns will need to be interpreted via  ExternalIdentifier tables
    # to make sure all added columns get labeled well, make sure 'Name' and 'Id' are existing columns
    if 'Name' not in row.columns:
        row.loc[:,'Name'] = None
    if 'Id' not in row.columns:
        row.loc[:,'Id'] = None


    # split row into a df for ballot measures and a df for contests
    bm_selections = cdf_d['BallotMeasureSelection']['Selection'].to_list()

    bm_row = row[row['BallotMeasureSelection'].isin(bm_selections)]
    cc_row = row[~row['BallotMeasureSelection'].isin(bm_selections)]
    cc_row=cc_row.drop(['BallotMeasureSelection','BallotMeasureContest'],axis=1) # not necessary but cleaner for debugging

    process_ballot_measures = input('Process Ballot Measures (y/n)?\n')
    process_candidate_contests = input('Process Candidate Contests (y/n)?\n')
    vote_count_dframe_list = []

    if process_ballot_measures == 'y':
        # Process rows with ballot measures and selections
        print('WARNING: all ballot measure contests assumed to have the whole state as their district')

        # bm_df = cc_row[cc_row['BallotMeasureSelection'].isin(bm_selections)][['BallotMeasureContest', 'BallotMeasureSelection']].drop_duplicates()
        bm_df = bm_row[['BallotMeasureContest', 'BallotMeasureSelection']].drop_duplicates()
        bm_df.columns = ['Name', 'Selection']  # internal db name for ballot measure contest matches name in file
        bm_df.loc[:,'ElectionDistrict_Id'] = state_id  # append column for ElectionDistrict Id

        # Load BallotMeasureContest table to cdf schema
        cdf_d['BallotMeasureContest'] = dbr.dframe_to_sql(bm_df[['Name', 'ElectionDistrict_Id']].drop_duplicates(), session,cdf_schema, 'BallotMeasureContest')

        # add BallotMeasure-related ids needed later
        bm_row = bm_row.merge(cdf_d['BallotMeasureSelection'],left_on='BallotMeasureSelection',right_on='Selection',suffixes=['','_Selection'])
        bm_row.rename(columns={'Id_Selection':'Selection_Id'},inplace=True)
        bm_row = bm_row.merge(cdf_d['BallotMeasureContest'],left_on='BallotMeasureContest',right_on='Name',suffixes=['','_Contest'])
        bm_row.rename(columns={'Id_Contest':'Contest_Id'},inplace=True)

        # Load BallotMeasureContestSelectionJoin table to cdf schema

        bm_df = bm_df.merge(cdf_d['BallotMeasureSelection'],left_on='Selection',right_on='Selection',suffixes=['','_Selection'])
        bm_df = bm_df.merge(cdf_d['BallotMeasureContest'],left_on='Name',right_on='Name',suffixes=['','_Contest'])
        bmcsj_df = bm_df.drop(labels=['Name','Selection','ElectionDistrict_Id','Id','ElectionDistrict_Id_Contest'],axis=1)
        bmcsj_df.rename(columns={'Id_Selection':'BallotMeasureSelection_Id','Id_Contest':'BallotMeasureContest_Id'},inplace=True)
        cdf_d['BallotMeasureContestSelectionJoin'] = dbr.dframe_to_sql(bmcsj_df,session,cdf_schema,'BallotMeasureContestSelectionJoin')

        # Load ElectionContestJoin table (for ballot measures)
        ecj_df = cdf_d['BallotMeasureContest'].copy()
        ecj_df.loc[:,'Election_Id'] = election_id
        ecj_df.rename(columns={'Id': 'Contest_Id'}, inplace=True)
        cdf_d['ElectionContestJoin'] = dbr.dframe_to_sql(ecj_df,session,cdf_schema,'ElectionContestJoin')

        # create dframe of vote counts (with join info) for ballot measures
        bm_vote_counts = bm_row[col_list]
        bm_vote_counts=bm_vote_counts.melt(id_vars=['Election_Id','Contest_Id','Selection_Id','ReportingUnit_Id'],value_vars=['election-day', 'early', 'absentee-mail', 'provisional', 'total'],var_name='CountItemType',value_name='Count')
        bm_vote_counts=enum_col_to_id_othertext(bm_vote_counts,'CountItemType',cdf_d['CountItemType'])
        if not bm_vote_counts.empty:
            vote_count_dframe_list.append(bm_vote_counts)

    if process_candidate_contests == 'y':
        # process rows with candidate contests


        # Find CandidateContest_external and CandidateContest_Id and omit rows with contests not  given in CandidateContest table (filled from context)
        add_munged_column(cc_row,munge,'CandidateContest','CandidateContest_external')
        cc_row=get_internal_ids_from_context(cc_row,context_ei,cdf_d['CandidateContest'],'CandidateContest','Name',drop_unmatched=True)
        cc_row.rename(columns={'CandidateContest_Id':'Contest_Id'},inplace=True)

        # load Candidate table
        add_munged_column(cc_row,munge,'Candidate','BallotName')
        candidate_df = cc_row[['BallotName','Party_Id','Election_Id']].copy().drop_duplicates()
        candidate_df = candidate_df[candidate_df['BallotName'].notnull()]
        # TODO add  other notnull criteria
        cdf_d['Candidate'] = dbr.dframe_to_sql(candidate_df,session,cdf_schema,'Candidate')

        # load CandidateSelection
        cs_df = cdf_d['Candidate'].copy()
        cs_df.rename(columns={'Id':'Candidate_Id'},inplace=True)
        cdf_d['CandidateSelection'] = dbr.dframe_to_sql(cs_df,session,cdf_schema,'CandidateSelection')


        # add Candidate ids needed later
        cc_row = cc_row.merge(cs_df,left_on='BallotName',right_on='BallotName',suffixes=['','_Candidate'])
        cc_row.rename(columns={'Id_Candidate':'Candidate_Id'},inplace=True)
        cc_row = cc_row.merge(cdf_d['CandidateSelection'],left_on='Candidate_Id',right_on='Candidate_Id',suffixes=['','_Selection'])
        cc_row.rename(columns={'Id_Selection':'Selection_Id'},inplace=True)


        # Office depends on the election type and the CandidateContest.
        if election_type == 'general':
            # Office name is same as CandidateContest name
            cc_row['Office'] = cc_row['CandidateContest']
        elif election_type == 'primary':
            # Office name is derived from CandidateContest name
            cc_row['Office'] = cc_row['CandidateContest'].str.split(' Primary;')[0]
            # TODO check that this works for primaries
        else:
            raise Exception('Election type not recognized by the code: ' + election_type) # TODO add all election types
        cc_row.merge(cdf_d['Office'],left_on='Office',right_on='Name',suffixes=['','_Office'])
        cc_row.rename(columns={'Id_Office':'Office_Id'})

        # load ElectionContestJoin for Candidate Contests
        ecj_df = cc_row[['Contest_Id','Election_Id']].drop_duplicates()
        cdf_d['ElectionContestJoin'] = dbr.dframe_to_sql(ecj_df,session,cdf_schema,'ElectionContestJoin')

        #  load CandidateContestSelectionJoin
        ccsj_df = cc_row[['Contest_Id','Selection_Id','Election_Id']].drop_duplicates()
        cdf_d['CandidateContestSelectionJoin'] = dbr.dframe_to_sql(ccsj_df,session,cdf_schema,'CandidateContestSelectionJoin')

        # load candidate counts
        # TODO check that every merge for row creates suffix as appropriate so no coincidently named columns are dropped
        cc_vote_counts = cc_row[col_list]
        cc_vote_counts=cc_vote_counts.melt(id_vars=['Election_Id','Contest_Id','Selection_Id','ReportingUnit_Id'],value_vars=['election-day', 'early', 'absentee-mail', 'provisional', 'total'],var_name='CountItemType',value_name='Count')
        cc_vote_counts = cc_vote_counts.merge(cdf_d['CountItemType'],left_on='CountItemType',right_on='Txt')    # TODO loses rows of 'other' types
        cc_vote_counts.rename(columns={'Id':'CountItemType_Id'},inplace=True)
        vote_count_dframe_list.append(cc_vote_counts)

    vote_counts = pd.concat(vote_count_dframe_list)
    # TODO put OhterCountItemType into vote_counts
    vote_counts = vote_counts.drop(['Txt','CountItemType'],axis=1)
    # To get 'VoteCount_Id' attached to the correct row, temporarily add columns to VoteCount
    # add SelectionElectionContestJoin columns to VoteCount
    q = 'ALTER TABLE {0}."VoteCount" ADD COLUMN "Election_Id" INTEGER, ADD COLUMN "Contest_Id" INTEGER,  ADD COLUMN "Selection_Id" INTEGER'
    sql_ids=[cdf_schema]
    strs = []
    dbr.raw_query_via_SQLALCHEMY(session,q,sql_ids,strs)
    print('Upload to VoteCount')
    start = time.time()
    vote_counts_fat = dbr.dframe_to_sql(vote_counts,session,cdf_schema,'VoteCount',raw_to_votecount=True)
    vote_counts_fat.rename(columns={'Id':'VoteCount_Id'},inplace=True)
    session.commit()
    end = time.time()
    print('\tSeconds required to upload VoteCount: '+ str(round(end - start)))
    print('Upload to SelectionElectionContestVoteCountJoin')
    start = time.time()

    cdf_d['SelectionElectionContestVoteCountJoin'] = dbr.dframe_to_sql(vote_counts_fat,session,cdf_schema,'SelectionElectionContestVoteCountJoin')
    end = time.time()
    print('\tSeconds required to upload SelectionElectionContestVoteCountJoin: '+ str(round(end - start)))
    print('Drop columns from cdf table')
    q = 'ALTER TABLE {0}."VoteCount" DROP COLUMN "Election_Id", DROP COLUMN "Contest_Id" ,  DROP COLUMN "Selection_Id" '
    sql_ids=[cdf_schema]
    strs = []
    dbr.raw_query_via_SQLALCHEMY(session,q,sql_ids,strs)

    return

def raw_dframe_to_cdf(session,raw_rows,s,mu,cdf_schema,context_schema,e,state_id = 0,id_type_other_id = 0,cdf_table_filepath='CDF_schema_def_info/tables.txt'):
    """ munger-agnostic raw-to-cdf script; ***
    dframe is dataframe, mu is munger """

    assert isinstance(e,an.Election),'Argument should be an Election instance'
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
    for t in ['ElectionType', 'Election','ReportingUnitType','ReportingUnit']:
        cdf_d[t] = pd.read_sql_table(t, session.bind, cdf_schema, index_col='Id')


    election_id = e.Election_Id

    # if state_id is not passed as parameter, select-or-insert state, get id (default Reporting Unit for ballot questions)
    if state_id == 0:
        state_type_id = cdf_d['ReportingUnitType'][cdf_d['ReportingUnitType']['Txt']=='state'].index.values[0]
        state_id = cdf_d['ReportingUnit'][cdf_d['ReportingUnit']['ReportingUnitType_Id']==state_type_id].index.values[0]

    bulk_elements_to_cdf(session, mu,raw_rows, cdf_schema, context_schema, e.Election_Id, e.ElectionType,state_id)

    return

if __name__ == '__main__':

    print('Done!')
    exit()

