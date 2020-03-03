#!/usr/bin/python3
# munge_routines/__init__.py
# under construction

# TODO How to handle FL candidate names? Candidate might or might not have middle name. FL munger gives candidate without middle name two spaces
# TODO e.g. 'Stephanie  Singer' or 'Stephanie Frank Singer'.

import db_routines as dbr
import pandas as pd
import numpy as np
import time
import analyze as an
import re
import os

from context import fill_externalIdentifier_table,fill_composing_reporting_unit_join
from db_routines import dframe_to_sql


def add_munged_column(row_df,munge_dictionary,munge_key,new_col_name):
    """Alters dataframe <row_df> (in place), adding or redefining <new_col_name>
    via the string corresponding to <munge_key>, per <munge_dictionary>"""
    if row_df.empty:
        return row_df
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
        return row_df

def contest_type_split(row,mu):
    if mu.ballot_measure_style=='yes_and_no_are_candidates':
        bm_row = row[row[mu.ballot_measure_selection_col].isin(mu.ballot_measure_selection_list)]
        cc_row = row[~row[mu.ballot_measure_selection_col].isin(mu.ballot_measure_selection_list)]
    elif mu.ballot_measure_style == 'yes_and_no_are_columns':
        bm_count_cols=mu.count_columns[mu.count_columns.ContestType=='BallotMeasure']
        cc_row=row.copy()
        # TODO check that all CountItemTypes have either 2 or 0 columns for ballot measures
        # if any CountItemType has numbers in both yes and no bm columns, assume not a candidate contest
        for cit in bm_count_cols.CountItemType.unique():
            # each count item
            yes_col= bm_count_cols[(bm_count_cols.CountItemType==cit) & (bm_count_cols.BallotMeasureSelection=='Yes')].iloc[0].RawName
            no_col= bm_count_cols[(bm_count_cols.CountItemType==cit) & (bm_count_cols.BallotMeasureSelection=='No')].iloc[0].RawName
            cc_row=cc_row[ ~(cc_row[yes_col].isdigit()  & cc_row[no_col].isdigit())]
        bm_row = row[~row.index.isin(cc_row.index)]
    else:
        raise Exception('Ballot measure style {} not recognized'.format(mu.ballot_measure_style))
    return bm_row, cc_row

def get_internal_ids_from_context(row_df,ctxt_ei_df,table_df,table_name,internal_name_column,unmatched_dir,drop_unmatched=False):
    """replace columns in <df> with external identifier values by columns with internal names
    Note: this requires using the ExternalIdentifier table from the context schema, which has the info about which table the element is in. """
    assert os.path.isdir(unmatched_dir), 'Argument {} is not a directory'.format(unmatched_dir)
    if drop_unmatched:
        how='inner'
    else:
        how='left'
    # join the 'Name' from the ExternalIdentifier table -- this is the internal name field,
    # no matter what name the corresponding field has in the internal element table
    row_df = row_df.merge(ctxt_ei_df[ctxt_ei_df['Table'] == table_name],how=how,left_on=table_name + '_external',right_on='ExternalIdentifierValue',suffixes=['','_' + table_name+'_ei'])

    # save any unmatched elements (if drop_unmatched=False)
    unmatched = row_df[row_df['ExternalIdentifierValue'].isnull()].loc[:,table_name+'_external'].unique()
    if unmatched.size>0:
        unmatched_path=unmatched_dir+'unmatched_'+table_name+'.txt'
        np.savetxt(unmatched_path,unmatched,fmt="%s")
        print('WARNING: Some elements unmatched, saved to {}.\nIF THESE ELEMENTS ARE NECESSARY, USER MUST put them in both the munger ExternalIdentifier.txt and in the {}.txt file in the context directory'.format(unmatched_path,table_name))

    row_df = row_df.drop(['ExternalIdentifierValue','Table','ExternalIdentifierType','index',table_name+'_external'],axis=1)

    # ensure that there is a column in row_df called by the table_name
    # containing the internal name of the element
    if 'Name_'+table_name+'_ei' in row_df.columns:
        row_df.rename(columns={'Name_'+table_name+'_ei':table_name},inplace=True)
    else:
        row_df.rename(columns={'Name':table_name},inplace=True)

    # join the element table Id and name columns.
    # This will create two columns with the internal name field,
    # whose names will be table_name (from above)
    # and either internal_name_column or internal_name_column_table_name
    row_df = row_df.merge(table_df[['Id',internal_name_column]],how='left',left_on=table_name,right_on=internal_name_column,suffixes=['','_' + table_name])
    if internal_name_column+'_' + table_name in row_df.columns:
        row_df=row_df.drop(internal_name_column+'_' + table_name,axis=1)
    else:
        row_df=row_df.drop([internal_name_column],axis=1)
    row_df.rename(columns={'Id_'+table_name:table_name + '_Id'},inplace=True)
    return row_df

def add_non_id_cols_from_id(row_df,cdf_table,table_name):
    row_df=row_df.merge(cdf_table,left_on=table_name+'_Id',right_on='Id',how='left',suffixes=['','_'+table_name])
    row_df=row_df.drop('Id_'+table_name,axis=1)
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

def raw_elements_to_cdf(session,mu,row,contest_type,cdf_schema,context_schema,election_id,election_type,state_id):
    """
    NOTE: Tables from context assumed to exist already in db
    (e.g., BallotMeasureSelection, Party, ExternalIdentifier, ComposingReportingUnitJoin, Election, ReportingUnit etc.)
    Create tables, which are repetitive,
    and don't come from context
    and whose db Ids are needed for other insertions.
    `row` is a dataframe of the raw data file
    Assumes table 'ExternalIdentifier' in the context schema
    """
    # TODO some of this could be done once for both BallotMeasure and Candidate contests. Rearrange for efficiency?
    assert contest_type in ['BallotMeasure','Candidate'], 'Contest type {} not recognized'.format(contest_type)
    cdf_d = {}  # dataframe for each table
    for t in ['ExternalIdentifier','Party','BallotMeasureSelection','ReportingUnit','Office','CountItemType','CandidateContest']:
        cdf_d[t] = pd.read_sql_table(t, session.bind, cdf_schema)   # note: keep 'Id as df column (not index) so we have access in merges below.
    context_ei = pd.read_sql_table('ExternalIdentifier',session.bind,context_schema)
    context_ei = context_ei[ (context_ei['ExternalIdentifierType']== mu.name)]  # limit to our munger

    # get vote count column mapping for our munger
    # TODO may differ by contest_type
    fpath = mu.path_to_munger_dir
    vc_col_d = pd.read_csv('{}count_columns.txt'.format(fpath),sep='\t',index_col='RawName').to_dict()['CountItemType'] # TODO use munger attribute dataframe count_columns
    col_list = list(vc_col_d.values()) + ['Election_Id','ReportingUnit_Id',
                                          'ReportingUnitType_Id', 'OtherReportingUnitType', 'CountItemStatus_Id',
                                          'OtherCountItemStatus','Selection_Id','Contest_Id']    # is ElectionDistrict_Id necessary?
    vote_type_list=list({v for k,v in vc_col_d.items()})
    munge = pd.read_csv(fpath + 'cdf_tables.txt',sep='\t',index_col='CDFTable').to_dict()['ExternalIdentifier']

    row.rename(columns=vc_col_d,inplace=True)  # standardize vote-count column names

    # add columns corresponding to cdf fields
    # election id
    row.loc[:,'Election_Id'] = election_id

    # to make sure all added columns get labeled well, make sure 'Name' and 'Id' are existing columns
    if 'Name' not in row.columns:
        row.loc[:,'Name'] = None
    if 'Id' not in row.columns:
        row.loc[:,'Id'] = None

    for t in ['Party','ReportingUnit']:  # tables that were filled from context
        #  merge works for all columns, even if, say, Party is null, because of how='left' (left join) in add_munged_column function
        add_munged_column(row,munge,t,t+'_external')
        row=get_internal_ids_from_context(row,context_ei,cdf_d[t],t,"Name",mu.path_to_munger_dir)
        row=add_non_id_cols_from_id(row,cdf_d[t],t)

    # some columns are their own internal names (no external identifier map needed)
    for t in ['BallotMeasureSelection','BallotMeasureContest']:
        add_munged_column(row,munge,t,t)
    # NOTE: this will put, e.g., candidate names into the BallotMeasureSelection column; beware!
    # some columns will need to be interpreted via  ExternalIdentifier tables

    if contest_type=='BallotMeasure':
        # Process rows with ballot measures and selections

        bm_contest_selection = row[['BallotMeasureContest','BallotMeasureSelection']].drop_duplicates()
        bm_contest_selection.columns = ['Name','Selection']  # internal db name for ballot measure contest matches name in file
        bm_contest_selection.loc[:,'ElectionDistrict_Id'] = state_id  # append column for ElectionDistrict Id
        print('WARNING: all ballot measure contests assumed to have the whole state as their district')

        # Load BallotMeasureContest table to cdf schema
        cdf_d['BallotMeasureContest'] = dbr.dframe_to_sql(bm_contest_selection[['Name','ElectionDistrict_Id']].drop_duplicates(),session,cdf_schema,'BallotMeasureContest')

        # add BallotMeasure-related ids needed later
        bm_row = row.merge(cdf_d['BallotMeasureSelection'],left_on='BallotMeasureSelection',right_on='Selection',suffixes=['','_Selection'])
        bm_row.rename(columns={'Id_Selection':'Selection_Id'},inplace=True)
        bm_row = bm_row.merge(cdf_d['BallotMeasureContest'],left_on='BallotMeasureContest',right_on='Name',suffixes=['','_Contest'])
        bm_row.rename(columns={'Id_Contest':'Contest_Id'},inplace=True)

        # Load BallotMeasureContestSelectionJoin table to cdf schema
        bmcsj_df = bm_row[['Contest_Id','Selection_Id']].drop_duplicates()
        cdf_d['BallotMeasureContestSelectionJoin'] = dbr.dframe_to_sql(bmcsj_df,session,cdf_schema,'BallotMeasureContestSelectionJoin')

        # Load ElectionContestJoin table (for ballot measures)
        ecj_df = cdf_d['BallotMeasureContest'].copy()
        ecj_df.loc[:,'Election_Id'] = election_id
        ecj_df.rename(columns={'Id': 'Contest_Id'}, inplace=True)
        cdf_d['ElectionContestJoin'] = dbr.dframe_to_sql(ecj_df,session,cdf_schema,'ElectionContestJoin')

        # create dframe of vote counts (with join info) for ballot measures
        vote_count_dframe_list = []
        bm_vote_counts = bm_row[col_list]
        bm_vote_counts=bm_vote_counts.melt(id_vars=['Election_Id','Contest_Id','Selection_Id','ReportingUnit_Id'],value_vars=vote_type_list,var_name='CountItemType',value_name='Count')
        bm_vote_counts=enum_col_to_id_othertext(bm_vote_counts,'CountItemType',cdf_d['CountItemType'])
        if not bm_vote_counts.empty:
            vote_count_dframe_list.append(bm_vote_counts)

    if contest_type=='Candidate':
        # process rows with candidate contests
        # Find CandidateContest_external and CandidateContest_Id and omit rows with contests not  given in CandidateContest table (filled from context)
        add_munged_column(row,munge,'CandidateContest','CandidateContest_external')
        cc_row=get_internal_ids_from_context(row,context_ei,cdf_d['CandidateContest'],'CandidateContest','Name',mu.path_to_munger_dir,drop_unmatched=True)
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
            cc_row['Office'] = cc_row['CandidateContest'].str.split(' Primary;').iloc[0][0]
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
        vote_count_dframe_list=[]
        cc_vote_counts = cc_row[col_list]
        cc_vote_counts=cc_vote_counts.melt(id_vars=['Election_Id','Contest_Id','Selection_Id','ReportingUnit_Id'],value_vars=vote_type_list,var_name='CountItemType',value_name='Count')
        cc_vote_counts=enum_col_to_id_othertext(cc_vote_counts,'CountItemType',cdf_d['CountItemType'])
        vote_count_dframe_list.append(cc_vote_counts)

    vote_counts = pd.concat(vote_count_dframe_list)

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
    print('\tSeconds required to upload VoteCount: {}'.format(round(end - start)))
    print('Upload to SelectionElectionContestVoteCountJoin')
    start = time.time()

    cdf_d['SelectionElectionContestVoteCountJoin'] = dbr.dframe_to_sql(vote_counts_fat,session,cdf_schema,'SelectionElectionContestVoteCountJoin')
    end = time.time()
    print('\tSeconds required to upload SelectionElectionContestVoteCountJoin: {}'.format(round(end - start)))
    print('Drop columns from cdf table')
    q = 'ALTER TABLE {0}."VoteCount" DROP COLUMN "Election_Id", DROP COLUMN "Contest_Id" ,  DROP COLUMN "Selection_Id" '
    sql_ids=[cdf_schema]
    strs = []
    dbr.raw_query_via_SQLALCHEMY(session,q,sql_ids,strs)

    return

def raw_dframe_to_cdf(session,raw_rows,mu,cdf_schema,context_schema,e,state_id = 0,id_type_other_id = 0,cdf_table_filepath='CDF_schema_def_info/tables.txt'):
    """ munger-agnostic raw-to-cdf script; ***
    dframe is dataframe, mu is munger """

    assert isinstance(e,an.Election),'Argument should be an Election instance'
    cdf_d = {}  # to hold various dataframes from cdf db tables

    # add to context.ExternalIdentifier table for this munger
    # TODO rename context schema to reflect munger info
    fill_externalIdentifier_table(session,cdf_schema,context_schema,mu)

    # get id for IdentifierType 'other' if it was not passed as parameter
    if id_type_other_id == 0:
        cdf_d['IdentifierType'] = pd.read_sql_table('IdentifierType', session.bind, cdf_schema, index_col='Id')
        id_type_other_id = cdf_d['IdentifierType'].index[cdf_d['IdentifierType']['Txt'] == 'other'].to_list()[0]
        if not id_type_other_id:
            raise Exception('No Id found for IdentifierType \'other\'; fix IdentifierType table and rerun.')

    # if state_id is not passed as parameter, get id (default Reporting Unit for ballot questions)
    for t in ['ReportingUnitType','ReportingUnit']:
        cdf_d[t] = pd.read_sql_table(t, session.bind, cdf_schema, index_col='Id')
    if state_id == 0:
        state_type_id = cdf_d['ReportingUnitType'][cdf_d['ReportingUnitType']['Txt']=='state'].index.values[0]
        state_id = cdf_d['ReportingUnit'][cdf_d['ReportingUnit']['ReportingUnitType_Id']==state_type_id].index.values[0]

    # split raw_rows into a df for ballot measures and a df for contests
    bm_row,cc_row = contest_type_split(raw_rows,mu)

    if bm_row.empty:
        print('No ballot measures to process')
        process_ballot_measures = 'empty'
    else:
        process_ballot_measures = input('Process Ballot Measures (y/n)?\n')
    if process_ballot_measures == 'y':
        raw_elements_to_cdf(session,mu,bm_row,'BallotMeasure',cdf_schema,context_schema,e.Election_Id,e.ElectionType,state_id)
    if cc_row.empty:
        print('No candidate contests to process')
        process_candidate_contests='empty'
    else:
        process_candidate_contests = input('Process Candidate Contests (y/n)?\n')
    if process_candidate_contests=='y':
        raw_elements_to_cdf(session,mu,cc_row,'Candidate',cdf_schema,context_schema,e.Election_Id,e.ElectionType,state_id)





    return


def context_schema_to_cdf(session,s,enum_table_list,cdf_def_dirpath = 'CDF_schema_def_info/'):
    """Takes the info from the tables in the state's context schema and inserts anything new into the cdf schema.
    Assumes enumeration tables are already filled.
    """
    # TODO assumes number elected is same for primary and general for same office, but that's not always true
    context_cdframe = {}    # dictionary of dataframes from context info
    cdf_d = {}  # dict of dframes for CDF db tables

    # create and fill enum dframes and associated dictionaries
    enum_dframe = {}        # dict of dataframes of enumerations, taken from db
    enum_id_d = {}  # maps raw Type string to an Id
    enum_othertype_d = {}  # maps raw Type string to an othertype string
    for e in enum_table_list:
        enum_id_d[e] = {}  # maps raw Type string to an Id
        enum_othertype_d[e] = {}  # maps raw Type string to an othertype string

        # pull enumeration table into a DataFrame
        # enum_dframe[e] = pd.read_sql_table(e, session.bind, schema='cdf', index_col='Id')
        enum_dframe[e] = pd.read_sql_table(e, session.bind, schema='cdf', index_col=None)
    # pull list of tables in CDF
    if not cdf_def_dirpath[-1] == '/': cdf_def_dirpath += '/'
    with open(cdf_def_dirpath+'tables.txt','r') as f:
        table_def_list = eval(f.read())

    # process other tables; need Office after ReportingUnit and after Party
    for t in ['ReportingUnit','Party','Office','Election']:
        table_def = next(x for x in table_def_list if x[0] == t)
        # create DataFrames of relevant context information
        context_cdframe[t] = pd.read_sql_table(t,session.bind,schema='context',index_col='index')

        for e in table_def[1]['enumerations']:  # e.g., e = "ReportingUnitType"
            # for every instance of the enumeration in the current table, add id and othertype columns to the dataframe
            if e in context_cdframe[t].columns: # some enumerations (e.g., CountItemStatus for t = ReportingUnit) are not available from context.
                context_cdframe[t] = enum_col_to_id_othertext(context_cdframe[t],e,enum_dframe[e])
        #  commit info in context_cdframe to corresponding cdf table to db
        cdf_d[t] = dframe_to_sql(context_cdframe[t], session, 'cdf', t)

        if t == 'Office':
            # Check that all ElectionDistrictTypes are recognized
            for edt in context_cdframe['Office']['ElectionDistrictType'].unique():
                assert edt in list(enum_dframe['ReportingUnitType']['Txt'])

            # insert corresponding ReportingUnits (the ones that don't already exist in cdf ReportingUnit table).
            cdf_d['ReportingUnit'] = pd.read_sql_table('ReportingUnit',session.bind,'cdf',index_col=None)
            # note: cdf Id column is *not* the index for the dataframe cdf_d['ReportingUnit'].

            new_ru = context_cdframe['Office'].drop(['Name'],axis=1)
            ru_list = list(cdf_d['ReportingUnit']['Name'].unique())
            new_ru = new_ru[~new_ru['ElectionDistrict'].isin(ru_list)]
            new_ru = new_ru.rename(columns={'ElectionDistrict':'Name','ElectionDistrictType':'ReportingUnitType'})
            new_ru = enum_col_to_id_othertext(new_ru,'ReportingUnitType',enum_dframe['ReportingUnitType'])
            cdf_d['ReportingUnit'] = dframe_to_sql(new_ru,session,'cdf','ReportingUnit',index_col=None)

            # create corresponding CandidateContest records for general and primary election contests (and insert in cdf db if they don't already exist)
            cc_data = context_cdframe['Office'].merge(cdf_d['Office'],left_on='Name',right_on='Name').merge(cdf_d['ReportingUnit'],left_on='Name',right_on='Name',suffixes=['','_ru'])
            # restrict to the columns we need, and set order
            cc_data = cc_data[['Name','VotesAllowed','NumberElected','NumberRunoff','Id','Id_ru','IsPartisan']]
            # rename columns as necesssary
            cc_data.rename(columns={'Id_ru':'ElectionDistrict_Id','Id':'Office_Id'},inplace=True)
            # insert values for 'PrimaryParty_Id' column
            cc_data.loc[:,'PrimaryParty_Id'] = None
            cc_d_gen = cc_data.copy()
            for party_id in cdf_d['Party']['Id'].to_list():
                pcc = cc_d_gen[cc_d_gen['IsPartisan']]    # non-partisan contests don't have party primaries, so omit them.
                pcc['PrimaryParty_Id'] = party_id
                pcc['Name'] = pcc['Name'] + ' Primary;' + cdf_d['Party'][cdf_d['Party']['Id'] == party_id].iloc[0]['Name']
                cc_data = pd.concat([cc_data,pcc])



            cdf_d['CandidateContest'] = dframe_to_sql(cc_data,session,'cdf','CandidateContest')

            # create corresponding CandidateContest records for primary contests (and insert in cdf db if they don't already exist)


    # Fill the ComposingReportingUnitJoin table
    cdf_d['ComposingReportingUnitJoin'] = fill_composing_reporting_unit_join(session,'cdf',pickle_dir=s.path_to_state_dir+'pickles/')
    # TODO put pickle directory info into README.md; or better yet, create nesting table in context schema
    session.flush()
    return

if __name__ == '__main__':

    print('Done!')
    exit()

