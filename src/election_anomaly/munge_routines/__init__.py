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

from db_routines import dframe_to_sql


def load_context_dframe_into_cdf(session,source_df,element,CDF_schema_def_dir='CDF_schema_def_info/'):
    """source_df should have all info needed for insertion into cdf:
    for enumerations, the value of the enumeration (e.g., 'precinct')
    for other fields, the value of the field (e.g., 'North Carolina;Alamance County'"""
    # TODO check that source_df has the right format
    # TODO check that ReportingUnit.CountItemStatus_Id and ReportingUnit.OtherCountItemStatus are done right.

    enums = pd.read_csv('{}Tables/{}/enumerations.txt'.format(CDF_schema_def_dir,element))
    # get all relevant enumeration tables
    for e in enums['enumeration']:  # e.g., e = "ReportingUnitType"
        cdf_e = pd.read_sql_table(e,session.bind,schema='cdf')
        # for every instance of the enumeration in the current table, add id and othertype columns to the dataframe
        if e in source_df.columns:  # some enumerations (e.g., CountItemStatus for t = ReportingUnit) are not available from context.
            source_df = enum_col_to_id_othertext(source_df,e,cdf_e)
    #  commit info in source_df to corresponding cdf table to db
    cdf_element = dframe_to_sql(source_df,session,'cdf',element)

    if element == 'Office':
        cdf_rut = pd.read_sql_table('ReportingUnitType',session.bind,schema='cdf')
        # Check that all ElectionDistrictTypes are recognized
        for edt in source_df['ElectionDistrictType'].unique():
            assert edt in list(cdf_rut['Txt'])

        # insert corresponding ReportingUnits (the ones that don't already exist in cdf ReportingUnit table).
        cdf_ru = pd.read_sql_table('ReportingUnit',session.bind,'cdf',index_col=None)
        # note: cdf Id column is *not* the index for the dataframe cdf_d['ReportingUnit'].

        new_ru = source_df.drop(['Name'],axis=1)
        ru_list = list(cdf_ru['Name'].unique())
        new_ru = new_ru[~new_ru['ElectionDistrict'].isin(ru_list)]
        new_ru = new_ru.rename(columns={'ElectionDistrict':'Name','ElectionDistrictType':'ReportingUnitType'})
        new_ru = enum_col_to_id_othertext(new_ru,'ReportingUnitType',cdf_rut)
        cdf_ru = dframe_to_sql(new_ru,session,'cdf','ReportingUnit',index_col=None)

        # create corresponding CandidateContest records for general and primary election contests (and insert in cdf db if they don't already exist)
        cc_data = source_df.merge(cdf_element,left_on='Name',right_on='Name').merge(
            cdf_ru,left_on='Name',right_on='Name',suffixes=['','_ru'])
        # restrict to the columns we need, and set order
        cc_data = cc_data[['Name','VotesAllowed','NumberElected','NumberRunoff','Id','Id_ru','IsPartisan']]
        # rename columns as necesssary
        cc_data.rename(columns={'Id_ru':'ElectionDistrict_Id','Id':'Office_Id'},inplace=True)
        # insert values for 'PrimaryParty_Id' column
        cc_data.loc[:,'PrimaryParty_Id'] = None
        cc_d_gen = cc_data.copy()

        cdf_p = pd.read_sql_table('Party',session.bind,'cdf',index_col=None)
        for party_id in cdf_p['Id'].to_list():
            pcc = cc_d_gen[cc_d_gen['IsPartisan']]  # non-partisan contests don't have party primaries, so omit them.
            pcc['PrimaryParty_Id'] = party_id
            pcc['Name'] = pcc['Name'] + ' Primary;' + cdf_p[cdf_p['Id'] == party_id].iloc[0]['Name']
            cc_data = pd.concat([cc_data,pcc])

        dframe_to_sql(cc_data,session,'cdf','CandidateContest')
    return


def add_munged_column(row_df,mu,cdf_element,new_col_name):
    """Alters dataframe <row_df> (in place), adding or redefining <new_col_name>
    via the string corresponding to <cdf_element>, per <munge_dictionary>"""
    if row_df.empty:
        return row_df
    else:
        # use regex to turn value string in munge dictionary into the corresponding commands (e.g., decode '<County>;<Precinct>'
        p = re.compile('(?P<text>[^<>]*)<(?P<field>[^<>]+)>')   # pattern to find text,field pairs
        q = re.compile('(?<=>)[^<]*$')                          # pattern to find text following last pair
        text_field_list = re.findall(p,mu.cdf_tables.loc[cdf_element,'raw_identifier_formula'])
        last_text = re.findall(q,mu.cdf_tables.loc[cdf_element,'raw_identifier_formula'])

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
            cc_row=cc_row[~((cc_row[yes_col]!='') & (cc_row[no_col]!=''))]
        bm_row = row[~row.index.isin(cc_row.index)]
    else:
        raise Exception('Ballot measure style {} not recognized'.format(mu.ballot_measure_style))
    return bm_row.copy(), cc_row.copy()


def get_internal_ids(row_df,mu,table_df,element,internal_name_column,unmatched_dir,drop_unmatched=False):
    """replace columns in <df> with raw_identifier values by columns with internal names
    """
    assert os.path.isdir(unmatched_dir), 'Argument {} is not a directory'.format(unmatched_dir)
    if drop_unmatched:
        how='inner'
    else:
        how='left'
    # join the 'cdf_internal_name' from the raw_identifier table -- this is the internal name field value,
    # no matter what the name field name is in the internal element table (e.g. 'Name', 'BallotName' or 'Selection')
    row_df = row_df.merge(mu.raw_identifiers[mu.raw_identifiers['cdf_element'] == element],how=how,left_on=element + '_external',right_on='raw_identifier_value',suffixes=['','_' + element + '_ei'])

    # save any unmatched elements (if drop_unmatched=False)
    # TODO move these warnings to the method on Munger() that pre-checks the munger.
    unmatched = row_df[row_df['raw_identifier_value'].isnull()].loc[:,element + '_external'].unique()
    if unmatched.size > 0:
        unmatched_path = '{}unmatched_{}.txt'.format(unmatched_dir,element)
        np.savetxt(unmatched_path,unmatched,fmt="%s")
        print('WARNING: Some instances of {1} unmatched, saved to {0}.'.format(unmatched_path,element))
        print('IF THESE ELEMENTS ARE NECESSARY, USER MUST put them in both the munger raw_identifiers.txt and in the {1}.txt file in the context directory'.format(unmatched_path,element))

    row_df = row_df.drop(['raw_identifier_value','cdf_element',element + '_external'],axis=1)

    # ensure that there is a column in row_df called by the element
    # containing the internal name of the element
    if 'Name_'+element+ '_ei' in row_df.columns:
        row_df.rename(columns={'Name_' + element + '_ei':element},inplace=True)
    else:
        row_df.rename(columns={'cdf_internal_name':element},inplace=True)

    # join the element table Id and name columns.
    # This will create two columns with the internal name field,
    # whose names will be element (from above)
    # and either internal_name_column or internal_name_column_table_name
    row_df = row_df.merge(table_df[['Id',internal_name_column]],how='left',left_on=element,right_on=internal_name_column,suffixes=['','_' + element])
    if internal_name_column+'_' + element in row_df.columns:
        row_df=row_df.drop(internal_name_column +'_' + element,axis=1)
    else:
        row_df=row_df.drop([internal_name_column],axis=1)
    row_df.rename(columns={'Id_' + element:element + '_Id'},inplace=True)
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


def raw_elements_to_cdf(session,mu,row,contest_type,cdf_schema,election_id,election_type,state_id):
    """
    NOTE: Tables from context assumed to exist already in db
    (e.g., Party, ComposingReportingUnitJoin, Election, ReportingUnit etc.)
    """
    # TODO some of this could be done once for both BallotMeasure and Candidate contests. Rearrange for efficiency?
    assert contest_type in ['BallotMeasure','Candidate'], 'Contest type {} not recognized'.format(contest_type)


    cdf_d = {}  # dataframe for each table
    for t in ['Party','BallotMeasureSelection','ReportingUnit','Office','CountItemType','CandidateContest']:
        cdf_d[t] = pd.read_sql_table(t, session.bind, cdf_schema)   # note: keep 'Id as df column (not index) so we have access in merges below.


    # get vote count column mapping for our munger
    vc_col_d = {x['RawName']:x['CountItemType'] for i,x in mu.count_columns[mu.count_columns.ContestType==contest_type][['RawName','CountItemType']].iterrows()}
    col_list = list(vc_col_d.values()) + ['Election_Id','ReportingUnit_Id',
                                          'ReportingUnitType_Id', 'OtherReportingUnitType', 'CountItemStatus_Id',
                                          'OtherCountItemStatus','Selection_Id','Contest_Id']    # is ElectionDistrict_Id necessary?
    vote_type_list=list({v for k,v in vc_col_d.items()})

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
        add_munged_column(row,mu,t,t+'_external')
        row=get_internal_ids(row,mu,cdf_d[t],t,"Name",mu.path_to_munger_dir)
        row=add_non_id_cols_from_id(row,cdf_d[t],t)

    vote_count_dframe_list=[] # TODO remove; don't need to concatenate if bmcs and ccs are handled separately

    if contest_type=='BallotMeasure':
        add_munged_column(row,mu,'BallotMeasureContest','BallotMeasureContest')

        # create DataFrame bm_contest_selections of contest-selection-id tuples
        if mu.ballot_measure_style == 'yes_and_no_are_columns':
            bmc={}
            new_rows={}
            bm_contests = row['BallotMeasureContest'].drop_duplicates()
            for sel in ['Yes','No']:    # internal BallotMeasureSelection options
                bmc[sel]=pd.DataFrame(zip(bm_contests,[sel] * len(bm_contests)),columns=['BallotMeasureContest','BallotMeasureSelection'])
                new_rows[sel]=row.copy()
                # add column for selection
                new_rows[sel].loc[:,'BallotMeasureSelection'] = sel
                # melt vote counts into appropriately titled columns
                for vt in vote_type_list:
                    raw_name = mu.count_columns[(mu.count_columns.BallotMeasureSelection==sel) & (mu.count_columns.CountItemType==vt)].iloc[0]['RawName']
                    new_rows[sel].loc[:,vt]=row[raw_name]
            row = pd.concat([new_rows[k] for k in new_rows.keys()])
            bm_contest_selection = pd.concat([bmc[k] for k in bmc.keys()])

        elif mu.ballot_measure_style == 'yes_and_no_are_candidates':
            add_munged_column(row,mu,'BallotMeasureSelection','BallotMeasureSelection')
            bm_contest_selection = row[['BallotMeasureContest','BallotMeasureSelection']].drop_duplicates()
            row.rename(columns=vc_col_d,inplace=True)  # standardize vote-count column names
        else:
            raise Exception('Ballot measure style \'{}\' not recognized'.format(mu.ballot_measure_style))
        bm_contest_selection.columns = ['Name','Selection']
        # internal db name for ballot measure contest matches name in file
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
        bmcsj_df.rename(columns={'Contest_Id':'BallotMeasureContest_Id','Selection_Id':'BallotMeasureSelection_Id'},inplace=True)
        cdf_d['BallotMeasureContestSelectionJoin'] = dbr.dframe_to_sql(bmcsj_df,session,cdf_schema,'BallotMeasureContestSelectionJoin')

        # Load ElectionContestJoin table (for ballot measures)
        # TODO are we pulling ballot measure contests from other elections and assigning them to current election?
        ecj_df = cdf_d['BallotMeasureContest'].copy()
        ecj_df.loc[:,'Election_Id'] = election_id
        ecj_df.rename(columns={'Id': 'Contest_Id'}, inplace=True)
        cdf_d['ElectionContestJoin'] = dbr.dframe_to_sql(ecj_df,session,cdf_schema,'ElectionContestJoin')

        # create dframe of vote counts (with join info) for ballot measures
        bm_vote_counts = bm_row[col_list]

        bm_vote_counts=bm_vote_counts.melt(id_vars=['Election_Id','Contest_Id','Selection_Id','ReportingUnit_Id'],value_vars=vote_type_list,var_name='CountItemType',value_name='Count')
        bm_vote_counts=enum_col_to_id_othertext(bm_vote_counts,'CountItemType',cdf_d['CountItemType'])
        if not bm_vote_counts.empty:
            vote_count_dframe_list.append(bm_vote_counts)

    if contest_type=='Candidate':
        # process rows with candidate contests
        # Find CandidateContest_external and CandidateContest_Id
        # and omit rows with contests not  given in CandidateContest table (filled from context)
        add_munged_column(row,mu,'CandidateContest','CandidateContest_external')
        cc_row=get_internal_ids(row,mu,cdf_d['CandidateContest'],'CandidateContest','Name',mu.path_to_munger_dir,drop_unmatched=True)
        cc_row.rename(columns={'CandidateContest_Id':'Contest_Id'},inplace=True)

        # load Candidate table
        add_munged_column(cc_row,mu,'Candidate','BallotName')
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
        cc_row.rename(columns={'Id_Office':'Office_Id'},inplace=True)

        # load ElectionContestJoin for Candidate Contests
        ecj_df = cc_row[['Contest_Id','Election_Id']].drop_duplicates()
        cdf_d['ElectionContestJoin'] = dbr.dframe_to_sql(ecj_df,session,cdf_schema,'ElectionContestJoin')

        #  load CandidateContestSelectionJoin
        ccsj_df = cc_row[['Contest_Id','Selection_Id']].drop_duplicates()
        ccsj_df.rename(columns={'Contest_Id':'CandidateContest_Id','Selection_Id':'CandidateSelection_Id'},inplace=True)
        cdf_d['CandidateContestSelectionJoin'] = dbr.dframe_to_sql(ccsj_df,session,cdf_schema,'CandidateContestSelectionJoin')

        # load candidate counts
        cc_row.rename(columns=vc_col_d,inplace=True)  # standardize vote-count column names
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
    q = 'ALTER TABLE {0}."VoteCount" DROP COLUMN "Election_Id",' \
        ' DROP COLUMN "Contest_Id" ,  DROP COLUMN "Selection_Id" '
    sql_ids=[cdf_schema]
    strs = []
    dbr.raw_query_via_SQLALCHEMY(session,q,sql_ids,strs)

    return


def raw_dframe_to_cdf(session,raw_rows,mu,e,state_id = 0):
    """ munger-agnostic raw-to-cdf script; ***
    dframe is dataframe, mu is munger """

    assert isinstance(e,an.Election),'Argument should be an Election instance'
    cdf_d = {}  # to hold various dataframes from cdf db tables

    # if state_id is not passed as parameter, get id (default Reporting Unit for ballot questions)
    for t in ['ReportingUnitType','ReportingUnit']:
        cdf_d[t] = pd.read_sql_table(t, session.bind, 'cdf', index_col='Id')
    if state_id == 0:
        state_type_id = cdf_d['ReportingUnitType'][cdf_d['ReportingUnitType']['Txt'] == 'state'].index.values[0]
        state_id = cdf_d['ReportingUnit'][cdf_d['ReportingUnit']['ReportingUnitType_Id'] == state_type_id].index.values[0]

    # split raw_rows into a df for ballot measures and a df for contests
    bm_row,cc_row = contest_type_split(raw_rows,mu)

    if bm_row.empty:
        print('No ballot measures to process')
        process_ballot_measures = 'empty'
    else:
        process_ballot_measures = input('Process Ballot Measures (y/n)?\n')
    if process_ballot_measures == 'y':
        raw_elements_to_cdf(session,mu,bm_row,'BallotMeasure','cdf',e.Election_Id,e.ElectionType,state_id)
    if cc_row.empty:
        print('No candidate contests to process')
        process_candidate_contests='empty'
    else:
        process_candidate_contests = input('Process Candidate Contests (y/n)?\n')
    if process_candidate_contests=='y':
        raw_elements_to_cdf(session,mu,cc_row,'Candidate','cdf',e.Election_Id,e.ElectionType,state_id)

    return


# TODO this isn't really munging, so should it be elsewhere?
def context_schema_to_cdf(session,s,enum_table_list):
    """Takes the info from the tables in the state's context schema and inserts anything new into the cdf schema.
    Assumes enumeration tables are already filled.
    """
    # TODO assumes number elected is same for primary and general for same office, but that's not always true
    cdf_d = {}  # dict of dframes for CDF db tables

    # create and fill enum dframes and associated dictionaries
    enum_dframe = {}        # dict of dataframes of enumerations, taken from db
    enum_id_d = {}  # maps raw Type string to an Id
    enum_othertype_d = {}  # maps raw Type string to an othertype string
    for e in enum_table_list:
        enum_id_d[e] = {}  # maps raw Type string to an Id
        enum_othertype_d[e] = {}  # maps raw Type string to an othertype string

        # pull enumeration table into a DataFrame
        enum_dframe[e] = pd.read_sql_table(e, session.bind, schema='cdf', index_col=None)
    # pull list of tables in CDF

    # process other tables; need Office after ReportingUnit and after Party
    for t in ['ReportingUnit','Party','Office','Election']:

        # create DataFrames of relevant context information
        source_df = pd.read_csv('{}context/{}.txt'.format(s.path_to_state_dir,t),sep='\t')
        load_context_dframe_into_cdf(session,source_df,t)

    # TODO update CRUJ when munger is updated?
    # Fill the ComposingReportingUnitJoin table
    cdf_d['ComposingReportingUnitJoin'] = dbr.fill_composing_reporting_unit_join(session)
    session.flush()
    return


if __name__ == '__main__':

    print('Done!')
    exit()

