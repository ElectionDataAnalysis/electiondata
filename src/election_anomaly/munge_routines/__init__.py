#!/usr/bin/python3
# munge_routines/__init__.py
# under construction

import db_routines as dbr
import pandas as pd
import time

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
    # filter the cdf_value_d by the relevant value_d conditions #
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
    # check that dframe columns are 'Id' and 'Txt'
    assert 'Txt' in dframe.columns, 'dframe must have a Txt column'
    if t_dframe_Id_is_index:
        id_list = dframe.index[dframe['Txt'] == txt].to_list()
    else:
        assert 'Id' in dframe.columns, 'When flag t_dframe_Id_is_index is false, there must be an Id column in dframe'
        id_list = dframe[dframe['Txt'] == txt].to_list()
    if len(id_list) == 1:
        return([id_list[0],''])
    elif len(id_list) == 0:
        return[id_type_other_id,txt]
    else:
         raise Exception('Dataframe has duplicate rows with value ' + txt + ' in Txt column')

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

    # get vote count column mapping for our munger
    fpath = mu.path_to_munger_dir
    vc_col_d = pd.read_csv(fpath + 'count_columns.txt',sep='\t',index_col='RawName').to_dict()['CountItemType']
    munge = pd.read_csv(fpath + 'cdf_tables.txt',sep='\t',index_col='CDFTable').to_dict()['ExternalIdentifier']


    #munge = {}
    #for t in ['Office','Party','Candidate','ReportingUnit','BallotMeasureContest']:
        #munge[t] = mu.content_dictionary['fields_dictionary'][t][0]['ExternalIdentifier']

    # add columns for ids needed later
    row['Election_Id'] = [election_id] * row.shape[0]
    row['ReportingUnit_external'] = eval(munge['ReportingUnit'])

    cdf_d['ReportingUnit'] = pd.read_sql_table('ReportingUnit',session.bind,cdf_schema)
    row = row.merge(context_ei[context_ei['Table']=='ReportingUnit'],left_on='ReportingUnit_external',right_on='ExternalIdentifierValue',suffixes=['','_ReportingUnit']).drop(['ExternalIdentifierValue','Table'],axis=1)
    row.rename(columns={'Name':'ReportingUnit'},inplace=True)
    row = row.merge(cdf_d['ReportingUnit'],left_on='ReportingUnit',right_on='Name',suffixes=['','_ReportingUnit']).drop('Name',axis=1)
    row.rename(columns={'Id':'ReportingUnit_Id'},inplace=True)

    # to make sure all added columns get labeled well, make sure 'Name' and 'Id' are existing columns
    if 'Name' not in row.columns:
        row['Name'] = [None]*row.shape[0]
    if 'Id' not in row.columns:
        row['Id']  = [None]*row.shape[0]

    # split row into a df for ballot measures and a df for contests
    bm_selections = cdf_d['BallotMeasureSelection']['Selection'].to_list()
    munge['BallotMeasureSelection'] = "row['Choice']"


    bm_row = row[eval(munge['BallotMeasureSelection']).isin(bm_selections)]
    cc_row = row[~(eval(munge['BallotMeasureSelection']).isin(bm_selections))]

    process_ballot_measures = input('Process Ballot Measures (y/n)?\n')
    process_candidate_contests = input('Process Candidate Contests (y/n)?\n')
    vote_count_dframe_list = []


    if process_ballot_measures:
        # Process rows with ballot measures and selections
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
        row.rename(columns={'Id_Selection':'Selection_Id'},inplace=True)
        row = row.merge(cdf_d['BallotMeasureContest'],left_on='BallotMeasureContest',right_on='Name',suffixes=['','_Contest'])
        row.rename(columns={'Id_Contest':'Contest_Id'},inplace=True)

        # Load BallotMeasureContestSelectionJoin table
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

        # create dframe of vote counts (with join info) for ballot measures
        print('Create vote_counts dframe')
        bm_vote_counts = row.drop(['County','Election Date','Precinct','Contest Group ID','Contest Type','Contest Name','Choice','Choice Party','Vote For','Real Precinct','ReportingUnit_external','ReportingUnit','index','ExternalIdentifierType','ReportingUnitType_Id','OtherReportingUnitType','CountItemStatus_Id','OtherCountItemStatus','BallotMeasureContest','Name','BallotMeasureSelection','Selection', 'ElectionDistrict_Id'],axis=1)
        # vc_col_d = {k:v['CountItemType'] for k,v in mu.content_dictionary['counts_dictionary'].items()}
        bm_vote_counts.rename(columns=vc_col_d,inplace=True)
        print('Reshape')
        bm_vote_counts=bm_vote_counts.melt(id_vars=['Election_Id','Contest_Id','Selection_Id','ReportingUnit_Id'],value_vars=['election-day', 'early', 'absentee-mail', 'provisional', 'total'],var_name='CountItemType',value_name='Count')
        print('Merge CountItemType')
        bm_vote_counts = bm_vote_counts.merge(cdf_d['CountItemType'],left_on='CountItemType',right_on='Txt')
        bm_vote_counts.rename(columns={'Id':'CountItemType_Id'},inplace=True)
        vote_count_dframe_list.append(bm_vote_counts)

    if process_candidate_contests:
        # process rows with candidate contests
        row = cc_row

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
        cc_df.rename(columns={'Id_Office':'Office_Id','Id':'ElectionDistrict_Id'},inplace=True)
        cdf_d['CandidateContest'] = dbr.dframe_to_sql(cc_df,session,cdf_schema,'CandidateContest')

        # drop some columns we won't need any more
        row.drop(
            ['County','Election Date','Precinct','Contest Group ID','Contest Type','Contest Name','Choice','Choice Party',
             'Vote For','Real Precinct','ReportingUnit_external','ReportingUnit','index','ExternalIdentifierType'],axis=1)

        # add Candidate & Contest ids needed later
        row = row.merge(cdf_d['Candidate'],left_on='Candidate',right_on='BallotName',suffixes=['','_Candidate'])
        row.rename(columns={'Id_Candidate':'Candidate_Id'},inplace=True)
        row = row.merge(cdf_d['CandidateSelection'],left_on='Candidate_Id',right_on='Candidate_Id',suffixes=['','_Selection'])
        row.rename(columns={'Id_Selection':'CandidateSelection_Id'},inplace=True)
        row = row.merge(cdf_d['CandidateContest'],left_on='Office_Name',right_on='Name',suffixes=['','_Contest'])
        row.rename(columns={'Id_Contest':'CandidateContest_Id'},inplace=True)

        # load ElectionContestJoin for Candidate Contests
        ecj_df = row[['CandidateContest_Id','Election_Id']].drop_duplicates()
        ecj_df.rename(columns={'CandidateContest_Id':'Contest_Id'},inplace=True)
        cdf_d['ElectionContestJoin'] = dbr.dframe_to_sql(ecj_df,session,cdf_schema,'ElectionContestJoin')

        #  load CandidateContestSelectionJoin
        ccsj_df = row[['CandidateContest_Id','CandidateSelection_Id','Election_Id']].drop_duplicates()
        cdf_d['CandidateContestSelectionJoin'] = dbr.dframe_to_sql(ccsj_df,session,cdf_schema,'CandidateContestSelectionJoin')

        # load candidate counts
        # create dframe of Candidate Contest vote counts (with join info) for ballot measures
        print('Create vote_counts dframe for Candidate Contests')
        cc_vote_counts = row.drop(['County','Election Date','Precinct','Contest Group ID','Contest Type','Contest Name','Choice','Choice Party','Vote For','Real Precinct','ReportingUnit_external','ReportingUnit','index','ExternalIdentifierType','ReportingUnitType_Id','OtherReportingUnitType','CountItemStatus_Id','OtherCountItemStatus','Name', 'ElectionDistrict_Id'],axis=1)
        cc_vote_counts.rename(columns=vc_col_d,inplace=True)
        print('Reshape')
        cc_vote_counts.rename(columns={'CandidateContest_Id':'Contest_Id','CandidateSelection_Id':'Selection_Id'},inplace=True)
        cc_vote_counts=cc_vote_counts.melt(id_vars=['Election_Id','Contest_Id','Selection_Id','ReportingUnit_Id'],value_vars=['election-day', 'early', 'absentee-mail', 'provisional', 'total'],var_name='CountItemType',value_name='Count')
        print('Merge CountItemType')
        cc_vote_counts = cc_vote_counts.merge(cdf_d['CountItemType'],left_on='CountItemType',right_on='Txt')
        cc_vote_counts.rename(columns={'Id':'CountItemType_Id'},inplace=True)
        vote_count_dframe_list.append(cc_vote_counts)

    vote_counts = pd.concat(vote_count_dframe_list)

    # To get 'VoteCount_Id' attached to the correct row, temporarily add columns to VoteCount
    # add SelectionElectionContestJoin columns to VoteCount
    print('Add columns to cdf table')
    q = 'ALTER TABLE {0}."VoteCount" ADD COLUMN "Election_Id" INTEGER, ADD COLUMN "Contest_Id" INTEGER,  ADD COLUMN "Selection_Id" INTEGER'
    sql_ids=[cdf_schema]
    strs = []
    dbr.raw_query_via_SQLALCHEMY(session,q,sql_ids,strs)
    print('Upload to VoteCount')
    start = time.time()
    vote_counts_fat = dbr.dframe_to_sql(vote_counts,session,cdf_schema,'VoteCount')
    vote_counts_fat.rename(columns={'Id':'VoteCount_Id'},inplace=True)
    session.commit()
    end = time.time()
    print('\tSeconds required to upload VoteCount: '+ str(end - start))
    print('Upload to SelectionElectionContestVoteCountJoin')
    start = time.time()

    cdf_d['SelectionElectionContestVoteCountJoin'] = dbr.dframe_to_sql(vote_counts_fat,session,cdf_schema,'SelectionElectionContestVoteCountJoin')
    end = time.time()
    print('\tSeconds required to upload SelectionElectionContestVoteCountJoin: '+ str(end - start))
    print('Drop columns from cdf table')
    q = 'ALTER TABLE {0}."VoteCount" DROP COLUMN "Election_Id", DROP COLUMN "Contest_Id",  DROP COLUMN "Selection_Id" '
    sql_ids=[cdf_schema]
    strs = []
    dbr.raw_query_via_SQLALCHEMY(session,q,sql_ids,strs)

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

    # read raw data rows from db
    raw_rows = pd.read_sql_table(df.table_name,session.bind,schema=df.state.schema_name)

    # bulk_items_already_loaded = input('Are bulk items (Candidate, etc.) already loaded (y/n)?\n')
    # if bulk_items_already_loaded != 'y':
    bulk_elements_to_cdf(session, mu,raw_rows, cdf_schema, context_schema, election_id, id_type_other_id,ids_d['state'])

    return str(ids_d)

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
    df = sf.create_datafile(s, 'General Election 2018-11-06', 'filtered_yancey2018.txt', mf, mu)
    row = pd.read_sql_table('GeneralElection20181106filtered_results_pct_20181106txt',session.bind,s.schema_name)

    election_id = 3218
    id_type_other_id = 35
    state_id = 59
    bulk_elements_to_cdf(session, mu, row, cdf_schema, s.schema_name, election_id, id_type_other_id, state_id)


    raw_records_to_cdf(session,meta,df,mu,cdf_schema,s.schema_name,0,0,'../CDF_schema_def_info/tables.txt')
    print('Done!')

