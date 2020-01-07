#!usr/bin/python3

import numpy as np
from scipy import stats as stats
import scipy.spatial.distance as dist
import pandas as pd
import matplotlib.pyplot as plt
import db_routines as dbr
import os
import states_and_files as sf

def count_by_selection_and_precinct(con,schema,Election_Id): # TODO rename more precisely
    q = """SELECT
       cruj."ChildReportingUnit_Id" AS "Precinct_Id", secvcj."Contest_Id", secvcj."Selection_Id", COALESCE(sum(vc."Count"),0) AS vote_count
    FROM
        {0}."SelectionElectionContestVoteCountJoin" secvcj 
        LEFT JOIN {0}."VoteCount" vc ON secvcj."VoteCount_Id" = vc."Id"
        LEFT JOIN {0}."ComposingReportingUnitJoin" cruj ON vc."ReportingUnit_Id" = cruj."ChildReportingUnit_Id"
        LEFT JOIN {0}."ReportingUnit" ru ON ru."Id" = cruj."ChildReportingUnit_Id"            
    WHERE
        secvcj."Election_Id" = %(Election_Id)s
        AND cruj."ParentReportingUnit_Id" = %(ReportingUnit_Id)s
        AND vc."CountItemType_Id" = %(CountItemType_Id)s
        AND ru."ReportingUnitType_Id" = 25
    GROUP BY secvcj."Contest_Id",cruj."ChildReportingUnit_Id", secvcj."Selection_Id"
    """.format(schema) # TODO remove hard-coded precinct ReportingUnitType_Id (25)
    params = {'Election_Id': Election_Id,'ReportingUnit_Id':ReportingUnit_Id,'CountItemType_Id':CountItemType_Id}
    df = pd.read_sql_query(sql=q, con = con,params=params)
    return df

def precinct_count(con,schema,Election_Id,Contest_Id): # TODO remove hard-coded precinct ReportingUnitType_Id (25)
    q = """SELECT
       vc."ReportingUnit_Id" AS "Precinct_Id",  secvcj."Selection_Id", vc."CountItemType_Id", COALESCE(vc."Count",0) AS "Count"
    FROM
        {0}."SelectionElectionContestVoteCountJoin" secvcj 
        LEFT JOIN {0}."VoteCount" vc ON secvcj."VoteCount_Id" = vc."Id"
        LEFT JOIN {0}."ReportingUnit" ru ON vc."ReportingUnit_Id" = ru."Id"
    WHERE
        secvcj."Election_Id" = %(Election_Id)s
        AND secvcj."Contest_Id" = %(Contest_Id)s
        AND ru."ReportingUnitType_Id" = 25
    """.format(schema)
    params = {'Election_Id': Election_Id,'Contest_Id':Contest_Id}
    df = pd.read_sql_query(sql=q, con = con,params=params)
    return df

def rollup_count(con,schema,Election_Id,Contest_Id,ReportingUnitType_Id): # TODO remove hard-coded precinct ReportingUnitType_Id (25)
    q = """SELECT
       cruj."ParentReportingUnit_Id" AS "ReportingUnit_Id",  secvcj."Selection_Id", vc."CountItemType_Id", COALESCE(sum(vc."Count"),0) AS "Count"
    FROM
        {0}."SelectionElectionContestVoteCountJoin" secvcj 
        LEFT JOIN {0}."VoteCount" vc ON secvcj."VoteCount_Id" = vc."Id"
        LEFT JOIN {0}."ComposingReportingUnitJoin" cruj ON vc."ReportingUnit_Id" = cruj."ChildReportingUnit_Id"
        LEFT JOIN {0}."ReportingUnit" ru_c ON ru_c."Id" = cruj."ChildReportingUnit_Id"
        LEFT JOIN {0}."ReportingUnit" ru_p ON ru_p."Id" = cruj."ParentReportingUnit_Id"
        
    WHERE
        secvcj."Election_Id" = %(Election_Id)s
        AND secvcj."Contest_Id" = %(Contest_Id)s
        AND ru_c."ReportingUnitType_Id" = 25
        AND ru_p."ReportingUnitType_Id" = %(ReportingUnitType_Id)s
    GROUP BY cruj."ParentReportingUnit_Id",  secvcj."Selection_Id", vc."CountItemType_Id"
    """.format(schema)
    params = {'Election_Id': Election_Id,'Contest_Id':Contest_Id,'ReportingUnitType_Id':ReportingUnitType_Id}
    df = pd.read_sql_query(sql=q, con = con,params=params)
    return df   # TODO use sqlalchemy instead

def contest_id_to_name(con,meta,schema,contest_id):
    contest_name = dbr.read_single_value_from_id(con,
                                meta, schema, 'CandidateContest', 'Name', contest_id)
    if not contest_name:
        contest_name = dbr.read_single_value_from_id(con,
                                                     meta, schema, 'BallotMeasureContest', 'Name', contest_id)
    return contest_name

def id_values_to_name(con,meta,schema,df):
    """Input is a df is a dataframe with some columns that are labeled with db id fields, e.g., 'Selection_Id'
    Return a df where id values in all these columns have been replaced by the corresponding name
    (per the given db schema at con)
    """ # TODO check all table names handled correctly
    cf = df.copy()
    for col in cf.columns:
        if col[-3:] == '_Id':
            table_name = col[:-3]
            id_list = list(eval('cf.'+col+'.unique()'))
            if table_name == 'Selection': # ballot question or candidate contest
                sel_cand_id_d = dbr.read_some_value_from_id(con,
                                meta, schema,'CandidateSelection','Candidate_Id',id_list)
                cand_id_list = list(sel_cand_id_d.values())
                cand_id_to_bname_d = dbr.read_some_value_from_id(con,
                                meta, schema,'Candidate','BallotName',cand_id_list)
                sel_to_bname_d = {k:cand_id_to_bname_d.get(sel_cand_id_d.get(k)) for k in sel_cand_id_d.keys() } # TODO error handling
                id_to_name_d = {**sel_to_bname_d,
                                **dbr.read_some_value_from_id(con,
                                meta, schema, 'BallotMeasureSelection','Selection', id_list)
                                }
            elif table_name == 'ReportingUnit': # use only last part of ReportingUnit.Name
                id_to_long_name_d = dbr.read_some_value_from_id(con,
                                            meta, schema, table_name, 'Name', id_list)
                id_to_name_d = { k: v.split(';')[-1] for k,v in id_to_long_name_d.items()}
            elif table_name[-4:] == 'Type':
                id_to_name_d = dbr.read_some_value_from_id(con,
                                            meta, schema, table_name, 'Txt', id_list)
            else:
                id_to_name_d = dbr.read_some_value_from_id(con,
                                            meta, schema, table_name, 'Name', id_list)
            cf[table_name] = cf[col].map(id_to_name_d)
            cf=cf.drop(col,axis=1)
    return cf

def create_pct_df(df):
    """ df is a pandas dataframe """
    #%%
    cf = df.copy()  # to avoid altering the df object passed to the function
    col_list = list(cf.columns)
    cf["sum"] = cf.sum(axis=1)
    bf = cf.loc[:,col_list].div(cf["sum"], axis=0)
    return bf

def vector_list_to_scalar_list(li):
    """Given a list of vectors, all of same length,
    return list of scalars ,
    for each vector, sum metric distance from all other vectors of metric function
    """
    a = [sum([dist.euclidean(x,y) for x in li]) for y in li]
    return a

def euclidean_zscore(li):
    """Take a list of vectors -- all in the same R^k,
    returns a list of the z-scores of the vectors -- each relative to the ensemble"""
    return list(stats.zscore([sum([dist.euclidean(x,y) for x in li]) for y in li]))

def stash(state,dframe,filename,description):
    """Put the dataframe dframe into a file in the state's tmp directory
    and add the filename, description pair to the state's stored_dataframe.dict file.
    Return the path to the file"""
    if not os.path.isdir(state.path_to_state_dir+'tmp/'):
        # create the directory
        os.mkdir(state.path_to_state_dir+'tmp/')
    #%% write the new or updated dictionary to the stored_dataframe.dict file
    dpath = state.path_to_state_dir+'tmp/stored_dataframe.dict'
    if os.path.isfile(dpath):
        with open(dpath,'r') as f:
            d = eval(f.read())
    else:
        d = {}
    d[filename]=description
    with open(dpath,'w')  as f:
        f.write(str(d))

    #%% write the dataframe to the file
    fpath = state.path_to_state_dir+'tmp/'+filename
    dframe.to_pickle(fpath)
    return

def unstash(state,filename):
    """Retrieve a dataframe stashed in a file"""
    fpath = state.path_to_state_dir+'tmp/'+filename
    try:
        if not os.path.isfile(fpath):
            raise ValueError('No such file: '+fpath)
        df = pd.read_pickle(state.path_to_state_dir+'tmp/'+filename)
        return df
    except ValueError as ve:
        print(ve)
        return

def create_and_stash_rollup(con,cdf_schema,Election_Id,CandidateContest_Id,childReportingUnitType_Id,state,filename,description):
    df = rollup_count(con,cdf_schema,Election_Id,CandidateContest_Id,childReportingUnitType_Id)

    named_df = id_values_to_name(con,meta,cdf_schema,df)
    stash(state,named_df,filename,description)
    return

def bar_charts(rollup,contest_name=''):
    CountItemType_list = rollup['CountItemType'].unique()
    for type in CountItemType_list:
        type_df = rollup[rollup['CountItemType']== type]
        type_pivot = type_df.pivot_table(index='ReportingUnit',columns='Selection',values='Count')
        type_pivot.plot.bar()
        plt.title(contest_name+'\n'+type+' (vote totals)')

        type_pct_pivot = create_pct_df(type_pivot)
        type_pct_pivot.plot.bar()
        plt.title(contest_name+'\n'+type+' (vote totals)')
    plt.show()
    return

def dropoff_anomaly_score(rollup_list):
    """given two of contest roll-ups by same ReportingUnitType, find any anomalies
    in the margin between votes cast in the two contests
    among the set of ReportingUnits of given type
    """ # TODO what if districts aren't same for both contests?
    # enforce(?) that list of rus must be the same for both rollups.
    # TODO

    #%% create dframe with totals over selections from the two rollup dframess
    # then create corresponding percentage-diff dframe (series)
    cf = df.copy()  # to avoid altering the df object passed to the function
    col_list = list(cf.columns)
    cf["sum"] = rollup.sum(axis=1)
    bf = cf.loc[:, col_list].div(cf["sum"], axis=0)

    #%% find outlier in percentage-diff series
    # TODO

    #%% if outlier is 'anomalous enough', make scatter plot for the two rollups
    # TODO
    return

if __name__ == '__main__':
#    scenario = input('Enter xx or nc\n')
    scenario = 'nc'
    use_stash = 0
    if scenario == 'xx':
        s = sf.create_state('XX','../../local_data/XX/')
        schema = 'cdf_xx'
        Election_Id = 262
        ReportingUnit_Id = 62
        childReportingUnitType_Id = 25
        CountItemType = 'election-day'
        CandidateContest_Id_list = [922]
        filename = 'eday.txt'
        description = 'election-day'
    elif scenario == 'nc':
        schema = 'cdf_nc'
        s = sf.create_state('NC','../../local_data/NC/')
        Election_Id = 15834
        ReportingUnit_Id = 59
        childReportingUnitType_Id = 19  # county
        CountItemType = 'absentee-mail'
        CandidateContest_Id_list = [16410,16573,19980]
        filename = 'absentee.txt'
        description = 'absentee'

    if not use_stash:
        con, meta = dbr.sql_alchemy_connect(paramfile='../../local_data/database.ini')
        contest_name_d = {}
        for CandidateContest_Id in CandidateContest_Id_list:
            create_and_stash_rollup(con,schema,Election_Id,CandidateContest_Id,
                                    childReportingUnitType_Id,s,str(CandidateContest_Id)+filename,description+' Contest '+str(CandidateContest_Id))
            contest_name_d[CandidateContest_Id] = contest_id_to_name(con,meta,schema,CandidateContest_Id)
#%% start with stashed data
    rollup_d = {}
    for CandidateContest_Id in CandidateContest_Id_list:
        rollup_d[CandidateContest_Id] = unstash(s,str(CandidateContest_Id)+filename)
        contest_name = contest_name_d.get(CandidateContest_Id)
        bar_charts(rollup_d[CandidateContest_Id],contest_name)

    if con:
        con.dispose()


    print('Done')
