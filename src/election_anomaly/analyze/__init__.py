#!usr/bin/python3


import numpy as np
from scipy import stats as stats
import scipy.spatial.distance as dist
import pandas as pd
import matplotlib.pyplot as plt
import db_routines as dbr

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
                id_to_name_d = {**dbr.read_some_value_from_id(con,
                                meta, schema,'CandidateSelection','Candidate_Id',id_list),
                                **dbr.read_some_value_from_id(con,
                                meta, schema, 'BallotMeasureSelection','Selection', id_list)
                                }
            elif table_name[-4:] == 'Type':
                id_to_name_d = dbr.read_some_value_from_id(con,
                                            meta, schema, table_name, 'Txt', id_list)
            else:
                id_to_name_d = dbr.read_some_value_from_id(con,
                                            meta, schema, table_name, 'Name', id_list)
            cf[table_name] = cf[col].map(id_to_name_d)
            cf=cf.drop(col,axis=1)
    # TODO
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

if __name__ == '__main__':
#    scenario = input('Enter xx or nc\n')
    scenario = 'nc'
    if scenario == 'xx':
        schema = 'cdf_xx'
        Election_Id = 262
        ReportingUnit_Id = 62
        childReportingUnitType_Id = 25
        CountItemType_Id = 52
        CandidateContest_Id = 922
    elif scenario == 'nc':
        schema = 'cdf_nc'
        Election_Id = 15834
        ReportingUnit_Id = 59
        childReportingUnitType_Id = 19  # county
        CountItemType_Id = 50   # absentee-mail
        CandidateContest_Id = 16410


    con, meta = dbr.sql_alchemy_connect(paramfile='../../local_data/database.ini')

#    df = precinct_count(con, schema,  Election_Id,CandidateContest_Id)
    df_county = rollup_count(con,schema,Election_Id,CandidateContest_Id,19)

    named_df_county = id_values_to_name(con,meta,schema,df_county)

#    df_county_pivot = pd.pivot_table(df_county,index=['County_Id','Selection_Id'],values = 'Count',
#                                     aggfunc= np.sum)

    abs = df_county[df_county['CountItemType_Id']== CountItemType_Id]
    abs_pivot2 = abs.pivot_table(index='ReportingUnit_Id',columns='Selection_Id',values='Count',aggfunc=np.sum)
    abs_pivot1 = abs.pivot_table(index='ReportingUnit_Id',columns='Selection_Id',values='Count')
    abs_pivot1.plot.bar()

    abs_pct_pivot = create_pct_df(abs_pivot1)
    abs_pct_pivot.plot.bar()

    plt.show()



    plt.show()

    print('Done')
