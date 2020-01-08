#!usr/bin/python3


from scipy import stats as stats
import scipy.spatial.distance as dist
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import db_routines as dbr
import os
import states_and_files as sf

class ContestRollup:

    def percentages(self,pivot_index=['ReportingUnit','CountItemType']):


        af = self.dataframe_by_name.copy()
        pivot_columns = ['ReportingUnit','CountItemType','Selection']
        for x in pivot_index: pivot_columns.remove(x) # TODO improve
        cf = af.pivot_table(index = pivot_index,columns=pivot_columns,values='Count',aggfunc=np.sum)
#        cf['sum'] = cf.sum(axis=1)
        bf = cf.div(cf["total"], axis=0)

        # TODO
        return bf

    @staticmethod
    def BarCharts(self):
        rollup = self.dataframe_by_name
        CountItemType_list = rollup['CountItemType'].unique()
        for type in CountItemType_list:
            type_df = rollup[rollup['CountItemType'] == type]
            type_pivot = type_df.pivot_table(index='ReportingUnit', columns='Selection', values='Count')
            type_pivot.plot.bar()
            plt.title(self.ContestName + '\n' + type + ' (vote totals)')

            type_pct_pivot = create_pct_df(type_pivot)
            type_pct_pivot.plot.bar()
            plt.title(self.ContestName + '\n' + type + ' (vote totals)')
        plt.show()
        return

    def __init__(self,dataframe_by_id,dataframe_by_name,cdf_schema,Election_Id,Contest_Id,childReportingUnitType_Id,ElectionName,ContestName,childReportingUnitType,
                 contest_type,pickle_dir):
        self.dataframe_by_id=dataframe_by_id
        self.dataframe_by_name=dataframe_by_name
        self.cdf_schema=cdf_schema
        self.Election_Id=Election_Id
        self.Contest_Id=Contest_Id
        self.childReportingUnitType_Id=childReportingUnitType_Id
        self.ElectionName=ElectionName
        self.ContestName=ContestName
        self.childReportingUnitType=childReportingUnitType
        self.contest_type=contest_type # either BallotMeasure or Candidate
        self.pickle_file_path=pickle_dir

def create_contest_rollup(con,meta,cdf_schema,Election_Id,Contest_Id,childReportingUnitType_Id,contest_type,pickle_dir):
    assert isinstance(cdf_schema,str) ,'cdf_schema must be a string'
    assert isinstance(Election_Id,int), 'Election_Id must be an integer'
    assert isinstance(Contest_Id,int), 'Contest_Id must be an integer'
    assert isinstance(childReportingUnitType_Id,int), 'childReportingUnitType_Id must be an integer'
    assert contest_type == 'BallotMeasure' or contest_type == 'Candidate', 'contest_type must be either \'BallotMeasure\' or \'Candidate\''
    assert os.path.isdir(pickle_dir) , 'No such directory: '+pickle_dir+'\nCurrent directory is: '+os.getcwd()
    if not pickle_dir[-1] == '/': pickle_dir += '/' # ensure directory ends with slash

    ElectionName = dbr.read_single_value_from_id(con,meta,cdf_schema,'Election','Name',Election_Id)
    ContestName = dbr.read_single_value_from_id(con,meta,cdf_schema,contest_type+'Contest','Name',Contest_Id)
    childReportingUnitType = dbr.read_single_value_from_id(con,meta,cdf_schema,'ReportingUnitType','Txt',childReportingUnitType_Id)

    
    f_by_id = pickle_dir + cdf_schema + 'eid' + str(Election_Id) + 'ccid' + str(Contest_Id) + 'crut' + str(
        childReportingUnitType_Id) + '_by_id'
    if os.path.exists(f_by_id):
        dataframe_by_id = pd.read_pickle(f_by_id)
    else:
        dataframe_by_id = rollup_count(con, cdf_schema, Election_Id, Contest_Id, childReportingUnitType_Id)
        dataframe_by_id.to_pickle(f_by_id)

    f_by_name = pickle_dir + cdf_schema + 'eid' + str(Election_Id) + 'ccid' + str(Contest_Id) + 'crut' + str(
        childReportingUnitType_Id) + '_by_name'
    if os.path.exists(f_by_name):
        dataframe_by_name = pd.read_pickle(f_by_name)
    else:
        dataframe_by_name = id_values_to_name(con,meta,cdf_schema,dataframe_by_id)
        dataframe_by_name.to_pickle(f_by_name)

    return ContestRollup(dataframe_by_id,dataframe_by_name,cdf_schema,Election_Id,Contest_Id,childReportingUnitType_Id,ElectionName,ContestName,childReportingUnitType,
                 contest_type,pickle_dir)

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
    assert isinstance(df,pd.DataFrame), 'Argument must be dataframe'
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

def dropoff_anomaly_score(left_dframe,right_dframe,left_value_column = 'sum',right_value_column='sum',on = 'ReportingUnit'):
    """given two named dataframes indexed by ReportingUnit, find any anomalies
    in the margin between the specified columns (default is 'sum')
    among the set of ReportingUnits of given type.
    Only ReportingUnits shared by both contests are considered
    """
#    assert isinstance(left_dframe,pd.DataFrame) and isinstance(right_dframe,DataFrame), 'Certain argument(s) are not DataFrames'
    assert left_value_column in left_dframe.columns, 'Missing column: '+ left_value_column
    assert right_value_column in right_dframe.columns, 'Missing column: '+ right_value_column
    combined = left_dframe[['ReportingUnit','CountItemType',left_value_column]].merge(right_dframe[['ReportingUnit','CountItemType',right_value_column]],how = 'inner',left_on = on,right_on=on)
    #%% find outlier in percentage-diff series
    # TODO

    #%% if outlier is 'anomalous enough', make scatter plot for the two rollups
    # TODO
    return combined

if __name__ == '__main__':
#    scenario = input('Enter xx or nc\n')
    scenario = 'nc'
    use_stash = 0
    use_existing_rollups = 0
    pickle_file_dir = '../../local_data/tmp/'
    if scenario == 'xx':
        s = sf.create_state('XX','../../local_data/XX/')
        cdf_schema = 'cdf_xx'
        Election_Id = 262
        ReportingUnit_Id = 62
        childReportingUnitType_Id = 25
        CountItemType = 'election-day'
        CandidateContest_Id_list = [922]
        filename = 'eday.txt'
        description = 'election-day'
    elif scenario == 'nc':
        cdf_schema = 'cdf_nc'
        s = sf.create_state('NC','../../local_data/NC/')
        Election_Id = 15834
        ReportingUnit_Id = 59
        childReportingUnitType_Id = 19  # county
        CountItemType = 'absentee-mail'
        CandidateContest_Id_list = [16410,16573,19980]
        filename = 'absentee.txt'
        description = 'absentee'

    ContestRollup_dict = {}
    if not use_existing_rollups:
        con, meta = dbr.sql_alchemy_connect(paramfile='../../local_data/database.ini')
        # create and pickle
        for Contest_Id in CandidateContest_Id_list:
            rollup = create_contest_rollup(con, meta, cdf_schema, Election_Id, Contest_Id, childReportingUnitType_Id,
                                      'Candidate', pickle_file_dir)
            ContestRollup_dict[Contest_Id] = rollup
            pct = rollup.percentages(pivot_index=['ReportingUnit','Selection'])
            pct2 = rollup.percentages(pivot_index=['ReportingUnit'])
    [d1,d2,d3] =ContestRollup_dict[16410].dataframe_by_name, ContestRollup_dict[16573].dataframe_by_name,ContestRollup_dict[19980].dataframe_by_name
    a = dropoff_anomaly_score(ContestRollup_dict[16573].dataframe_by_name,
                              ContestRollup_dict[19980].dataframe_by_name,
                              left_value_column='Count', right_value_column='Count',
                              on = ['ReportingUnit','CountItemType'])

    for cru in ContestRollup_dict.values():
        cru.BarCharts()

    if con:
        con.dispose()


    print('Done')
