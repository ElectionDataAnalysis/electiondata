#!usr/bin/python3


from scipy import stats as stats
import scipy.spatial.distance as dist
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import db_routines as dbr
import os
import states_and_files as sf
try:
    import cPickle as pickle
except:
    import pickle

class Election(object): # TODO check that object is necessary (apparently for pickling)
    def pull_rollup_from_db(self, by_ReportingUnitType_Id, atomic_ReportingUnitType_Id, db_paramfile='../../local_data/database.ini'):
        assert isinstance(by_ReportingUnitType_Id, int), 'by_ReportingUnitType_Id must be an integer'
        assert isinstance(atomic_ReportingUnitType_Id, int), 'atomicReportingUnitType_Id must be an integer'

        con, meta = dbr.sql_alchemy_connect(schema=self.cdf_schema)

        q = """SELECT
            secvcj."Contest_Id", cruj."ParentReportingUnit_Id" AS "ReportingUnit_Id",  secvcj."Selection_Id", vc."CountItemType_Id", COALESCE(sum(vc."Count"),0) AS "Count"
         FROM
             {0}."SelectionElectionContestVoteCountJoin" secvcj 
             LEFT JOIN {0}."VoteCount" vc ON secvcj."VoteCount_Id" = vc."Id"
             LEFT JOIN {0}."ComposingReportingUnitJoin" cruj ON vc."ReportingUnit_Id" = cruj."ChildReportingUnit_Id"
             LEFT JOIN {0}."ReportingUnit" ru_c ON ru_c."Id" = cruj."ChildReportingUnit_Id"
             LEFT JOIN {0}."ReportingUnit" ru_p ON ru_p."Id" = cruj."ParentReportingUnit_Id"

         WHERE
             secvcj."Election_Id" = %(Election_Id)s
             AND ru_c."ReportingUnitType_Id" = %(roll_up_fromReportingUnitType_Id)s
             AND ru_p."ReportingUnitType_Id" = %(roll_up_toReportingUnitType_Id)s
         GROUP BY secvcj."Contest_Id", cruj."ParentReportingUnit_Id",  secvcj."Selection_Id", vc."CountItemType_Id"
         """.format(self.cdf_schema)
        params = {'Election_Id': self.Election_Id,
                  'roll_up_toReportingUnitType_Id': by_ReportingUnitType_Id,
                  'roll_up_fromReportingUnitType_Id': atomic_ReportingUnitType_Id}
        self.rollup_dframe = pd.read_sql_query(sql=q, con=con, params=params)
        if con:
            con.dispose()
        return self.rollup_dframe

    def anomaly_scores(self,con,meta,schema): # TODO pass anomaly algorithm and name as parameters. Here euclidean z-score
        pickle_path = self.pickle_dir+'anomaly_rollup'
        if os.path.isfile(pickle_path):
            print('Anomaly dataframe will not be calculated, but will be read from file:\n\t'+pickle_path)
            print('To calculate anomaly dataframe anew, move or rename the file.')
            self.anomaly_dframe = pd.read_pickle(pickle_path)

        else:
            contest_id_list = self.rollup_dframe.Contest_Id.unique()
            for contest_id in contest_id_list:
                anomaly_list = []
                cr = create_contest_rollup_from_election(con,meta,self, contest_id)
                print('Calculating anomalies for '+cr.ContestName)
                for column_field in ['ReportingUnit','CountItemType','Selection']:
                    #print('\tColumn field is '+column_field)
                    temp_list = ['ReportingUnit','CountItemType','Selection']
                    temp_list.remove(column_field)
                    for filter_field in temp_list:
                        #print('\cdf_table\tfilter field is '+filter_field)
                        for filter_value in cr.dataframe_by_name[filter_field].unique():
                            # print('\cdf_table\cdf_table\tfilter value is '+filter_value)
                            z_score_totals, z_score_pcts = cr.euclidean_z_score(column_field, [[filter_field,filter_value]])
                            anomaly_list.append(pd.Series([cr.ContestName,column_field,filter_field,filter_value,'euclidean z-score',
                                                 max(z_score_totals), max(z_score_pcts)],index=self.anomaly_dframe.columns))
                self.anomaly_dframe = self.anomaly_dframe.append(anomaly_list) # less efficient to update anomaly_dframe contest-by-contest, but better for debug
        self.anomaly_dframe.to_pickle(pickle_path)
        return

    def worst_bar_for_each_contest(self,con,meta,anomaly_min=0):
        if self.anomaly_dframe.empty:
            print('anomaly dataframe is empty')
            return
        else:
            for contest_id in self.rollup_dframe.Contest_Id.unique():
                cr = create_contest_rollup_from_election(con,meta,self, contest_id)
                contestname = cr.ContestName
                df = self.anomaly_dframe[self.anomaly_dframe.ContestName == contestname]
                max_pct_anomaly = df.anomaly_values_pcts.max()
                max_tot_anomaly = df.anomaly_value_totals.max()
                if max_pct_anomaly > anomaly_min:
                # find and plot worst bar charts from anomaly_dframe
                    for index,row in df.iterrows():
                        if row['anomaly_values_pcts'] == max_pct_anomaly:
                            plot_pivot(contestname, cr.dataframe_by_name, col_field=row['column_field'], filter=[row['filter_field'],row['filter_value']],mode='pct')
                if max_tot_anomaly > anomaly_min:
                    for index,row in df.iterrows():
                        if row['anomaly_value_totals']  == max_tot_anomaly:
                            plot_pivot(contestname,cr.dataframe_by_name,col_field=row['column_field'],filter=[row['filter_field'],row['filter_value']],mode='raw')

            return

    def __init__(self, cdf_schema, Election_Id, rollup_dframe, anomaly_dframe,roll_up_to_ReportingUnitType,roll_up_to_ReportingUnitType_Id,atomic_ReportingUnitType,atomic_ReportingUnitType_Id,pickle_dir):
        self.cdf_schema=cdf_schema
        self.Election_Id=Election_Id
        self.rollup_dframe=rollup_dframe
        self.anomaly_dframe=anomaly_dframe
        self.pickle_dir=pickle_dir
        self.roll_up_to_ReportingUnitType=roll_up_to_ReportingUnitType
        self.roll_up_to_ReportingUnitType_Id=roll_up_to_ReportingUnitType_Id
        self.atomic_ReportingUnitType=atomic_ReportingUnitType
        self.atomic_ReportingUnitType_Id=atomic_ReportingUnitType_Id

def create_election(session,meta,cdf_schema,Election_Id,roll_up_to_ReportingUnitType='county',atomic_ReportingUnitType='precinct',pickle_dir='../../local_data/tmp/',paramfile = '../../local_data/database.ini'):
    if not pickle_dir[-1] == '/': pickle_dir += '/'  # ensure directory ends with slash
    assert os.path.isdir(pickle_dir), 'No such directory: ' + pickle_dir + '\nCurrent directory is: ' + os.getcwd()
    assert isinstance(cdf_schema, str), 'cdf_schema must be a string'
    assert isinstance(Election_Id, int), 'Election_Id must be an integer'
    assert os.path.isfile(paramfile), 'No such file: '+paramfile+'\n\tCurrent directory is: ' + os.getcwd()

    # TODO pass session variable?
    roll_up_to_Id = dbr.read_id_from_enum(session, meta, cdf_schema, 'ReportingUnitType', roll_up_to_ReportingUnitType)
    roll_up_from_Id = dbr.read_id_from_enum(session, meta, cdf_schema, 'ReportingUnitType', atomic_ReportingUnitType)

    print('Creating basic Election object')
    e = Election(cdf_schema,Election_Id,pickle_dir = pickle_dir,rollup_dframe = None, anomaly_dframe = None,
                 roll_up_to_ReportingUnitType=roll_up_to_ReportingUnitType,roll_up_to_ReportingUnitType_Id=roll_up_to_Id,
                 atomic_ReportingUnitType=atomic_ReportingUnitType,atomic_ReportingUnitType_Id=roll_up_from_Id)

    #%% rollup dataframe
    print('Getting rolled-up data for all contests')
    ElectionName = dbr.read_single_value_from_id(session,meta,cdf_schema,'Election','Name',Election_Id).replace(' ','')
    rollup_filepath = pickle_dir+'rollup_to_'+roll_up_to_ReportingUnitType

    #%% get the roll-up dframe from the db, or from pickle
    # if rollup already pickled into pickle_dir
    if os.path.isfile(rollup_filepath):
        e.rollup_dframe = pd.read_pickle(rollup_filepath)
        print('Rollup by '+ roll_up_to_ReportingUnitType +' will not be calculated, but will be read from file:\n\t' + rollup_filepath)
        print('To calculate rollup dataframe anew, move or rename the file.')
    # if rollup not already pickled, create and pickle it
    else:
        e.pull_rollup_from_db(roll_up_to_Id,roll_up_from_Id)
        e.rollup_dframe.to_pickle(rollup_filepath)

    #%% anomaly dataframe
    print('Getting anomaly scores for all contests')
    anomaly_filepath = pickle_dir+ElectionName+'_anomalies'
    # if anomaly dataframe already pickled
    if os.path.isfile(anomaly_filepath):
        e.anomaly_dframe = pd.read_pickle(anomaly_filepath)
    # if rollup not already pickled, create and pickle it
    else:
        e.anomaly_dframe = pd.DataFrame(data=None,index=None,
                            columns=['ContestName','column_field','filter_field','filter_value','anomaly_algorithm',
                                     'anomaly_value_totals','anomaly_values_pcts'])
    if con:
        con.dispose()
    return e

class ContestRollup:
    def dropoff_vote_count(self,contest_rollup):
        # TODO
        return

    def pivot(self, col_field='Selection', filter=[],mode='raw'):
        """
        gives a pivot of a contest roll-up
        where rows are filtered by the field-conditions in filter,
        columns are labeled by values of the col_field
        where rows are labeled by an index made up of all remaining fields.
        mode == 'raw' gives raw vote totals; mode == 'pct' give percentages
        """
        return pivot(self.dataframe_by_name,col_field,filter)

    def plot_pivot(self,col_field='Selection',filter=[]):
        plot_pivot(self.ContestName,self.dataframe_by_name,col_field,filter)
        return

    def percentages(self,pivot_index=['ReportingUnit','CountItemType'],filter=[]):
        af = self.dataframe_by_name.copy()
        pivot_columns = ['ReportingUnit','CountItemType','Selection']

        for col,val in filter:
            assert col in ('ReportingUnit','Selection','CountItemType')
            af = af[af[col] == val]
            pivot_columns.remove(col)
        for col in pivot_index: pivot_columns.remove(col) # TODO improve
        cf = af.pivot_table(index = pivot_index,columns=pivot_columns,values='Count',aggfunc=np.sum)
        if 'total' not in cf.columns:   # TODO understand why this was necessary and whether it's OK
            cf['total'] = cf.sum(axis=1)

        bf = cf.div(cf["total"], axis=0)

        # TODO
        return bf

    def BarCharts(self):
        rollup = self.dataframe_by_name
        CountItemType_list = rollup['CountItemType'].unique()
        for type in CountItemType_list:
            type_df = rollup[rollup['CountItemType'] == type]
            type_pivot = type_df.pivot_table(index='ReportingUnit', columns='Selection', values='Count')
            type_pivot.plot.bar()
            plt.title(self.ContestName + '\n' + type + ' (vote totals)')

            type_pct_pivot = pct_dframe(type_pivot)
            type_pct_pivot.plot.bar()
            plt.title(self.ContestName + '\n' + type + ' (vote percentages)')
        plt.show()
        return

    def euclidean_z_score(self,column_field,filter_field_value_pair_list):
        z_score_totals = pframe_to_zscore(self.pivot(col_field=column_field, filter=filter_field_value_pair_list))
        z_score_pcts =  pframe_to_zscore(self.pivot(col_field=column_field, filter=filter_field_value_pair_list,mode = 'pct'))
        return z_score_totals, z_score_pcts

    def anomaly_scores(self):
        dframe = self.dataframe_by_name
        filter_list = [  ['CountItemType',x] for x in dframe.CountItemType.unique() ] + [  ['ReportingUnit',x] for x in dframe.ReportingUnit.unique() ]

        raw_score_list = []
        pct_score_list = []
        for cv in filter_list:
            pframe = self.pivot(col_field='Selection', filter=[cv])
            raw_score_list.append( max(pframe_to_zscore(pframe)))

            pctframe = self.pivot(col_field='Selection', filter=[cv],mode = 'pct')
            pct_score_list.append(max(pframe_to_zscore(pctframe)))

        return filter_list,raw_score_list,pct_score_list

    def __init__(self,dataframe_by_id,dataframe_by_name,cdf_schema,Election_Id,Contest_Id,childReportingUnitType_Id,
                 ElectionName,ContestName,childReportingUnitType,
                 contest_type,pickle_dir=None):
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

class ContestDataFrame(pd.DataFrame):
    def __init__(self,Election_ID,Contest_Id,data=None, index=None, columns=['ReportingUnit_Id','Selection_ID','CountItemType_Id','Count'], dtype=None, copy=False):
        pd.Dataframe.__init__(self,data, index, columns, dtype, copy)
        self.Election_ID=Election_ID
        self.Contest_Id=Contest_Id

def plot_pivot(contestname,dataframe_by_name,col_field='Selection',filter=[],mode='raw'):
    title_string = contestname
    if filter:
        title_string += '\n'+filter[1]   # TODO dict is more natural, with filter[0] is column and filter[1] is value

    type_pivot= pivot(dataframe_by_name,col_field, [filter],mode)
    try:    # TODO get exact criteria for skipping
        type_pivot.plot.bar()
        plt.title(title_string+'\nVote Totals')
        type_pct_pivot = pct_dframe(type_pivot)
        type_pct_pivot.plot.bar()
        plt.title(title_string+'\nVote Percentages')
        plt.show()
    except:
        a =1 # TODO placeholder


def pivot(dataframe_by_name, col_field='Selection', filter=[],mode='raw'):
    """
    gives a pivot of a contest dataframe by name
    where rows are filtered by the field-conditions in filter,
    columns are labeled by values of the col_field
    where rows are labeled by an index made up of all remaining fields.
    mode == 'raw' gives raw vote totals; mode == 'pct' give percentages
    """
    assert mode == 'raw' or mode == 'pct', 'mode not recognized: '+mode
    af = dataframe_by_name.copy()

    label_columns = ['ReportingUnit', 'CountItemType', 'Selection']
    for col, val in filter:
        assert col in label_columns
        af = af[af[col] == val]
        label_columns.remove(col)
    label_columns.remove(col_field)
    cf = af.pivot_table(index=label_columns, columns=[col_field], values='Count', aggfunc=np.sum)
    if mode == 'pct':
        if 'total' not in cf.columns:  # TODO understand why this was necessary and whether it's OK
            cf['total'] = cf.sum(axis=1)
        cf = cf[cf.total != 0]
        cf = cf.div(cf["total"], axis=0)
    return cf


def create_contest_rollup_from_election(session,meta,e,Contest_Id):   # TODO get rid of con/meta/schema here by making names part of the Election def?
    assert isinstance(e,Election),'election must be an instance of the Election class'
    # assert isinstance(Contest_Id,int), 'Contest_Id must be an integer' # TODO why did this fail?
    ElectionName = dbr.read_single_value_from_id(session, meta, e.cdf_schema,'Election','Name', e.Election_Id)
    contest_type = dbr.contest_type_from_contest_id(session.bind,meta,e.cdf_schema,Contest_Id) # Candidate or BallotMeasure
    contesttable = contest_type + 'Contest'
    ContestName = dbr.read_single_value_from_id(session,meta,e.cdf_schema,contesttable,'Name',Contest_Id) # TODO candidte or ballotmeasure

    dataframe_by_id = e.rollup_dframe[e.rollup_dframe.Contest_Id == Contest_Id].drop('Contest_Id',axis=1)
    dataframe_by_name = id_values_to_name(session.bind, meta, e.cdf_schema, dataframe_by_id)
    by_ReportingUnitType = e.roll_up_to_ReportingUnitType
    by_ReportingUnitType_Id = e.roll_up_to_ReportingUnitType_Id
    return ContestRollup(dataframe_by_id, dataframe_by_name, e.cdf_schema, e.Election_Id, Contest_Id,
                         by_ReportingUnitType_Id, ElectionName, ContestName, by_ReportingUnitType,
                         contest_type, None)

def create_contest_rollup(session, meta, cdf_schema, Election_Id, Contest_Id, by_ReportingUnitType_Id, atomic_ReportingUnitType_Id,contest_type, pickle_dir):
    assert isinstance(cdf_schema,str) ,'cdf_schema must be a string'
    assert isinstance(Election_Id,int), 'Election_Id must be an integer'
    assert isinstance(Contest_Id,int), 'Contest_Id must be an integer'
    assert isinstance(by_ReportingUnitType_Id, int), 'childReportingUnitType_Id must be an integer'
    assert contest_type == 'BallotMeasure' or contest_type == 'Candidate', 'contest_type must be either \'BallotMeasure\' or \'Candidate\''
    assert os.path.isdir(pickle_dir) , 'No such directory: '+pickle_dir+'\nCurrent directory is: '+os.getcwd()
    if not pickle_dir[-1] == '/': pickle_dir += '/' # ensure directory ends with slash

    ElectionName = dbr.read_single_value_from_id(session,meta,cdf_schema,'Election','Name',Election_Id)
    ContestName = dbr.read_single_value_from_id(session,meta,cdf_schema,contest_type+'Contest','Name',Contest_Id)
    childReportingUnitType = dbr.read_single_value_from_id(session, meta, cdf_schema,'ReportingUnitType','Txt', by_ReportingUnitType_Id)

    
    f_by_id = pickle_dir + cdf_schema + 'eid' + str(Election_Id) + 'ccid' + str(Contest_Id) + 'crut' + str(
        by_ReportingUnitType_Id) + '_by_id'
    if os.path.exists(f_by_id):
        dataframe_by_id = pd.read_pickle(f_by_id)
    else:
        dataframe_by_id = rollup_count(session.bind, cdf_schema, Election_Id, Contest_Id, by_ReportingUnitType_Id,atomic_ReportingUnitType_Id)
        dataframe_by_id.to_pickle(f_by_id)

    f_by_name = pickle_dir + cdf_schema + 'eid' + str(Election_Id) + 'ccid' + str(Contest_Id) + 'crut' + str(
        by_ReportingUnitType_Id) + '_by_name'
    if os.path.exists(f_by_name):
        dataframe_by_name = pd.read_pickle(f_by_name)
    else:
        dataframe_by_name = id_values_to_name(session.bind,meta,cdf_schema,dataframe_by_id)
        dataframe_by_name.to_pickle(f_by_name)

    return ContestRollup(dataframe_by_id, dataframe_by_name, cdf_schema, Election_Id, Contest_Id, by_ReportingUnitType_Id, ElectionName, ContestName, childReportingUnitType,
                         contest_type, pickle_dir)

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

def rollup_count(con, schema, Election_Id, Contest_Id, roll_up_toReportingUnitType_Id,roll_up_fromReportingUnitType_Id):
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
        AND ru_c."ReportingUnitType_Id" = %(roll_up_fromReportingUnitType_Id)s
        AND ru_p."ReportingUnitType_Id" = %(roll_up_toReportingUnitType_Id)s
    GROUP BY cruj."ParentReportingUnit_Id",  secvcj."Selection_Id", vc."CountItemType_Id"
    """.format(schema)
    params = {'Election_Id': Election_Id,'Contest_Id':Contest_Id,
              'roll_up_toReportingUnitType_Id':roll_up_toReportingUnitType_Id,
              'roll_up_fromReportingUnitType_Id':roll_up_fromReportingUnitType_Id}
    dframe = pd.read_sql_query(sql=q, con = con,params=params)
    return dframe

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
            elif table_name == 'Contest': # ballot question or candidate contest
                cand_contest_id_to_name_d = dbr.read_some_value_from_id(con,meta,schema,'CandidateContest','Name',id_list)
                ballot_measure_contest_id_to_name_d = dbr.read_some_value_from_id(con,meta,schema,'BallotMeasureContest','Name',id_list)
                id_to_name_d = {**cand_contest_id_to_name_d,**ballot_measure_contest_id_to_name_d}
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

def pct_dframe(df):
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
    distance_list = [sum([dist.euclidean(item,y) for y in li]) for item in li]
    if len(set(distance_list)) == 1:
        # if all distances are the same, which yields z-score nan values

        return [0]*len(li)
    else:
        return list(stats.zscore(distance_list))

def diff_anomaly_score(left_dframe, right_dframe, left_value_column ='sum', right_value_column='sum', on ='ReportingUnit', title=''):
    """given two named dataframes indexed by ReportingUnit, find any anomalies
    in the margin between the specified columns (default is 'sum')
    among the set of ReportingUnits of given type.
    Only ReportingUnits shared by both contests are considered
    """
#    assert isinstance(left_dframe,pd.DataFrame) and isinstance(right_dframe,DataFrame), 'Certain argument(s) are not DataFrames'
    assert left_value_column in left_dframe.columns, 'Missing column on left: '+ left_value_column
    assert right_value_column in right_dframe.columns, 'Missing column on right: '+ right_value_column
    c = left_dframe[['ReportingUnit','CountItemType',left_value_column]].merge(right_dframe[['ReportingUnit','CountItemType',right_value_column]],how = 'inner',left_on = on,right_on=on)
    if c.empty:
        return 0
    else:
        c['diff'] = c[left_value_column+'_x'] - c[right_value_column+'_y']
        #%% find diff outlier
        zscores = stats.zscore(c['diff'])
        anomaly_score = max(zscores)
        if anomaly_score > 2:   # TODO remove hard-coding of 2
            print('Anomaly found')
            # ('Anomaly found comparing:\n\cdf_table'+left_dframe+','+left_value_column+'\n\cdf_table'+right_dframe+','+right_value_column)
        return anomaly_score

def pframe_to_zscore(pframe):
    """ for a pivoted dataframe, calculate z-score """
    if pframe.empty:
        print('Empty dataframe assigned score list of [0]')
        return [0]
    else:
        # TODO check that all rows are numerical vectors, with all other info in the index
        row_vectors = [list(pframe.iloc[x, :]) for x in range(len(pframe))]   # TODO there's gotta be a better way to get the list of row vectors, no?
        return euclidean_zscore(row_vectors)

if __name__ == '__main__':
    con, meta = dbr.sql_alchemy_connect(schema, paramfile='../../local_data/database.ini')

    nc_2018 = create_election(session,meta,'cdf_nc',15834)
    schema = 'cdf_nc'
    nc_2018.anomaly_scores(con,meta,schema)

    if con:
        con.dispose()
    anomaly_picklepath = nc_2018.pickle_dir+'nc_2018_anomalies'
    nc_2018.anomaly_dframe.to_pickle(anomaly_picklepath)

#    scenario = input('Enter xx or nc\n')
    scenario = 'nc'
    use_stash = 0
    use_existing_rollups = 0

    number_of_charts = 1
    pickle_file_dir = '../../local_data/tmp/'
    if scenario == 'xx':
        s = sf.create_state('XX','../../local_data/XX/')
        cdf_schema = 'cdf_xx'
        Election_Id = 262
        ReportingUnit_Id = 62
        atomic_ReportingUnitType_Id = 25 # precinct
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
        atomic_ReportingUnitType_Id = 25 # precinct
        childReportingUnitType_Id = 19  # county
        CountItemType = 'absentee-mail'
        CandidateContest_Id_list = [16410,16573,19980]
        filename = 'absentee.txt'
        description = 'absentee'

    ContestRollup_dict = {}



    con, meta, Session = dbr.sql_alchemy_connect(schema=cdf_schema,paramfile='../../local_data/database.ini')
    # create and pickle if not existing already
    for Contest_Id in CandidateContest_Id_list:
        if not use_existing_rollups:
            for f in [pickle_file_dir + cdf_schema + 'eid' + str(Election_Id) + 'ccid' + str(Contest_Id) + 'crut' +str(childReportingUnitType_Id) + '_by_id', pickle_file_dir + cdf_schema + 'eid' + str(Election_Id) + 'ccid' + str(Contest_Id) + 'crut' + str(childReportingUnitType_Id) + '_by_name']:
                if os.path.isfile(f):
                    os.remove(f)

        rollup = create_contest_rollup(con, meta, cdf_schema, Election_Id, Contest_Id, childReportingUnitType_Id,atomic_ReportingUnitType_Id,
                                  'Candidate', pickle_file_dir)
        ContestRollup_dict[Contest_Id] = rollup
        filter_list,vote_scores,pct_scores = rollup.anomaly_scores() # TODO this calculates pivots; if we calculate them again in the plotting routine, that's inefficient.
        top_vote_charts = sorted(zip(vote_scores,filter_list), reverse=True)[:number_of_charts]
        top_pct_charts = sorted(zip(pct_scores,filter_list), reverse=True)[:number_of_charts]
        for filter in set(tuple(x[1]) for x in top_vote_charts + top_pct_charts) :
                rollup.plot_pivot(filter=filter)


    [d1,d2,d3] =ContestRollup_dict[16410].dataframe_by_name, ContestRollup_dict[16573].dataframe_by_name,ContestRollup_dict[19980].dataframe_by_name
    a = diff_anomaly_score(ContestRollup_dict[16573].dataframe_by_name,
                           ContestRollup_dict[19980].dataframe_by_name,
                           left_value_column='Count', right_value_column='Count',
                           on = ['ReportingUnit','CountItemType'], title='values')


    anomalies = []
    for i in [16410,16573,19980]:
        for j in [16410,16573,19980]:
            if i < j:
                anomalies.append([i, j, diff_anomaly_score(ContestRollup_dict[i].dataframe_by_name,
                                                           ContestRollup_dict[j].dataframe_by_name,
                                                           left_value_column='Count', right_value_column='Count',
                                                           on = ['ReportingUnit','CountItemType'], title='values')])
    print (anomalies)


#    for cru in ContestRollup_dict.values():
#        cru.BarCharts()

    if con:
        con.dispose()


    print('Done')
