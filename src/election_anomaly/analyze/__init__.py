#!usr/bin/python3
import os.path

from scipy import stats as stats
import scipy.spatial.distance as dist
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import db_routines as dbr
import os
import pathlib
import states_and_files as sf
try:
    import cPickle as pickle
except:
    import pickle

class AnomalyDataFrame(object):
    def __init__(self,rollup):
        assert isinstance(rollup,ContestRollup),'One argument must be an instance of the Election class'
        pickle_path = rollup.election.pickle_dir+'anomalies_by_'+rollup.roll_up_to_ru_type+'_from_'+rollup.atomic_ru_type
        if os.path.isfile(pickle_path):
            print('Anomalies will not be calculated, but will be read from existing file:\n\t'+pickle_path)
            self=pd.read_pickle(pickle_path)
        else:
            self.contestrollup=rollup  # the contest rollup we're analyzing

            self.dframe=pd.DataFrame(data=None,index=None,
                                columns=['ContestName','column_field','filter_field','filter_value','anomaly_algorithm',
                                         'anomaly_value_raw','anomaly_value_pct'])
            for contest_name in rollup.contest_name_list:
                anomaly_list = []
                c = rollup.restrict_by_contest_name([contest_name])
                print('Calculating anomalies for '+contest_name)

                for column_field in ['ReportingUnit','CountItemType','Selection']:
                    temp_list = ['ReportingUnit','CountItemType','Selection']
                    temp_list.remove(column_field)
                    for filter_field in temp_list:
                        for filter_value in c.dframe[filter_field].unique():
                            z_score_totals, z_score_pcts = c.euclidean_z_score(column_field, [[filter_field,filter_value]])
                            anomaly_list.append(pd.Series([contest_name,column_field,filter_field,filter_value,'euclidean z-score',
                                                 max(z_score_totals), max(z_score_pcts)],index=self.dframe.columns))
                if anomaly_list:
                    self.dframe = self.dframe.append(anomaly_list) # less efficient to update anomaly_dframe contest-by-contest, but better for debug
                else:
                    print('No anomalies found for contest ' + contest_name)
            self.dframe.to_pickle(pickle_path)
            print('AnomalyDataFrame calculated, stored in a pickled DataFrame at '+pickle_path)

class Election(object): # TODO check that object is necessary (apparently for pickling)
    def most_anomalous(self,n=3,mode='pct'):
        """ returns a list of the n most anomalous contests for the election
        mode is 'pct' or 'raw'
        """
        assert mode == 'pct' or mode == 'raw','\'pct\' and \'raw\' are the only recognized modes'
        adf = self.anomaly_dframe
        col = 'anomaly_value_'+mode

        most_anomalous = adf.nlargest(n,col,keep='all')
        return most_anomalous
    
    def pull_rollup_from_db(self, by_ReportingUnitType_Id, atomic_ReportingUnitType_Id, contest_name_list=[],db_paramfile='../../local_data/database.ini'):
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

    def pull_rollup_from_db_by_types(self, roll_up_to_ru_type, atomic_ru_type='precinct', contest_name_list=None,db_paramfile='../local_data/database.ini'):

        con, meta = dbr.sql_alchemy_connect(schema='cdf',paramfile=db_paramfile,db_name=self.state.short_name)

        # Get necessary tables from cdf schema
        cdf_d={}
        for t in ['ReportingUnitType','CandidateContest','BallotMeasureContest','BallotMeasureSelection','CandidateSelection','Candidate','CountItemType','ReportingUnit']:
            cdf_d[t]=pd.read_sql_table(t,con,schema='cdf',index_col=None)
        candidate_name_by_selection_id = cdf_d['CandidateSelection'].merge(cdf_d['Candidate'],left_on='Candidate_Id',right_on='Id',suffixes=['','Candidate'])

        # create and return id-to-contest-type dict for this election.
        contest_type = {}
        contest_name = {}
        selection_name = {}

        for i,r in cdf_d['CandidateContest'].iterrows():
            contest_type[r['Id']] = 'Candidate'
            contest_name[r['Id']] = r['Name']
        for i,r in cdf_d['BallotMeasureContest'].iterrows():
            contest_type[r['Id']] = 'BallotMeasure'
            contest_name[r['Id']] = r['Name']
        for i,r in cdf_d['BallotMeasureSelection'].iterrows():
            selection_name[r['Id']] = r['Selection']
        for i,r in candidate_name_by_selection_id.iterrows():
            selection_name[r['Id']] = r['BallotName']


        roll_up_to_ru_type_id = int(cdf_d['ReportingUnitType'][cdf_d['ReportingUnitType']['Txt']==roll_up_to_ru_type].iloc[0]['Id'])
        atomic_ru_type_id = int(cdf_d['ReportingUnitType'][cdf_d['ReportingUnitType']['Txt']==atomic_ru_type].iloc[0]['Id'])

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
         """.format('cdf')
        params = {'Election_Id': self.Election_Id,
                  'roll_up_toReportingUnitType_Id': roll_up_to_ru_type_id,
                  'roll_up_fromReportingUnitType_Id': atomic_ru_type_id}
        rollup_dframe = pd.read_sql_query(sql=q, con=con, params=params)
        # add columns to rollup dframe
        rollup_dframe['contest_type'] = rollup_dframe['Contest_Id'].map(contest_type)
        rollup_dframe['Contest'] = rollup_dframe['Contest_Id'].map(contest_name)
        rollup_dframe['Selection'] = rollup_dframe['Selection_Id'].map(selection_name)
        for t in ['CountItemType','ReportingUnit']:
            rollup_dframe = rollup_dframe.merge(cdf_d[t],left_on=t+'_Id',right_on='Id')
        rollup_dframe=rollup_dframe[['Contest_Id','ReportingUnit_Id','Selection_Id','CountItemType_Id','Contest','Name','Selection','Txt','Count','contest_type']]
        rollup_dframe.rename(columns={'Txt':'CountItemType','Name':'ReportingUnit'},inplace=True)
        if contest_name_list:
            rollup_dframe=rollup_dframe[rollup_dframe['Contest'].isin(contest_name_list)]

        if con:
            con.dispose()
        return rollup_dframe

    def draw_most_anomalous(self,session,meta,n=3,mode='pct'):
        """ plot the 3 (or n) most anomalous bar charts for the election"""
        print('Most anomalous contests by votes '+mode+':')
        for index,row in self.most_anomalous(n,mode).iterrows():
            print('\t' + row['ContestName'])
            print('\tby ' + row['column_field'])
            print('\t' + row['filter_value'] + ' only')
            print('\tAnomaly value ' + row['anomaly_algorithm'] + ': ' + str(row['anomaly_value_pct']) + '\n')
            cr = create_contest_rollup_from_election_rollup(session,meta,self,row['ContestId'])

            plot_pivot(row['ContestName'],cr.dataframe_by_name,col_field=row['column_field'],
                          filter=[row['filter_field'],row['filter_value']],
                          mode=mode)

    def worst_bar_for_selected_contests(self,session,meta_gen,anomaly_min=0,contest_type='Candidate'):
        dont_stop = input('Create worst bar charts for a single contest (y/n)?\n')
        while dont_stop == 'y':
            contest_id = choose_by_id(session,meta_gen,self.cdf_schema,contest_type+'Contest',filter=[{"FilterTable":'ElectionContestJoin', "FilterField":'Election_Id', "FilterValue":self.Election_Id , "ForeignIdField":'Contest_Id'}])
            #     contest_id = an.choose_by_id(session,meta_cdf_schema,cdf_schema,'CandidateContest',filter=[{'FilterTable':'ElectionContestJoin','FilterField':'Election_Id','FilterValue':election_id,'ForeignIdField':'Contest_Id'}]
            #                               )
            self.worst_bar_for_each_contest(session,meta_gen,anomaly_min=anomaly_min,contest_id_list=[contest_id])
            dont_stop = input('Create worst bar charts for another contest (y/n)?\n')
        return

    def worst_bar_for_each_contest(self,session,meta_gen,anomaly_min=0,contest_id_list=[]):
        if self.anomaly_dframe.empty:
            print('anomaly dataframe is empty')
            return
        else:
            if contest_id_list==[]:
                contest_id_list = self.rollup_dframe.Contest_Id.unique()
            for contest_id in contest_id_list:
                cr = create_contest_rollup_from_election_rollup(session,meta_gen,self,contest_id)
                contestname = cr.ContestName
                df = self.anomaly_dframe[self.anomaly_dframe.ContestName == contestname]
                max_pct_anomaly = df.anomaly_value_pct.max()
                max_tot_anomaly = df.anomaly_value_raw.max()

                # don't plot total vote counts
                df_to_plot= cr.dataframe_by_name[cr.dataframe_by_name['CountItemType'] != 'total']
                if max_pct_anomaly > anomaly_min:
                    # find and plot worst bar charts from anomaly_dframe
                    for index,row in df.iterrows():
                        if row['anomaly_value_pct'] == max_pct_anomaly:
                            print('\t' + row['ContestName'])
                            print('\tby ' + row['column_field'])
                            print('\t' + row['filter_value'] + ' only')
                            print('\tAnomaly value ' + row['anomaly_algorithm'] + ': ' + str(
                                row['anomaly_value_pct']) + '\n')

                            plot_pivot(contestname, df_to_plot, col_field=row['column_field'], filter=[row['filter_field'],row['filter_value']],mode='pct')
                if max_tot_anomaly > anomaly_min:
                    for index,row in df.iterrows():
                        if row['anomaly_value_raw']  == max_tot_anomaly:
                            print('\t' + row['ContestName'])
                            print('\tby ' + row['column_field'])
                            print('\t' + row['filter_value'] + ' only')
                            print('\tAnomaly value ' + row['anomaly_algorithm'] + ': ' + str(
                                row['anomaly_value_pct']) + '\n')

                            plot_pivot(contestname,df_to_plot,col_field=row['column_field'],filter=[row['filter_field'],row['filter_value']],mode='raw')

            return

    def __init__(self, session,state,short_name):
        assert isinstance(state,sf.State)
        self.short_name=short_name
        context_el = pd.read_sql_table('Election',session.bind,schema='context',index_col='index',parse_dates=['StartDate','EndDate'])
        el = context_el[context_el['ShortName'] == short_name].iloc[0]
        self.name=el['Name']
        self.state=state
        self.ElectionType=el['ElectionType']
        # perhaps election is already in the cdf schema
        try:
            cdf_el = pd.read_sql_table('Election',session.bind,schema='cdf')
            eldf = cdf_el[cdf_el['Name']== self.name]
            assert not eldf.empty, 'Election does not have a record in the cdf schema yet'
        except:
            cdf_etypes = pd.read_sql_table('ElectionType',session.bind,schema='cdf')
            try:
                ty = cdf_etypes[cdf_etypes['Txt']== el['ElectionType']]
                assert not ty.empty
                et_id = ty.iloc[0]['Id']
                et_other = ''
            except:
                ty = cdf_etypes[cdf_etypes['Txt']== 'other']
                et_id = ty.iloc[0]['Id']
                et_other = el['ElectionType']
            el_d = {'Name':self.name,'EndDate':el['EndDate'],'StartDate':el['StartDate'],'ElectionType_Id':et_id,'OtherElectionType':et_other}
            row_as_dframe = pd.DataFrame(pd.Series(el_d)).transpose()
            row_as_dframe.ElectionType_Id = row_as_dframe.ElectionType_Id.astype('int32')
            eldf = dbr.dframe_to_sql(row_as_dframe,session,'cdf','Election',index_col=None)
        self.Election_Id=int(eldf[eldf['Name']==self.name].iloc[0]['Id'])
        self.ElectionType_Id=eldf.iloc[0]['ElectionType_Id']
        self.OtherElectionType=eldf.iloc[0]['OtherElectionType']

        self.pickle_dir=state.path_to_state_dir + 'pickles/' + short_name
        #self.rollup_dframe=None # will be obtained when analysis is done
        #self.anomaly_dframe=None # will be obtained when analysis is done
        # TODO rollups and anomalies depend on atomic and roll_up_to ReportingTypes
        # TODO put roll_up_to_ReportingUnitType def and atomic_ReportingUnitType in appropriate place (where?)

# TODO obsolete, delete
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
                            columns=['ContestId','ContestName','column_field','filter_field','filter_value','anomaly_algorithm',
                                     'anomaly_value_raw','anomaly_value_pct'])
    return e

class ContestRollup:
    """Holds roll-up of one or more contests (from same election)"""
    def pivot(self, col_field='Selection', filter=[],mode='raw'):
        """
        gives a pivot of a contest roll-up
        where rows are filtered by the field-conditions in filter,
        columns are labeled by values of the col_field
        where rows are labeled by an index made up of all remaining fields.
        mode == 'raw' gives raw vote totals; mode == 'pct' give percentages
        """
        return pivot(self.dframe,col_field,filter,mode)

    def euclidean_z_score(self,column_field,filter_field_value_pair_list):
        assert len(self.contest_name_list) == 1, 'ContestRollup does not have exactly one contest'
        z_score_totals = pframe_to_zscore(self.pivot(col_field=column_field, filter=filter_field_value_pair_list))
        z_score_pcts =  pframe_to_zscore(self.pivot(col_field=column_field, filter=filter_field_value_pair_list,mode = 'pct'))
        return z_score_totals, z_score_pcts

    def restrict_by_contest_name(self,contest_name_list):
        # TODO should be more efficient, just filter self.dframe.
        if self.contest_name_list:
            new_list = [x for x in contest_name_list if x in self.contest_name_list]
        else:
            new_list = contest_name_list
        return ContestRollup(self.election,self.roll_up_to_ru_type,self.atomic_ru_type,contest_name_list=new_list)

    def __init__(self,election,roll_up_to_ru_type,atomic_ru_type,contest_name_list = None):
        self.election=election
        self.atomic_ru_type=atomic_ru_type
        self.roll_up_to_ru_type=roll_up_to_ru_type
        self.dframe=self.election.pull_rollup_from_db_by_types(self.roll_up_to_ru_type,atomic_ru_type=atomic_ru_type,contest_name_list=contest_name_list)
        if not contest_name_list:
            contest_name_list = list(self.dframe['Contest'].unique())
        self.contest_name_list=contest_name_list

def plot_pivot(contestname,dataframe_by_name,col_field='Selection',filter=[],mode='raw'):
    title_string = contestname
    if filter:
        title_string += '\n'+filter[1]   # TODO dict is more natural, with filter[0] is column and filter[1] is value

    type_pivot= pivot(dataframe_by_name,col_field, [filter],mode)

    # don't plot the total created by the pivoting
    if 'total' in type_pivot.columns:
        type_pivot.drop(labels=['total'],axis=1)
    try:    # TODO get exact criteria for skipping
        if mode == 'raw':
            type_pivot.plot.bar()
            plt.title(title_string+'\nVotes by '+type_pivot.index.name)
        if mode == 'pct':
            type_pct_pivot = pct_dframe(type_pivot)
            type_pct_pivot.plot.bar()
            plt.title(title_string+'\nVote Percentages by ' + type_pct_pivot.index.name)
        plt.show()
    except:
        print('Plotting failed')


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
        cf=cf.drop(labels=['total'],axis=1)
    return cf

def create_contest_rollup_from_election_rollup(session,meta,e,Contest_Id,contest_type,ContestName):   # TODO get rid of con/meta/schema here by making names part of the Election def?
    """
    contest_type is 'BallotMeasure' or 'Candidate'
    """
    assert isinstance(e,Election),'election must be an instance of the Election class'
    if not isinstance(Contest_Id,int):
        Contest_Id = int(Contest_Id)

    dataframe_by_id = e.rollup_dframe[e.rollup_dframe.Contest_Id == Contest_Id].drop('Contest_Id',axis=1)
    dataframe_by_name = id_values_to_name(session.bind, meta, 'cdf', dataframe_by_id)
    by_ReportingUnitType = e.roll_up_to_ReportingUnitType
    by_ReportingUnitType_Id = e.roll_up_to_ReportingUnitType_Id
    return ContestRollup(dataframe_by_id, dataframe_by_name, 'cdf', e.Election_Id, Contest_Id,
                         by_ReportingUnitType_Id,e.name,  ContestName, by_ReportingUnitType,
                         contest_type, None)

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

def choose_by_id(session,meta,cdf_schema,table,filter=[],default=0):
    """
    Gives the user a list of items and invites user to choose one by entering its Id.
    `table` is the table of items; `filter` is a list of dictionaries,
    each describing a filter to be applied, with keys "FilterTable", "FilterField", "FilterValue" and "ForeignIdField"
    default is the default Id to be chosen if user enters nothing.
    """
    t_dframe = dbr.table_list(session,meta,cdf_schema,table)

    for f in filter:
        assert 'FilterTable' in f.keys() and 'FilterField' in f.keys() and 'FilterValue' in f.keys() and 'ForeignIdField' in f.keys(),'Each filter must have four keys: "FilterTable", "FilterField", "FilterValue" and "ForeignIdField"'
        f_table = pd.read_sql_table(f['FilterTable'],session.bind,schema=cdf_schema)
        t_dframe = t_dframe.merge(f_table,left_on='Id',right_on=f['ForeignIdField'],suffixes=['','_filter'])
        t_dframe = t_dframe[t_dframe[f['FilterField']] == f['FilterValue']]
    if t_dframe.shape[0] == 0:
        raise Exception('No corresponding records in ' + table)

    print('Available ' + table + 's:')
    for index,row in t_dframe.iterrows():
        print(row['Name'] + ' (Id is ' + str(row['Id']) + ')')

    id = input('Enter Id of desired item \n\t(default is ' + str(default) + ')\n') or default
    # TODO add error-checking on user input
    return int(id)

def get_election_id_type_name(session,meta,cdf_schema,default=0):
    election_id = choose_by_id(session,meta,cdf_schema,'Election',filter=[],default=default)

    e_df = pd.read_sql_table('Election',session.bind,schema=cdf_schema)
    e_type_df = pd.read_sql_table('ElectionType',session.bind,schema=cdf_schema)
    e_df = e_df.merge(e_type_df,left_on='ElectionType_Id',right_on='Id',suffixes=['_election','_type'])

    election_type = e_df[e_df['Id_election'] == election_id].iloc[0]['Txt']
    election_name = e_df[e_df['Id_election'] == election_id].iloc[0]['Name']
    return election_id,election_type,election_name

def get_anomaly_scores_OLD(session,meta,cdf_schema,election_id,election_name):
    """
    Creates an election object and finds anomaly scores for each contest in that election
    """
    find_anomalies = input('Find anomalies for '+ election_name+ ' (y/n)?\n')
    if find_anomalies == 'y':
        default = '../local_data/database.ini'
        paramfile = input('Enter path to database parameter file (default is ' + default + ')\n') or default


        election_short_name = 'election_'+str(election_id)

        default = 'precinct'
        atomic_ru_type = input(
            'Enter the \'atomic\' Reporting Unit Type on which you wish to base your rolled-up counts (default is ' + default + ')\n') or default

        default = 'county'
        roll_up_to_ru_type = input(
            'Enter the (larger) Reporting Unit Type whose counts you want to analyze (default is ' + default + ')\n') or default

        default = '../local_data/pickles/' + election_short_name + '/'
        pickle_dir = input(
            'Enter the directory for storing pickled dataframes (default is ' + default + ')\nNB: if this directory doesn\'t exist, create it now before reponding!') or default
        assert os.path.isdir(pickle_dir), 'Non-existent directory: ' + pickle_dir

        e = create_election(session,meta,cdf_schema,election_id,roll_up_to_ru_type,
                               atomic_ru_type,pickle_dir,paramfile)
        e.anomaly_scores(session,meta)

        print('Anomaly scores calculated')
        return e
    else:
        return None

def get_anomaly_scores(session,e,atomic_ru_type=None,roll_up_to_ru_type=None):
    """
    Finds anomaly scores for each contest in the election e,
    rolling up from 'atomic' reporting units to 'roll_up_to' reporting units
    """
    find_anomalies = input('Find anomalies for '+ e.name + ' (y/n)?\n')
    if find_anomalies == 'y':
        default = '../local_data/database.ini'
        paramfile = input('Enter path to database parameter file (default is ' + default + ')\n') or default

        if not atomic_ru_type:
            default = 'precinct'
            atomic_ru_type = input(
                'Enter the \'atomic\' Reporting Unit Type on which you wish to base your rolled-up counts (default is ' + default + ')\n') or default

        if not roll_up_to_ru_type:
            default = 'county'
            roll_up_to_ru_type = input(
                'Enter the (larger) Reporting Unit Type whose counts you want to analyze (default is ' + default + ')\n') or default

        # TODO what's the purpose of this?
        pickle_dir = e.state.path_to_state_dir+'pickles'+ e.short_name + '/'
        pathlib.path.mkdir(pickle_dir,parents=True,exist_ok=True)

        e.anomaly_scores(session,meta)

        print('Anomaly scores calculated')
        return e
    else:
        return None

if __name__ == '__main__':
    con, meta = dbr.sql_alchemy_connect(schema, paramfile='../../local_data/database.ini')


    if con:
        con.dispose()


    print('Done')


