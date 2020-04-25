#!usr/bin/python3
import os.path
from scipy import stats as stats
import scipy.spatial.distance as dist
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import db_routines as dbr
import states_and_files as sf
import user_interface as ui
from pathlib import Path
import csv
import datetime
import analyze_via_pandas as avp



class AnomalyDataFrame(object):
    def worst_bar_for_selected_contests(self,anomaly_min=0,contest_type='Candidate'):
        dont_stop = input('Create worst bar charts for a single contest (y/n)?\n')

        while dont_stop == 'y':
            contest_name = choose_by_name(self.contestrollup.contest_name_list)
            self.worst_bar_for_each_contest(anomaly_min=anomaly_min,contest_name_list=[contest_name])
            dont_stop = input('Create worst bar charts for another contest (y/n)?\n')
        return

    def worst_bar_for_each_contest(self,anomaly_min=0,contest_name_list=None):
        if self.dframe.empty:
            print('anomaly dataframe is empty')
            return
        else:
            if not contest_name_list:
                contest_name_list = list(self.contestrollup.dframe.ContestName.unique())
            for contest_name in contest_name_list:
                cr = self.contestrollup.restrict_by_contest_name([contest_name])
                df = self.dframe[self.dframe.ContestName == contest_name]
                max_pct_anomaly = df.anomaly_value_pct.max()
                max_tot_anomaly = df.anomaly_value_raw.max()

                # don't plot total vote counts
                df_to_plot= cr.dframe[cr.dframe['CountItemType'] != 'total']
                if max_pct_anomaly > anomaly_min:
                    # find and plot worst bar charts from anomaly_dframe
                    for index,row in df.iterrows():
                        if row['anomaly_value_pct'] == max_pct_anomaly:
                            print('\t' + row['ContestName'])
                            print('\tby ' + row['column_field'])
                            print('\t' + row['filter_value'] + ' only')
                            print('\tAnomaly value ' + row['anomaly_algorithm'] + ': ' + str(
                                row['anomaly_value_pct']) + '\n')

                            plot_pivot(contest_name, df_to_plot, col_field=row['column_field'], filter=[row['filter_field'],row['filter_value']],mode='pct')
                if max_tot_anomaly > anomaly_min:
                    for index,row in df.iterrows():
                        if row['anomaly_value_raw']  == max_tot_anomaly:
                            print('\t' + row['ContestName'])
                            print('\tby ' + row['column_field'])
                            print('\t' + row['filter_value'] + ' only')
                            print('\tAnomaly value ' + row['anomaly_algorithm'] + ': ' + str(
                                row['anomaly_value_pct']) + '\n')

                            plot_pivot(contest_name,df_to_plot,col_field=row['column_field'],filter=[row['filter_field'],row['filter_value']],mode='raw')
            return

    def most_anomalous(self,n=3,mode='pct'):
        """ returns a list of the n most anomalous contests
        mode is 'pct' or 'raw'
        """
        assert mode == 'pct' or mode == 'raw','\'pct\' and \'raw\' are the only recognized modes'
        adf = self.dframe
        col = 'anomaly_value_' + mode

        most_anomalous = adf.nlargest(n,col,keep='all')
        return most_anomalous

    def draw_most_anomalous(self,n=3,mode='pct'):
        """ plot the 3 (or n) most anomalous bar charts for the election"""
        print('Most anomalous contests by votes '+mode+':')
        for index,row in self.most_anomalous(n,mode).iterrows():
            print('\t' + row['ContestName'])
            print('\tby ' + row['column_field'])
            print('\t' + row['filter_value'] + ' only')
            print('\tAnomaly value ' + row['anomaly_algorithm'] + ': ' + str(row['anomaly_value_pct']) + '\n')
            single_contest = self.contestrollup.restrict_by_contest_name(row['ContestName'])                # TODO ineffficient; recalculates rollup

            plot_pivot(row['ContestName'],single_contest.dframe,col_field=row['column_field'],
                          filter=[row['filter_field'],row['filter_value']],
                          mode=mode)

    def __init__(self,rollup):
        assert isinstance(rollup,ContestRollup),'One argument must be an instance of the Election class'
        self.contestrollup=rollup  # the contest rollup we're analyzing

        aframe_columnlist=['ContestName','column_field','filter_field','filter_value','anomaly_algorithm',
                                     'anomaly_value_raw','raw_look_at','anomaly_value_pct','pct_look_at']
        self.dframe=pd.DataFrame(data=None,index=None,
                            columns=aframe_columnlist)
        for contest_name in rollup.contest_name_list:
            c = rollup.restrict_by_contest_name([contest_name])
            try:
                a_list = anomaly_list(contest_name,c,aframe_columnlist=aframe_columnlist)
            except:
                a_list=[]
            if a_list:
                self.dframe = self.dframe.append(a_list) # less efficient to update anomaly_dframe contest-by-contest
            else:
                print('No anomalies found for contest ' + contest_name)


class Election(object):
    def pull_rollup_from_db_by_types(
            self, roll_up_to_ru_type, atomic_ru_type='precinct',contest_name_list=None,db_paramfile=None):

        eng, meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=self.jurisdiction.short_name)

        # Get necessary tables from cdf schema
        cdf_d={}
        for element in ['ReportingUnitType','CountItemType','ReportingUnit']:
            cdf_d[element]=pd.read_sql_table(element,eng,index_col=None)

        contest_type,contest_name,selection_name,contest_district_type = avp.contest_info_by_id(eng)

        roll_up_to_ru_type_id = int(
            cdf_d['ReportingUnitType'][cdf_d['ReportingUnitType']['Txt']==roll_up_to_ru_type].iloc[0]['Id'])
        atomic_ru_type_id = int(
            cdf_d['ReportingUnitType'][cdf_d['ReportingUnitType']['Txt']==atomic_ru_type].iloc[0]['Id'])

        q = """SELECT
            secvcj."Contest_Id", cruj."ParentReportingUnit_Id" AS "ReportingUnit_Id",  secvcj."Selection_Id", vc."CountItemType_Id", COALESCE(sum(vc."Count"),0) AS "Count"
         FROM
             "SelectionElectionContestVoteCountJoin" secvcj 
             LEFT JOIN "VoteCount" vc ON secvcj."VoteCount_Id" = vc."Id"
             LEFT JOIN "ComposingReportingUnitJoin" cruj ON vc."ReportingUnit_Id" = cruj."ChildReportingUnit_Id"
             LEFT JOIN "ReportingUnit" ru_c ON ru_c."Id" = cruj."ChildReportingUnit_Id"
             LEFT JOIN "ReportingUnit" ru_p ON ru_p."Id" = cruj."ParentReportingUnit_Id"

         WHERE
             secvcj."Election_Id" = %(Election_Id)s
             AND ru_c."ReportingUnitType_Id" = %(roll_up_fromReportingUnitType_Id)s
             AND ru_p."ReportingUnitType_Id" = %(roll_up_toReportingUnitType_Id)s
         GROUP BY secvcj."Contest_Id", cruj."ParentReportingUnit_Id",  secvcj."Selection_Id", vc."CountItemType_Id"
         """
        params = {'Election_Id': self.Election_Id,
                  'roll_up_toReportingUnitType_Id': roll_up_to_ru_type_id,
                  'roll_up_fromReportingUnitType_Id': atomic_ru_type_id}
        rollup_dframe = pd.read_sql_query(sql=q, con=eng, params=params)
        # add columns to rollup dframe
        rollup_dframe['contest_type'] = rollup_dframe['Contest_Id'].map(contest_type)
        rollup_dframe['Contest'] = rollup_dframe['Contest_Id'].map(contest_name)
        rollup_dframe['Selection'] = rollup_dframe['Selection_Id'].map(selection_name)
        for element in ['CountItemType','ReportingUnit']:
            rollup_dframe = rollup_dframe.merge(cdf_d[element],left_on=element+'_Id',right_on='Id')
        rollup_dframe=rollup_dframe[['Contest_Id','ReportingUnit_Id','Selection_Id','CountItemType_Id','Contest',
                                     'Name','Selection','Txt','Count','contest_type']]
        rollup_dframe.rename(columns={'Txt':'CountItemType','Name':'ReportingUnit'},inplace=True)
        if contest_name_list:
            rollup_dframe=rollup_dframe[rollup_dframe['Contest'].isin(contest_name_list)]
        if eng:
            eng.dispose()
        return rollup_dframe

    def summarize_results(
            self,atomic_ru_type='precinct',summary_ru_type='state',mode='top',skip_total_column=True,
            db_paramfile=None,count_item_status='unknown'):
        if not db_paramfile:
            db_paramfile = ui.pick_paramfile(ui.get_project_root())

        rollup=self.pull_rollup_from_db_by_types(
            summary_ru_type,atomic_ru_type=atomic_ru_type,db_paramfile=db_paramfile)
        rollup=rollup.drop(
            ['Contest_Id','ReportingUnit_Id','Selection_Id','CountItemType_Id','contest_type'],axis=1)
        if mode=='top':
            if skip_total_column:
                rollup=rollup[rollup['CountItemType']!='total']
            output = rollup.groupby(['Contest','Selection']).sum()
            count_item_type = 'total'
            # TODO bug FL crashes here
            output.loc[:,'CountItemType'] = count_item_type
        elif mode=='by_vote_type':
            output = rollup.groupby(['Contest','Selection','CountItemType']).sum()
            count_item_type = 'multiple'
        else:
            raise Exception(f'Mode {mode} not recognized')

        count_item = f'TYPE{count_item_type}_STATUS{count_item_status}'
        inventory_columns = [
            'Election','ReportingUnitType','CountItemType','CountItemStatus',
            'source_db_url','timestamp']
        inventory_values = [
            self.short_name,summary_ru_type,count_item_type,count_item_status,
            str(self.source_url),datetime.date.today()
        ]   # TODO source_db_url

        export_to_inventory_file_tree(
            os.path.join(self.jurisdiction.path_to_juris_dir,'results_from_cdf_db'),
            f'{self.short_name}/{summary_ru_type}',
            f'{count_item}.txt',
            inventory_columns,
            inventory_values,output)

        return output

    def __init__(self,session,jurisdiction,project_root):
        assert isinstance(jurisdiction,sf.Jurisdiction)
        cdf_el = pd.read_sql_table('Election',session.bind,index_col='Id')
        election_idx,electiontype = ui.get_or_create_election_in_db(session,project_root)

        self.short_name = cdf_el.loc[election_idx,'Name']
        self.jurisdiction=jurisdiction    # TODO feature: generalize to other ReportingUnits?
        self.ElectionType = electiontype
        self.Election_Id = election_idx
        self.ElectionType_Id = cdf_el.loc[election_idx,'ElectionType_Id']
        self.OtherElectionType = cdf_el.loc[election_idx,'OtherElectionType']
        self.source_url=session.bind.url


class ContestRollup:
    """Holds roll-up of one or more contests (from same election)"""

    def pivot(self, col_field='Selection', filter=[],mode='raw',drop_column=[]):
        """
        gives a pivot of a contest roll-up
        where rows are filtered by the field-conditions in filter,
        columns are labeled by values of the col_field
        where rows are labeled by an index made up of all remaining fields.
        mode == 'raw' gives raw vote totals; mode == 'pct' give percentages
        Any columns in the drop_column list are dropped before pivoting
        """
        pivot_frame = pivot(self.dframe,col_field,filter,mode)

        for c in drop_column:
            if c in pivot_frame.columns:
                pivot_frame = pivot_frame.drop(c,axis=1)

        return pivot_frame

    def euclidean_z_score(self,column_field,filter_field_value_pair_list,mode_list=['raw','pct']):
        """Returns two vectors of z-scores"""
        assert len(self.contest_name_list) == 1, 'ContestRollup does not have exactly one contest'
        z_score = {}
        look_at = {}
        max_z = {}
        for mode in mode_list:

            z_score[mode],max_z[mode],look_at[mode] =  numframe_to_zscore(self.pivot(col_field=column_field,filter=filter_field_value_pair_list,mode =mode,drop_column=['total']))
        return max_z,look_at

    def restrict_by_contest_name(self,contest_name_list):
        # TODO should be more efficient, just filter self.dframe.
        if self.contest_name_list:
            new_list = [x for x in contest_name_list if x in self.contest_name_list]
        else:
            new_list = contest_name_list
        return ContestRollup(self.election,self.roll_up_to_ru_type,self.atomic_ru_type,contest_name_list=new_list)

    def restrict_by_contest_name_NEW(self,contest_name_list):
        # TODO test: does this change the original adf?
        new_adf = self
        if self.contest_name_list:
            new_adf.dframe = self.dframe[self.dframe['ContestName'].isin(contest_name_list)]
        return new_adf

    def __init__(self,election,roll_up_to_ru_type,atomic_ru_type,contest_name_list = None):
        self.election=election
        self.atomic_ru_type=atomic_ru_type
        self.roll_up_to_ru_type=roll_up_to_ru_type
        self.dframe=self.election.pull_rollup_from_db_by_types(
            self.roll_up_to_ru_type,atomic_ru_type=atomic_ru_type,contest_name_list=contest_name_list)
        if not contest_name_list:
            contest_name_list = list(self.dframe['Contest'].unique())
        self.contest_name_list=contest_name_list
        self.contest_name_list.sort()


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
    Any columns in the drop_column list are dropped before pivoting
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
        if 'total' not in cf.columns:
            cf['total'] = cf.sum(axis=1)
        cf = cf[cf.total != 0]
        cf = cf.div(cf["total"], axis=0)
        cf=cf.drop(labels=['total'],axis=1)
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


def diff_anomaly_score(
        left_dframe, right_dframe, left_value_column ='sum', right_value_column='sum',
        on ='ReportingUnit', anomaly_score_min=2):
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
        if anomaly_score > anomaly_score_min:
            print('Anomaly found')
            # ('Anomaly found comparing:\n\cdf_table'+left_dframe+','+left_value_column+'\n\cdf_table'+right_dframe+','+right_value_column)
        return anomaly_score


def numframe_to_zscore(pframe,tolerance = 0.05):
    """ for a dataframe whose values are all numerical, calculate list of z-scores for the rows of the dataframe """
    if pframe.empty:
        print('Empty dataframe assigned score list of [0]')
        return [0]
    else:
        frame=pframe.copy()
        frame['vector'] = frame.apply(lambda x: x.tolist(),axis=1)
        frame['ezs']=euclidean_zscore(frame['vector'].tolist())
        max_ezs=frame['ezs'].max()
        max_frame= frame[frame['ezs'] > max_ezs-abs(max_ezs)*tolerance] # empty if max_ezs==0
        look_at_list = list(max_frame.index.values)
        score_list = list(frame['ezs'])
        # TODO assert that all rows are numerical vectors, with all other info in the index
        return score_list,max_ezs, look_at_list


def choose_by_name(name_list):
    """
    Gives the user a list of items and invites user to choose one by entering its Id.
    `table` is the table of items; `filter` is a list of dictionaries,
    each describing a filter to be applied, with keys "FilterTable", "FilterField", "FilterValue" and "ForeignIdField"
    default is the default Id to be chosen if user enters nothing.
    """

    print('Available items:')
    name_list.sort()
    for name in name_list:
        print(f'{name_list.index(name)}\t{name}')
    default=0
    id = input(f'Enter Id of desired item \n\t(default is {default})\n') or default
    # TODO add error-checking on user input
    return name_list[int(id)]


def anomaly_list(contest_name, c, aframe_columnlist=None):
    if aframe_columnlist is None:
        aframe_columnlist = ['ContestName', 'column_field', 'filter_field', 'filter_value', 'anomaly_algorithm',
                             'anomaly_value_raw', 'raw_look_at', 'anomaly_value_pct', 'pct_look_at']
    anomaly_list = []
    print(f'Calculating anomalies for {contest_name}')

    for column_field in ['ReportingUnit','CountItemType','Selection']:
        temp_list = ['ReportingUnit','CountItemType','Selection']
        temp_list.remove(column_field)
        for filter_field in temp_list:
            for filter_value in c.dframe[filter_field].unique():
                max_z,look_at = c.euclidean_z_score(column_field,[[filter_field,filter_value]],
                                                    mode_list=['raw','pct'])
                anomaly_list.append(
                    pd.Series([contest_name,column_field,filter_field,filter_value,'euclidean z-score',
                               max_z['raw'],look_at['raw'],max_z['pct'],look_at['pct']],index=aframe_columnlist))

    return anomaly_list


def export_to_inventory_file_tree(target_dir,target_sub_dir,target_file,inventory_columns,inventory_values,df):
    # TODO standard order for columns
    # export to file system
    out_path = os.path.join(
        target_dir,target_sub_dir)
    Path(out_path).mkdir(parents=True,exist_ok=True)

    while os.path.isfile(os.path.join(out_path,target_file)):
        target_file = (f'There is already a file called {target_file}. Pick another name.\n')

    out_file = os.path.join(out_path,target_file)
    df.to_csv(out_file,sep='\t')

    # create record in inventory.txt
    inventory_file = os.path.join(target_dir,'inventory.txt')
    inv_exists = os.path.isfile(inventory_file)
    if inv_exists:
        # check that header matches inventory_columns
        with open(inventory_file,newline='') as f:
            reader = csv.reader(f,delimiter='\t')
            file_header = next(reader)
            # TODO: offer option to delete inventory file
            assert file_header == inventory_columns, \
                f'Header of file {f} is\n{file_header},\ndoesn\'t match\n{inventory_columns}.'

    with open(inventory_file,'a',newline='') as csv_file:
        wr = csv.writer(csv_file,delimiter='\t')
        if not inv_exists:
            wr.writerow(inventory_columns)
        wr.writerow(inventory_values)

    print(f'Results exported to {out_file}')

    return


if __name__ == '__main__':
    print('Use routine in src/test directory (e.g., pull_top_counts.py')
