import csv
import os.path

import pandas as pd
from election_anomaly import user_interface as ui
from election_anomaly import munge_routines as mr
import datetime
import os
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from pandas.api.types import is_numeric_dtype
from election_anomaly import db_routines as dbr
import scipy.spatial.distance as dist
from scipy import stats
import math


def child_rus_by_id(session,parents,ru_type=None):
	"""Given a list <parents> of parent ids (or just a single parent_id), return
	list containing all children of those parents.
	(By convention, a ReportingUnit counts as one of its own 'parents',)
	If (ReportingUnitType_Id,OtherReportingUnit) pair <rutype> is given,
	restrict children to that ReportingUnitType"""
	cruj = pd.read_sql_table('ComposingReportingUnitJoin',session.bind)
	children = list(cruj[cruj.ParentReportingUnit_Id.isin(parents)].ChildReportingUnit_Id.unique())
	if ru_type:
		assert len(ru_type) == 2,f'argument {ru_type} does not have exactly 2 elements'
		ru = pd.read_sql_table('ReportingUnit',session.bind,index_col='Id')
		right_type_ru = ru[(ru.ReportingUnitType_Id == ru_type[0]) & (ru.OtherReportingUnitType == ru_type[1])]
		children = [x for x in children if x in right_type_ru.index]
	return children


def create_rollup(
		cursor, target_dir: str, top_ru_id: int, sub_rutype_id: int,
		election_id: int, datafile_list=None, by='Id') -> str:
	"""<target_dir> is the directory where the resulting rollup will be stored.
	<election_id> identifies the election; <datafile_id_list> the datafile whose results will be rolled up.
	<top_ru_id> is the internal cdf name of the ReportingUnit whose results will be reported
	<sub_rutype_id> identifies the ReportingUnitType
	of the ReportingUnits used in each line of the results file
	created by the routine. (E.g., county or ward)
	<datafile_list> is a list of files, with entries from field <by> in _datafile table.
	If no <datafile_list> is given, return all results for the given election.
	"""

	if not datafile_list:
		datafile_list, e = dbr.data_file_list(cursor, [election_id], by='Id')
		if e:
			return e
		by = 'Id'
		if len(datafile_list) == 0:
			return f'No datafiles found for Election_Id {election_id}'

	# set exclude_total
	vote_type_list, err_str = dbr.vote_type_list(cursor, datafile_list, by=by)
	if err_str:
		return err_str
	elif len(vote_type_list) == 0:
		return f'No vote types found for datafiles with {by} in {datafile_list} '

	if len(vote_type_list) > 1 and 'total' in vote_type_list:
		exclude_total = True
	else:
		exclude_total = False

	# get names from ids
	top_ru = dbr.name_from_id(cursor,'ReportingUnit',top_ru_id).replace(" ","-")
	election = dbr.name_from_id(cursor,'Election',election_id).replace(" ","-")
	sub_rutype = dbr.name_from_id(cursor, 'ReportingUnitType', sub_rutype_id)

	# create path to export directory
	leaf_dir = os.path.join(target_dir, election, top_ru, f'by_{sub_rutype}')
	Path(leaf_dir).mkdir(parents=True, exist_ok=True)

	# prepare inventory
	inventory_file = os.path.join(target_dir,'inventory.txt')
	inv_exists = os.path.isfile(inventory_file)
	if inv_exists:
		inv_df = pd.read_csv(inventory_file,sep='\t')
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


def create_scatter(session, jurisdiction_id, subdivision_type_id, 
            h_election_id, h_category, h_count_id, h_type,
			v_election_id, v_category, v_count_id, v_type):
	# Get name of db for error messages
	dfh = get_data_for_scatter(session, jurisdiction_id, subdivision_type_id, h_election_id, \
		h_category, h_count_id, h_type)
	dfv = get_data_for_scatter(session, jurisdiction_id, subdivision_type_id, v_election_id, \
		v_category, v_count_id, v_type)
	unsummed = pd.concat([dfh, dfv])
	# package into dictionary
	if h_count_id == -1:
		x = f'All {h_type}'
	elif h_type == 'candidates':
		x = dbr.name_from_id(session, 'Candidate', h_count_id) 
	elif h_type == 'contests':
		x = dbr.name_from_id(session, 'CandidateContest', h_count_id) 
	if v_count_id == -1:
		y = f'All {v_type}'
	elif v_type == 'candidates':
		y = dbr.name_from_id(session, 'Candidate', v_count_id) 
	elif v_type == 'contests':
		y = dbr.name_from_id(session, 'CandidateContest', v_count_id) 
	jurisdiction = dbr.name_from_id(session, 'ReportingUnit', jurisdiction_id)
	pivot_df = pd.pivot_table(unsummed, values='Count',
		index=['Name'], columns='Selection').reset_index()
	
	# package up results
	results = package_results(pivot_df, jurisdiction, x, y)
	results["x-election"] = dbr.name_from_id(session, 'Election', h_election_id)
	results["y-election"] = dbr.name_from_id(session, 'Election', v_election_id)
	results["subdivision_type"] = dbr.name_from_id(session, 'ReportingUnitType', subdivision_type_id)
	results["x-count_item_type"] = h_category
	results["y-count_item_type"] = v_category

	# only keep the ones where there are an (x, y) to graph
	to_keep = []
	for result in results['counts']:
		if len(result) == 3: #need reporting unit, x, and y
			to_keep.append(result)
	results['counts'] = to_keep
	return results


def package_results(data, jurisdiction, x, y, restrict=None):
	results = {
		"jurisdiction": jurisdiction,
		"x": x,
		"y": y,
		"counts": []		
	}
	for i, row in data.iterrows():
		results['counts'].append({
			'name': row['Name'],
			'x': row[x],
			'y': row[y],
		})
		if restrict and i == (restrict - 1):
			break
	return results


def get_data_for_scatter(session, jurisdiction_id, subdivision_type_id, 
	election_id, count_item_type, filter_id, count_type):
	"""Since this could be data across 2 elections, grab data one election at a time"""
	df = pull_data_tables(session)
	ru, sub_ru, ru_children = create_hierarchies(session, df, jurisdiction_id, subdivision_type_id)
	candidate_columns = ['Contest_Id','Contest','Selection_Id','Selection','ElectionDistrict_Id',
		'Candidate_Id']
	contest_selection = create_contests(df, ru, candidate_columns)
	#  limit to relevant data
	# Based on count_type param, we either filter on contest or candidate
	if count_type == 'candidates':
		filter_column = 'Candidate_Id'
	elif count_type == 'contests':
		filter_column = 'Contest_Id'
	if filter_id != -1:
		csj = contest_selection[contest_selection[filter_column].isin([filter_id])]
	else:
		csj = contest_selection

	# find ReportingUnits of the correct type that are subunits of top_ru
	sub_ru_ids = child_rus_by_id(session,[jurisdiction_id],ru_type=[subdivision_type_id, ''])
	if not sub_ru_ids:
		# TODO better error handling (while not sub_ru_list....)
		raise Exception(f'Database {db} shows no ReportingUnits of type {sub_rutype} nested inside {top_ru}')
	sub_ru = df['ReportingUnit'].loc[sub_ru_ids]

	# find all subReportingUnits of top_ru
	all_subs_ids = child_rus_by_id(session,[jurisdiction_id])

	# find all children of subReportingUnits
	children_of_subs_ids = child_rus_by_id(session,sub_ru_ids)
	ru_children = df['ReportingUnit'].loc[children_of_subs_ids]

def short_name(text,sep=';'):
	return text.split(sep)[-1]
	# check for any reporting units that should be included in roll-up but were missed
	# TODO list can be long and irrelevant. Instead list ReportingUnitTypes of the missing
	# missing = [str(x) for x in all_subs_ids if x not in children_of_subs_ids]
	# if missing:
	# TODO report these out to the export directory
	#	ui.report_problems(missing,msg=f'The following reporting units are nested in {top_ru["Name"]} '
	#							f'but are not nested in any {sub_rutype} nested in {top_ru["Name"]}')

	# limit to relevant vote counts
	ecsvcj = df['ElectionContestSelectionVoteCountJoin'][
		(df['ElectionContestSelectionVoteCountJoin'].ElectionContestJoin_Id.isin(ecj.index)) &
		(df['ElectionContestSelectionVoteCountJoin'].ContestSelectionJoin_Id.isin(csj.index))]

	# calculate specified dataframe with columns [ReportingUnit,Contest,Selection,VoteCount,CountItemType]
	#  1. create unsummed dataframe of results
	unsummed = ecsvcj.merge(
		df['VoteCount'],left_on='VoteCount_Id',right_index=True).merge(
		df['ComposingReportingUnitJoin'],left_on='ReportingUnit_Id',right_on='ChildReportingUnit_Id').merge(
		ru_children,left_on='ChildReportingUnit_Id',right_index=True).merge(
		sub_ru,left_on='ParentReportingUnit_Id',right_index=True,suffixes=['','_Parent'])
	unsummed.rename(columns={'Name_Parent':'ReportingUnit'},inplace=True)
	# add columns with names
	unsummed = mr.enum_col_from_id_othertext(unsummed,'CountItemType',df['CountItemType'])
	unsummed = unsummed.merge(contest_selection,how='left',left_on='ContestSelectionJoin_Id',right_index=True)

	# Now process data	
	# filter based on vote count type
	unsummed = unsummed[unsummed['CountItemType'] == count_item_type]

	# cleanup for purposes of flexibility
	unsummed = unsummed[['Name', 'Count', 'Selection', 'Contest_Id', 'Candidate_Id']]

	# if filter_id is -1, then that means we have all contests or candidates
	# so we need to group by
	if filter_id == -1:
		unsummed['Selection'] = f'All {count_type}'
		unsummed['Contest_Id'] = filter_id
		unsummed['Candidate_Id'] = filter_id

	if count_type == 'contests' and filter_id != -1:
		selection = dbr.name_from_id(session, 'CandidateContest', filter_id)
		unsummed['Selection'] = selection
	elif count_type == 'contests' and filter_id == -1:
		unsummed['Selection'] = 'All contests'

	columns = list(unsummed.drop(columns='Count').columns)
	unsummed = unsummed.groupby(columns)['Count'].sum().reset_index()

	return unsummed


def create_bar(session, top_ru_id, contest_type, contest, election_id, datafile_id_list):
	top_ranked = get_most_anomalous(session, election_id, 3)

	# package into list of dictionary
	result_list = []
	ids = top_ranked['unit_id'].unique()
	for id in ids:
		temp_df = top_ranked[top_ranked['unit_id'] == id]

		# some cleaning here to make the pivoting work
		scores_df = temp_df[temp_df['rank'] != 1]
		scores_df = scores_df[['ReportingUnit_Id', 'score', 'margins']]
		scores_df.rename(columns={'score': 'max_score', 'margins': 'max_margins'}, inplace=True)
		temp_df = temp_df.merge(scores_df, how='inner', on='ReportingUnit_Id')
		temp_df.drop(columns=['score', 'margins'], inplace=True)
		temp_df.rename(columns={'max_score': 'score', 'max_margins': 'margins'}, inplace=True)

		candidates = temp_df['Candidate_Id'].unique()
		x = dbr.name_from_id(session, 'Candidate', candidates[0])
		y = dbr.name_from_id(session, 'Candidate', candidates[1]) 
		results = {
			"election": dbr.name_from_id(session, 'Election', election_id),
			"jurisdiction": dbr.name_from_id(session, 'ReportingUnit', top_ru_id),
			"contest": dbr.name_from_id(session, 'CandidateContest', temp_df.iloc[0]['Contest_Id']),
			# TODO: remove hard coded subdivision type
			"subdivision_type": dbr.name_from_id(session, 'ReportingUnitType', 
				temp_df.iloc[0]['ReportingUnitType_Id']),
			"count_item_type": temp_df.iloc[0]['CountItemType'],
			"x": x,
			"y": y,
			"margin": temp_df.iloc[0]['margins'],
			"votes_at_stake": temp_df.iloc[0]['margin_ratio'], 
			"counts": []
		}
		pivot_df = pd.pivot_table(temp_df, values='Count', 
			index=['ReportingUnit_Id', 'Name', 'score', 'margins'], \
			columns='Selection', aggfunc=np.mean).sort_values('score', ascending=False).reset_index()
		for i, row in pivot_df.iterrows():
			results['counts'].append({
				'name': row['Name'],
				'x': row[x],
				'y': row[y],
				'score': row['score'],
				'margin': row['margins']
			})
			if i == 7:
				break
		result_list.append(results)
		
	return result_list


def assign_anomaly_score(data):
	"""adds a new column called score between 0 and 1; 1 is more anomalous.
	Also adds a `unit_id` column which assigns a score to each unit of analysis
	that is considered. For example, we may decide to look at anomalies across each
	distinct combination of contest, reporting unit type, and vote type. Each 
	combination of those would get assigned an ID. This means rows may get added
	to the dataframe if needed."""

	######### FOR TESTING PURPOSES ONLY!!!!! ###########
	#data = data[data['Contest_Id'] == 14949] # only 2 candidates
	#data = data[data['Contest_Id'] == 14777] # 3 candidates
	data = data[(data['Contest_Id'] == 14949) | (data['Contest_Id'] == 14777)]
	#data = data[data['Contest_Id'] == 14917] # this has infinite margins

	# Assign a ranking for each candidate by votes for each contest
	total_data = data[(data['CountItemType'] == 'total') & 
					(data['ReportingUnit_Id'] == data['ParentReportingUnit_Id'])]
	ranked_df = total_data.groupby(['Contest_Id', 'Selection', 'Selection_Id'], as_index=False)['Count'] \
				.sum().sort_values(['Contest_Id', 'Count'], ascending=False)
	ranked_df['rank'] = ranked_df.groupby('Contest_Id')['Count'].rank('dense', ascending=False)
	ranked_df['contest_total'] = ranked_df['Count'].sum()
	ranked_df.rename(columns={'Count':'ind_total'}, inplace=True)

	# Group data by parent info. This works because each child is also its own
	# parent in the DB table
	grouped_df = data.groupby(['ParentReportingUnit_Id', 'ParentName', 
			'ParentReportingUnitType_Id', 'Candidate_Id', 'CountItemType_Id',
            'CountItemType', 'Contest_Id', 'Contest', 'Selection', 
            'contest_type', 'contest_district_type'], as_index=False)['Count'].sum().reset_index()
	grouped_df.drop(columns='index', inplace=True)
	grouped_df.rename(columns={
		'ParentReportingUnit_Id': 'ReportingUnit_Id',
		'ParentName': 'Name',	
		'ParentReportingUnitType_Id': 'ReportingUnitType_Id'
	}, inplace=True)
	grouped_df = grouped_df.merge(ranked_df, how='inner', on=['Contest_Id', 'Selection'])

	# assign unit_ids to unique combination of contest, ru_type, and count type
	df_unit = grouped_df[['Contest_Id', 'ReportingUnitType_Id', 'CountItemType']].drop_duplicates()
	df_unit = df_unit.reset_index()
	df_unit['unit_id'] = df_unit.index
	df_with_units = grouped_df.merge(df_unit, how='left', on=['Contest_Id', 'ReportingUnitType_Id', 'CountItemType'])

	# loop through each unit ID and assign anomaly scores
	unit_ids = df_with_units['unit_id'].unique()
	df = pd.DataFrame()
	# for each unit ID
	for unit_id in unit_ids:
		# grab all the data there
		temp_df = df_with_units[df_with_units['unit_id'] == unit_id]
		total = temp_df.groupby('ReportingUnit_Id')['Count'].sum().reset_index()
		total.rename(columns={'Count': 'reporting_unit_total'}, inplace=True)
		temp_df = temp_df.merge(total, how='inner', on='ReportingUnit_Id')
		# pivot so each candidate gets own column
		scored_df = pd.DataFrame()
		for i in range(2, int(temp_df['rank'].max()) + 1):
			selection_df = temp_df[temp_df['rank'].isin([1, i])]
			if selection_df.shape[0] >= 5:
				pivot_df = pd.pivot_table(selection_df, values='Count', \
					index=['ReportingUnit_Id', 'reporting_unit_total'], \
					columns='Selection').sort_values('ReportingUnit_Id').reset_index()
				pivot_df_values = pivot_df.drop(columns=['ReportingUnit_Id', 'reporting_unit_total'])
				to_drop = [selection_df[selection_df['rank'] == 1]['Selection'].unique()[0], \
					selection_df[selection_df['rank'] == i]['Selection'].unique()[0]]
				# pass in proportions instead of raw vlaues
				vote_proportions = pivot_df_values.div(pivot_df['reporting_unit_total'], axis=0)
				np.nan_to_num(vote_proportions, copy=False)
				# assign z score and then add back into final DF
				scored = euclidean_zscore(vote_proportions.to_numpy())
				#scored = density_score(vote_proportions)
				pivot_df['score'] = scored
				pivot_df = pivot_df[['ReportingUnit_Id', to_drop[1], 'score']]
				pivot_df['Selection'] = to_drop[1]
				pivot_df.rename(columns={to_drop[1]: 'Count'}, inplace=True)
				selection_df = selection_df.merge(pivot_df, how='left', \
					on=['ReportingUnit_Id', 'Selection', 'Count'])
				scored_df = pd.concat([scored_df, selection_df])
		df = pd.concat([df, scored_df])
	df['score'] = df['score'].fillna(0)
	return df


def get_most_anomalous(session, election_id, n):
	"""gets the n contests with the highest margin ratio score"""
	# TODO maybe all this work should go in one function
	df = dbr.load_anomaly_scores(session, election_id, n)
	counts_df = pd.read_sql_table('CountItemType', session.bind, index_col='Id')
	selection_df = pd.read_sql_table('CandidateSelection', session.bind, \
		index_col='Id')
	candidate_df = pd.read_sql_table('Candidate', session.bind, \
		index_col='Id')
	reportingunit_df = pd.read_sql_table('ReportingUnit', session.bind, \
		index_col='Id')
	data = df.merge(counts_df, how='inner', left_on='CountItemType_Id', 
		right_index=True)
	data = data.merge(selection_df, how='inner', left_on='Selection_Id', \
		right_index=True)
	data = data.merge(candidate_df, how='inner', left_on='Candidate_Id', \
		right_index=True)
	data = data.merge(reportingunit_df, how='inner', left_on='ReportingUnit_Id', \
		right_index=True, suffixes=[None, '_y'])
	data.rename(columns={"Txt": "CountItemType", "BallotName": "Selection"}, inplace=True)

	zeros_df = data[['Contest_Id', 'ReportingUnitType_Id', 'CountItemType', 'ReportingUnit_Id', 'Count']]
	zeros_df = zeros_df.groupby(['Contest_Id', 'ReportingUnitType_Id', 'ReportingUnit_Id', 'CountItemType']).sum()
	zeros_df = zeros_df.reset_index()
	no_zeros = zeros_df[zeros_df['Count'] != 0]
	data = data.merge(no_zeros, how='inner', on=['Contest_Id', 'ReportingUnitType_Id', 'ReportingUnit_Id', 'CountItemType'])
	data.rename(columns={'Count_x':'Count'}, inplace=True)
	data.drop(columns=['Count_y'], inplace=True)

	margins = list(data['margin_ratio'].unique())
	margins.sort(reverse=True)
	top_margins = margins[0:n]
	data = data[data['margin_ratio'].isin(top_margins)]

	# now we get the top 8 reporting unit IDs, in terms of anomaly score, of the winner and most anomalous
	ids = data['unit_id'].unique()
	df = pd.DataFrame()
	for id in ids:
		temp_df = data[data['unit_id'] == id]
		max_score = temp_df['score'].max()
		rank = temp_df[temp_df['score'] == max_score].iloc[0]['rank']
		temp_df = temp_df[temp_df['rank'].isin([1, rank])]
		scores = list(temp_df['score'].unique())
		scores.sort(reverse=True)
		top_scores = scores[0:8]
		reporting_units = temp_df[temp_df['score'].isin(top_scores)]['ReportingUnit_Id'].unique()
		df_final = temp_df[temp_df['ReportingUnit_Id'].isin(reporting_units)]
		df = pd.concat([df, df_final])
	return df


def euclidean_zscore(li):
    """Take a list of vectors -- all in the same R^k,
    returns a list of the z-scores of the vectors -- each relative to the ensemble"""
    distance_list = [sum([dist.euclidean(item,y) for y in li]) for item in li]
    if len(set(distance_list)) == 1:
        # if all distances are the same, which yields z-score nan values
        return [0]*len(li)
    else:
        return list(stats.zscore(distance_list))


def density_score(points):
	"""Take a list of vectors -- all in the same R^k,
	return a list of comparison of density with or without the anomaly"""
	density_list = [0] * len(points)
	x_order = list(points[:, 0])
	xs = points[:, 0]
	xs.sort()
	head, *tail = xs
	density = (tail[-2] - tail[0]) / (len(tail) - 1)
	total_density = (xs[-2] - xs[0]) / (len(xs) - 1)
	density_asc = total_density / density
	density_asc_xval = xs[0]

	# Sort in reverse order
	xs = xs[::-1]
	head, *tail = xs
	density = (tail[-2] - tail[0]) / (len(tail) - 1)
	total_density = (xs[-2] - xs[0]) / (len(xs) - 1)
	density_desc = total_density / density
	density_desc_xval = xs[0]
	if density_asc > density_desc:
		i = x_order.index(density_asc_xval)
		density_list[i] = density_asc
	else:
		i = x_order.index(density_desc_xval)
		density_list[i] = density_desc
	return density_list


def calculate_margins(data):
	"""Takes a dataframe with an anomaly score and assigns
	a margin score"""
	rank_1_df = data[data['rank'] == 1][['unit_id', 'ReportingUnit_Id', 'Count']]
	rank_1_df = rank_1_df.rename(columns={'Count': 'rank_1_total'})
	data = data.merge(rank_1_df, how='inner', on=['unit_id', 'ReportingUnit_Id'])
	data['margins'] = (data['rank_1_total'] - data['Count']) / data['contest_total']
	data = data[data['margins'] != math.inf]
	data = data[data['margins'] != -math.inf]
	return data


def calculate_votes_at_stake(data):
	"""Move the most anomalous pairing to the equivalent of the second-most anomalous
	and calculate the differences in votes that would be returned"""
	df = pd.DataFrame()
	unit_ids = data['unit_id'].unique()
	for unit_id in unit_ids:
		temp_df = data[data['unit_id'] == unit_id]
		if temp_df.shape[0] > 2:
			max_anomaly_score = temp_df['score'].max() 
			reporting_unit_id = temp_df[temp_df['score'] == max_anomaly_score] \
				.iloc[0]['ReportingUnit_Id']
			selection = temp_df.loc[temp_df['score'] == max_anomaly_score] \
				.iloc[0]['Selection']
			anomalous_df = temp_df[(temp_df['ReportingUnit_Id'] == reporting_unit_id) & \
				(((temp_df['Selection'] == selection)  & \
					(temp_df['score'] == max_anomaly_score)) |
				(temp_df['rank'] == 1))].sort_values('score', ascending=False)
			next_max_score = temp_df[temp_df['score'] < max_anomaly_score]['score'].max()
			next_reporting_unit_id = temp_df.loc[temp_df['score'] == next_max_score] \
				.iloc[0]['ReportingUnit_Id']
			next_anomalous_df =temp_df[(temp_df['ReportingUnit_Id'] == next_reporting_unit_id) & \
				(((temp_df['Selection'] == selection)  & \
					(temp_df['score'] == next_max_score)) |
				(temp_df['rank'] == 1))].sort_values('score', ascending=False)
			temp_df['margin_ratio'] = (anomalous_df.iloc[0]['margins'] - next_anomalous_df.iloc[0]['margins']) \
				/ anomalous_df.iloc[0]['margins'] 
		else:
			temp_df['margin_ratio'] = 0
		df = pd.concat([df, temp_df])
	df = df[df['margin_ratio'] != math.inf]
	df = df[df['margin_ratio'] != -math.inf]
	return df


def pull_data_tables(session):
	# pull relevant tables
	df = {}
	for element in [
		'ElectionContestSelectionVoteCountJoin','VoteCount','ContestSelectionJoin',
		'ComposingReportingUnitJoin','Election','ReportingUnit',
		'ElectionContestJoin','CandidateContest','CandidateSelection','BallotMeasureContest',
		'BallotMeasureSelection','Office','Candidate']:
		# pull directly from db, using 'Id' as index
		df[element] = pd.read_sql_table(element,session.bind,index_col='Id')
	# avoid conflict if by chance VoteCount table has extra columns during data upload
	df['VoteCount'] = df['VoteCount'][['Count','CountItemType_Id','OtherCountItemType','ReportingUnit_Id']]
	# pull enums from db, keeping 'Id as a column, not the index
	for enum in ["ReportingUnitType","CountItemType"]:
		df[enum] = pd.read_sql_table(enum,session.bind)
	return df


def create_candidate_contests(df, columns):
	contest_df = df['ContestSelectionJoin'].merge(
		df['CandidateContest'],how='right',left_on='Contest_Id',right_index=True).rename(
		columns={'Name':'Contest','Id':'ContestSelectionJoin_Id'}).merge(
		df['CandidateSelection'],how='left',left_on='Selection_Id',right_index=True).merge(
		df['Candidate'],how='left',left_on='Candidate_Id',right_index=True).rename(
		columns={'BallotName':'Selection'}).merge(
		df['Office'],how='left',left_on='Office_Id',right_index=True)
	contest_df = contest_df[columns]
	if contest_df.empty:
		contest_df['contest_type'] = None
	else:
		contest_df = mr.add_constant_column(contest_df,'contest_type','Candidate')
	return contest_df


def create_ballot_measure_contests(df, columns):
	ballotmeasure_df = df['ContestSelectionJoin'].merge(
		df['BallotMeasureContest'],how='right',left_on='Contest_Id',right_index=True).rename(
		columns={'Name':'Contest'}).merge(
		df['BallotMeasureSelection'],how='left',left_on='Selection_Id',right_index=True)
	ballotmeasure_df = ballotmeasure_df[columns]
	if ballotmeasure_df.empty:
		ballotmeasure_df['contest_type'] = None
	else:
		ballotmeasure_df = mr.add_constant_column(ballotmeasure_df,'contest_type','BallotMeasure')
	return ballotmeasure_df


def create_contests(df, reporting_units, candidate_columns=None, ballotmeasure_columns=None):
	c_df = pd.DataFrame()
	bm_df = pd.DataFrame()
	if candidate_columns:
		c_df = create_candidate_contests(df, candidate_columns)
	if ballotmeasure_columns:
		bm_df = create_ballot_measure_contests(df, candidate_columns)
	contest_selection = pd.concat([c_df, bm_df])
	contest_selection = contest_selection.merge(reporting_units,
		how='left', left_on='ElectionDistrict_Id', right_index=True)
	contest_selection = mr.enum_col_from_id_othertext(contest_selection,'ReportingUnitType',df['ReportingUnitType'])
	contest_selection.rename(columns={'ReportingUnitType':'contest_district_type'},inplace=True)
	return contest_selection


def create_hierarchies(session, df, jurisdiction_id, subdivision_type_id=None, 
	subdivision_type_other=None):
	ru = df['ReportingUnit'][['ReportingUnitType_Id','OtherReportingUnitType']]
	if not subdivision_type_id:
		return ru
	# find ReportingUnits of the correct type that are subunits of top_ru
	if not subdivision_type_other:
		subdivision_type_other = ''
	sub_ru_ids = child_rus_by_id(session, [jurisdiction_id], 
		ru_type=[subdivision_type_id, subdivision_type_other])
	if not sub_ru_ids:
		# TODO better error handling (while not sub_ru_list....)
		raise Exception(f'Database shows no ReportingUnits of selected subdivision type nested in jursidiction')
	sub_ru = df['ReportingUnit'].loc[sub_ru_ids]
	# find all children of subReportingUnits
	children_of_subs_ids = child_rus_by_id(session,sub_ru_ids)
	ru_children = df['ReportingUnit'].loc[children_of_subs_ids]
	return ru, sub_ru, ru_children


def create_vote_selections(df, contest_selection, election_id):
	ecj = df['ElectionContestJoin'][df['ElectionContestJoin'].Election_Id == election_id]
	contest_ids = ecj.Contest_Id.unique()
	csj = contest_selection[contest_selection.Contest_Id.isin(contest_ids)]
	ecsvcj = df['ElectionContestSelectionVoteCountJoin'][
		(df['ElectionContestSelectionVoteCountJoin'].ElectionContestJoin_Id.isin(ecj.index)) &
		(df['ElectionContestSelectionVoteCountJoin'].ContestSelectionJoin_Id.isin(csj.index))]
	return ecsvcj


def create_vote_counts(df, ecsvcj, contest_selection, ru_children, sub_ru):
	unsummed = ecsvcj.merge(
		df['VoteCount'],left_on='VoteCount_Id',right_index=True).merge(
		df['ComposingReportingUnitJoin'],left_on='ReportingUnit_Id',right_on='ChildReportingUnit_Id').merge(
		ru_children,left_on='ChildReportingUnit_Id',right_index=True).merge(
		sub_ru,left_on='ParentReportingUnit_Id',right_index=True,suffixes=['','_Parent'])
	rename = {
		'Name_Parent': 'ParentName',
		'ReportingUnitType_Id_Parent': 'ParentReportingUnitType_Id'
	}
	unsummed.rename(columns=rename, inplace=True)
	# add columns with names
	unsummed = mr.enum_col_from_id_othertext(unsummed,'CountItemType',df['CountItemType'],drop_old=False)
	unsummed = unsummed.merge(contest_selection,how='left',left_on='ContestSelectionJoin_Id',right_index=True)
	return unsummed
