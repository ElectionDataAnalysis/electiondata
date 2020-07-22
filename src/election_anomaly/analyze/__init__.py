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


def create_scatter(session, top_ru_id, sub_rutype_id, election_id, datafile_id_list,
	candidate_1_id, candidate_2_id, count_item_type):
	"""<target_dir> is the directory where the resulting rollup will be stored.
	<election_id> identifies the election; <datafile_id_list> the datafile whose results will be rolled up.
	<top_ru_id> is the internal cdf name of the ReportingUnit whose results will be reported
	<sub_rutype_id>,<sub_rutype_othertext> identifies the ReportingUnitType
	of the ReportingUnits used in each line of the results file
	created by the routine. (E.g., county or ward)
	If <exclude_total> is True, don't include 'total' CountItemType
	(unless 'total' is the only CountItemType)"""
	# Get name of db for error messages
	db = session.bind.url.database

	top_ru_id, top_ru = ui.pick_record_from_db(session,'ReportingUnit',required=True,db_idx=top_ru_id)
	election_id,election = ui.pick_record_from_db(session,'Election',required=True,db_idx=election_id)

	sub_rutype = dbr.name_from_id(session, 'ReportingUnitType', sub_rutype_id)

	# pull relevant tables
	df = {}
	for element in [
		'ElectionContestSelectionVoteCountJoin','VoteCount','CandidateContestSelectionJoin',
		'BallotMeasureContestSelectionJoin','ComposingReportingUnitJoin','Election','ReportingUnit',
		'ElectionContestJoin','CandidateContest','CandidateSelection','BallotMeasureContest',
		'BallotMeasureSelection','Office','Candidate']:
		# pull directly from db, using 'Id' as index
		df[element] = pd.read_sql_table(element,session.bind,index_col='Id')

	# pull enums from db, keeping 'Id as a column, not the index
	for enum in ["ReportingUnitType","CountItemType"]:
		df[enum] = pd.read_sql_table(enum,session.bind)

	#  limit to relevant Election-Contest pairs
	ecj = df['ElectionContestJoin'][df['ElectionContestJoin'].Election_Id == election_id]

	# create contest_selection dataframe, adding Contest, Selection and ElectionDistrict_Id columns
	contest_selection = df['CandidateContestSelectionJoin'].merge(
		df['CandidateContest'],how='left',left_on='CandidateContest_Id',right_index=True).rename(
		columns={'Name':'Contest','Id':'ContestSelectionJoin_Id'}).merge(
		df['CandidateSelection'],how='left',left_on='CandidateSelection_Id',right_index=True).merge(
		df['Candidate'],how='left',left_on='Candidate_Id',right_index=True).rename(
		columns={'BallotName':'Selection','CandidateContest_Id':'Contest_Id',
				'CandidateSelection_Id':'Selection_Id'}).merge(
		df['Office'],how='left',left_on='Office_Id',right_index=True)
	contest_selection = contest_selection[['Contest_Id','Contest','Selection_Id','Selection','ElectionDistrict_Id',
		'Candidate_Id']]
	if contest_selection.empty:
		contest_selection['contest_type'] = None
	else:
		inv_df = pd.DataFrame()
	inventory = {'Election': election, 'ReportingUnitType': sub_rutype,
				 'source_db_url': cursor.connection.dsn, 'timestamp': datetime.date.today()}

	for contest_type in ['BallotMeasure','Candidate']:
		# export data
		rollup_file = f'{cursor.connection.info.dbname}_{contest_type}_results.txt'
		while os.path.isfile(os.path.join(leaf_dir, rollup_file)):
			rollup_file = input(f'There is already a file called {rollup_file}. Pick another name.\n')

		err = dbr.export_rollup_to_csv(
			cursor, top_ru, sub_rutype, contest_type, datafile_list,
			os.path.join(leaf_dir, rollup_file), by=by, exclude_total=exclude_total
		)
		if err:
			err_str = err
		else:
			# create record for inventory.txt
			inv_df = inv_df.append(inventory, ignore_index=True).fillna('')
			err_str = None

	# export to inventory file
	inv_df.to_csv(inventory_file, index=False, sep='\t')
	return err_str

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

	# filter based on vote count type
	unsummed = unsummed[unsummed['CountItemType'] == count_item_type]
	return unsummed
	"""

	# package into dictionary
	x = dbr.name_from_id(session, 'Candidate', candidate_1_id)
	y = dbr.name_from_id(session, 'Candidate', candidate_2_id) 
	results = {
		"election": dbr.name_from_id(session, 'Election', election_id),
		"jurisdiction": dbr.name_from_id(session, 'ReportingUnit', top_ru_id),
		"subdivision_type": dbr.name_from_id(session, 'ReportingUnitType', sub_rutype_id),
		"count_item_type": count_item_type,
		"x": x,
		"y": y,
		"counts": {}
	}
	reporting_units = unsummed.Name.unique()
	for reporting_unit in reporting_units:
		results["counts"][reporting_unit] = {}

	for i, row in unsummed.iterrows():
		if row.Selection == x:
			results["counts"][row.Name]["x"] = row.Count
		elif row.Selection == y:
			results["counts"][row.Name]["y"] = row.Count
		
	return results

def create_bar(session, top_ru_id, contest_type, contest, election_id, datafile_id_list):
	"""<target_dir> is the directory where the resulting rollup will be stored.
	<election_id> identifies the election; <datafile_id_list> the datafile whose results will be rolled up.
	<top_ru_id> is the internal cdf name of the ReportingUnit whose results will be reported
	<sub_rutype_id>,<sub_rutype_othertext> identifies the ReportingUnitType
	of the ReportingUnits used in each line of the results file
	created by the routine. (E.g., county or ward)
	If <exclude_total> is True, don't include 'total' CountItemType
	(unless 'total' is the only CountItemType)"""
	# Get name of db for error messages
	db = session.bind.url.database

	top_ru_id, top_ru = ui.pick_record_from_db(session,'ReportingUnit',required=True,db_idx=top_ru_id)
	election_id,election = ui.pick_record_from_db(session,'Election',required=True,db_idx=election_id)

	#sub_rutype = dbr.name_from_id(session, 'ReportingUnitType', sub_rutype_id)

	# pull relevant tables
	df = {}
	for element in [
		'ElectionContestSelectionVoteCountJoin','VoteCount','CandidateContestSelectionJoin',
		'BallotMeasureContestSelectionJoin','ComposingReportingUnitJoin','Election','ReportingUnit',
		'ElectionContestJoin','CandidateContest','CandidateSelection','BallotMeasureContest',
		'BallotMeasureSelection','Office','Candidate']:
		# pull directly from db, using 'Id' as index
		df[element] = pd.read_sql_table(element,session.bind,index_col='Id')

	# pull enums from db, keeping 'Id as a column, not the index
	for enum in ["ReportingUnitType","CountItemType"]:
		df[enum] = pd.read_sql_table(enum,session.bind)

	#  limit to relevant Election-Contest pairs
	ecj = df['ElectionContestJoin'][df['ElectionContestJoin'].Election_Id == election_id]

	# create contest_selection dataframe, adding Contest, Selection and ElectionDistrict_Id columns
	contest_selection = df['CandidateContestSelectionJoin'].merge(
		df['CandidateContest'],how='left',left_on='CandidateContest_Id',right_index=True).rename(
		columns={'Name':'Contest','Id':'ContestSelectionJoin_Id'}).merge(
		df['CandidateSelection'],how='left',left_on='CandidateSelection_Id',right_index=True).merge(
		df['Candidate'],how='left',left_on='Candidate_Id',right_index=True).rename(
		columns={'BallotName':'Selection','CandidateContest_Id':'Contest_Id',
				'CandidateSelection_Id':'Selection_Id'}).merge(
		df['Office'],how='left',left_on='Office_Id',right_index=True)
	contest_selection = contest_selection[['Contest_Id','Contest','Selection_Id','Selection','ElectionDistrict_Id',
		'Candidate_Id']]
	if contest_selection.empty:
		contest_selection['contest_type'] = None
	else:
		contest_selection.loc[:,'contest_type'] = 'Candidate'

	# append contest_district_type column
	ru = df['ReportingUnit'][['ReportingUnitType_Id','OtherReportingUnitType']]
	contest_selection = contest_selection.merge(ru,how='left',left_on='ElectionDistrict_Id',right_index=True)
	contest_selection = mr.enum_col_from_id_othertext(contest_selection,'ReportingUnitType',df['ReportingUnitType'])
	contest_selection.rename(columns={'ReportingUnitType':'contest_district_type'},inplace=True)

	if contest_type:
		contest_selection = contest_selection[contest_selection['contest_district_type'] == contest_type]
	if contest:
		contest_selection = contest_selection[contest_selection['Contest'] == contest]
	# limit to relevant ContestSelection pairs
	contest_ids = ecj.Contest_Id.unique()
	csj = contest_selection[contest_selection.Contest_Id.isin(contest_ids)]

	# find ReportingUnits of the correct type that are subunits of top_ru
	# sub_ru_ids = child_rus_by_id(session,[top_ru_id],ru_type=[sub_rutype_id, ''])
	# if not sub_ru_ids:
	# 	# TODO better error handling (while not sub_ru_list....)
	# 	raise Exception(f'Database {db} shows no ReportingUnits of type {sub_rutype} nested inside {top_ru}')
	sub_ru = df['ReportingUnit']

	# find all subReportingUnits of top_ru
	all_subs_ids = child_rus_by_id(session,[top_ru_id])

	# find all children of subReportingUnits
	children_of_subs_ids = child_rus_by_id(session,all_subs_ids)
	ru_children = df['ReportingUnit'].loc[children_of_subs_ids]

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

	# Some super kludgy stuff to remove duplicates that get introduced somewhere
	# TODO: FIXME
	unsummed = unsummed[(unsummed['ChildReportingUnit_Id'] != unsummed['ParentReportingUnit_Id'])]
	unsummed = unsummed.drop(columns=['OtherReportingUnitType', 'ChildReportingUnit_Id', 
				'ParentReportingUnit_Id', 'ReportingUnitType_Id_Parent'])
	unsummed = unsummed.drop_duplicates()
	unsummed = unsummed[unsummed['ReportingUnitType_Id'] != 19] # is not a state
	unsummed = unsummed[~unsummed['ReportingUnit'].isin(['North Carolina', 'Colorado', 'Pennsylvania', 
				'Michigan', 'Maryland', 'Virginia'])]


	# add columns with names
	unsummed = mr.enum_col_from_id_othertext(unsummed,'CountItemType',df['CountItemType'])
	unsummed = unsummed.merge(contest_selection,how='left',left_on='ContestSelectionJoin_Id',right_index=True)

	ranked = assign_anomaly_score(unsummed)
	top_ranked = get_most_anomalous(ranked, 3)

	# package into list of dictionary
	result_list = []
	ids = top_ranked['Contest_Id'].unique()
	for id in ids:
		temp_df = top_ranked[top_ranked['Contest_Id'] == id]

		candidates = temp_df['Candidate_Id'].unique()
		x = dbr.name_from_id(session, 'Candidate', candidates[0])
		y = dbr.name_from_id(session, 'Candidate', candidates[1]) 
		results = {
			"election": dbr.name_from_id(session, 'Election', election_id),
			"jurisdiction": dbr.name_from_id(session, 'ReportingUnit', top_ru_id),
			"subdivision_type": dbr.name_from_id(session, 'ReportingUnit', top_ru_id),
			"count_item_type": temp_df.iloc[0]['CountItemType'],
			"x": x,
			"y": y,
			"counts": {}
		}
		reporting_units = temp_df.Name.unique()
		for reporting_unit in reporting_units:
			results["counts"][reporting_unit] = {}

		for i, row in temp_df.iterrows():
			if row.Selection == x:
				results["counts"][row.Name]["x"] = row.Count
			elif row.Selection == y:
				results["counts"][row.Name]["y"] = row.Count
		result_list.append(results)
		
	return result_list


def assign_anomaly_score(data):
	"""adds a new column called score between 0 and 1; 1 is more anomalous"""
	import numpy as np
	data['score'] = np.random.rand(data.shape[0])
	return data


def get_most_anomalous(data, n):
	"""gets the n contests with the highest individual anomaly score"""
	df = data.groupby('Contest_Id')['score'].max().reset_index()
	df.rename(columns={'score': 'max_score'}, inplace=True)
	data = data.merge(df, on='Contest_Id')
	# data.drop('score_x', axis=1, inplace=True)
	# data.rename(columns={'score_y': 'score'}, inplace=True)
	unique_scores = sorted(set(df['max_score']), reverse=True)
	top_scores = unique_scores[:n]

	result = data[data['max_score'].isin(top_scores)]

	# Eventually we want to return the winner and the most anomalous
	# for each contest. For now, just 2 random ones
	ids = result['Contest_Id'].unique()
	df = pd.DataFrame()
	for id in ids:
		temp_df = result[result['Contest_Id'] == id]
		unique = temp_df['Candidate_Id'].unique()
		candidates = unique[0:2]
		candidate_df = temp_df[temp_df['Candidate_Id'].isin(candidates)]
		unique = candidate_df['ReportingUnit_Id'].unique()
		reporting_units = unique[0:6]
		df_final = candidate_df[candidate_df['ReportingUnit_Id'].isin(reporting_units)]. \
			sort_values(['ReportingUnit_Id', 'score'], ascending=False)
		# TODO: remove this filter on total
		df_final = df_final[df_final['CountItemType'] == 'total']
		df = pd.concat([df, df_final])

	return df