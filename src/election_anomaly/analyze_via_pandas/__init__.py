#!usr/bin/python3

import pandas as pd
import user_interface as ui
import munge_routines as mr
import analyze as an
import datetime
import os
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import shutil
import csv



def export_and_inventory(target_dir,target_sub_dir,target_file,inventory_columns,inventory_values,temp_file):
	# TODO standard order for columns
	# export to file system
	out_path = os.path.join(
		target_dir,target_sub_dir)
	Path(out_path).mkdir(parents=True,exist_ok=True)

	while os.path.isfile(os.path.join(out_path,target_file)):
		target_file = (f'There is already a file called {target_file}. Pick another name.\n')

	out_file = os.path.join(out_path,target_file)
	shutil.copy2(temp_file,out_file)

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


def contest_info_by_id(eng):
	"""create and return dictionaries of info about contest & selection by id"""

	df = {}
	for element in [
		"CandidateContest","BallotMeasureContest",
		"BallotMeasureSelection","CandidateSelection","Candidate",
		"ReportingUnitType","ComposingReportingUnitJoin","ReportingUnit"]:
		df[element] = pd.read_sql_table(element,eng,index_col='Id')
	for enum in ["ReportingUnitType"]:
		df[enum] = pd.read_sql_table(enum,eng)

	candidate_name_by_selection_id = df['CandidateSelection'].merge(
		df['Candidate'],left_on='Candidate_Id',right_index=True)

	district_id_by_contest_id = pd.concat(
		[df['CandidateContest'][['Name','ElectionDistrict_Id']],
		df['BallotMeasureContest'][['Name','ElectionDistrict_Id']]])
	district_type_id_other_by_contest_id = district_id_by_contest_id.merge(
		df['ReportingUnit'],left_on='ElectionDistrict_Id',right_index=True)

	contest_type = {}
	contest_name = {}
	selection_name = {}
	contest_district_type = {}

	for i,r in df['CandidateContest'].iterrows():
		contest_type[i] = 'Candidate'
		contest_name[i] = r['Name']
	for i,r in df['BallotMeasureContest'].iterrows():
		contest_type[i] = 'BallotMeasure'
		contest_name[i] = r['Name']
	for i,r in df['BallotMeasureSelection'].iterrows():
		selection_name[i] = r['Selection']
	for i,r in candidate_name_by_selection_id.iterrows():
		selection_name[i] = r['BallotName']
	for i,r in district_type_id_other_by_contest_id.iterrows():
		contest_district_type[i] = mr.get_enum_value_from_id_othertext(
			df['ReportingUnitType'],r['ReportingUnitType_Id'],r['OtherReportingUnitType'])
	return contest_type,contest_name,selection_name,contest_district_type


def child_rus_by_id(session,parents,ru_type=None):
	"""Given a list <parents> of parent ids, return
	list of those parents along with all children of those parents.
	If (ReportingUnitType_Id,OtherReportingUnit) pair <rutype> is given,
	restrict children to that ReportingUnitType"""
	assert len(ru_type) == 2,f'argument {ru_type} does not have exactly 2 elements'
	cruj = pd.read_sql_table('ComposingReportingUnitJoin',session.bind)
	children = list(cruj[cruj.ParentReportingUnit_Id.isin(parents)].ChildReportingUnit_Id.unique()) + parents
	if ru_type:
		ru = pd.read_sql_table('ReportingUnit',session.bind,index_col='Id')
		right_type_ru = ru[(ru.ReportingUnitType_Id == ru_type[0]) & (ru.OtherReportingUnitType == ru_type[1])]
		children = [x for x in children if x in right_type_ru.index]
	# TODO test
	return children


def get_id_check_unique(df,conditions=None):
	"""Finds the index of the unique row of <df> satisfying the <conditions>.
	Raises exception if there is no unique row"""
	if conditions is None:
		conditions = {}
	found = df.loc[(df[list(conditions)] == pd.Series(conditions)).all(axis=1)]
	if found.shape[0] == 0:
		raise Exception(f'None found')
	elif found.shape[0] > 1:
		raise Exception(f'More than one found')
	else:
		return found.first_valid_index()


def create_rollup(session,top_ru,sub_ru_type,atomic_ru_type,election,target_dir,exclude_total=True):
	"""<top_ru> is the internal cdf name of the ReportingUnit whose results will be reported
	(e.g., Florida or Pennsylvania;Philadelphia).
	<sub_ru_type> is the ReportingUnitType of the ReportingUnits used in each line of the results file
	for <election> created by the routine. (E.g., county or ward)
	<atomic_ru_type> is the ReportingUnitType in the database from which the results
	at the <sub_ru_type> level are calculated. (E.g., county or precinct)
	<session> and <db> provide access to the db containing results.
	If <exclude_total> is True, don't include 'total' CountItemType
	(unless 'total' the only CountItemType)"""

	# Get name of db for error messages
	db = session.bind.url.database

	# pull relevant tables
	df = {}
	for element in [
		"SelectionElectionContestVoteCountJoin","VoteCount",
		"ComposingReportingUnitJoin","Election","ReportingUnit",
		"ComposingReportingUnitJoin"]:
		df[element] = pd.read_sql_table(element,session.bind,index_col='Id')
	for enum in ["ReportingUnitType","CountItemType"]:
		df[enum] = pd.read_sql_table(
			enum,session.bind)  # keep 'Id' as col, not index
	count_item_type = {r.Id:r.Txt for i,r in df['CountItemType'].iterrows()}

	# Get id for top reporting unit, election
	top_ru_id = get_id_check_unique(df['ReportingUnit'],conditions={'Name':top_ru})
	election_id = get_id_check_unique(df['Election'],conditions={'Name':election})

	atomic_ru_type_id, atomic_other_ru_type = mr.get_id_othertext_from_enum_value(
		df['ReportingUnitType'],atomic_ru_type)
	sub_ru_type_id, sub_other_ru_type = mr.get_id_othertext_from_enum_value(
		df['ReportingUnitType'],sub_ru_type)

	# limit to atomic and sub RUs nested inside the top RU
	atomic_ru_list = child_rus_by_id(session,[top_ru_id],ru_type=[atomic_ru_type_id, atomic_other_ru_type])
	if not atomic_ru_list:
		raise Exception(f'Database {db} shows no ReportingUnits of type {atomic_ru_type} nested inside {top_ru}')
	# atomic_ru = df['ReportingUnit'].loc[atomic_ru_list]

	sub_ru_list = child_rus_by_id(session,[top_ru_id],ru_type=[sub_ru_type_id, sub_other_ru_type])
	if not sub_ru_list:
		raise Exception(f'Database {db} shows no ReportingUnits of type {sub_ru_type} nested inside {top_ru}')
	# sub_ru = df['ReportingUnit'].loc[sub_ru_list]

	atomic_in_sub_list = child_rus_by_id(session,sub_ru_list,ru_type=[atomic_ru_type_id, atomic_other_ru_type])
	missing_atomic = [x for x in atomic_ru_list if x not in atomic_in_sub_list]
	if missing_atomic:
		missing_names = set(df['ReportingUnit'].loc[missing_atomic,'Name'])
		ui.show_sample(missing_names,'atomic ReportingUnits',f'are not in any {sub_ru_type}')

	# calculate specified dataframe with columns [ReportingUnit,Contest,Selection,VoteCount,CountItemType]
	ru_c = df['ReportingUnit'].loc[atomic_ru_list]
	ru_p = df['ReportingUnit'].loc[sub_ru_list]
	secvcj = df['SelectionElectionContestVoteCountJoin'][
		df['SelectionElectionContestVoteCountJoin'].Election_Id == election_id
		]

	if sub_ru_type == atomic_ru_type:
		unsummed = secvcj.merge(
			df['VoteCount'],left_on='VoteCount_Id',right_index=True).merge(
			df['ReportingUnit'],left_on='ReportingUnit_Id',right_index=True)
		unsummed.rename(columns={'Name':'ReportingUnit'},inplace=True)
	else:
		unsummed = secvcj.merge(
			df['VoteCount'],left_on='VoteCount_Id',right_index=True).merge(
			df['ComposingReportingUnitJoin'],left_on='ReportingUnit_Id',right_on='ChildReportingUnit_Id').merge(
			ru_c,left_on='ChildReportingUnit_Id',right_index=True).merge(
			ru_p,left_on='ParentReportingUnit_Id',right_index=True,suffixes=['','_Parent'])
		unsummed.rename(columns={'Name_Parent':'ReportingUnit'},inplace=True)
	# TODO check this merge -- does it need how='inner'?

	# add columns with names
	contest_type,contest_name,selection_name,contest_district_type = contest_info_by_id(session.bind)
	unsummed['contest_type'] = unsummed['Contest_Id'].map(contest_type)
	unsummed['Contest'] = unsummed['Contest_Id'].map(contest_name)
	unsummed['Selection'] = unsummed['Selection_Id'].map(selection_name)
	unsummed['CountItemType'] = unsummed['CountItemType_Id'].map(count_item_type)
	unsummed['contest_district_type'] = unsummed['Contest_Id'].map(contest_district_type)

	cis = 'unknown'
	cit_list = unsummed['CountItemType'].unique()
	if len(cit_list) > 1:
		cit = 'mixed'
		if exclude_total:
			unsummed = unsummed[unsummed.CountItemType != 'total']
	elif len(cit_list) == 1:
		cit = cit_list[0]
	else:
		raise Exception(
			f'Results dataframe has no CountItemTypes; maybe dataframe is empty?')
	count_item = f'TYPE{cit}_STATUS{cis}'

	# sum by groups
	summed_by_name = unsummed[[
		'contest_type','Contest','contest_district_type','Selection','ReportingUnit','CountItemType','Count']].groupby(
		['contest_type','Contest','contest_district_type','Selection','ReportingUnit','CountItemType']).sum()

	inventory_columns = [
		'Election','ReportingUnitType','CountItemType','CountItemStatus',
		'source_db_url','timestamp']
	inventory_values = [
		election,sub_ru_type,cit,cis,
		str(session.bind.url),datetime.date.today()]
	sub_dir = os.path.join(f'FROMDB_{session.bind.url.database}',election,top_ru,f'by_{sub_ru_type}')
	an.export_to_inventory_file_tree(
		target_dir,sub_dir,f'{count_item}.txt',
		inventory_columns,inventory_values,summed_by_name)
	return summed_by_name


def rollup_df(input_fpath):
	"""Gets rollup dataframe stored in file <input_fpath>"""
	# TODO: read and return info about dataframe?
	# TODO: check that rollup really is a rollup?
	while not os.path.isfile(input_fpath):
		print(f'File not found:\n{input_fpath}')
		input_fpath = input('Enter alternate file path to continue (or just hit return to stop).\n')
		if not input_fpath:
			return None
	df = pd.read_csv(input_fpath,sep='\t')
	return df


def by_contest_columns(
		election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,
		contest_group_types=None,contest_types=['Candidate']):
	"""<rollup_dir> contains the election folder of the rollup file tree.
	Find the rollup file determined by <top_ru>,<sub_ru_type>,<count_type>,<count_status>
	and create from it a dataframe of contest totals by county.
	For each ReportingUnitType in <contest_group_types>, use union of all contests whose
	election district is of that type rather than individual contests.
	<contest_type> is a set or list containing 'Candidate' or 'BallotMeasure' or both."""
	# TODO handle multiple contest group in same districts, e.g., state party members by congressional district,
	#  without overcounting
	rollup = rollup_df(
		os.path.join(rollup_dir,election,top_ru,f'by_{sub_ru_type}',f'TYPE{count_type}_STATUS{count_status}.txt'))

	# filter by contest type
	rollup = rollup[rollup.contest_type.isin(contest_types)]

	# map contests to themselves or to group with which they should be counted
	if contest_group_types:
		contest_to_group = dict(np.array(rollup[['Contest','contest_district_type']].drop_duplicates()))
		for k in contest_to_group.keys():
			if contest_to_group[k] not in contest_group_types:
				contest_to_group[k] = k
		# use contest-to-group map to rename 'Contest' values
		rollup['Contest'] = rollup['Contest'].map(contest_to_group)

	sum_by_contest_sub_ru = rollup.groupby(
		by=['Contest','ReportingUnit']).sum().reset_index().pivot(
		index='ReportingUnit',columns='Contest',values='Count')
	# TODO do we need to groupby ReportingUnit?

	return sum_by_contest_sub_ru


def append_total_and_pcts(df):
	"""Input is a dataframe <df> with numerical columns
	Output is a dataframe with all cols of <df>, as well as
	a total column and pct columns corresponding to cols of <df>"""
	# TODO allow weights for columns of df
	# TODO check all columns numeric, none named 'total'
	df_copy = df.copy()
	df_copy['total'] = df_copy.sum(axis=1)
	for col in df.columns:
		df_copy[pct_column(col)] = df_copy[col]/df_copy['total']
	return df_copy


def diff_column(col_name):
	return f'diff_{col_name}'


def pct_column(col_name):
	return f'{col_name}_pct'


def complement_column(col_name):
	return f'{col_name}_complement_in_column'


def diff_from_avg(df,col_list,mode='selections'):
	"""For each record in <df>, for the columns in <col_list>,
	add columns expressing the right vote diff.
	For mode 'selections':
	calculate (and add columns for) the pct_diff, i.e.,
	the value of the <col_list> vector for that record
	minus the average value of the <col_list> vector for all other records.
	Then add columns for the abs_diff, i.e., the pct_diff times the total.
	For mode 'dropoff':
	calculate (and add a column for) the ratio of
	the sum of all other votes in all columns for the given record
	to the sum of all other votes in all other columns"""
	if mode == 'selections':
		diffs_df = append_total_and_pcts(df[col_list])
		diff_col_d = {pct_column(c):diff_column(pct_column(c)) for c in col_list}
		pct_col_list = [pct_column(c) for c in col_list]
		diff_col_list = [diff_column(pct_column(c)) for c in col_list]

		# initialize diff_pct columns, will be filled row by row
		for c in col_list:
			diffs_df.loc[:,diff_column(pct_column(c))] = None

		for i in df.index:
			# subtract average pcts over all other records from pcts for record i
			pct_diff = (diffs_df.loc[i,pct_col_list] - diffs_df[pct_col_list].drop(i).mean(axis=0)).rename(diff_col_d)
			diffs_df.loc[i,diff_col_list] = pct_diff

		# append columns for absolute diff
		for c in col_list:
			diffs_df.loc[:,diff_column(c)] = diffs_df[diff_column(pct_column(c))] * diffs_df['total']
			
	elif mode == 'dropoff':
		diffs_df = df[col_list].copy()
		# initialize dropoff factor column and complement columns
		diffs_df.loc[:,'dropoff_factor'] = None

		# initialize complement columns, will be filled row by row
		for c in col_list:
			diffs_df.loc[:,complement_column(c)] = None

		for i in df.index:
			# find multiplicative factor
			diffs_df.loc[i,'dropoff_factor'] = diffs_df.loc[i].sum() / diffs_df[col_list].drop(i).sum(axis=1).sum()

			# for each contest find sum of votes in the other records
			for c in col_list:
				diffs_df.loc[i,complement_column(c)] = diffs_df[c].drop(i).sum()

		# append columns for absolute diff
		for c in col_list:
			diffs_df.loc[:,diff_column(c)] = diffs_df[c] - diffs_df['dropoff_factor'] * diffs_df[complement_column(c)]

	else:
		print(f'mode {mode} not recognized')
		return None

	return diffs_df.fillna(0)


def single_contest_selection_columns(rollup,contest,count_type,output_dir=None):
	"""Given a single contest <contest>, 
	NB: <count_type> must be from the CountType enumeration
	Returns dataframe with columns labeled by selections """ # TODO

	# filter by contest and vote type
	df = rollup[(rollup.Contest==contest) & (rollup.CountItemType==count_type)][
		['Contest','Selection','ReportingUnit','Count']].pivot(
		index='ReportingUnit',columns='Selection',values='Count'
	)
	if output_dir:
		# TODO store in output_dir
		pass
	return df


def top_two_total_columns(df):
	"""returns the dataframe <df> restricted to the top two columns (by total)"""
	df_copy = df.copy()
	top_two = df_copy.sum().nlargest(n=2,keep='all').index
	return df_copy[top_two]


def dropoff_from_rollup(
		election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,output_dir,contest,
		comparison_contests,contest_type,
		contest_group_types=None):
	"""<contests> is a list of contests (or contest groups such as 'state-house'
	<contest_type> is a dictionary whose keys include all items in <contests>.
	<contest> is a contest in the comparison_contests list
	"""
	# TODO check: all items in <contests> are either ocntests or contest group types in <contest_group_types>
	# find all contest types represented in contests
	types = {contest_type[c] for c in comparison_contests}

	by_cc = by_contest_columns(
		election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,
		contest_group_types=contest_group_types,contest_types=types)
	dfa = diff_from_avg(by_cc,comparison_contests,mode='dropoff')
	d = diff_column(contest)

	extremes = [dfa[d].idxmax(),dfa[d].idxmin()]

	# open text file for reporting
	with open(os.path.join(output_dir,election,f'info.txt'),'w') as f:
		f.write(
			f'{election}\t{top_ru}\t{sub_ru_type}\t{count_type}\t{count_status}\t{comparison_contests}\n')
		f.write(f'\n{contest}\n')

		# report diffs of extremes
		for e in extremes:
			f.write(f'{e}\t{round(dfa.loc[e,d])}\n')

	# scatter plot total votes vs. diff
	plt.scatter(dfa[contest]/1000,dfa[d]/1000)
	plt.xlabel(f'Vote Totals (thousands)')
	plt.ylabel(f'Dropoff correcion (thousands)')
	comps_text = "\n".join(comparison_contests)
	plt.suptitle(
		f'{contest} dropoff by {sub_ru_type}\nCorrection relative to contests\n{comps_text}',
	fontsize=10)
	for e in extremes:
		plt.annotate(e,(dfa.loc[e,contest]/1000,dfa.loc[e,d]/1000))
	plt.savefig(os.path.join(output_dir,election,f'scatter_{contest}.png'))
	plt.clf()

	return dfa.loc[extremes]


def dropoff_analysis(
		election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,output_dir,
		comparison_contests,contest_type,
		contest_group_types=None):

	extremes = {}
	for contest in comparison_contests:
		extremes[contest] = dropoff_from_rollup(
			election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,output_dir,contest,comparison_contests,
			contest_type,contest_group_types=contest_group_types)
		# add column
		extremes[contest].loc[:,'Contest'] = contest

	all_extremes = pd.concat([extremes[contest] for contest in comparison_contests])
	all_extremes.set_index(['Contest','ReportingUnit'],inplace=True)
	three_most_undervoted = all_extremes.nsmallest(n=3,keep='all')
	three_most_overvoted = all_extremes.nlargest(n=3,keep='all')
	comp_text = '_'.join(comparison_contests)
	inv_cols = ['Election','Comparison_Contests','Jurisdiction','by']
	inv_vals = [election,comp_text,top_ru,sub_ru_type]
	an.export_to_inventory_file_tree(
		output_dir,f'{election}/{comp_text}/{top_ru}/by_{sub_ru_type}','dropoff_extremes.txt',inv_cols,inv_vals)
	return pd.concat([three_most_undervoted,three_most_overvoted])



def process_single_contest(rollup,contest,output_dir):
	""" """  # TODO

	counttypes = rollup['CountItemType'].unique()
	selections = rollup['Selection'].unique()
	for ct in counttypes:
		df = single_contest_selection_columns(rollup,ct)
		top_two_diff = diff_from_avg(top_two_total_columns(df),selections)  # diffs of top two vote-getting selections
		for s in top_two_diff.columns:
			extremes = [top_two_diff[f'{s}_diff'].idxmax(),top_two_diff[f'{s}_diff'].idxmin()]

	return