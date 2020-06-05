import csv
import os.path

import pandas as pd
import user_interface as ui
import munge_routines as mr
import datetime
import os
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from pandas.api.types import is_numeric_dtype


def contest_info_by_id(eng):
	"""create and return dictionaries of info about contest & selection by id"""

	df = {}
	for element in [
		"CandidateContest","BallotMeasureContest",
		"BallotMeasureSelection","CandidateSelection","Candidate",
		"ReportingUnitType","ComposingReportingUnitJoin","ReportingUnit","Office"]:
		df[element] = pd.read_sql_table(element,eng,index_col='Id')
	for enum in ["ReportingUnitType"]:
		df[enum] = pd.read_sql_table(enum,eng)

	candidate_name_by_selection_id = df['CandidateSelection'].merge(
		df['Candidate'],left_on='Candidate_Id',right_index=True)

	# TODO need to update: Office has ElectionDistrict; CC does not, but has Office
	df_cc_district = df['CandidateContest'].merge(df['Office'],how='left',left_on='Office_Id',right_index=True,suffixes=['','_Office'])
	district_id_by_contest_id = pd.concat(
		[df_cc_district[['Name','ElectionDistrict_Id']],
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
		contest_district_type[i] = mr.enum_value_from_id_othertext(
			df['ReportingUnitType'],r['ReportingUnitType_Id'],r['OtherReportingUnitType'])
	return contest_type,contest_name,selection_name,contest_district_type


def child_rus_by_id(session,parents,ru_type=None):
	"""Given a list <parents> of parent ids (or just a single parent_id), return
	list containing those parents along with all children of those parents.
	If (ReportingUnitType_Id,OtherReportingUnit) pair <rutype> is given,
	restrict children to that ReportingUnitType"""
	cruj = pd.read_sql_table('ComposingReportingUnitJoin',session.bind)
	children = list(cruj[cruj.ParentReportingUnit_Id.isin(parents)].ChildReportingUnit_Id.unique()) + parents
	if ru_type:
		assert len(ru_type) == 2,f'argument {ru_type} does not have exactly 2 elements'
		ru = pd.read_sql_table('ReportingUnit',session.bind,index_col='Id')
		right_type_ru = ru[(ru.ReportingUnitType_Id == ru_type[0]) & (ru.OtherReportingUnitType == ru_type[1])]
		children = [x for x in children if x in right_type_ru.index]
	return children


def get_id_check_unique(df,conditions=None):
	"""Finds the index of the unique row of <df> satisfying the <conditions>.
	Raises exception if there is no unique row"""
	if conditions is None:
		conditions = {}
	found = df.loc[(df[list(conditions)] == pd.Series(conditions)).all(axis=1)]
	if found.shape[0] == 0:
		# TODO allow user to revise?
		raise Exception(f'None found')
	elif found.shape[0] > 1:
		raise Exception(f'More than one found')
	else:
		return found.first_valid_index()


def create_rollup(
		session,target_dir,top_ru_id=None,sub_rutype_id=None,sub_rutype_othertext=None,election_id=None,
		datafile_id_list=None,by_vote_type=True,exclude_total=True):
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

	# ask user to select any info not supplied
	if top_ru_id is None:
		print('Select the type of the top ReportingUnit for the rollup.')
		top_rutype_id, top_rutype_othertext, top_rutype = ui.pick_enum(session,'ReportingUnitType')
		print('Select the top ReportingUnit for the rollup')
		top_ru_id, top_ru = ui.pick_record_from_db(
			session,'ReportingUnit',known_info_d={
				'ReportingUnitType_Id':top_rutype_id, 'OtherReportingUnitType':top_rutype_othertext},required=True)
	else:
		top_ru_id, top_ru = ui.pick_record_from_db(session,'ReportingUnit',required=True,db_idx=top_ru_id)
	if election_id is None:
		print('Select the Election')
	election_id,election = ui.pick_record_from_db(session,'Election',required=True,db_idx=election_id)

	if datafile_id_list is None:
		# TODO allow several datafiles to be picked
		# TODO restrict to datafiles whose ReportingUnit intersects top_ru?
		# TODO note/enforce that no datafile double counts anything?
		print('Select the datafile')
		datafile_id_list = ui.pick_record_from_db(
			session,'_datafile',required=True,known_info_d={'Election_Id':election_id})[0]
	if sub_rutype_id is None:
		# TODO restrict to types that appear as sub-reportingunits of top_ru?
		#  Or that appear in VoteCounts associated to one of the datafiles?
		print('Select the ReportingUnitType for the lines of the rollup')
		sub_rutype_id, sub_rutype_othertext,sub_rutype = ui.pick_enum(session,'ReportingUnitType')
	else:
		sub_rutype = mr.enum_value_from_id_othertext('ReportingUnitType',sub_rutype_id,sub_rutype_othertext)

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
	cc = df['CandidateContestSelectionJoin'].merge(
		df['CandidateContest'],how='left',left_on='CandidateContest_Id',right_index=True).rename(
		columns={'Name':'Contest','Id':'ContestSelectionJoin_Id'}).merge(
		df['CandidateSelection'],how='left',left_on='CandidateSelection_Id',right_index=True).merge(
		df['Candidate'],how='left',left_on='Candidate_Id',right_index=True).rename(
		columns={'BallotName':'Selection','CandidateContest_Id':'Contest_Id',
				'CandidateSelection_Id':'Selection_Id'}).merge(
		df['Office'],how='left',left_on='Office_Id',right_index=True)
	cc = cc[['Contest_Id','Contest','Selection_Id','Selection','ElectionDistrict_Id']]
	if cc.empty:
		cc['contest_type'] = None
	else:
		cc.loc[:,'contest_type'] = 'Candidate'

	# create ballotmeasure_selection dataframe
	bm = df['BallotMeasureContestSelectionJoin'].merge(
		df['BallotMeasureContest'],how='left',left_on='BallotMeasureContest_Id',right_index=True).rename(
		columns={'Name':'Contest'}).merge(
		df['BallotMeasureSelection'],how='left',left_on='BallotMeasureSelection_Id',right_index=True).rename(
		columns={'BallotMeasureSelection_Id':'Selection_Id','BallotMeasureContest_Id':'Contest_Id'}
	)
	bm = bm[['Contest_Id','Contest','Selection_Id','Selection','ElectionDistrict_Id']]
	if bm.empty:
		bm['contest_type'] = None
	else:
		bm.loc[:,'contest_type'] = 'BallotMeasure'

	#  combine all contest_selections into one dataframe
	contest_selection = pd.concat([cc,bm])

	# append contest_district_type column
	ru = df['ReportingUnit'][['ReportingUnitType_Id','OtherReportingUnitType']]
	contest_selection = contest_selection.merge(ru,how='left',left_on='ElectionDistrict_Id',right_index=True)
	contest_selection = mr.enum_col_from_id_othertext(contest_selection,'ReportingUnitType',df['ReportingUnitType'])
	contest_selection.rename(columns={'ReportingUnitType':'contest_district_type'},inplace=True)

	#  limit to relevant ContestSelection pairs
	contest_ids = ecj.Contest_Id.unique()
	csj = contest_selection[contest_selection.Contest_Id.isin(contest_ids)]

	# find ReportingUnits of the correct type that are subunits of top_ru
	sub_ru_ids = child_rus_by_id(session,[top_ru_id],ru_type=[sub_rutype_id, sub_rutype_othertext])
	if not sub_ru_ids:
		# TODO better error handling (while not sub_ru_list....)
		raise Exception(f'Database {db} shows no ReportingUnits of type {sub_rutype} nested inside {top_ru}')
	sub_ru = df['ReportingUnit'].loc[sub_ru_ids]

	# find all subReportingUnits of top_ru
	all_subs_ids = child_rus_by_id(session,[top_ru_id])

	# find all children of subReportingUnits
	children_of_subs_ids = child_rus_by_id(session,sub_ru_ids)
	ru_children = df['ReportingUnit'].loc[children_of_subs_ids]

	# TODO check for any reporting units that should be included in roll-up but were missed
	missing = [x for x in all_subs_ids if x not in children_of_subs_ids]
	if missing:
		ui.report_problems(missing,msg=f'The following reporting units are nested in {top_ru["Name"]} '
								f'but are not nested in any {sub_rutype} nested in {top_ru["Name"]}')

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

	cis = 'unknown'  # TODO placeholder while CountItemStatus is unused
	if by_vote_type:
		cit_list = unsummed['CountItemType'].unique()
	else:
		cit_list = ['all']
		if exclude_total:
			unsummed = unsummed[unsummed.CountItemType != 'total']
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

	if by_vote_type:
		index_cols = ['contest_type','Contest','contest_district_type','Selection','ReportingUnit','CountItemType']
	else:
		index_cols = ['contest_type','Contest','contest_district_type','Selection','ReportingUnit']

	# sum by groups
	summed_by_name = unsummed[index_cols + ['Count']].groupby(index_cols).sum()

	inventory_columns = [
		'Election','ReportingUnitType','CountItemType','CountItemStatus',
		'source_db_url','timestamp']
	inventory_values = [
		election['Name'],sub_rutype,cit,cis,
		str(session.bind.url),datetime.date.today()]
	sub_dir = os.path.join(election['Name'],top_ru["Name"],f'by_{sub_rutype}')
	export_to_inventory_file_tree(
		target_dir,sub_dir,f'{count_item}.txt',inventory_columns,inventory_values,summed_by_name)

	return summed_by_name


def rollup_df(input_fpath):
	"""Gets rollup dataframe stored in file <input_fpath>"""
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
	#  without overcounting. Could do this with OfficeGroup element of CDF
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
	assert all([is_numeric_dtype(df[c]) for c in df.columns]), 'All columns of dataframe must be numeric'
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


def short_name(text,sep=';'):
	return text.split(sep)[-1]


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


def single_contest_selection_columns(rollup,contest,count_type):
	"""Given a rollup dataframe <rollup> and a single contest <contest> from <rollup>,
	and given a <count_type> from the CountType enumeration (e.g., 'absentee-mail')
	Returns dataframe of vote counts restricted to <contest> and <count_type>
	with columns labeled by selections """

	# filter by contest and vote type
	df = rollup[(rollup.Contest==contest) & (rollup.CountItemType==count_type)][
		['Contest','Selection','ReportingUnit','Count']].pivot(
		index='ReportingUnit',columns='Selection',values='Count'
	)
	return df


def top_two_total_columns(df):
	"""returns the dataframe <df> restricted to the top two columns (by total)"""
	df_copy = df.copy()
	top_two = df_copy.sum().nlargest(n=2,keep='all').index
	return df_copy[top_two]


def dropoff_one_contest(
		election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,output_dir,contest,
		comparison_contests,contest_type,
		contest_group_types=None):
	"""<contests> is a list of contests (or contest groups such as 'state-house'
	<contest_type> is a dictionary whose keys include all items in <contests>.
	<contest> is a contest in the comparison_contests list
	"""
	# TODO check: all items in <contests> are either contests or contest group types in <contest_group_types>
	# find all contest types represented in contests
	types = {contest_type[c] for c in comparison_contests}

	by_cc = by_contest_columns(
		election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,
		contest_group_types=contest_group_types,contest_types=types)
	dfa = diff_from_avg(by_cc,comparison_contests,mode='dropoff')
	d = diff_column(contest)

	extremes = [dfa[d].idxmax(),dfa[d].idxmin()]

	# create/find directory for output
	out_path = os.path.join(output_dir,election)
	Path(out_path).mkdir(parents=True,exist_ok=True)

	# open text file for reporting
	with open(os.path.join(out_path,f'{contest}.txt'),'w') as f:
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
		f'{contest} dropoff by {sub_ru_type}\nCorrection relative to contests\n{comps_text}',fontsize=10)
	for e in extremes:
		plt.annotate(short_name(e),(dfa.loc[e,contest]/1000,dfa.loc[e,d]/1000))
	plt.savefig(os.path.join(output_dir,election,f'scatter_{contest}.png'))
	plt.clf()

	out_df = dfa.loc[extremes,[diff_column(contest)]]
	out_df.loc[:,'Contest'] = contest
	out_df.reset_index(inplace=True)
	return out_df


def dropoff_contest_group(
		election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,output_dir,
		comparison_contests,contest_type,
		contest_group_types=None):
	# TODO identify "worst" contests
	"""Under construction""" # TODO
	extremes = {}
	for contest in comparison_contests:
		extremes[contest] = dropoff_one_contest(
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
	export_to_inventory_file_tree(
		output_dir,f'{election}/{comp_text}/{top_ru}/by_{sub_ru_type}','dropoff_extremes.txt',inv_cols,inv_vals)
	return pd.concat([three_most_undervoted,three_most_overvoted])


def process_single_contest(rollup,contest,output_dir):
	""" """  # TODO

	# create/find directory for output
	out_path = output_dir	# TODO pass more info for path, or simplify
	Path(out_path).mkdir(parents=True,exist_ok=True)

	# open text file for reporting
	with open(os.path.join(out_path,f'{contest}.txt'),'w') as f:
		f.write(f'{contest}\n\n')

		count_types = rollup['CountItemType'].unique()
		selections = rollup['Selection'].unique()

		for ct in count_types:
			f.write(f'\t{ct}\n')
			# filter out for contest and count_type
			df = single_contest_selection_columns(rollup,contest,ct)
			top_two = top_two_total_columns(df)
			top_selections = top_two.columns
			top_two_diff = diff_from_avg(top_two,top_selections)  # diffs of top two vote-getting selections
			# add column for margin
			top_two.loc[:,'margin'] = top_two.iloc[:,0] - top_two.iloc[:,1]
			for s in top_selections:
				f.write(f'\t\t{s}\n')
				extremes = [top_two_diff[diff_column(s)].idxmax(),top_two_diff[diff_column(s)].idxmin()]
				for e in extremes:
					f.write(f'\t\t\t{e}\tmargin {round(top_two.loc[e,"margin"],0)}\tdiff {round(top_two_diff.loc[e,diff_column(s)],0)}')

			# bar plot of top two results (votes)
			top_two[top_selections].plot.bar()
			plt.title(f'{short_name(contest)},{ct} votes')
			plt.savefig(os.path.join(output_dir,f'bar_votes_{ct}.png'))
			plt.clf()

			# bar plot of top two results (pcts)
			top_two_diff[[pct_column(s) for s in top_selections]].plot.bar()
			plt.title(f'{short_name(contest)},{ct} vote percentages')
			plt.savefig(os.path.join(output_dir,f'bar_pcts_{ct}.png'))
			plt.clf()

			# scatter plot margin vs. diff (winner - loser)
			plt.scatter(top_two['margin'],top_two_diff[diff_column(top_selections[0])])
			plt.xlabel(f'Margin')
			plt.ylabel(f'Correction')
			plt.suptitle(
				f'{contest}\n{ct}\nCorrect each county by average of others',fontsize=10)
			for e in extremes:
				plt.annotate(short_name(e),(top_two.loc[e,'margin'],top_two_diff.loc[e,diff_column(top_selections[0])]))
			plt.savefig(os.path.join(output_dir,f'scatter_{ct}.png'))
			plt.clf()

	# TODO return anomaly rating for contest
	return


def export_to_inventory_file_tree(target_dir,target_sub_dir,target_file,inventory_columns,inventory_values,df):
	# TODO standard order for columns
	# export to file system
	out_path = os.path.join(
		target_dir,target_sub_dir)
	Path(out_path).mkdir(parents=True,exist_ok=True)

	while os.path.isfile(os.path.join(out_path,target_file)):
		target_file = input(f'There is already a file called {target_file}. Pick another name.\n')

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
