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
		# TODO allow passage of top_ru name, from which id is deduced. Similarly for other args.
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


def short_name(text,sep=';'):
	return text.split(sep)[-1]


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
