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
