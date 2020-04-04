#!usr/bin/python3

import pandas as pd
import user_interface as ui
import db_routines as dbr
import munge_routines as mr

def rollup(session,db,top_ru,sub_ru_type,atomic_ru_type,election):
	"""<top_ru> is the internal cdf name of the ReportingUnit whose results will be reported
	(e.g., Florida or Pennsylvania;Philadelphia).
	<sub_ru_type> is the ReportingUnitType of the ReportingUnits used in each line of the results file
	for <election> created by the routine. (E.g., county or ward)
	<atomic_ru_type> is the ReportingUnitType in the database from which the results
	at the <sub_ru_type> level are calculated. (E.g., county or precinct)
	<session> and <db> provide access to the db containing results"""

	# Get id for top reporting unit
	ru = pd.read_sql_table('ReportingUnit',session.bind,index_col='Id')
	top_ru_ids = ru[ru.Name == top_ru]
	if top_ru_ids.shape[0] == 0:
		raise Exception(f'No ReportingUnit named {top_ru} in database {db}')
	elif top_ru_ids.shape[0] > 1:
		raise Exception(f'More than one ReportingUnit named {top_ru} in database {db}.')
	else:
		top_ru_id = top_ru_ids.first_valid_index

	rut = pd.read_sql_table('ReportingUnitType',session.bind)
	atomic_ru_type_id, atomic_other_ru_type = mr.get_id_othertext_from_enum_value(rut,atomic_ru_type)

	# find atomic RUs nested inside the top RU
	atomic_ru = ru[
		ru.ReportingUnitType_Id == atomic_ru_type_id & ru.OtherReportingUnitType == atomic_other_ru_type
		]
	cruj = pd.read_sql_table('ComposingReportingUnitJoin',session.bind)
	in_top_ru = cruj[cruj.ParentReportingUnit_Id == top_ru_id].ChildReportingUnit.unique().to_list() + [top_ru_id]
	atomic_ru = atomic_ru.iloc[in_top_ru]
	if atomic_ru.empty:
		raise Exception(f'Database {db} shows no ReportingUnits of type {atomic_ru_type} nested inside {top_ru}')

	sub_ru_type_id, sub_other_ru_type = mr.get_id_othertext_from_enum_value(rut,sub_ru_type)

	# TODO check completeness: do RUs of type sub_ru_type cover the relevant RUs of atomic_ru_type?

	# TODO pull relevant tables

	# TODO calculate specified dataframe with columns [ReportingUnit,Contest,Selection,Count,CountItemType]

	# TODO export to appropriate file
	return