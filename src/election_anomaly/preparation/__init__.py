# Routines to aid in preparing Jurisdiction and Munger files
import pandas as pd
import os
import munge_routines as mr
import user_interface as ui
import juris_and_munger as jm
from pathlib import Path


def primary(row: pd.Series, party: str, mode: str) -> str:
	if mode == 'internal':
		pr = f'{row["contest_internal"]} ({party})'
	elif mode == 'raw':
		pr = f'{row["contest_raw"]} ({party})'
	else:
		pr = None
	return pr


def primary_contests_no_dictionary(contests: pd.DataFrame, parties: pd.DataFrame) -> pd.DataFrame:
	"""Returns a dataframe <p_contests> of all primary contests corresponding to lines in <contests>
	and parties in <parties>,"""
	c_df = {}

	for i, p in parties.iterrows():
		c_df[i] = contests.copy()
		c_df[i]['Name'] = contests["Name"] + f' ({p["Name"]} Primary)'
		c_df[i]['PrimaryParty'] = p['Name']

	p_contests = pd.concat([c_df[i] for i in parties.index])
	return p_contests


def get_element(juris_path: str,element: str) -> pd.DataFrame:
	"""<juris> is path to jurisdiction directory. Info taken
	from <element>.txt file in that directory. If file doesn't exist,
	empty dataframe returned"""
	f_path = os.path.join(juris_path,f'{element}.txt')
	if os.path.isfile(f_path):
		element_df = pd.read_csv(f_path,sep='\t')
	else:
		element_df = pd.DataFrame()
	return element_df


def write_element(juris_path: str, element: str, df: pd.DataFrame):
	"""<juris> is path to jurisdiction directory. Info taken
	from <element>.txt file in that directory"""
	df.drop_duplicates().fillna('').to_csv(os.path.join(juris_path,f'{element}.txt'),index=False,sep='\t')
	return


def add_default_parties(juris_path: str):
	party_list = ['Democratic','Republican','Green','Libertarian']
	old_party = get_element(juris_path,'Party')
	new_party = pd.DataFrame([[f'{p} Party'] for p in party_list],columns=['Name'])
	write_element(juris_path,'Party',pd.concat([old_party,new_party]).drop_duplicates())
	return


def add_primary_contests(juris_path: str,error: dict) -> dict:
	"""Revise CandidateContest.txt
	to add all possible primary contests. """

	# get all contests that are not already primaries
	contests = get_element(juris_path,'CandidateContest')
	contests = contests[contests['PrimaryParty'].isnull()]
	if contests.empty:
		add_or_append_msg(error,'jurisdiction','CandidateContest.txt is missing or has no non-primary contests. No primary contests added.')
		return error
	parties = get_element(juris_path,'Party')
	if parties.empty:
		add_or_append_msg(error,'jurisdiction','Party.txt is missing or empty. No primary contests added.')
		return error

	p_contests = primary_contests_no_dictionary(contests, parties)

	# overwrite CandidateContest.txt with new info
	new_contests = pd.concat([contests, p_contests]).drop_duplicates().fillna('')
	new_contests.to_csv(os.path.join(juris_path,'CandidateContest.txt'),sep='\t',index=False)

	return error


def add_or_append_msg(d: dict, key: str, msg: str):
	if key in d.keys():
		d[key].append(msg)
	else:
		d[key] = [msg]
	return d


def add_district_contests(juris_path: str,count: dict,ru_type: dict):
	"""<juris> is path to jurisdiction directory.
	Keys of <count> are contest family names;
	value is the number of districts for that
	family of contests"
	Keys of <ru_type> are the contest family names
	value is reporting unit type for that contests family"""

	w_office = get_element(juris_path,'Office')
	w_ru = get_element(juris_path,'ReportingUnit')
	w_cc = get_element(juris_path,'CandidateContest')
	new_office = {}
	new_ru = {}
	new_cc = {}
	cols_off = ['Name','ElectionDistrict']
	cols_ru = ['Name','ReportingUnitType']
	cols_cc = ['Name','NumberElected','Office','PrimaryParty']
	for k in count.keys():
		w_office = w_office.append(pd.DataFrame([
			[f'{k} District {i+1}',f'{k} District {i+1}'] for i in range(count[k])
		],columns=cols_off),ignore_index=True)
		w_ru = w_ru.append(pd.DataFrame([
			[f'{k} District {i+1}',ru_type[k]] for i in range(count[k])
		],columns=cols_ru),ignore_index=True)
		w_cc = w_cc.append(pd.DataFrame([
			[f'{k} District {i + 1}',1,f'{k} District {i + 1}',''] for i in range(count[k])
		],columns=cols_cc),ignore_index=True)

	write_element(juris_path,'Office',w_office.drop_duplicates())
	write_element(juris_path,'ReportingUnit',w_ru.drop_duplicates())
	write_element(juris_path,'CandidateContest',w_cc.drop_duplicates())
	return


def new_juris_files(
		juris_path: str, project_root: str, abbr: str,state_house: int,state_senate: int,congressional: int,
		error: dict, other_districts: dict=None):
	"""<juris_path> identifies the directory where the files will live.
	<abbr> is the two-letter abbreviation for state/district/territory.
	<state_house>, etc., gives the number of districts;
	<other_districts> is a dictionary of other district names, types & counts, e.g.,
	{'Circuit Court':{'ReportingUnitType':'judicial','count':5}}
	"""
	# create directory if it doesn't exist
	error['jurisdiction'] = jm.ensure_jurisdiction_dir(juris_path,project_root,ignore_empty=True)

	# add default parties
	add_default_parties(juris_path)

	# add all district Offices/RUs/CandidateContests
	count = {f'{abbr} House':state_house,f'{abbr} Senate':state_senate,f'US House {abbr}':congressional}
	ru_type = {f'{abbr} House':'state-house',f'{abbr} Senate':'state-senate',f'US House {abbr}':'congressional'}
	if other_districts:
		for k in other_districts.keys():
			count[k] = other_districts[k]['count']
			ru_type[k] = other_districts[k]['ReportingUnitType']
	add_district_contests(juris_path,count,ru_type)

	# add all primary CandidateContests
	error = add_primary_contests(juris_path,error)
	return error


def add_elements_from_datafile(
		juris: str,results: pd.DataFrame,mu: jm.Munger,element: str, error: dict,
		name_field='Name') -> dict:
	"""Add lines in dictionary.txt and <element>.txt corresponding to munged names not already in dictionary
	or not already in <element>.txt"""
	wr =results.copy()
	# append <element>_raw
	wr.columns = [f'{x}_SOURCE' for x in wr.columns]
	mr.add_munged_column(wr, mu, element, error, mode=mu.cdf_elements.loc[element,'source'])
	# find <element>_raw values not in dictionary.txt.raw_identifier_value;
	#  add corresponding lines to dictionary.txt
	wd = get_element(juris,'dictionary')
	old_raw = wd[wd.cdf_element==element]['raw_identifier_value'].to_list()
	new_raw = [x for x in wr[f'{element}_raw'] if x not in old_raw]
	new_raw_df = pd.DataFrame(
		[[element,x,x] for x in new_raw],
		columns=['cdf_element','cdf_internal_name','raw_identifier_value'])
	wd = pd.concat([wd,new_raw_df]).drop_duplicates()
	write_element(juris,'dictionary',wd)

	# TODO find cdf_internal_names that are not in <element>.txt and add them to <element>.txt
	we = get_element(juris,element)
	old_internal = we[name_field].to_list()
	new_internal = [x for x in wd[wd.cdf_element == element]['cdf_internal_name'] if x not in old_internal]
	new_internal_df = pd.DataFrame([[x] for x in new_internal],columns=[name_field])
	we = pd.concat([we,new_internal_df]).drop_duplicates()
	write_element(juris,element,we)
	# if <element>.txt has columns other than <name_field>, notify user
	if we.shape[1] > 1 and not new_internal_df.empty:
		add_or_append_msg(
			error,'Jurisdiction',
			f'New rows added to {element}.txt, but data may be missing from some fields in those rows.')
	return error


def add_primaries_to_dict(juris_path):
	primaries = {}
	# read CandidateContest.txt, Party.txt and dictionary.txt
	cc = get_element(juris_path,'CandidateContest')
	p = get_element(juris_path,'Party')
	d = get_element(juris_path,'dictionary')
	# for each CandidateContest line in dictionary.txt with cdf_identifier in CandidateContest.txt
	# and for each Party line in dictionary.txt with cdf_identifier in Party.txt
	# append corresponding line in dictionary.txt
	party_d = d[(d['cdf_element']=='Party') & (d['cdf_internal_name'].isin(p['Name'].tolist()))]
	contest_d = d[(d['cdf_element']=='CandidateContest') & (d['cdf_internal_name'].isin(cc['Name'].tolist()))]
	for i, p in party_d.iterrows():
		primaries[p['raw_identifier_value']] = contest_d.copy().rename(
			columns={'cdf_internal_name':'contest_internal','raw_identifier_value':'contest_raw'})
		primaries[p['raw_identifier_value']]['cdf_internal_name'] = primaries[p['raw_identifier_value']].apply(
			lambda row: primary(row, p['cdf_internal_name'], 'internal'),axis=1)
		primaries[p['raw_identifier_value']]['raw_identifier_value'] = primaries[p['raw_identifier_value']].apply(
			lambda row: primary(row, p['raw_identifier_value'], 'raw'),axis=1)

	if primaries:
		df_list = [df[['cdf_element','cdf_internal_name','raw_identifier_value']] for df in primaries.values()]
		df_list.append(d)
		new_dictionary = pd.concat(df_list)
	else:
		new_dictionary = d
	write_element(juris_path,'dictionary',new_dictionary)
	return

