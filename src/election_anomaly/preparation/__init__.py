# Routines to aid in preparing Jurisdiction and Munger files
import pandas as pd
import os
import munge_routines as mr
import user_interface as ui
import juris_and_munger as jm


def primary_contests(
		dictionary: pd.DataFrame, contests: pd.DataFrame, parties: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
	"""Returns a dataframe of all primary contests corresponding to lines in <contests>
	and parties in <parties>"""
	c_df = {}
	d_df = {}
	# add column with raw identifiers to <parties>
	get_party = dictionary[dictionary['cdf_element'] == 'Party']
	parties = parties.merge(
		get_party[['cdf_internal_name', 'raw_identifier_value']],
		how='left',left_on='Name',right_on='cdf_internal_name')

	get_con = dictionary[dictionary['cdf_element'] == 'CandidateContest']
	for i, p in parties.iterrows():
		c_df[i] = contests.copy()
		working = contests.merge(
			get_con,
			how='inner', left_on='Name', right_on='cdf_internal_name'
		)
		# assign 'cdf_element' to 'CandidateContest, since we're creating CandidateContest lines
		# TODO generalize next line to handle other cases
		working['raw_identifier_value'] = working['raw_identifier_value'] + ';' + \
											p['raw_identifier_value'] + ' Primary'

		c_df[i]['Name'] = working['cdf_internal_name'] = \
			contests["Name"] + f' ({p["Name"]} Primary)'
		c_df[i]['PrimaryParty'] = p['Name']

		d_df[i] = working[['cdf_element', 'cdf_internal_name', 'raw_identifier_value']]

	p_contests = pd.concat([c_df[i] for i in parties.index])
	p_dictionary_rows = pd.concat([d_df[i] for i in parties.index])
	return p_contests, p_dictionary_rows


def get_element(juris: str, element: str) -> pd.DataFrame:
	"""<juris> is path to jurisdiction directory. Info taken
	from <element>.txt file in that directory"""
	element_df = pd.read_csv(os.path.join(juris, f'{element}.txt'),sep='\t')
	return element_df


def add_primary_contests(juris: str):
	"""Revise CandidateContest.txt and dictionary.txt
	to add all possible primary contests. Assume raw identifier
	is built as <contest> (<party>)"""
	# TODO allow user to specify other raw identifiers.

	contests = get_element(juris, 'CandidateContest')
	parties = get_element(juris, 'Party')
	dictionary = get_element(juris, 'dictionary')

	[p_contests, p_dictionary] = primary_contests(dictionary, contests, parties)

	new_contests = pd.concat([contests, p_contests]).drop_duplicates().fillna('')
	new_dictionary = pd.concat([dictionary,p_dictionary]).drop_duplicates().fillna('')

	# TODO overwrite CandidateContest.txt and dictionary.txt
	new_contests.to_csv(os.path.join(juris,'revised_CandidateContest.txt'),sep='\t',index=False)
	new_dictionary.to_csv(os.path.join(juris,'revised_dictionary.txt'),sep='\t',index=False)
	return


def add_candidates_from_datafile(juris: str,results_df: pd.DataFrame,mu: jm.Munger,warn: dict) -> dict:
	"""Add candidate names to Candidate.txt and dictionary.txt.
	Set cdf_internal_name equal to raw_identifier"""
	err = {}
	# list fields needed for candidate and party
	c_fields = mu.cdf_elements.loc['Candidate','fields']
	p_fields = mu.cdf_elements.loc['Party','fields']
	fields = list(set(c_fields).union(set(p_fields)))

	# extract raw identifiers of candidates and parties
	can_par_df = results_df[fields].drop_duplicates()
	can_par_df.columns = [f'{x}_SOURCE' for x in can_par_df.columns]
	mr.add_munged_column(can_par_df,mu,'Party',err, mode=mu.cdf_elements.loc['Party','source'])
	mr.add_munged_column(can_par_df,mu,'Candidate', err, mode=mu.cdf_elements.loc['Candidate','source'])

	# append columns with internal cdf_names (using _raw value if item is not in dictionary)
	old_dictionary = get_element(juris,'dictionary')
	party_map = {r['raw_identifier_value']:r['cdf_internal_name'] for
			i,r in old_dictionary.iterrows() if r['cdf_element'] == 'Party'}
	candidate_map = {r['raw_identifier_value']:r['cdf_internal_name'] for
			i,r in old_dictionary.iterrows() if r['cdf_element'] == 'Candidate'}
	can_par_df['Candidate.BallotName'] = can_par_df['Candidate_raw'].map(candidate_map)
	can_par_df.loc[(can_par_df['Candidate.BallotName'].isnull()),'Candidate.BallotName'] = can_par_df.loc[
		(can_par_df['Candidate.BallotName'].isnull()),'Candidate_raw']
	can_par_df['Party.Name'] = can_par_df['Party_raw'].map(party_map)
	can_par_df.loc[(can_par_df['Party.Name'].isnull()),'Party.Name'] = can_par_df.loc[
		(can_par_df['Party.Name'].isnull()),'Party_raw']

	# parties
	old_party = get_element(juris,'Party')
	results_party = pd.DataFrame(columns=old_party.columns)
	results_party['raw_identifier_value'] = can_par_df['Party_raw'].drop_duplicates()
	results_party['Name'] = results_party['raw_identifier_value'].map(party_map)
	# find any novel parties; prepare to add them to party.txt and dictionary.txt
	novel_party = [p for p in results_party['Name'].to_list() if p not in old_party['Name'].to_list()]
	if novel_party:
		w = f'New parties will be added: {novel_party}'
		if 'Jurisdiction' in warn.keys():
			warn['Jurisdiction'].append(w)
		else:
			warn['Juridiction'] = [w]
		novel_party_df = results_party[results_party['Name'].isin(novel_party)]
		novel_dictionary_p = novel_party_df.copy()
		novel_dictionary_p.rename(columns={'Name','cdf_internal_name'},inplace=True)
		mr.add_constant_column(novel_dictionary_p,'cdf_element','Party')
	else:
		novel_dictionary_p = pd.DataFrame()

	# candidates
	old_candidate = get_element(juris,'Candidate')
	results_candidate = can_par_df[['Candidate_raw','Party_raw']].drop_duplicates()

	# TODO find novel candidates, prepare to add to Candidate.txt and dictionary.txt
	novel = results_candidate[['BallotName','Party']][
		~results_candidate[['BallotName','Party']].isin(old_candidate[['BallotName','Party']].to_dict('list')).all(1)]
	if novel.empty:
		novel_dictionary_c = pd.DataFrame()
	else:
		w = f'New candidates will be added:\n {novel}'
		if 'Jurisdiction' in warn.keys():
			warn['Jurisdiction'].append(w)
		else:
			warn['Juridiction'] = [w]
		novel_dictionary_c = novel.copy()
		novel_dictionary_c.rename(columns={'BallotName','cdf_internal_name'},inplace=True)
		mr.add_constant_column(novel_dictionary_c,'cdf_element','Candidate')

	results_dictionary = pd.DataFrame(columns=old_dictionary.columns)
	results_dictionary.loc[:,['cdf_internal_name','raw_identifier_value']] = can_par_df[
		['Candidate_raw','Candidate_raw']].drop_duplicates()
	results_dictionary.loc[:,'cdf_element'] = 'Candidate'

#######

	write_candidate = pd.concat([old_candidate,novel_candidate])
	write_dictionary = pd.concat([old_dictionary,novel_dictionary_p,novel_dictionary_c])

	# TODO overwrite Candidate.txt and Dictionary.txt and Party.txt

	return warn


