from election_anomaly import db_routines as dbr
from election_anomaly import user_interface as ui
from election_anomaly import munge_routines as mr
from sqlalchemy.orm import sessionmaker
import datetime
from pathlib import Path
import os
import pandas as pd
from pprint import pprint
import sys
import ntpath
from election_anomaly import analyze_via_pandas as avp
from election_anomaly import juris_and_munger as jm
from election_anomaly import preparation as prep

# constants
data_loader_pars = [
	'project_root','juris_name','db_paramfile','db_name','munger_name',
	'results_file', 'results_short_name', 'results_download_date', 'results_source', 'results_note',
	'top_reporting_unit','election']

single_data_loader_pars = ['jurisdiction_path', 'munger_name', 'results_file',
	'results_short_name', 'results_download_date', 'results_source', 'results_note',
	'top_reporting_unit', 'election', 'aux_data_dir']

multi_data_loader_pars = ['project_root', 'db_paramfile', 'db_name', 'results_dir', 'archive_dir']

prep_pars = ['project_root', 'jurisdiction_path', 'name', 'abbreviated_name',
				'count_of_state_house_districts',
				'count_of_state_senate_districts',
				'count_of_us_house_districts',
				'reporting_unit_type']

optional_prep_pars = ['results_file', 'munger_name']

analyze_pars = ['db_paramfile', 'db_name', 'results_file_short_name']

# classes
class MultiDataLoader():
	def __new__(self):
		""" Checks if parameter file exists and is correct. If not, does
		not create MultiDataLoader object. """
		try:
			d, parameter_err = ui.get_runtime_parameters(
				multi_data_loader_pars,param_file='multi.par')
		except FileNotFoundError as e:
			print("Parameter file multi.par not found. Ensure that it is located" \
				  " in the current directory. MultiDataLoader object not created.")
			return None

		if parameter_err:
			print("Parameter file missing requirements.")
			print(parameter_err)
			print("DataLoader object not created.")
			return None

		return super().__new__(self)

	def __init__(self):
		# grab parameters
		self.d, self.parameter_err = ui.get_runtime_parameters(multi_data_loader_pars,param_file='multi.par')
	
		# create db if it does not already exist
		error = dbr.establish_connection(paramfile=self.d['db_paramfile'], db_name=self.d['db_name'])
		if error:
			dbr.create_new_db(self.d['project_root'], self.d['db_paramfile'],  self.d['db_name'])
	
		# connect to db
		self.engine = dbr.sql_alchemy_connect(paramfile=self.d['db_paramfile'],  db_name=self.d['db_name'])
		Session = sessionmaker(bind=self.engine)
		self.session = Session()

	def load_all(self):
		"""returns a dictionary of any files that threw an error"""
		err = dict()
		munger_path = os.path.join(self.d['project_root'],'mungers')
		par_files = [f for f in os.listdir(self.d['results_dir']) if f[-4:] == '.par']
		for f in par_files:
			sdl = SingleDataLoader(self.d['results_dir'], f, self.d['project_root'], self.session, munger_path)
			errors = sdl.check_errors()
			if errors == (None,None,None,None,None):
				# try to load data
				load_error = sdl.load_results()
				if load_error:
					err[f] = load_error
				else:
					# move results file and its parameter file to the archive directory
					ui.archive_results(f, self.d['results_dir'], self.d['archive_dir'])
					ui.archive_results(sdl.d['results_file'], self.d['results_dir'], self.d['archive_dir'])
			else:
				err[f] = errors
		return err


class SingleDataLoader():
	def __init__(self, results_dir, par_file_name, project_root, session, munger_path):
		# adopt passed variables needed in future as attributes
		self.project_root = project_root
		self.session = session
		self.results_dir = results_dir
		# grab parameters
		par_file = os.path.join(results_dir,par_file_name)
		self.d, self.parameter_err = ui.get_runtime_parameters(
			single_data_loader_pars, optional_keys=['aux_data_dir'], param_file=par_file)

		# convert comma-separated list to python list
		# TODO document
		self.munger_list = [x.strip() for x in self.d['munger_name'].split(',')]

		# set aux_data_dir to None if appropriate
		if self.d['aux_data_dir'] in ['None','']:
			self.d['aux_data_dir'] = None

		# pick jurisdiction
		self.juris, self.juris_err = ui.pick_juris_from_filesystem(
			self.d['jurisdiction_path'],project_root,check_files=True)

		if self.juris:
			self.juris_load_err = self.juris.load_juris_to_db(session, project_root)
		else:
			self.juris_load_err = None

		# pick mungers
		self.munger = dict()
		self.munger_err = dict()
		for mu in self.munger_list:
			self.munger[mu], self.munger_err[mu] = ui.pick_munger(
				mungers_dir=munger_path,
				project_root=project_root, munger_name=mu)
		# if no munger throws an error:
		if all([x is None for x in self.munger_err.values()]):
			self.munger_err = None

	def check_errors(self):
		juris_exists = None
		if not self.juris:
			juris_exists = {"juris_created": False}
		
		return self.parameter_err, self.juris_err, juris_exists, \
			self.juris_load_err, self.munger_err

	def track_results(self):
		filename = self.d['results_file']
		top_reporting_unit_id = dbr.name_to_id(self.session,'ReportingUnit', self.d['top_reporting_unit'])
		election_id = dbr.name_to_id(self.session,'Election',self.d['election'])

		data = pd.DataFrame(
			[[self.d['results_short_name'],filename,
			  self.d['results_download_date'], self.d['results_source'],
				self.d['results_note'], top_reporting_unit_id, election_id,datetime.datetime.now()]],
			columns=['short_name', 'file_name',
					 'download_date', 'source',
					 'note', 'ReportingUnit_Id', 'Election_Id','created_at'])
		[df,e] = dbr.dframe_to_sql(data,self.session,'_datafile')
		if e:
			return [0, 0], e
		else:
			datafile_id = df[(df['short_name']== self.d['results_short_name']) &
						 (df['file_name']==filename) & (df['ReportingUnit_Id']==top_reporting_unit_id) &
						 (df['Election_Id']==election_id)]['Id'].to_list()[0]
			return [datafile_id, election_id], e

	def load_results(self):
		results_info, e = self.track_results()
		if e:
			err = {'database':e}
		else:
			err = dict()
			for mu in self.munger_list:
				f_path = os.path.join(self.results_dir, self.d['results_file'])
				emu = ui.new_datafile(
					self.session, self.munger[mu], f_path ,self.project_root,
					self.juris, results_info=results_info, aux_data_dir=self.d['aux_data_dir'])
				if emu:
					err[mu] = emu
		if err == dict():
			err = None
		return err


class Analyzer():
	def __new__(self):
		""" Checks if parameter file exists and is correct. If not, does
		not create DataLoader object. """
		try:
			d, parameter_err = ui.get_runtime_parameters(analyze_pars, param_file='analyze.par')
		except FileNotFoundError as e:
			print("Parameter file not found. Ensure that it is located" \
				" in the current directory. Analyzer object not created.")
			return None

		if parameter_err:
			print("Parameter file missing requirements.")
			print(parameter_err)
			print("Analyzer object not created.")
			return None

		return super().__new__(self)

	def __init__(self):
		self.d, self.parameter_err = ui.get_runtime_parameters(analyze_pars,param_file='analyze.par')

		eng = dbr.sql_alchemy_connect(paramfile=self.d['db_paramfile'],
			db_name=self.d['db_name'])
		Session = sessionmaker(bind=eng)
		self.session = Session()

	def display_options(self, input):
		results = dbr.get_input_options(self.session, input)
		if results:
			return results
		return None

	def top_counts_by_vote_type(self, rollup_unit, sub_unit):
		d, error = ui.get_runtime_parameters(['rollup_directory'], param_file='analyze.par')
		if error:
			print("Parameter file missing requirements.")
			print(error)
			print("Data not created.")
			return
		else:
			rollup_unit_id = dbr.name_to_id(self.session, 'ReportingUnit', rollup_unit)
			sub_unit_id = dbr.name_to_id(self.session, 'ReportingUnitType', sub_unit)
			results_info = dbr.get_datafile_info(self.session, self.d['results_file_short_name'])
			rollup = avp.create_rollup(self.session, d['rollup_directory'], top_ru_id=rollup_unit_id,
				sub_rutype_id=sub_unit_id, sub_rutype_othertext='', datafile_id_list=results_info[0], 
				election_id=results_info[1])
			return

	def top_counts(self, rollup_unit, sub_unit):
		d, error = ui.get_runtime_parameters(['rollup_directory'], param_file='analyze.par')
		if error:
			print("Parameter file missing requirements.")
			print(error)
			print("Data not created.")
			return
		else:
			rollup_unit_id = dbr.name_to_id(self.session, 'ReportingUnit', rollup_unit)
			sub_unit_id = dbr.name_to_id(self.session, 'ReportingUnitType', sub_unit)
			results_info = dbr.get_datafile_info(self.session, self.d['results_file_short_name'])
			rollup = avp.create_rollup(self.session, d['rollup_directory'], top_ru_id=rollup_unit_id,
				sub_rutype_id=sub_unit_id, sub_rutype_othertext='', datafile_id_list=results_info[0], 
				election_id=results_info[1], by_vote_type=False)
			return


class JurisdictionPrepper():
	def __new__(cls):
		""" Checks if parameter file exists and is correct. If not, does
		not create JurisdictionPrepper object. """
		param_file = 'jurisdiction_prep.par'
		try:
			d, parameter_err = ui.get_runtime_parameters(prep_pars, param_file='jurisdiction_prep.par')
		except FileNotFoundError as e:
			print(f"File {param_file} not found. Ensure that it is located" \
				  " in the current directory. DataLoader object not created.")
			return None

		if parameter_err:
			print(f"File {param_file} missing requirements.")
			print(parameter_err)
			print("JurisdictionPrepper object not created.")
			return None
		return super().__new__(cls)
	
	def new_juris_files(self, other_districts: dict = None):
		"""<juris_path> identifies the directory where the files will live.
		<abbr> is the two-letter abbreviation for state/district/territory.
		<state_house>, etc., gives the number of districts;
		<other_districts> is a dictionary of other district names, types & counts, e.g.,
		{'Circuit Court':{'ReportingUnitType':'judicial','count':5}}
		"""
		# TODO Feature: allow other districts to be set in paramfile
		error = dict()
		# create directory if it doesn't exist
		jm.ensure_jurisdiction_dir(self.d['jurisdiction_path'], self.d['project_root'], ignore_empty=True)

		# add default entries
		templates = os.path.join(self.d['project_root'],'templates/jurisdiction_templates')
		for element in ['Party','Election']:
			prep.add_defaults(self.d['jurisdiction_path'],templates,element)

		# add all standard Offices/RUs/CandidateContests
		self.add_standard_contests()

		# Feature create starter dictionary.txt with cdf_internal name
		#  used as placeholder for raw_identifier_value
		e = self.starter_dictionary()
		if e:
			ui.add_erro(error,'dictionary',e)
		return error

	def add_primaries_to_dict(self) -> str:
		error = None
		# TODO add real error handling
		primaries = {}
		# read CandidateContest.txt, Party.txt and dictionary.txt
		cc = prep.get_element(self.d['jurisdiction_path'], 'CandidateContest')
		p = prep.get_element(self.d['jurisdiction_path'], 'Party')
		d = prep.get_element(self.d['jurisdiction_path'], 'dictionary')
		# for each CandidateContest line in dictionary.txt with cdf_identifier in CandidateContest.txt
		# and for each Party line in dictionary.txt with cdf_identifier in Party.txt
		# append corresponding line in dictionary.txt
		party_d = d[(d['cdf_element'] == 'Party') & (d['cdf_internal_name'].isin(p['Name'].tolist()))]
		contest_d = d[(d['cdf_element'] == 'CandidateContest') & (d['cdf_internal_name'].isin(cc['Name'].tolist()))]
		for i, p in party_d.iterrows():
			primaries[p['raw_identifier_value']] = contest_d.copy().rename(
				columns={'cdf_internal_name': 'contest_internal', 'raw_identifier_value': 'contest_raw'})
			primaries[p['raw_identifier_value']]['cdf_internal_name'] = primaries[p['raw_identifier_value']].apply(
				lambda row: prep.primary(row, p['cdf_internal_name'], 'contest_internal'), axis=1)
			primaries[p['raw_identifier_value']]['raw_identifier_value'] = primaries[p['raw_identifier_value']].apply(
				lambda row: prep.primary(row, p['raw_identifier_value'], 'contest_raw'), axis=1)

		if primaries:
			df_list = [df[['cdf_element', 'cdf_internal_name', 'raw_identifier_value']] for df in primaries.values()]
			df_list.append(d)
			new_dictionary = pd.concat(df_list)
		else:
			new_dictionary = d
		prep.write_element(self.d['jurisdiction_path'], 'dictionary', new_dictionary)
		return error

	def add_standard_contests(self, juriswide_contests: list=None, other_districts: dict=None):
		"""If <juriswide_contest> is None, use standard list hard-coded."""
		name = self.d['name']
		abbr = self.d["abbreviated_name"]
		count = {f'{abbr} House': self.state_house, f'{abbr} Senate': self.state_senate, f'US House {abbr}': self.congressional}
		ru_type = {f'{abbr} House': 'state-house', f'{abbr} Senate': 'state-senate',
				f'US House {abbr}': 'congressional'}
		if other_districts:
			for k in other_districts.keys():
				count[k] = other_districts[k]['count']
				ru_type[k] = other_districts[k]['ReportingUnitType']

		w_office = prep.get_element(self.d['jurisdiction_path'], 'Office')
		w_ru = prep.get_element(self.d['jurisdiction_path'], 'ReportingUnit')
		w_cc = prep.get_element(self.d['jurisdiction_path'], 'CandidateContest')
		cols_off = ['Name', 'ElectionDistrict']
		cols_ru = ['Name', 'ReportingUnitType']
		cols_cc = ['Name', 'NumberElected', 'Office', 'PrimaryParty']

		# add all district offices/contests/reportingunits
		for k in count.keys():
			w_office = w_office.append(pd.DataFrame([
				[f'{k} District {i + 1}', f'{name};{k} District {i + 1}'] for i in range(count[k])
			], columns=cols_off), ignore_index=True)
			w_ru = w_ru.append(pd.DataFrame([
				[f'{name};{k} District {i + 1}', ru_type[k]] for i in range(count[k])
			], columns=cols_ru), ignore_index=True)
			w_cc = w_cc.append(pd.DataFrame([
				[f'{k} District {i + 1}', 1, f'{k} District {i + 1}', ''] for i in range(count[k])
			], columns=cols_cc), ignore_index=True)

		# append top jurisdiction reporting unit
		top_ru = {'Name': self.d['name'], 'ReportingUnitType':self.d['reporting_unit_type']}
		w_ru = w_ru.append(top_ru, ignore_index=True)

		# add standard jurisdiction-wide offices
		if not juriswide_contests:
			juriswide_contests = [f'US President ({abbr})',f'{abbr} Governor', f'US Senate {abbr}', f'{abbr} Attorney General',
								  f'{abbr} Lieutenant Governor', f'{abbr} Treasurer']
		# append jurisdiction-wide offices
		jw_off = pd.DataFrame(
			[[x, self.d['name']] for x in juriswide_contests],columns=cols_off)
		w_office = w_office.append(jw_off,ignore_index=True)

		# append jurisdiction-wide contests
		jw_cc = pd.DataFrame(
			[[x, 1, x, ''] for x in juriswide_contests], columns=cols_cc
		)
		w_cc = w_cc.append(jw_cc,ignore_index=True)

		prep.write_element(self.d['jurisdiction_path'], 'Office', w_office.drop_duplicates())
		prep.write_element(self.d['jurisdiction_path'], 'ReportingUnit', w_ru.drop_duplicates())
		prep.write_element(self.d['jurisdiction_path'], 'CandidateContest', w_cc.drop_duplicates())
		return

	def add_primaries_to_candidate_contest(self):
		primaries = {}
		error = None

		# get contests that are not already primaries
		contests = prep.get_element(self.d['jurisdiction_path'], 'CandidateContest')
		non_p_contests = contests[contests['PrimaryParty'].isnull()]
		if non_p_contests.empty:
			error = 'CandidateContest.txt is missing or has no non-primary contests. No primary contests added.'
			return error

		# get parties
		parties = prep.get_element(self.d['jurisdiction_path'], 'Party')
		if parties.empty:
			if error:
				error += '\n Party.txt is missing or empty. No primary contests added.'
			else:
				error = '\n Party.txt is missing or empty. No primary contests added.'
			return error

		for i, party in parties.iterrows():
			p = party['Name']
			primaries[p] = non_p_contests.copy()
			primaries[p]['Name'] = non_p_contests.apply(lambda row: prep.primary(row,p,'Name'),axis=1)
			primaries[p]['PrimaryParty'] = p

		all_primaries = [primaries[p] for p in parties.Name.unique()]
		prep.write_element(
			self.d['jurisdiction_path'], 'CandidateContest',pd.concat([contests] + all_primaries))
		return error

	def add_sub_county_rus_from_results_file(
			self, error: dict, sub_ru_type: str='precinct', results_file_path=None, munger_name=None, **kwargs) -> dict:
		"""Assumes precincts (or other sub-county reporting units)
		are munged from row of the results file.
		Adds corresponding rows to ReportingUnit.txt and dictionary.txt
		using internal County name correctly"""

		# get parameters from arguments; otherwise from self.d; otherwise throw error
		kwargs, missing = ui.get_params_to_read_results(self.d, results_file_path, munger_name)
		if missing:
			ui.add_error(error,'datafile',f'Parameters missing: {missing}. Results file cannot be processed.')
			return error

		# read data from file (appending _SOURCE)
		wr, munger, error = ui.read_results(kwargs,error)

		# reduce <wr> in size
		fields = [f'{field}_SOURCE' for field in munger.cdf_elements.loc['ReportingUnit','fields']]
		wr = wr[fields].drop_duplicates()

		# get formulas from munger
		ru_formula = munger.cdf_elements.loc['ReportingUnit', 'raw_identifier_formula']
		try:
			[county_formula,sub_ru_formula] = ru_formula.split(';')
		except ValueError:
			ui.add_error(error,'munge_error',f'ReportingUnit formula in munger {munger.name} has wrong format (should have two parts separated by ;)')
			return error

		# add columns for county and sub_ru
		wr, error = mr.add_column_from_formula(wr,county_formula, 'County_raw', error, suffix='_SOURCE')
		wr, error = mr.add_column_from_formula(wr,sub_ru_formula, 'Sub_County_raw', error, suffix='_SOURCE')

		# add column for county internal name
		ru_dict_old = prep.get_element(self.d['jurisdiction_path'],'dictionary')
		ru_dict_new = ru_dict_old[ru_dict_old.cdf_element=='ReportingUnit']
		wr = wr.merge(ru_dict_new,how='left',left_on='County_raw',right_on='raw_identifier_value').rename(columns={'cdf_internal_name':'County_internal'})

		# add required new columns
		wr = mr.add_constant_column(wr,'ReportingUnitType',sub_ru_type)
		wr = mr.add_constant_column(wr,'cdf_element','ReportingUnit')
		wr['Name'] = wr.apply(lambda x: f'{x["County_internal"]};{x["Sub_County_raw"]}',axis=1)
		wr['raw_identifier_value'] = wr.apply(lambda x: f'{x["County_raw"]};{x["Sub_County_raw"]}',axis=1)

		# add info to ReportingUnit.txt
		ru_add = wr[['Name','ReportingUnitType']]
		ru_old = prep.get_element(self.d['jurisdiction_path'],'ReportingUnit')
		prep.write_element(self.d['jurisdiction_path'],'ReportingUnit',pd.concat([ru_old,ru_add]))

		# add info to dictionary
		wr.rename(columns={'Name':'cdf_internal_name'},inplace=True)
		dict_add = wr[['cdf_element','cdf_internal_name','raw_identifier_value']]
		prep.write_element(self.d['jurisdiction_path'],'dictionary',pd.concat([ru_dict_old,dict_add]))		# TODO test this!!!
		return error

	def add_sub_county_rus_from_multi_results_file(self, dir: str, error: dict, sub_ru_type: str='precinct') -> dict:
		"""Adds all elements in <elements> to <element>.txt and, naively, to <dictionary.txt>
		for each file in <dir> named (with munger) in a .par file in the directory"""
		for par_file_name in [x for x in os.listdir(dir) if x[-4:]=='.par']:
			par_file = os.path.join(dir, par_file_name)
			file_dict, missing_params = ui.get_runtime_parameters(
				['results_file','munger_name'], optional_keys=['aux_data_dir'], param_file=par_file)
			file_dict['sub_ru_type'] = sub_ru_type
			file_dict['results_file_path'] = os.path.join(dir,file_dict['results_file'])
			if missing_params:
				ui.add_error(error, 'parameter_file', f'Parameters missing from {par_file_name}:{missing_params}')
			else:
				error = self.add_sub_county_rus_from_results_file(error, ** file_dict)
		return error

	def add_elements_from_multi_results_file(self, elements: iter, dir: str, error: dict):
		"""Adds all elements in <elements> to <element>.txt and, naively, to <dictionary.txt>
		for each file in <dir> named (with munger) in a .par file in the directory"""
		for par_file_name in [x for x in os.listdir(dir) if x[-4:]=='.par']:
			par_file = os.path.join(dir, par_file_name)
			file_dict, missing_params = ui.get_runtime_parameters(
				['results_file','munger_name'], optional_keys=['aux_data_dir'], param_file=par_file)
			file_dict['results_file_path'] = os.path.join(dir,file_dict['results_file'])
			if missing_params:
				ui.add_error(error, 'parameter_file', f'Parameters missing from {par_file_name}:{missing_params}')
			else:
				error = self.add_elements_from_results_file(elements, error, ** file_dict)
		return error

	def add_elements_from_results_file(self, elements: iter, error: dict, results_file_path=None, munger_name=None, **kwargs) -> dict:
		"""Add lines in dictionary.txt and <element>.txt corresponding to munged names not already in dictionary
		or not already in <element>.txt for each <element> in <elements>"""

		# get parameters from arguments; otherwise from self.d; otherwise throw error
		# get parameters from arguments; otherwise from self.d; otherwise throw error
		kwargs, missing = ui.get_params_to_read_results(self.d, results_file_path, munger_name)
		if missing:
			ui.add_error(error,'datafile',f'Parameters missing: {missing}. Results file cannot be processed.')
			return error

		# read data from file (appending _SOURCE)
		wr, mu, error = ui.read_results(kwargs,error)

		for element in elements:
			name_field = dbr.get_name_field(element)
			# append <element>_raw
			wr, error = mr.add_munged_column(
				wr, mu, element, error, mode=mu.cdf_elements.loc[element, 'source'],
				inplace=False)
			# find <element>_raw values not in dictionary.txt.raw_identifier_value;
			#  add corresponding lines to dictionary.txt
			wd = prep.get_element(self.d['jurisdiction_path'], 'dictionary')
			old_raw = wd[wd.cdf_element == element]['raw_identifier_value'].to_list()
			new_raw = [x for x in wr[f'{element}_raw'] if x not in old_raw]
			new_raw_df = pd.DataFrame(
				[[element, x, x] for x in new_raw],
				columns=['cdf_element', 'cdf_internal_name', 'raw_identifier_value'])
			wd = pd.concat([wd, new_raw_df]).drop_duplicates()
			prep.write_element(self.d['jurisdiction_path'], 'dictionary', wd)

			# find cdf_internal_names that are not in <element>.txt and add them to <element>.txt
			we = prep.get_element(self.d['jurisdiction_path'], element)
			old_internal = we[name_field].to_list()
			new_internal = [x for x in wd[wd.cdf_element == element]['cdf_internal_name'] if x not in old_internal]
			# TODO guide user to check dictionary for bad stuff before running this
			#  e.g., primary contests already in dictionary cause a problem.
			new_internal_df = pd.DataFrame([[x] for x in new_internal], columns=[name_field])
			we = pd.concat([we, new_internal_df]).drop_duplicates()
			prep.write_element(self.d['jurisdiction_path'], element, we)
			# if <element>.txt has columns other than <name_field>, notify user
			if we.shape[1] > 1 and not new_internal_df.empty:
				ui.add_error(error,'preparation',
							 f'New rows added to {element}.txt, but data may be missing from some fields in those rows.')
		return error

	def starter_dictionary(self,include_existing=True) -> str:
		"""Creates a starter file for dictionary.txt, assuming raw_identifiers are the same as cdf_internal names.
		Puts file in the current directory"""
		w = dict()
		elements = ['BallotMeasureContest','Candidate','CandidateContest','Election','Office','Party','ReportingUnit']
		old = prep.get_element(self.d['jurisdiction_path'],'dictionary')
		if not include_existing:
			old.drop()
		for element in elements:
			w[element] = prep.get_element(self.d['jurisdiction_path'],element)
			name_field = dbr.get_name_field(element)
			w[element] = mr.add_constant_column(w[element],'cdf_element',element)
			w[element].rename(columns={name_field:'cdf_internal_name'},inplace=True)
			w[element]['raw_identifier_value'] = w[element]['cdf_internal_name']

		starter_file_name = f'{self.d["abbreviated_name"]}_starter_dictionary.txt'
		starter = pd.concat(
				[w[element][[
					'cdf_element',
					'cdf_internal_name',
					'raw_identifier_value']] for element in elements]).drop_duplicates()
		err = prep.write_element(
			'.','dictionary',starter,
			file_name=starter_file_name)
		print(f'Starter dictionary created in current directory (not in jurisdiction directory):\n{starter_file_name}')
		return err

	def __init__(self):
		self.d, self.parameter_err = ui.get_runtime_parameters(
			prep_pars,optional_keys=optional_prep_pars,param_file='jurisdiction_prep'
																  '.par')
		self.state_house = int(self.d['count_of_state_house_districts'])
		self.state_senate = int(self.d['count_of_state_senate_districts'])
		self.congressional = int(self.d['count_of_us_house_districts'])


def make_par_files(dir: str, munger_name: str, top_ru: str, election: str, download_date: str, source: str,
				   results_note: str=None, aux_data_dir: str=''):
	"""Utility to create parameter files for multiple files. Makes a parameter file for each file in <dir>,
	once all other necessary parameters are specified. """
	data_file_list = os.listdir(dir)
	for f in data_file_list:
		par_text = f'[election_anomaly]\nresults_file_name={f}\njuris_name=Florida\nmunge_with={munger_name}\ntop_reporting_unit={top_ru}\nelection={election}\nresults_short_name={top_ru}_{f}\nresults_download_date={download_date}\nresults_source={source}\nresults_note={results_note}\naux_data_dir={aux_data_dir}\n'
		par_name = '.'.join(f.split('.')[:-1]) + '.par'
		with open(os.path.join(dir,par_name),'w') as p:
			p.write(par_text)
	return

def get_filename(path):
	head, tail = ntpath.split(path)
	return tail or ntpath.basename(head)
