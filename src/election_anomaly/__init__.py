from election_anomaly import db_routines as dbr
from election_anomaly import user_interface as ui
from election_anomaly import munge_routines as mr
from sqlalchemy.orm import sessionmaker
import datetime
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

single_data_loader_pars = ['juris_name', 'munge_with', 'results_file_name',
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
					ui.archive_results(sdl.d['results_file_name'], self.d['results_dir'], self.d['archive_dir'])
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
		self.munger_list = [x.strip() for x in self.d['munge_with'].split(',')]

		# set aux_data_dir to None if appropriate
		if self.d['aux_data_dir'] in ['None','']:
			self.d['aux_data_dir'] = None

		# pick jurisdiction
		self.juris, self.juris_err = ui.pick_juris_from_filesystem(
			project_root,juris_name=self.d['juris_name'],check_files=True)

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
		if set(self.munger_err.values()) == {None}:
			self.munger_err = None

	def check_errors(self):
		juris_exists = None
		if not self.juris:
			juris_exists = {"juris_created": False}
		
		return self.parameter_err, self.juris_err, juris_exists, \
			self.juris_load_err, self.munger_err

	def track_results(self):
		filename = self.d['results_file_name']
		top_reporting_unit_id = dbr.name_to_id(self.session,'ReportingUnit', self.d['top_reporting_unit'])
		election_id = dbr.name_to_id(self.session,'Election',self.d['election'])

		data = pd.DataFrame(
			[[self.d['results_short_name'],filename,
			  self.d['results_download_date'], self.d['results_source'],
				self.d['results_note'], top_reporting_unit_id, election_id]],
			columns=['short_name', 'file_name',
					 'download_date', 'source',
					 'note', 'ReportingUnit_Id', 'Election_Id'])
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
				f_path = os.path.join(self.results_dir, self.d['results_file_name'])
				emu = ui.new_datafile(
					self.session, self.munger[mu], f_path ,self.project_root,
					self.juris, results_info=results_info, aux_data_dir=self.d['aux_data_dir'])
				if emu:
					err[mu] = emu
		if err == dict():
			err = None
		return err


class DataLoader():
	def __new__(self):
		""" Checks if parameter file exists and is correct. If not, does
		not create DataLoader object. """
		try:
			d, parameter_err = ui.get_runtime_parameters(data_loader_pars, param_file='dataloader.par')
		except FileNotFoundError as e:
			print("Parameter file not found. Ensure that it is located" \
				" in the current directory. DataLoader object not created.")
			return None

		if parameter_err:
			print("Parameter file missing requirements.")
			print(parameter_err)
			print("DataLoader object not created.")
			return None

		return super().__new__(self)

	def __init__(self):
		# grab parameters
		self.d, self.parameter_err = ui.get_runtime_parameters(
			data_loader_pars, optional_keys=['aux_data_dir'], param_file='dataloader.par')

		# results_file is the entire path, the _short version is just
		# the filename
		self.d['results_file_short'] = get_filename(self.d['results_file'])

		# pick jurisdiction
		self.juris, self.juris_err = ui.pick_juris_from_filesystem(
			self.d['project_root'],juris_name=self.d['juris_name'],check_files=True)

		# create db if it does not already exist
		error = dbr.establish_connection(paramfile=self.d['db_paramfile'], 
			db_name=self.d['db_name'])
		if error:
			dbr.create_new_db(self.d['project_root'], self.d['db_paramfile'], 
				self.d['db_name'])

		# connect to db
		self.engine = dbr.sql_alchemy_connect(paramfile=self.d['db_paramfile'],
			db_name=self.d['db_name'])
		Session = sessionmaker(bind=self.engine)
		self.session = Session()

		if self.juris:
			self.juris_load_err = self.juris.load_juris_to_db(self.session,
				self.d['project_root'])
		else:
			self.juris_load_err = None

		# pick munger
		self.munger, self.munger_err = ui.pick_munger(
			mungers_dir=os.path.join(self.d['project_root'],'mungers'),
			project_root=self.d['project_root'],
			munger_name=self.d['munger_name'])
	
	def check_errors(self):
		juris_exists = None
		if not self.juris:
			juris_exists = {"juris_created": False}
		
		return self.parameter_err, self.juris_err, juris_exists, \
			self.juris_load_err, self.munger_err


	def reload_requirements(self):
		if self.session:
			self.session.close()
		if self.engine:
			self.engine.dispose()

		self.d, self.parameter_err = ui.get_runtime_parameters(data_loader_pars, param_file='dataloader.par')
		self.d['results_file_short'] = get_filename(self.d['results_file'])

		# pick jurisdiction
		self.juris, self.juris_err = ui.pick_juris_from_filesystem(
			self.d['project_root'],juris_name=self.d['juris_name'],check_files=True)

		# create db if it does not already exist
		error = dbr.establish_connection(paramfile=self.d['db_paramfile'],
			db_name=self.d['db_name'])
		if error:
			dbr.create_new_db(self.d['project_root'], self.d['db_paramfile'], 
				self.d['db_name'])

		# connect to db
		eng = dbr.sql_alchemy_connect(paramfile=self.d['db_paramfile'],
			db_name=self.d['db_name'])
		Session = sessionmaker(bind=eng)
		self.session = Session()

		self.juris_load_err = self.juris.load_juris_to_db(self.session,
			self.d['project_root'])	

	
	def track_results(self):
		filename = self.d['results_file_short']
		top_reporting_unit_id = dbr.name_to_id(self.session,'ReportingUnit',self.d['top_reporting_unit'])
		election_id = dbr.name_to_id(self.session,'Election',self.d['election'])

		data = pd.DataFrame(
			[[self.d['results_short_name'],filename,
			  self.d['results_download_date'], self.d['results_source'],
				self.d['results_note'], top_reporting_unit_id, election_id]],
			columns=['short_name', 'file_name',
					 'download_date', 'source',
					 'note', 'ReportingUnit_Id', 'Election_Id'])
		[df,e] = dbr.dframe_to_sql(data,self.session,'_datafile')
		if e:
			return [0,0],e
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
			err = ui.new_datafile(self.session, self.munger, self.d['results_file'],self.d['project_root'],
			self.juris, results_info=results_info,aux_data_dir=self.d['aux_data_dir'])
		return err

# TODO allow rollups from several datafiles at once (e.g., if we get separate files from separate counties,
#  still want to be able to roll up to state.
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
		param_file = 'new_jurisdiction.par'
		try:
			d, parameter_err = ui.get_runtime_parameters(prep_pars, param_file='new_jurisdiction.par')
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
		error['dictionary'] = self.starter_dictionary()
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
				lambda row: prep.primary(row, p['cdf_internal_name'], 'internal'), axis=1)
			primaries[p['raw_identifier_value']]['raw_identifier_value'] = primaries[p['raw_identifier_value']].apply(
				lambda row: prep.primary(row, p['raw_identifier_value'], 'raw'), axis=1)

		if primaries:
			df_list = [df[['cdf_element', 'cdf_internal_name', 'raw_identifier_value']] for df in primaries.values()]
			df_list.append(d)
			new_dictionary = pd.concat(df_list)
		else:
			new_dictionary = d
		prep.write_element(self.d['jurisdiction_path'], 'dictionary', new_dictionary)
		return

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

	def add_elements_from_datafile(self, elements: iter, error: dict) -> dict:
		"""Add lines in dictionary.txt and <element>.txt corresponding to munged names not already in dictionary
		or not already in <element>.txt for each <element> in <elements>"""
		# read data from file
		if 'aux_data_dir' in self.d.keys():
			aux_data_dir = self.d['aux_data_dir']
		else:
			aux_data_dir = None
		mu = jm.Munger(
			os.path.join(self.d['project_root'], 'mungers', self.d['munger_name']), aux_data_dir=aux_data_dir,
			project_root=self.d['project_root'])
		wr, error = ui.read_combine_results(mu, self.d['results_file'], self.d['project_root'], error)
		wr.columns = [f'{x}_SOURCE' for x in wr.columns]

		missing = [x for x in ['results_file','munger_name'] if self.d[x] is None]

		if missing:
			ui.add_error(error,'datafile',f'Parameters missing: {missing}. Results file cannot be processed.')
			return error

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
			prep_pars,optional_keys=optional_prep_pars,param_file='new_jurisdiction.par')
		self.state_house = int(self.d['count_of_state_house_districts'])
		self.state_senate = int(self.d['count_of_state_senate_districts'])
		self.congressional = int(self.d['count_of_us_house_districts'])


def get_filename(path):
	head, tail = ntpath.split(path)
	return tail or ntpath.basename(head)
