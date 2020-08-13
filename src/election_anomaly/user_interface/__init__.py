from configparser import ConfigParser

from election_anomaly import db_routines
from election_anomaly import db_routines as dbr
from election_anomaly.db_routines import create_cdf_db as db_cdf
from election_anomaly import munge_routines as mr
import pandas as pd
from pandas.errors import ParserError, ParserWarning
import numpy as np
import csv
from sqlalchemy.orm import sessionmaker
import os
from pathlib import Path
import ntpath
import re
import datetime
from election_anomaly import juris_and_munger as jm
import random
from tkinter import filedialog
from configparser import MissingSectionHeaderError


recognized_encodings = {'iso2022jp', 'arabic', 'cp861', 'csptcp154', 'shiftjisx0213', '950', 'IBM775',
						'IBM861', 'shift_jis', 'euc_jp', 'ibm1026', 'ascii', 'IBM437', 'EBCDIC-CP-BE',
						'csshiftjis', 'cp1253', 'jisx0213', 'latin', 'cp874', '861', 'windows-1255', 'cp1361',
						'macroman', 'ms950', 'iso-2022-jp-3', 'iso8859_14', 'cp949', 'utf_16', '932', 'cp737',
						'iso2022_jp_2004', 'ks_c-5601', 'iso-2022-kr', 'ms936', 'cp819', 'iso-8859-3', 'windows-1258',
						'csiso2022kr', 'iso-8859-2', 'iso2022_jp_ext', 'hz', 'iso-8859-13', 'IBM855', 'cp1140', '866',
						'862', 'iso2022jp-2004', 'cp1250', 'windows-1254', 'cp1258', 'gb2312-1980', '936', 'L6',
						'iso-8859-6', 'ms932', 'macgreek', 'cp154', 'big5-tw', 'maccentraleurope', 'iso-8859-7',
						'ks_x-1001', 'csbig5', 'cp1257', 'latin1', 'mac_roman', 'euckr', 'latin3', 'eucjis2004',
						'437', 'cp500', 'mac_latin2', 'CP-GR', 'IBM863', 'hz-gb-2312', 'iso2022jp-3', 'iso-8859-15',
						'koi8_r', 'sjisx0213', 'windows-1252', '850', 'cp855', 'windows1256', 'eucjisx0213', 'hkscs',
						'gb18030', 'iso-2022-jp-2004', 'L1', 'cyrillic-asian', 'iso2022jp-ext', 'cp1006', 'utf16',
						'iso2022_kr', 'iso2022jp-2', 'shiftjis', 'IBM037', 'gb2312-80', 'IBM500', '865', 'UTF-16BE',
						'IBM864', 'EBCDIC-CP-CH', 'iso-8859-4', 'cp856', 'iso2022_jp_1', 'eucjp', 'iso-2022-jp-1',
						'iso8859_3', 'gb18030-2000', 'cp860', 'mskanji', 'iso2022jp-1', 'iso-8859-8',
						'iso-2022-jp-ext', 'csiso58gb231280', 'shift_jis_2004', 'L2', 'ms1361', 'cp852', 'ms949',
						'IBM865', 'cp437', 'iso8859_4', 'iso8859_2', 'cp1255', 'euc_jisx0213', 'cp1252', 'macturkish',
						'iso8859_9', 'ptcp154', '949', 'cp864', 's_jisx0213', 'big5-hkscs', 'korean', 'iso2022_jp_2',
						'cp932', 'euc-cn', 'latin5', 'utf_8', 'ibm1140', 'cp862', 'euc_kr', 'iso8859_8', 'iso-8859-9',
						'utf8', 'cp1251', '863', 'cp850', 'cp857', 'greek', 'latin8', 'iso2022_jp_3', 'iso-8859-10',
						'big5hkscs', 'ms-kanji', 'iso2022kr', '646', 'iso8859_7', 'koi8_u', 'mac_greek',
						'windows-1251', 'cp775', 'IBM860', 'u-jis', 'iso-8859-5', 'us-ascii', 'maccyrillic',
						'IBM866', 'L3', 'sjis2004', 'cp1256', 'sjis_2004', '852', 'windows-1250', 'latin4',
						'cp037', 'shift_jisx0213', 'greek8', 'latin6', 'latin2', 'mac_turkish', 'IBM862', 'iso8859-1',
						'cp1026', 'IBM852', 'pt154', 'iso-2022-jp-2', 'ujis', '855', 'iso-8859-14', 'iso-2022-jp',
						'utf_16_be', 'chinese', 'maclatin2', 'U7', 'hzgb', 'iso8859_5', '857', 'IBM850', '8859',
						'gb2312', 'cp866', 'CP-IS', 'latin_1', 'L4', 'euccn', 'cyrillic', 'IBM424', 'cp863',
						'UTF-16LE', 'mac_cyrillic', 'iso8859_10', 'L8', 'IBM869', 'ksc5601', '860', 'iso2022_jp',
						'hz-gb', 'UTF', 'utf8ascii', 'utf_7', 'cp936', 'euc_jis_2004', 'iso-ir-58', 'csiso2022jp',
						'IBM039', 'eucgb2312-cn', 'cp950', 'iso8859_13', 'shiftjis2004', 'sjis', 'U8', 'cp1254',
						's_jis', 'gbk', 'hebrew', 'U16', 'big5', 'cp865', 'cp424', 'uhc', 'windows-1257', '869',
						'iso-8859-1', 'windows-1253', 'ksx1001', 'johab', 'IBM857', 'L5', 'iso8859_6', 'cp869',
						'cp875', 'mac_iceland', 'iso8859_15', 'maciceland', 'utf_16_le', 'EBCDIC-CP-HE',
						'ks_c-5601-1987'}


def pick_path(initialdir='~/',mode='file'):
	"""Creates pop-up window for user to choose a <mode>, starting from <initialdir>.
	Returns chosen file path or directory path (depending on <mode>"""

	while True:
		fpath = input(
			f'Enter path to {mode} (or hit return to use pop-up window to find it).\n').strip()
		if not fpath:
			print(f'Use pop-up window to pick your {mode}.')
			if mode == 'file':
				fpath = filedialog.askopenfilename(
					initialdir=initialdir,title=f"Select {mode}",
					filetypes=(("text files","*.txt"),("csv files","*.csv"),("ini files","*.ini"),("all files","*.*")))
			elif mode == 'directory':
				fpath = filedialog.askdirectory(initialdir=initialdir,title=f'Select {mode}')
			else:
				print(f'Mode {mode} not recognized')
				return None

			print(f'The {mode} you chose is:\n\t{fpath}')
			break
		elif (mode == 'file' and not os.path.isfile(fpath)) or (mode == 'directory' and not os.path.isdir(fpath)):
			print(f'This is not a {mode}: {fpath}\nTry again.')
		else:
			break
	return fpath


def pick_paramfile(msg='Locate the parameter file for your postgreSQL database.'):
	print(msg)
	fpath= pick_path()
	return fpath


def make_par_files(dir: str, munger_name: str, top_ru,election,download_date,source,results_note=None,aux_data_dir=''):
	data_file_list = os.listdir(dir)
	for f in data_file_list:
		par_text = f'[election_anomaly]\nresults_file={f}\njuris_name=Florida\nmunger_name={munger_name}\ntop_reporting_unit={top_ru}\nelection={election}\nresults_short_name={top_ru}_{f}\nresults_download_date={download_date}\nresults_source={source}\nresults_note={results_note}\naux_data_dir={aux_data_dir}\n'
		par_name = '.'.join(f.split('.')[:-1]) + '.par'
		with open(os.path.join(dir,par_name),'w') as p:
			p.write(par_text)
	return


def get_params_to_read_results(d: dict, results_file, munger_name) -> (dict, list):
	kwargs = d
	if results_file:
		kwargs['results_file'] = results_file
	if munger_name:
		kwargs['munger_name'] = munger_name
	missing = [x for x in ['results_file', 'munger_name', 'project_root'] if kwargs[x] is None]
	return kwargs, missing

def read_results(params, error: dict) -> (pd.DataFrame, jm.Munger, dict):
	"""Reads results (appending '_SOURCE' to the columns)
	and initiates munger. <params> must include these keys: 
	'project_root', 'munger_name', 'results_file'"""
	if 'aux_data_dir' in params.keys():
		aux_data_dir = params['aux_data_dir']
	else:
		aux_data_dir = None
	mu = jm.Munger(
		os.path.join(params['project_root'], 'mungers', params['munger_name']), aux_data_dir=aux_data_dir,
		project_root=params['project_root'])
	wr, error = read_combine_results(mu, params['results_file'], params['project_root'], error)
	wr.columns = [f'{x}_SOURCE' for x in wr.columns]
	return wr, mu, error


def pick_juris_from_filesystem(juris_path,project_root,check_files=False):
	"""Returns a State object. <juris_path> is the path to the directory containing the
	defining files for the particular jurisdiction.
	"""

	missing_values = {}

	if check_files:
		missing_values = jm.ensure_jurisdiction_dir(juris_path,project_root)

	# initialize the jurisdiction
	if missing_values:
		ss = None
	else:
		ss = jm.Jurisdiction(juris_path)
	return ss, missing_values


def find_dupes(df):
	dupes_df = df[df.duplicated()].drop_duplicates(keep='first')
	deduped = df.drop_duplicates(keep='first')
	return dupes_df, deduped


def pick_munger(mungers_dir='mungers',project_root=None, munger_name=None):
	munger_path = os.path.join(project_root,mungers_dir,munger_name)
	error = jm.ensure_munger_files(munger_path,project_root=project_root)

	munger_path = os.path.join(mungers_dir,munger_name)

	if not error:
		munger = jm.Munger(munger_path, project_root=project_root,check_files=False)
		#munger_error is None unless internal inconsistency found
		munger_error = munger.check_against_self()
		return munger, munger_error
	else:
		return None, error


def read_single_datafile(munger: jm.Munger, f_path: str, err: dict) -> [pd.DataFrame, dict]:
	try:
		dtype = {c: str for c in munger.field_list}
		kwargs = {'thousands': munger.thousands_separator, 'dtype': dtype}

		if munger.field_name_row is None:
			kwargs['header'] = None
			kwargs['names'] = munger.field_names_if_no_field_name_row
		else:
			kwargs['header'] = list(range(munger.header_row_count))

		if munger.file_type in ['txt', 'csv']:
			kwargs['encoding'] = munger.encoding
			kwargs['quoting'] = csv.QUOTE_MINIMAL
			kwargs['index_col'] = None
			if munger.file_type == 'txt':
				kwargs['sep'] = '\t'
			df = pd.read_csv(f_path, **kwargs)
		elif munger.file_type in ['xls', 'xlsx']:
			df = pd.read_excel(f_path, **kwargs)
		else:
			e = f'Unrecognized file_type in munger: {munger.file_type}'
			if 'format.txt' in err.keys():
				err['format.txt'].append(e)
			else:
				err['format.txt'] = [e]
		if df.empty:
			e = f'Nothing read from datafile; file type {munger.file_type} may be inconsistent, or datafile may be empty.'
			if 'format.txt' in err.keys():
				err['format.txt'].append(e)
			else:
				err['format.txt'] = [e]
		else:
			df = mr.generic_clean(df)
			err = jm.check_results_munger_compatibility(munger, df, err)
		return [df, err]
	except UnicodeDecodeError as ude:
		e = f'Encoding error. Datafile not read completely.\n{ude}'
	except ParserError as pe:
		# DFs have trouble comparing against None. So we return an empty DF and 
		# check for emptiness below as an indication of an error.
		e = f'Error parsing results file.\n{pe}'
	if 'datafile' in err.keys():
		err['datafile'].append(e)
	else:
		err['datafile'] = [e]
	return [pd.DataFrame(), err]


def read_combine_results(mu: jm.Munger, results_file, project_root, err, aux_data_dir=None):
	working, err = read_single_datafile(mu, results_file, err)
	if [k for k in err.keys() if err[k] != None]:
		return pd.DataFrame(), err
	else:
		working = mr.cast_cols_as_int(working, mu.count_columns,mode='index')

		# merge with auxiliary files (if any)
		if aux_data_dir is not None:
			# get auxiliary data (includes cleaning and setting (multi-)index of primary key column(s))
			aux_data,err = mu.get_aux_data(aux_data_dir, err,project_root=project_root)
			for abbrev,r in mu.aux_meta.iterrows():
				# cast foreign key columns of main results file as int if possible
				foreign_key = r['foreign_key'].split(',')
				working = mr.cast_cols_as_int(working,foreign_key)
				# rename columns
				col_rename = {f'{c}':f'{abbrev}[{c}]' for c in aux_data[abbrev].columns}
				# merge auxiliary info into <working>
				a_d = aux_data[abbrev].rename(columns=col_rename)
				working = working.merge(a_d,how='left',left_on=foreign_key,right_index=True)

	return working, err


def archive_results(file_name: str, current_dir: str, archive_dir: str):
	"""Move <file_name> from <current_dir> to <archive_dir>. If <archive_dir> already has a file with that name,
	prefix <prefix> to the file name and try again. If that doesn't work, add prefix and timestamp"""
	archive = Path(archive_dir)
	archive.mkdir(parents=True, exist_ok=True)
	old_path = os.path.join(current_dir,file_name)
	new_path = os.path.join(archive_dir,file_name)
	i = 0
	while os.path.exists(new_path):
		i += 1
		new_path = os.path.join(archive_dir, f'{i}_{file_name}')

	os.rename(old_path, new_path)
	return


def new_datafile(
		session,munger: jm.Munger, raw_path: str, project_root: str, juris: jm.Jurisdiction,
		results_info: list=None, aux_data_dir: str=None) -> dict:
	"""Guide user through process of uploading data in <raw_file>
	into common data format.
	Assumes cdf db exists already"""
	err = dict()
	raw, err = read_combine_results(munger, raw_path, project_root,err,aux_data_dir=aux_data_dir)
	if raw.empty:
		e = f'No data read from datafile {raw_path}.'
		add_error(err,'datafile',e)
		return err
	
	count_columns_by_name = [raw.columns[x] for x in munger.count_columns]

	try:
		raw = mr.munge_clean(raw, munger)
	except:
		err['datafile'] = ['Cleaning of datafile failed. Results not loaded to database.']
		return err

	# NB: info_cols will have suffix added by munger

	# if jurisdiction changed, load to db
	juris.load_juris_to_db(session,project_root)

	try:
		err = mr.raw_elements_to_cdf(session,project_root,juris,munger,raw,count_columns_by_name,err,ids=results_info)
	except:
		e = 'Unspecified error during munging. Results not loaded to database.'
		if 'datafile' in err.keys():
			err['datafile'].append(e)
		else:
			err['datafile'] = [e]
		return err

	print(f'Results uploaded with munger {munger.name} '
		  f'to database {session.bind.engine}\nfrom file {raw_path}\n'
		  f'assuming jurisdiction {juris.path_to_juris_dir}')
	if err == dict():
		err = None
	return err


def config(filename=None, section='postgresql',msg='Pick parameter file for connecting to the database'):
	"""
	Creates a parameter dictionary <d> from the section <section> in <filename>
	default section is info needed to log into our db
	"""
	d = {}
	if not filename:
		# if parameter file is not provided, ask for it
		# initialize root widget for tkinter
		filename = pick_paramfile(msg=msg)

	# create a parser
	parser = ConfigParser()
	# read config file

	try:
		parser.read(filename)
	except MissingSectionHeaderError as e:
		print(e)
		d = config(filename=None,section=section)
		return d

	# get section, default to postgresql
	if parser.has_section(section):
		params = parser.items(section)
		for param in params:
			d[param[0]] = param[1]
	else:
		print(f'Section {section} not found in the {filename} file. Try again.')
		d = config(section=section,msg=msg)
	return d


def get_runtime_parameters(required_keys, optional_keys=None,param_file='run_time.par'):
	d = {}
	missing_required_params = {'missing':[]}

	parser = ConfigParser()
	p = parser.read(param_file)
	if len(p) == 0:
		raise FileNotFoundError

	for k in required_keys:
		try:
			d[k] = parser['election_anomaly'][k]
		except KeyError:
			missing_required_params['missing'].append(k)

	if optional_keys is None:
		optional_keys = []
	for k in optional_keys:
		try:
			d[k] = parser['election_anomaly'][k]
		except KeyError:
			d[k] = None

	if not missing_required_params['missing']:
		missing_required_params = None

	return d, missing_required_params

def add_error(err: dict, key: str, msg: str) -> dict:
	if msg:
		if msg in err.keys():
			err[key].append(msg)
		else:
			err[key] = msg
	return err



