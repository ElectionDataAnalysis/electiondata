from configparser import ConfigParser

from election_anomaly import db_routines
from election_anomaly import db_routines as dbr
from election_anomaly.db_routines import create_cdf_db as db_cdf
from election_anomaly import munge_routines as mr
import pandas as pd
import numpy as np
import csv
from sqlalchemy.orm import sessionmaker
import os
from pathlib import Path
import ntpath
import re
import datetime
from election_anomaly import juris_and_munger as sf
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


def get_project_root():
	p_root = os.getcwd().split('election_anomaly')[0]
	confirmed = False
	subdir_list = ['election_anomaly','jurisdictions','mungers']
	while not confirmed:
		missing = [x for x in subdir_list if x not in os.listdir(p_root)]
		print(f'\nSuggested project root directory is:\n\t{p_root}')
		if missing:
			print(f'The suggested directory does not contain required subdirectories {",".join(missing)}')
			new_pr = input(f'Designate a different project root (y/n)?\n')
			if new_pr == 'y':
				p_root = input(f'Enter absolute path of project root.\n')
			else:
				input('Add required subdirectories and hit return to continue.\n')
		elif input('Is this the correct project root (y/n)?\n') == 'y':
			confirmed = True
	return p_root


def pick_file_or_directory(description=None,mode=None):
	if not mode:
		print(f'No mode specified')
		return None
	elif mode not in ['file','directory']:
		print(f'Mode {mode} not recognized')
		return None
	else:
		if not description:
			description = f'the {mode}'
		print(f'Use the pop-up window to pick {description}.')
		directory = pick_path(mode=mode)
	return directory


def track_results_file(project_root,sess,results_file):
	filename = ntpath.basename(results_file)
	db_idx, datafile_record_d, datafile_enumeration_name_d, datafile_fk_name_d = pick_or_create_record(
		sess,project_root,'_datafile',known_info_d={'file_name':filename})
	# TODO typing url into debug window opens the web page; want it to just act like a string
	return [datafile_record_d, datafile_enumeration_name_d]


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


def pick_one(choices,return_col,item='row',required=False,max_rows=40):
	"""Returns index and <return_col> value of item chosen by user
	<choices> is a dataframe, unless <return_col> is None, in which case <choices>
	may be a list or a set"""

	if return_col is None:
		df = pd.DataFrame(np.array(list(choices)).transpose(),columns=[item])
		return_col = item
		choices = df  # regularizes 'choices.index[choice]' in return
	else:
		df = choices.copy()
		df.index = range(choices.shape[0])
	if df.empty:
		return None, None
	with pd.option_context('display.max_rows',max_rows,'display.max_columns',None):
		print(df)

	choice = -1  # guaranteed not to be in df.index

	while choice not in df.index:
		if not required:
			req_str=' (or nothing, if your choice is not on the list)'
		else:
			req_str=''
		choice_str = input(f'Enter the number of the desired {item}{req_str}:\n')
		if choice_str == '' and not required:
			return None,None
		else:
			try:
				choice = int(choice_str)
				if choice not in df.index:
					print(f'Enter an option from the leftmost column. Please try again.')
			except ValueError:
				print(f'You must enter a number{req_str}, then hit return. Please try again.')
	print(f'Chosen {item} is {df.loc[choice,return_col]}\n\n')

	return choices.index[choice], df.loc[choice,return_col]


def pick_paramfile(msg='Locate the parameter file for your postgreSQL database.'):
	print(msg)
	fpath= pick_path()
	return fpath


def show_sample(input_iter,items,condition,outfile='shown_items.txt',export_dir=None,export=False):
	print(f'There are {len(input_iter)} {items} that {condition}:')
	if len(input_iter) == 0:
		return
	if isinstance(input_iter,pd.DataFrame):
		st = input_iter.to_csv(sep='\t').split('\n')
	else:
		st = list(input_iter)
	st.sort()

	if len(st) < 11:
		show_list = st
	else:
		print('(sample)')
		show_list = random.sample(st,10)
		show_list.sort()
	for r in show_list:
		print(r)
	if len(st) > 10:
		show_all = input(f'Show all {len(st)} {items} that {condition} (y/n)?\n')
		if show_all == 'y':
			for r in st:
				print(f'{r}')
	if export:
		if export_dir is None:
			export_dir = input(f'Export all {len(st)} {items} that {condition}? If so, enter directory for export.'
						f'Existing file will be overwritten.\n'
						f'(Current directory is {os.getcwd()})\n')
		if os.path.isdir(export_dir):
			export = input(f'Export all {len(st)} {items} that {condition} to {outfile} (y/n)?\n')
			if export == 'y':
				with open(os.path.join(export_dir,outfile),'w') as f:
					f.write('\n'.join(st))
				print(f'{items} exported to {os.path.join(export_dir,outfile)}')
		elif export_dir != '':
			print(f'Directory {export_dir} does not exist.')
	return


def pick_database(project_root,paramfile=None,db_name=None):
	"""Establishes connection to db with name <db_name>,
	or creates a new cdf_db with that name.
	In any case, returns the name of the DB."""
	if not paramfile:
		paramfile = pick_paramfile()
	[con, paramfile] = dbr.establish_connection(paramfile=paramfile)
	print(f'Connection established to database {con.info.dbname}')
	cur = con.cursor()
	db_df = dbr.get_database_names(con)
	if db_name and db_name in db_df.datname.unique():
		print(f'Will use existing database {db_name}')
		desired_db = db_name
		create_new = False
	elif db_name:  # but not in existing
		desired_db = db_name
		dbr.create_database(con,cur,desired_db)
		create_new = True
	else:  # if no db_name given
		db_idx,desired_db = pick_one(db_df,'datname',item='database')
		if db_idx:
			create_new = False
		else:  # if we're going to need a brand new db
			desired_db = get_alphanumeric_from_user('Enter name for new database (alphanumeric only)')
			create_new = True
			while desired_db in db_df.datname.unique() and create_new:
				use_existing = input(f'Database {desired_db} exists! Use existing database {desired_db} (y/n)?\n')
				if use_existing == 'y':
					create_new = False
				else:
					desired_db = get_alphanumeric_from_user('Enter name for new database (alphanumeric only)')
			if create_new:  # then desired_db is not in the list of dbs
				dbr.create_database(con,cur,desired_db)
			# TODO otherwise check that desired_db has right format?

	if create_new: 	# if our db is brand new
		# connect to the desired_db
		eng = dbr.sql_alchemy_connect(paramfile=paramfile,db_name=desired_db)
		Session = sessionmaker(bind=eng)
		sess = Session()

		# load cdf tables
		db_cdf.create_common_data_format_tables(
			sess,dirpath=os.path.join(project_root,'election_anomaly','CDF_schema_def_info'))
		db_cdf.fill_cdf_enum_tables(
			sess,None,dirpath=os.path.join(project_root,'election_anomaly/CDF_schema_def_info/'))
		print(f'New database {desired_db} has been created using the common data format.')

	# clean up
	if cur:
		cur.close()
	if con:
		con.close()
	return desired_db


def pick_juris_from_filesystem(project_root,juriss_dir='jurisdictions',juris_name=None,check_files=False):
	"""Returns a State object.
	If <jurisdiction_name> is given, this just initializes based on info
	in the folder with that name; """

	path_to_jurisdictions = os.path.join(project_root,juriss_dir)
	# if no jurisdiction name provided, ask user to pick from file system
	if juris_name is None:
		# ask user to pick from the available ones
		print('Pick the filesystem directory for your jurisdiction.')
		choice_list = [x for x in os.listdir(path_to_jurisdictions) if
					os.path.isdir(os.path.join(path_to_jurisdictions,x))]
		juris_idx,juris_name = pick_one(choice_list,None,item='jurisdiction')

		# if nothing picked, ask user for alphanumeric name and create necessary files
		if not juris_idx:
			juris_name = get_alphanumeric_from_user('Enter a directory name for your jurisdiction files.')
			juris_path = os.path.join(path_to_jurisdictions,juris_name)
			sf.ensure_jurisdiction_files(juris_path,project_root)
		elif check_files:
			juris_path = os.path.join(path_to_jurisdictions,juris_name)
			sf.ensure_jurisdiction_files(juris_path,project_root)
		else:
			print(
				f'WARNING: if necessary files are missing from the directory {juris_name},\nsystem may fail.')
	else:
		if check_files:
			juris_path = os.path.join(path_to_jurisdictions,juris_name)
			sf.ensure_jurisdiction_files(juris_path,project_root)
		else:
			print(
				f'WARNING: if necessary files are missing from the directory {juris_name},\nsystem may fail.')

	# initialize the jurisdiction
	ss = sf.Jurisdiction(juris_name,path_to_jurisdictions)
	return ss


def find_dupes(df):
	dupes_df = df[df.duplicated()].drop_duplicates(keep='first')
	deduped = df.drop_duplicates(keep='first')
	return dupes_df, deduped


def pick_munger(mungers_dir='mungers',project_root=None,session=None,munger_name=None):
	"""pick (or create) a munger """
	if not project_root:
		project_root = get_project_root()
	if not munger_name:
		choice_list = os.listdir(mungers_dir)
		for choice in os.listdir(mungers_dir):
			c_path = os.path.join(mungers_dir,choice)
			if not os.path.isdir(c_path):  # remove non-directories from list
				choice_list.remove(choice)
			elif not os.path.isfile(os.path.join(c_path,'raw_columns.txt')):
				pass  # list any munger that doesn't have raw_columns.txt file yet
			else:
				elts = pd.read_csv(os.path.join(c_path,'cdf_elements.txt'),header=0,dtype=str,sep='\t')
				row_formulas = elts[elts.source=='row'].raw_identifier_formula.unique()
				necessary_cols = set()
				for formula in row_formulas:
					# extract list of necessary fields
					pattern = '<(?P<field>[^<>]+)>'  # finds field names
					p = re.compile(pattern)
					necessary_cols.update(p.findall(formula))

		munger_df = pd.DataFrame(choice_list,columns=['Munger'])
		munger_idx,munger_name = pick_one(munger_df,'Munger', item='munger')
		if munger_idx is None:
			# user chooses munger
			munger_name = get_alphanumeric_from_user(
				'Enter a short name (alphanumeric only, no spaces) for your munger (e.g., \'nc_primary18\')\n')
	sf.ensure_munger_files(munger_name,project_root=project_root)

	munger_path = os.path.join(mungers_dir,munger_name)
	munger = sf.Munger(munger_path,project_root=project_root)
	munger.check_against_self()
	if session:
		munger.check_against_db(session)
	return munger


def pick_or_create_record(sess,project_root,element,known_info_d=None):
	"""User picks record from database if exists.
	Otherwise user picks from file system if exists.
	Otherwise user enters all relevant info.
	Store record in file system and/or db if new
	Return index of record in database"""
	if not known_info_d:
		known_info_d = {}

	storage_dir = os.path.join(project_root,'db_records_entered_by_hand')
	# pick from database if possible
	db_idx, db_style_record = pick_record_from_db(sess,element,known_info_d=known_info_d)

	# if not from db
	if db_idx is None:
		# pick from file_system
		fs_idx, file_style_record = pick_record_from_file_system(storage_dir,element,known_info_d=known_info_d)
		# if not from file_system
		if fs_idx is None:
			# have user enter record
			db_style_record, enum_plaintext_dict, fk_plaintext_dict = get_record_info_from_user(
				sess,element,known_info_d=known_info_d)
			# save to db
			[db_idx, db_style_record, enum_plaintext_dict, fk_plaintext_dict,changed] = dbr.save_one_to_db(
				sess,element,db_style_record)
			# save to file system
			save_record_to_filesystem(storage_dir,element,db_style_record,enum_plaintext_dict)
		# if found in file system
		else:
			try:
				db_style_record = mr.db_record_from_file_record(sess,element,file_style_record)
				db_idx,db_style_record,enum_plaintext_dict,fk_plaintext_dict, changed = dbr.save_one_to_db(
					sess,element,db_style_record)
			except KeyError as e:
				print(e)
				input(
					f'Perhaps the file {element}.txt in {storage_dir} does not have all fields '
					f'required by the corresponding database table.\n'
					f'Revise {element}.txt and hit return to continue.')
				db_idx,db_style_record,enum_plaintext_dict,fk_plaintext_dict = pick_or_create_record(
					sess,project_root,element,known_info_d=known_info_d)
	# if picked from db
	else:
		enum_plaintext_dict = mr.enum_plaintext_dict_from_db_record(sess,element,db_style_record)
		fk_plaintext_dict = mr.fk_plaintext_dict_from_db_record(
			sess,element,db_style_record,excluded=enum_plaintext_dict.keys())
	return db_idx, db_style_record, enum_plaintext_dict, fk_plaintext_dict


def pick_record_from_db(sess,element,known_info_d=None,required=False,db_idx=None):
	"""Get id and info from database, if it exists.
	If <db_idx> is passed, return that index and a dictionary with the rest of the record"""
	if not known_info_d:
		known_info_d = {}

	element_df = pd.read_sql_table(element,sess.bind,index_col='Id')
	if element_df.empty:
		return None,None
	elif db_idx:
		return db_idx, element_df.loc[db_idx].to_dict()

	# add columns for plaintext of any enumerations
	# FIXME also add columns for foreign key plaintext
	enums = dbr.read_enums_from_db_table(sess,element)
	element_enhanced_df = element_df.copy()
	for e in enums:
		e_df = pd.read_sql_table(e,sess.bind,index_col='Id')
		element_enhanced_df = mr.enum_col_from_id_othertext(element_enhanced_df,e,e_df,drop_old=False)

	# filter by known_info_d
	d = {k:v for k,v in known_info_d.items() if k in element_enhanced_df.columns}
	filtered = element_enhanced_df.loc[(element_enhanced_df[list(d)] == pd.Series(d)).all(axis=1)]
	# TODO if filtered is empty, offer all
	if filtered.empty:
		print('Nothing meets the filter criteria. Unfiltered options will be offered.')
		filtered = element_enhanced_df

	print(f'Pick the {element} record from the database:')
	name_field = db_routines.get_name_field(element)
	element_idx, values = pick_one(filtered,name_field,element)
	if element_idx in element_df.index:
		d = dict(element_df.loc[element_idx])
	else:
		d = None
	if required and element_idx is None:
		# offer to filter by available enumerations
		enum_list = [x for x in dbr.get_enumerations(sess,element) if x not in known_info_d]
		if len(enum_list) == 0:
			print('No more filters available. You must choose from this list')
			element_idx, d = pick_record_from_db(sess,element,known_info_d=known_info_d)
		else:
			while element_idx is None and len(enum_list) > 0:
				e = enum_list[0]
				e_filter = input(f'Filter by {e} (y/n)?\n')
				if e_filter == 'y':
					known_info_d[f'{e}_Id'],known_info_d[f'Other{e}'],known_info_d[e] = pick_enum(sess,e)
					element_idx, d = pick_record_from_db(sess,element,known_info_d=known_info_d,required=True)
				enum_list.remove(e)

	return element_idx, d


def pick_enum(sess,e):
	e_df = pd.read_sql_table(e,sess.bind,index_col='Id')
	e_idx,e_plaintext = pick_one(e_df,'Txt',item=e,required=True)
	if e_plaintext == 'other':
		# get plaintext from user
		while e_plaintext in ['','other']:
			e_plaintext = get_alphanumeric_from_user(
				f'Enter the value for {e}, which cannot be empty and cannot be \'other\'',allow_hyphen=True)
		e_othertext = e_plaintext
	else:
		e_othertext = ''
	return e_idx,e_othertext,e_plaintext


def pick_record_from_file_system(storage_dir,table,known_info_d=None):
	""" Looks for record in file system.
	Returns a file-style <record> (with enums as plaintext).
	If no record found, <idx> is none;
	otherwise value of <idx> is irrelevant."""
	# initialize to keep syntax-checker happy
	filtered_file = None
	if not known_info_d:
		known_info_d = {}
	name_field = dbr.get_name_field(table)

	# identify/create the directory for storing individual records in file system
	if not os.path.isdir(storage_dir):
		os.makedirs(storage_dir)
	# read any info from <table>'s file within that directory
	storage_file = os.path.join(storage_dir,f'{table}.txt')
	if os.path.isfile(storage_file):
		from_file = pd.read_csv(storage_file,sep='\t')
		if not from_file.empty:
			# filter via known_info_d
			filtered_file = from_file.loc[(from_file[list(known_info_d)] == pd.Series(known_info_d)).all(axis=1)]
		else:
			filtered_file = from_file
		print(f'Pick a record from {table} list in file system:')
		idx, file_style_record = pick_one(filtered_file,name_field)
	else:
		idx, file_style_record = None, None
	if idx is not None:
		file_style_record = dict(filtered_file.loc[idx])
	else:
		file_style_record = None
	return idx, file_style_record


def save_record_to_filesystem(storage_dir,table,user_record,enum_plain_text_values):
	# identify/create the directory for storing individual records in file system
	for e in enum_plain_text_values.keys():
		user_record[e] = enum_plain_text_values[e]  # add plain text
		user_record.remove(f'{e}_Id')  # remove Id
		user_record.remove(f'Other{e}')  # remove other text

	if not os.path.isdir(storage_dir):
		os.makedirs(storage_dir)

	storage_file = os.path.join(storage_dir,f'{table}.txt')
	if os.path.isfile(storage_file):
		records = pd.read_csv(storage_file,sep='\t')
	else:
		# create empty, with all cols of from_db except Id
		records = pd.DataFrame([],columns=user_record.keys())
	records.append(user_record,ignore_index=True)
	records.to_csv(storage_file,sep='\t',index=False)
	return


def get_datatype(df,c):
	"""Kludge to get datatype"""
	if df.dtypes[c] in [np.dtype('M8[ns]'),np.dtype('datetime64')]:
		datatype_string = 'Date'
	elif df.dtypes[c] in [np.dtype('int64')]:
		datatype_string = 'Int'
	elif df.dtypes[c] in [np.object]:
		datatype_string = 'String'
	else:
		print(f'Datatype {df.dtypes[c]} not recognized. To fix this error, alter code in the `get_datatype` function.')
		datatype_string = None
	return datatype_string


def get_record_info_from_user(sess,element,known_info_d={},mode='database'):
	"""Collect new record info from user, with chance to confirm.
	For each enumeration, translate the user's plaintext input into id/othertext.

	Return the corresponding record (id/othertext only) and an enumeration-value
	dictionary. Depending on <mode> ('database', 'filesystem' or 'database_and_filesystem'),
	returns enum plaintext, or enum id/othertext pairs, or both.
	"""

	# read existing info from db
	all_from_db = pd.read_sql_table(element,sess.bind,index_col='Id')
	# initialize <show_user_cols>
	db_cols = list(all_from_db.columns)  # note: does not include 'Id'
	show_user_cols=db_cols.copy()

	# initialize value dictionaries to be returned
	enum_val = fk_val = new = {}
	enum_list = dbr.get_enumerations(sess,element)
	fk_df = dbr.get_foreign_key_df(sess,element)

	# get enumeration tables from db
	e_df = {}
	for e in enum_list:
		e_df[e] = pd.read_sql_table(e,sess.bind,index_col='Id')

	# add cols to all_from_db for showing user and update show_user_cols
	for e in enum_list:
		all_from_db = mr.enum_col_from_id_othertext(all_from_db,e,e_df[e],drop_old=False)
		show_user_cols.append(e)
		show_user_cols.remove(f'{e}_Id')
		show_user_cols.remove(f'Other{e}')
	for i,r in fk_df.iterrows():
		# exclude foreign ids pointing to enumerations
		if i[:-3] not in enum_list:
			all_from_db = dbr.add_foreign_key_name_col(
				sess,all_from_db,r['foreign_column_name'],r['foreign_table_name'],drop_old=False)
			show_user_cols.append(i[:-3])
			show_user_cols.remove(i)

	# collect and confirm info from user
	unconfirmed = True
	while unconfirmed:
		# solicit info from user and store values for db insertion
		new = {}
		print(f'Enter info for new {element} record.')
		for c in db_cols:
			# define new[c] if value is known
			if c in known_info_d.keys():
				new[c] = known_info_d[c]

			# if c is an enumeration Id
			if c[-3:] == '_Id' and c[:-3] in enum_list:
				c_plain = c[:-3]
				# if plaintext of enumeration is known
				if c_plain in new.keys():
					new[c], new[f'Other{c_plain}'] = mr.enum_value_to_id_othertext(e_df[c],new[c_plain])
				# if id/othertext of enumeration is known
				elif f'{c}_Id' in new.keys() and f'Other{c}' in new.keys():
					new[c] = mr.enum_value_from_id_othertext(e_df[c],new[f'{c}_Id'],new[f'Other{c}'])
				# otherwise
				else:
					new[c], new[f'Other{c_plain}'], new[c_plain] = pick_enum(sess,c_plain)
			# if c is an Other<enumeration>, new value was defined in loop through enum_list
			elif c[:5] == 'Other' and c[5:] in enum_list:
				pass
			# if c is a foreign key (and not an enumeration)
			elif c in fk_df.index:
				# if foreign key id is known
				c_plain = c[:-3]
				if c in new.keys():
					new[c_plain] = dbr.name_from_id(sess,fk_df.loc[c,'foreign_table_name'],new[c])
				# if foreign key plaintext is known
				elif c_plain in new.keys():
					new[c] = dbr.name_to_id(sess,fk_df.loc[c,'foreign_table_name'],new[c_plain])
				# otherwise
				else:
					print(f'Specify the {fk_df.loc[c,"foreign_table_name"]} for this {element}')
					idx, db_record = pick_record_from_db(sess,fk_df.loc[c,'foreign_table_name'],required=True)
					new[c_plain] = db_record[dbr.get_name_field(fk_df.loc[c,'foreign_table_name'])]
					# TODO pull from DB info about whether the foreign key is required
					new[c] = dbr.name_to_id(sess,fk_df.loc[c,'foreign_table_name'],new[c_plain])
			else:
				new[c] = enter_and_check_datatype(f'Enter the {c}',get_datatype(all_from_db,c))

		# present to user for confirmation
		entry = '\n\t'.join([f'{k}:\t{new[k]}' for k in show_user_cols])
		confirm = input(f'Confirm entry:\n\t{entry}\nIs this correct (y/n)?\n')
		if confirm == 'y':
			unconfirmed = False

	# get db_record, enum_val, fk_val
	db_record = {k:new[k] for k in db_cols}
	enum_val = {e:new[e] for e in enum_list}
	fk_val = {k[:-3]:new[k[:-3]] for k in fk_df.index}
	show_user = {k:new[k] for k in show_user_cols}

	if mode == 'database':
		return db_record, enum_val, fk_val
	elif mode == 'filesystem':
		return show_user, enum_val, fk_val
	elif mode == 'database_and_filesystem':
		return {**db_record,**show_user},enum_val, fk_val
	else:
		print(f'Mode {mode} not recognized.')
		return None, None, None


def check_datatype(answer,datatype):
	"""Datatype is typically 'Integer', 'String', 'Date' or 'Encoding'"""
	good = False
	if datatype == 'Date':
		default = datetime.datetime.today().date()
		try:
			answer = datetime.datetime.strptime(answer,'%Y-%m-%d').date()
			good = True
		except ValueError:
			use_default = input(f'Answer not recognized as {datatype}. Use default value of {default} (y/n)?\n')
			if use_default == 'y':
				answer = default
				good = True
			else:
				print('You need to enter a date in the form \'2018-11-06\'.')
		# express date as string, e.g., 2020-05-23
		answer = f'{answer}'
	elif datatype == 'Integer':
		try:
			int(answer)
			good = True
		except ValueError:
			print('You need to enter an integer.')
	else:
		# Nothing to check for String datatype
		good = True
	return good, answer


def enter_and_check_datatype(question,datatype):
	answer = input(f'{question}')
	good = False
	while not good:
		good,answer = check_datatype(answer,datatype)
		if not good:
			answer = input('Try again:\n')
	return answer


def read_datafile(munger,f_path):
	if munger.file_type in ['txt','csv']:
		kwargs = {'encoding':munger.encoding,'quoting':csv.QUOTE_MINIMAL,'header':list(range(munger.header_row_count)),
			'thousands':munger.thousands_separator}
		if munger.file_type == 'txt':
			kwargs['sep'] = '\t'
		df = pd.read_csv(f_path,**kwargs)

	elif munger.file_type in ['xls','xlsx']:
		df = pd.read_excel(f_path,dtype=str,thousands=munger.thousands_separator)
	else:
		raise mr.MungeError(f'Unrecognized file_type in munger: {munger.file_type}')
	return df


def new_datafile(session,munger,raw_path,project_root=None,juris=None):
	"""Guide user through process of uploading data in <raw_file>
	into common data format.
	Assumes cdf db exists already"""
	if not project_root:
		get_project_root()
	if not juris:
		juris = pick_juris_from_filesystem(
			project_root,juriss_dir='jurisdictions')
	raw = read_datafile(munger,raw_path)
	count_columns_by_name = [raw.columns[x] for x in munger.count_columns]

	raw = mr.clean_raw_df(raw,munger)
	# NB: info_cols will have suffix added by munger

	# check jurisdiction against raw results file, adapting jurisdiction files as necessary
	check_juris = input(f'Check jurisdiction {juris.short_name} against raw results (y/n)?\n')
	if check_juris == 'y':
		if juris.check_against_raw_results(raw,munger,count_columns_by_name):
			# if jurisdction changed, load to db
			juris.load_juris_to_db(session,project_root)

	mr.raw_elements_to_cdf(session,project_root,juris,munger,raw,count_columns_by_name)
	print(f'Datafile contents uploaded to database {session.bind.engine}')
	return


def pick_or_create_directory(root_path,d_name):
	allowed = re.compile(r'^\w+$')
	while not os.path.isdir(os.path.join(root_path,d_name)):
		print(f'No subdirectory {d_name} in {root_path}. Here are the existing subdirectories:')
		idx, d_name = pick_one(os.listdir(root_path),None,'directory')
		if idx is None:
			d_name = ''
			while not allowed.match(d_name):
				d_name = get_alphanumeric_from_user('Enter subdirectory name (alphanumeric and underscore only)')
			full_path = os.path.join(root_path,d_name)
			confirm = input(f'Confirm creation of directory (y/n)?\n{full_path}\n')
			if confirm:
				Path(full_path).mkdir(parents=True,exist_ok=True)
	return d_name


def get_alphanumeric_from_user(request,allow_hyphen=False):
	# specify regex
	if allow_hyphen:
		alpha = re.compile('^[\w\-]+$')
	else:
		alpha = re.compile('^\w+$')

	good = False
	s = input(f'{request}\n')
	while not good:
		if alpha.match(s):
			good = True
		else:
			s = input('Answer needs to be alphanumeric. Please try again.\n')
	return s


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


def report_problems(problems,msg='There are problems'):
	"""<problems> is a text list of problems to be reported to user"""
	prob_str = '\n\t'.join(problems)
	print(f'{msg}:\n\t{prob_str}\n')
	return


def get_runtime_parameters(keys):
	d = {}

	parser = ConfigParser()
	parser.read('run_time.ini')

	for k in keys:
		try:
			d[k] = parser['election_anomaly'][k]
		except KeyError:
			print(f'Warning: no value found for {k} in the parameter file.')
	for k in keys:
		print(f'{k}: {d[k]}')
	return d
