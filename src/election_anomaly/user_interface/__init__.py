#!usr/bin/python3

# TODO get rid of refs to 'unmunged_<element>'

import db_routines as dbr
import db_routines.Create_CDF_db as db_cdf
import munge_routines as mr
import pandas as pd
import numpy as np
import csv
from sqlalchemy.orm import sessionmaker
import os
from pathlib import Path
import ntpath
import re
import datetime
import states_and_files as sf
import random
from tkinter import filedialog


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


def pick_datafile(project_root,sess):
	print("Locate the datafile in the file system.")
	fpath = pick_filepath(initialdir=project_root)
	filename = ntpath.basename(fpath)
	db_idx, datafile_record_d, datafile_enumeration_name_d = create_record_in_db(
		sess,project_root,'_datafile','short_name',known_info_d={'file_name':filename})
	# TODO typing url into debug window opens the web page; want it to just act like a string
	return datafile_record_d, datafile_enumeration_name_d, fpath


def pick_filepath(initialdir='~/'):
	"""<r> is a tkinter root for a pop-up window.
	<fpath_root> is the directory where the pop-up window starts.
	Returns chosen file path"""

	while True:
		fpath = input(
			'Enter path to file (or hit return to use pop-up window to find it).\n').strip()
		if not fpath:
			print('Use pop-up window to pick your file.')
			fpath = filedialog.askopenfilename(
				initialdir=initialdir,title="Select file",
				filetypes=(("text files","*.txt"),("csv files","*.csv"),("ini files","*.ini"),("all files","*.*")))
			print(f'The file you chose is:\n\t{fpath}')
			break
		elif not os.path.isfile(fpath):
			print(f'This is not a file: {fpath}\nTry again.')
		else:
			break
	return fpath


def pick_one(choices,return_col,item='row',required=False):
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
	with pd.option_context('display.max_rows',None,'display.max_columns',None):
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


def pick_paramfile(project_root):
	print('Locate the parameter file for your postgreSQL database.')
	fpath= pick_filepath()
	return fpath


def resolve_nulls(df,source_file,col_list=None,kwargs={}):
	working_df = df.copy()
	if col_list:
		working_df = working_df[col_list]
	while df.isna().any().any():
		input(f'There are nulls. Edit {source_file} to remove any nulls or blanks, then hit return to continue.\n')
		df = pd.read_csv(source_file,**kwargs)
	return


def show_sample(input_iter,items,condition,outfile='shown_items.txt',dir=None,export=False):
	# TODO revise to allow sample of dataframe and export of dataframe. E.g.
	#  so that Candidate and Party together can be exported.
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
		if dir is None:
			dir = input(f'Export all {len(st)} {items} that {condition}? If so, enter directory for export.'
						f'Existing file will be overwritten.\n'
						f'(Current directory is {os.getcwd()})\n')
		if os.path.isdir(dir):
			export = input(f'Export all {len(st)} {items} that {condition} to {outfile} (y/n)?\n')
			if export == 'y':
				with open(os.path.join(dir,outfile),'w') as f:
					f.write('\n'.join(st))
				print(f'{items} exported to {os.path.join(dir,outfile)}')
		elif dir != '':
			print(f'Directory {dir} does not exist.')
	return


def pick_database(project_root,paramfile,db_name=None):
	"""Establishes connection to db with name <db_name>,
	or creates a new cdf_db with that name.
	In any case, returns the name of the DB."""
	if db_name:
		print(f'WARNING: will use db {db_name}, assumed to exist.')
		# TODO check that db actually exists and recover if not.
		return db_name
	con = dbr.establish_connection(paramfile=paramfile)  # TODO error handling for paramfile
	print(f'Connection established to database {con.info.dbname}')
	cur = con.cursor()
	db_df = dbr.get_database_names(con,cur)
	db_idx,desired_db = pick_one(db_df,0,item='database')
	if db_idx:
		create_new = False
	else:  # if we're going to need a brand new db
		desired_db = get_alphanumeric_from_user('Enter name for new database (alphanumeric only)')
		create_new = True
		while desired_db in db_df[0].unique():
			use_existing = input(f'Database {desired_db} exists! Use existing database {desired_db} (y/n)?\n')
			if use_existing == 'y':
				create_new = False
				break
			else:
				desired_db = get_alphanumeric_from_user('Enter name for new database (alphanumeric only)')
		if create_new:
			dbr.create_database(con,cur,desired_db)

	if desired_db != con.info.dbname:
		cur.close()
		con.close()
		con = dbr.establish_connection(paramfile,db_name=desired_db)
		cur = con.cursor()

	if create_new: 	# if our db is brand new
		eng,meta = dbr.sql_alchemy_connect(paramfile=paramfile,db_name=desired_db)
		# TODO remove second arg from sql_alchemy_connect?
		Session = sessionmaker(bind=eng)
		pick_db_session = Session()

		db_cdf.create_common_data_format_tables(
			pick_db_session,dirpath=os.path.join(project_root,'election_anomaly','CDF_schema_def_info'),
			delete_existing=False)
		db_cdf.fill_cdf_enum_tables(pick_db_session,None,dirpath=os.path.join(project_root,'election_anomaly/CDF_schema_def_info/'))
		print(f'New database {desired_db} has been created using the common data format.')

	# clean up
	if cur:
		cur.close()
	if con:
		con.close()
	return desired_db


def check_count_columns(df,file,mungerdir,cdf_schema_def_dir):
	"""Checks that <df> is a proper count_columns dataframe;
	If not, guides user to correct <file> and then upload its
	contents to a proper count_columns dataframe, which it returns"""
	# TODO format of count_columns depends on ballot_measure style. Change this -- add ballotmeasurecolumns.txt?

	# get count types from cdf_schema_def_dir and raw cols from munger directory once at beginning
	with open(os.path.join(cdf_schema_def_dir,'enumerations/CountItemType.txt'),'r') as f:
		type_list = f.read().split('\n')
	with open(os.path.join(mungerdir,'raw_columns.txt'),'r') as f:
		raw_col_list = f.read().split('\n')[1:]
	ok = 'unknown'
	while not ok == 'y':
		if df.empty:
			print(f'No count columns found. Make sure there is at least one entry in {file}.\n')
		elif len(df.columns) != 2 or df.columns.to_list() != ['RawName','CountItemType']:
			print(f'Column headers must be [\'RawName\',\'CountItemType\']\n')
		elif df.CountItemType.all() not in type_list:
			for idx,row in df.iterrows():
				if row.CountItemType not in type_list:
					print(f'CountItemType \'{row.CountItemType}\' is not recognized on line {idx+2}.')
		elif df.RawName.all() not in raw_col_list:
			for idx,row in df.iterrows():
				if row.RawName not in raw_col_list:
					print(f'Column name \'{row.RawName}\' on line {idx+2} does not appear in the datafile.')
		else:
			ok = 'y'
			print(f'{file} has correct form')
		if ok != 'y':
			input(f'Correct the file {file}\nand hit return to continue.\n')
		df = pd.read_csv(file,sep='\t')
	return df


def pick_juris_from_filesystem(
		project_root,path_to_jurisdictions='jurisdictions/',juris_name=None,check_files=False):
	"""Returns a State object.
	If <jurisdiction_name> is given, this just initializes based on info
	in the folder with that name; """

	# if no jurisdiction name provided, ask user to pick from file system
	if juris_name is None:
		# ask user to pick from the available ones
		print('Pick the filesystem directory for your jurisdiction.')
		choice_list = [x for x in os.listdir(path_to_jurisdictions) if
					   os.path.isdir(os.path.join(path_to_jurisdictions,x))]
		juris_idx,juris_name = pick_one(choice_list,None,item='jurisdiction')

		#if nothing picked, ask user for alphanumeric name and create necessary files
		if not juris_idx:
			juris_name = get_alphanumeric_from_user('Enter a directory name for your jurisdiction files.')
			juris_path = os.path.join(path_to_jurisdictions,juris_name)
			sf.ensure_jurisdiction_files(juris_path,project_root)
		elif check_files:
			juris_path = os.path.join(path_to_jurisdictions,juris_name)
			sf.ensure_jurisdiction_files(juris_path,project_root)
		else:
			print(f'WARNING: if necessary files are missing from the directory {juris_name},\n'
				  f'system may fail.')
	else:
		if check_files:
			juris_path = os.path.join(path_to_jurisdictions,juris_name)
			sf.ensure_jurisdiction_files(juris_path,project_root)
		else:
			print(f'WARNING: if necessary files are missing from the directory {juris_name},\n'
			  f'system may fail.')

	# initialize the jurisdiction
	ss = sf.Jurisdiction(juris_name,path_to_jurisdictions,project_root=project_root)
	return ss


def find_dupes(df):
	dupes = df[df.duplicated()].drop_duplicates(keep='first')
	deduped = df.drop_duplicates(keep='first')
	return dupes, deduped


def format_check_formula(formula,fields):
	"""
	Checks all strings encased in angle brackets in <formula>
	Returns list of such strings missing from <field_list>
	"""
	p=re.compile('<(?P<field>[^<>]+)>')
	m = p.findall(formula)
	missing = [x for x in m if x not in fields]
	return missing


def create_file_from_template(template_file,new_file,sep='\t'):
	"""For tab-separated files (or others, using <sep>); does not replace existing file
	but creates <new_file> with the proper header row
	taking the headers from the <template_file>"""
	template = pd.read_csv(template_file,sep=sep,header=0,dtype=str)
	if not os.path.isfile(new_file):
		# create file with just header row
		template.iloc[0:0].to_csv(new_file,index=None,sep=sep)
	return


def fill_context_file(context_path,template_dir_path,element,sep='\t'):
	"""Creates file context/<element>.txt if necessary.
	In any case, runs format and dupe checks on that file, inviting user corrections.
	Also checks that each <element> passes the test, i.e., that
	for each k,v in <tests>, every value in column k can be found in <v>.txt.
	Does not apply to ExternalIdentifier.txt or remark.txt or dictionary.txt"""
	template_file = os.path.join(template_dir_path,f'{element}.txt')
	template = pd.read_csv(template_file,sep='\t')
	context_file = os.path.join(context_path,f'{element}.txt')
	create_file_from_template(template_file,context_file,sep=sep)

	name_field = 'Name'
	if element == 'BallotMeasureContest':
		tests = {'ElectionDistrict':'ReportingUnit','Election':'Election'}
	elif element == 'Candidate':
		tests = {'Party':'Party'}
		name_field = 'BallotName'
	elif element == 'CandidateContest':
		tests = {'Office':'Office'}
	elif element == 'Office':
		tests = {'ElectionDistrict':'ReportingUnit'}
	else:
		tests = {}

	context_df = None
	in_progress = 'y'
	while in_progress == 'y':
		# check format of file
		context_df = pd.read_csv(context_file,sep=sep,header=0,dtype=str)
		dupes,deduped = find_dupes(context_df[name_field])
		if not context_df.columns.to_list() == template.columns.to_list():
			print(f'WARNING: {element}.txt is not in the correct format. Required columns are:\n'
				  f'{template.columns.to_list()}')
			input('Please correct the file and hit return to continue.\n')

		# check for empty
		elif context_df.empty:
			empty_ok = input(f'File context/{element}.txt has no data. Is that correct (y/n)?\n')
			if empty_ok == 'y':
				in_progress = 'n'
			else:
				input(f'Please fill file context/{element}.txt, then hit return to continue.\n')

		# check for dupes
		elif dupes.shape[0] >0:
			print(f'File {context_path}\n has duplicates in the {name_field} column.')
			show_sample(dupes,'names','appear on more than one line')
			input(f'Please correct and hit return to continue.\n')
		else:
			# report contents of file
			context_list = context_df.to_csv(header=None,index=False,sep='\t').strip('\n').split('\n')
			show_sample(context_list,'lines',f'are in {element}.txt')

			# check test conditions
			if tests is None:
				in_progress = 'n'
			else:
				problem = False
				for test_field in tests.keys():
					targets = pd.read_csv(os.path.join(context_path,f'{tests[test_field]}.txt'),sep='\t')
					bad_set = {idx for idx in context_df.index if
							   context_df.loc[idx,test_field] not in targets.Name.unique()}
					if len(bad_set) == 0:
						print(
							f'Congratulations! The {test_field} for each record in context/{element}.txt looks good!')
					else:  # if test condition fails
						show_sample(context_df.loc[bad_set],f'{element}s',f'have unrecognized {test_field}s.')
						problem = True
				if problem:
					# invite input
					input('Make alterations to fix unrecognized items, then hit return to continue')
					fill_context_file(context_path,template_dir_path,tests[test_field])
				else:
					in_progress = 'n'
	return context_df


def pick_munger(munger_dir='mungers',project_root=None,session=None):
	"""pick (or create) a munger """
	# TODO create option to pick munger without reference to db.
	if not project_root:
		project_root = get_project_root()
	if not session:
		pass
		# TODO use files instead of db for enum info

	choice_list = os.listdir(munger_dir)
	for choice in os.listdir(munger_dir):
		c_path = os.path.join(munger_dir,choice)
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
	template_dir = os.path.join(project_root,'templates/munger_templates')
	check_munger_files(munger_name,munger_dir,template_dir,project_root=project_root)

	munger_path = os.path.join(munger_dir,munger_name)
	munger = sf.Munger(munger_path)
	munger.check_against_self()
	if session:
		munger.check_against_db(session)
	return munger


def check_munger_files(munger_name,munger_dir,template_dir,session=None,project_root=None):
	if not project_root:
		project_root = get_project_root()

	munger_path = os.path.join(munger_dir,munger_name)
	# create munger directory if necessary
	try:
		os.mkdir(munger_path)
	except FileExistsError:
		print(f'Directory {munger_path} already exists, will be preserved')
	else:
		print(f'Directory {munger_path} created')

	file_list = ['cdf_elements.txt']
	if not all([os.path.isfile(os.path.join(munger_path,x)) for x in file_list]):
		for ff in file_list:
			create_file_from_template(os.path.join(template_dir,ff),os.path.join(munger_path,ff))

		# create format.txt
		if session:
			rut_df = pd.read_sql_table('ReportingUnitType',session.bind,index_col='Id')
		else:
			rut_df = pd.read_csv(
				os.path.join(
					project_root,
					'election_anomaly/CDF_schema_def_info/enumerations/ReportingUnitType.txt'),
			names=['Txt'])
		try:
			with open(os.path.join(munger_path,'format.txt'),'r') as f:
				arut=f.read()
			change = input(f'Atomic ReportingUnit type is {arut}. Do you need to change it (y/n)?\n')
		except FileNotFoundError:
			change = 'y'
		if change == 'y':
			arut_idx,arut = pick_one(rut_df,'Txt',item='\'atomic\' reporting unit type for results file',required=True)
			with open(os.path.join(munger_path,'atomic_reporting_unit.txt'),'w') as f:
				f.write(arut)

		# prepare cdf_elements.txt
		#  find db tables corresponding to elements: nothing starting with _, nothing named 'Join',
		#  and nothing with only 'Id' and 'Txt' columns
		if session:
			elements,enums,joins,others = dbr.get_cdf_db_table_names(session.bind)
		else:
			all_elements = os.listdir(os.path.join(project_root,'election_anomaly/CDF_schema_def_info/elements'))
			elements = [x for x in all_elements if x != '_datafile']
		prepare_cdf_elements_file(munger_path,elements)
	return


def prepare_cdf_elements_file(dir_path,elements):
	guided = input(f'Would you like guidance in preparing the cdf_elements.txt file (y/n)?\n')
	if guided != 'y':
		input('Prepare cdf_elements.txt and hit return to continue.')
	else:
		out_lines = []
		print(f'''Enter your formulas for reading the common-data-format elements values
				associated to any particular vote count value in your file. If the information
				is in the same row as the vote count, the source is 'row'. In this case, 
				put raw column names in angle brackets (<>).
				For example if the raw file has columns \'County\' and \'Precinct\',
				the formula for ReportingUnit might be \'<County>;<Precinct>\'.
				If the information must be read from a column header, the source is 'column.' 
				In this case use the row number of the header in angle brackets..
				For example, if the first row of the file has contest names, the second row has candidate names
				and then the data rows begin, Candidate formula is <2> and CandidateContest formula is <1>''')
		for element in elements:
			source = input(f'What is the source for {element} (row/column/other)?\n')
			while source not in ['row','column','other']:
				source = input(f'''Try again: your answer must be 'row' or 'column' or 'other'.''')
			if source == 'row':
				formula = input(f'Formula for {element} in terms of column names (e.g. <County>):\n')
			elif source == 'column':
				formula = input(f'Formula for {element} in terms of header row numbers (e.g. <1>):\n')
			else:
				formula = ''
			out_lines.append(f'{element}\t{formula}\t{source}')

		with open(os.path.join(dir_path,'cdf_elements.txt'),'a') as f:
			f.write('\n'.join(out_lines))
	return


def pick_juris_from_db(sess,project_root,juris_type='state'):
	ru = pd.read_sql_table('ReportingUnit',sess.bind,index_col='Id')
	rut = pd.read_sql_table('ReportingUnitType',sess.bind,index_col='Id')
	# TODO build uniqueness into Txt field of each enumeration on db creation

	juris_type_id,other_juris_type = mr.get_id_othertext_from_enum_value(rut,juris_type)
	jurisdictions = ru[ru.ReportingUnitType_Id == juris_type_id]
	if jurisdictions.empty:
		print(f'No {juris_type} record found in the database. Please create one.\n')
		juris_idx, juris_record_d, juris_enum_d = create_record_in_db(
			sess,project_root,'ReportingUnit',known_info_d={'ReportingUnitType':juris_type})

		juris_internal_db_name = juris_record_d['Name']
	else:
		jurisdictions = mr.enum_col_from_id_othertext(jurisdictions,'ReportingUnitType',rut)
		juris_idx, juris_internal_db_name = pick_one(jurisdictions,'Name',juris_type)
	return juris_idx, juris_internal_db_name


def pick_or_create_record(sess,project_root,element,known_info_d={}):
	"""User picks record from database if exists.
	Otherwise user picks from file system if exists.
	Otherwise user enters all relevant info.
	Store record in file system and/or db if new
	Return index of record in database"""

	storage_dir = os.path.join(project_root,'db_records_entered_by_hand')
	# pick from database if possible
	db_idx, db_values = pick_record_from_db(sess,element,known_info_d=known_info_d)

	# if not from db, pick from file_system
	if db_idx is None:
		fs_idx, user_record = pick_record_from_file_system(storage_dir,element,known_info_d=known_info_d)

		# if not from file_system, pick from scratch
		if fs_idx is None:
			# have user enter record; save it to file system
			user_record, enum_plain_text_values = new_record_info_from_user(sess,project_root,element,known_info_d=known_info_d)
			# TODO enter_new pulls from file system; would be better here to pull from db
			save_record_to_filesystem(storage_dir,element,user_record,enum_plain_text_values)


		# save record to db
		try:
			element_df = pd.read_sql_table(element,sess.bind,index_col='Id')
			enum_list = [x[5:] for x in element_df.columns if x[:5]=='Other']
			for e in enum_list:
				enum_df = pd.read_sql_table(e,sess.bind)
				user_record[f'{e}_Id'],user_record[f'Other{e}'] = mr.get_id_othertext_from_enum_value(enum_df,user_record[e])
				user_record.pop(e)
			element_df = dbr.dframe_to_sql(pd.DataFrame(user_record,index=[-1]),sess,element)
			# find index matching inserted element
			idx = element_df.loc[
				(element_df[list(user_record)] == pd.Series(user_record)).all(axis=1)]['Id'].to_list()[0]
		except dbr.CdfDbException:
			print('Insertion of new record to db failed, maybe because record already exists. Try again.')
			idx = pick_or_create_record(sess,project_root,element,known_info_d=known_info_d)
	else:
		idx = db_idx

	return idx


def get_or_create_election_in_db(sess,project_root):
	"""Get id and electiontype from database, creating record first if necessary"""
	print('Specify the election:')
	election_df = pd.read_sql_table('Election',sess.bind,index_col='Id')
	election_idx, election = pick_one(election_df,'Name','election')
	electiontype_df = pd.read_sql_table('ElectionType',sess.bind,index_col='Id')
	if election_idx is None:
		election_idx, election_record_d, election_enum_d = create_record_in_db(sess,project_root,'Election')
		electiontype = election_enum_d['ElectionType']
	else:
		et_row = election_df.loc[:,['ElectionType_Id','OtherElectionType']].merge(
			electiontype_df,left_on='ElectionType_Id',right_index=True)
		if et_row.loc[election_idx,'OtherElectionType'] != '':
			electiontype = et_row.loc[election_idx,'OtherElectionType']
		else:
			electiontype = et_row.loc[election_idx,'Txt']
	# TODO understand why election_idx is np.int64 and how to avoid that routinely
	return int(election_idx),electiontype


def pick_record_from_db(sess,element,known_info_d={}):
	"""Get id and info from database, if it exists"""
	element_df = pd.read_sql_table(element,sess.bind,index_col='Id')
	# TODO filter by known_info_d filtered_file = from_file.loc[(from_file[list(known_info_d)] == pd.Series(known_info_d)).all(axis=1)]
	if element_df.empty:
		return None,None

	# add columns for plaintext of any enumerations
	enums = dbr.read_enums_from_db_table(sess,element)
	for e in enums:
		e_df = pd.read_sql_table(e,sess.bind,index_col='Id')
		element_df = mr.enum_col_from_id_othertext(element_df,e,e_df)

	print(f'Pick the {element} from the database:')
	element_idx, values = pick_one(element_df,'Name',element)
	return element_idx, values


def pick_record_from_file_system(storage_dir,table,name_field='Name',known_info_d={}):
	"""<field_list> is list of fields for table
	(with plaintext enums instead of {enum}_Id and Other{enum}"""
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
		idx, record = pick_one(filtered_file,name_field)
	else:
		idx, record = None, None
	if idx is not None:
		record = dict(filtered_file.loc[idx])
	else:
		record = None
	return idx, record


def save_record_to_filesystem(storage_dir,table,user_record,enum_plain_text_values):
	# identify/create the directory for storing individual records in file system
	for e in enum_plain_text_values.keys():
		user_record[e] = enum_plain_text_values[e]  # add plain text
		user_record.pop(f'{e}_Id')  # remove Id
		user_record.pop(f'Other{e}')  # remove other text

	if not os.path.isdir(storage_dir):
		os.makedirs(storage_dir)

	storage_file = os.path.join(storage_dir,f'{table}.txt')
	if os.path.isfile(storage_file):
		records = pd.read_csv(storage_file,sep='\t')
	else:
		# create empty, with all cols of from_db except Id
		records = pd.DataFrame([],columns=user_record.keys())  # TODO create one-line dataframe from user_record
	records.append(user_record,ignore_index=True)
	records.to_csv(storage_file,sep='\t')
	return


def get_record_from_user(sess,table,enum_list,other_field_list):
	"""Returns record, with three columns for each enum: _Id, Other and plaintext"""
	new_record = {}  # to hold values of the record
	for e in enum_list:
		vals = pd.read_sql_table(e,sess.bind)
	# TODO
	for f in other_field_list:
		pass
	return new_record


def create_record_in_db(sess,root_dir,table,name_field='Name',known_info_d={}):
	"""create record in <table> table in database from user input
	(or from existing info db_records_entered_by_hand directory in file system)
	<known_info_d> is a dict of known field-value pairs.
	Return the record (in dict form) and any enumeration values (in dict form)
	Store the record (if new) in the db_records_entered_by_hand directory
	Note: storage in file system should use names of any enumerations, not
	internal <enum>_Id and Other<enum>.
	"""
	# initialize various items to make syntax-checker happy
	db_record = pd.Series()
	file_record = pd.Series()
	enum_val = {}
	db_idx = -1

	# for messages to user
	known_info_string = ' with\n' + '\n\t'.join([f'{k}:\t{v}' for k,v in known_info_d.items()])

	# get list -- from file system -- of all enumerations for the <table>
	enum_list = pd.read_csv(
		os.path.join(root_dir,'election_anomaly/CDF_schema_def_info/elements',table,'enumerations.txt'),
		sep='\t',header=0).enumeration.to_list()

	# identify/create the directory for storing individual records in file system
	storage_dir = os.path.join(root_dir,'db_records_entered_by_hand')
	if not os.path.isdir(storage_dir):
		os.makedirs(storage_dir)

	storage_file = os.path.join(storage_dir,f'{table}.txt')
	# read from file system (if file exists)
	if os.path.isfile(storage_file):
		all_from_file = pd.read_csv(storage_file,sep='\t')
	else:
		all_from_file = pd.DataFrame()  # empty

	# get dataframe for each enumeration
	e_df = {}
	for e in enum_list:
		e_df[e] = pd.read_sql_table(e,sess.bind)

	# read from db and filter
	all_from_db = pd.read_sql_table(table,sess.bind,index_col='Id')
	picked_from_db = False
	if all_from_db.empty:
		filtered_from_db = pd.DataFrame()
	else:
		filtered_from_db = all_from_db.loc[
			(all_from_db[list(known_info_d)] == pd.Series(known_info_d)).all(axis=1)]

		# add plaintext enum columns
		filtered_from_db_with_plaintext = filtered_from_db
		for e in enum_list:
			e_df[e] = pd.read_sql_table(e,sess.bind)
			filtered_from_db_with_plaintext = mr.enum_col_from_id_othertext(
				filtered_from_db_with_plaintext,e,e_df[e],drop_old=False)
	# ask user to pick from db
	if filtered_from_db.empty:
		print(f'No records in database {known_info_string}\n\n ')
	else:
		print('Is the desired record already in the database?')
		drop_list = [f'{e}_Id' for e in enum_list] + [f'Other{e}' for e in enum_list]
		show_filtered_from_db = filtered_from_db_with_plaintext.drop(drop_list,axis=1)
		db_idx,db_record_name = pick_one(show_filtered_from_db,name_field)
		# if user picks, define <db_record>
		if db_idx:
			db_record = filtered_from_db.loc[db_idx]
			picked_from_db = True

			# fill enum_val dictionary
			for e in enum_list:
				enum_val[e] = show_filtered_from_db.loc[db_idx,e]

	picked_from_file = False
	if not picked_from_db:
		# find record in file system if possible
		if not all_from_file.empty:
			# filter via known_info_d
			filtered_from_file = all_from_file.loc[(all_from_file[list(known_info_d)] == pd.Series(known_info_d)).all(axis=1)]

			# ask user to pick from file
			if not filtered_from_file.empty:
				print('Is the desired record already in the file system?')
				record_idx,file_record_short_name = pick_one(filtered_from_file,name_field)
				if record_idx is not None:  # if user picked one
					picked_from_file = True
					file_record = filtered_from_file.loc[record_idx]

			# if not, ask user to enter info for file_record
			if not picked_from_file:
				db_and_file_record,enum_val = new_record_info_from_user(
					sess,root_dir,table,known_info_d=known_info_d,mode='database_and_filesystem')
				file_record = db_and_file_record.copy()
				for e in enum_list:
					file_record.pop(f'{e}_Id')
					file_record.pop(f'Other{e}')
	if not picked_from_file:
		# append new record to all_from_file; write to file system
		to_file = pd.concat([all_from_file,pd.DataFrame.from_dict([file_record],orient='columns')]).drop_duplicates()
		to_file.to_csv(storage_file,sep='\t',index=False)

	if not picked_from_db:
		# define enum_val and db_record
		enum_val = {}
		db_record = file_record.copy()
		for e in enum_list:
			enum_val[e] = file_record[e]
			db_record[f'{e}_Id'],db_record[f'Other{e}'] = mr.get_id_othertext_from_enum_value(
				e_df[e],file_record[e])
			db_record.pop(e)
		from_db = dbr.dframe_to_sql(
			pd.DataFrame.from_dict([db_record],orient='columns'),sess,table,return_records='new')
		db_idx = from_db.Id.unique().to_list()[0]
	return db_idx, db_record, enum_val


def report_uniqueness_problem(new_record,table,root_dir,sess):
	"""
	<new_record> is a dictionary of values corresponding to a new  record.
	Check whether new_record satisfies the uniqueness constraints of the <table>
	"""

	table_from_db = pd.read_sql_table(table,sess.bind)
	uniques = pd.read_csv(
			os.path.join(root_dir,'election_anomaly/CDF_schema_def_info/elements',table,'unique_constraints.txt'),
			sep='\t')
	problems = []
	for idx, row in uniques.iterrows():
		c_fields = row['unique_constraint'].split(',')	# list of fields in constraint
		val_list = [new_record[cf] for cf in c_fields]
		for i,r in table_from_db.iterrows():
			if [r[cf] for cf in c_fields] == val_list:
				problems.append(f"Table {table} already has a record with the desired value(s) " \
					   f"of {c_fields}: {[new_record[cf] for cf in c_fields]}.\n")
				break
	if problems:
		return '\n'.join(problems)
	else:
		return None


def new_record_info_from_user(sess,root_dir,table,known_info_d={},mode='database'):
	"""Collect new record info from user, with chance to confirm.
	For each enumeration, translate the user's plaintext input into id/othertext.
	Return the correponding db record (id/othertext only) and an enumeration-value
	dictionary."""

	# initialize items to keep syntax-checker happy
	user_input_record = show_user = enum_val = {}

	# read necessary info about <table> from file system into dataframes
	df = {}
	for f in ['fields','enumerations','foreign_keys','unique_constraints']:
		df[f] = pd.read_csv(
			os.path.join(root_dir,'election_anomaly/CDF_schema_def_info/elements',table,f'{f}.txt'),
			sep='\t')

	edf = {}
	# read relevant enumerations from database into dataframes
	for idx in df['enumerations'].index:
		e = df['enumerations'].loc[idx,'enumeration']
		edf[e] = pd.read_sql_table(e,sess.bind,index_col='Id')

	unconfirmed = True
	while unconfirmed:
		print(f'Enter info for new {table} record.')
		user_input_record = {}  # dict to hold values of the record
		enum_val = {}

		for idx,row in df['fields'].iterrows():
			if row["fieldname"] in known_info_d.keys():
				user_input_record[row["fieldname"]] = known_info_d[row["fieldname"]]
			else:
				user_input_record[row["fieldname"]] = enter_and_check_datatype(
					f'Enter the {row["fieldname"]}',row['datatype'])

		for idx,row in df['foreign_keys'].iterrows():
			target = row['refers_to']
			fieldname = row['fieldname']
			choices = pd.read_sql_table(target,sess.bind,index_col='Id')
			if choices.empty:
				raise Exception(f'Cannot add record to {table} while {target} does not contain the required {fieldname}.\n')
			user_input_record[fieldname], name = pick_one(
				choices,choices.columns[0],required=True)

		show_user = user_input_record.copy()
		for e in edf.keys():
			# force user to pick from standard list (plus 'other')
			user_input_record[f'{e}_Id'], enum_txt = pick_one(edf[e],'Txt',e,required=True)
			if enum_txt == 'other':
				# get plaintext from user
				user_input_record[f'Other{e}'] = input(f'Enter the {e}:\n')
				# check against standard list
				std_enum_list = list(edf[e]['Txt'])
				std_enum_list.remove('other')
				if user_input_record[f'Other{e}'] in std_enum_list:
					# if user's plaintext is on standard list, change <e>_Id to match plaintext (rather than 'other')
					#  and change Other<e> back to blank
					user_input_record[f'{e}_Id'] = \
						edf[e][edf[e].Txt == user_input_record[f'Other{e}']].first_valid_index()
					user_input_record[f'Other{e}'] = ''
			else:
				user_input_record[f'Other{e}'] = ''
			# get plaintext from id/othertext
			enum_val[e] = show_user[e] = mr.get_enum_value_from_id_othertext(
				edf[e],user_input_record[f'{e}_Id'],user_input_record[f'Other{e}'])
		entry = '\n\t'.join([f'{k}:\t{v}' for k,v in show_user.items()])
		confirm = input(
			f'Confirm entry:\n\t{entry}\nIs this correct (y/n)?\n')
		if confirm == 'y':
			unconfirmed = False
	if mode == 'database':
		return user_input_record, enum_val
	elif mode == 'filesystem':
		return show_user, enum_val
	elif mode == 'database_and_filesystem':
		return {**user_input_record,**show_user},enum_val
	else:
		print(f'Mode {mode} not recognized.')
		return None, None


def enter_and_check_datatype(question,datatype):
	"""Datatype is typically 'Integer', 'String', 'Date' or 'Encoding'"""
	answer = input(f'{question}\n')
	good = False
	while not good:
		if datatype == 'Date':
			try:
				datetime.datetime.strptime(answer, '%Y-%m-%d').date()
				good = True
			except ValueError:
				answer = input('You need to enter a date in the form \'2018-11-06\'. Try again.\n')
		elif datatype == 'Integer':
			try:
				int(answer)
				good = True
			except ValueError:
				answer = input('You need to enter an integer. Try again.')
		elif datatype == 'Encoding':
			if answer in recognized_encodings:
				good = True
			else:
				go_on = input(f'This system does not recognize "{answer}" as an encoding. If you are sure it is right,'
								 f'continue.\nIf you do not know what the encoding is, try "iso-8859-1".\n'
								 f'For more information, see https://docs.python.org/2.4/lib/standard-encodings.html.\n'
								 f'Continue with {answer}, even though it is not recognized (y/n)?\n')
				if go_on == 'y':
					good = True
				else:
					answer = input(f'Enter a new encoding.\n')
		else:
			good = True
	return answer


def new_datafile(session,munger,raw_path,raw_file_sep,encoding,project_root=None,juris=None):
	"""Guide user through process of uploading data in <raw_file>
	into common data format.
	Assumes cdf db exists already"""
	if not project_root:
		get_project_root()
	if not juris:
		juris = pick_juris_from_filesystem(
			project_root,path_to_jurisdictions=os.path.join(project_root,'jurisdictions'))
	juris_idx, juris_internal_db_name = pick_juris_from_db(session,project_root)

	# TODO where do we update db from jurisdiction context file?

	raw = pd.read_csv(
		raw_path,sep=raw_file_sep,dtype=str,encoding=encoding,quoting=csv.QUOTE_MINIMAL,
		header=list(range(munger.header_row_count))
	)
	[raw,info_cols,numerical_columns] = mr.clean_raw_df(raw,munger)
	# NB: info_cols will have suffix added by munger

	# TODO munger.check_against_datafile(raw,cols_to_munge,numerical_columns)

	mr.raw_elements_to_cdf(session,project_root,juris,munger,raw,info_cols,numerical_columns)

	# TODO
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


def get_alphanumeric_from_user(request):
	good = False
	alpha = re.compile('^\w+$')
	s = input(f'{request}\n')
	while not good:
		if alpha.match(s):
			good = True
		else:
			s = input('Answer needs to be alphanumeric. Please try again.\n')
	return s



if __name__ == '__main__':
	print("Data loading routines moved to src/election_anomaly/test folder")

	exit()
