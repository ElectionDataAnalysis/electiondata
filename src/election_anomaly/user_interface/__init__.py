#!usr/bin/python3

import db_routines as dbr
import db_routines.Create_CDF_db as db_cdf
import munge_routines as mr
import pandas as pd
import numpy as np
import csv
from sqlalchemy.orm import sessionmaker
import os
import ntpath
import re
import datetime
import states_and_files as sf
import random
import tkinter as tk
from tkinter import filedialog


def find_datafile(r,project_root,sess):
	print('Use pop-up window to pick your datafile.')
	r.filename = filedialog.askopenfilename(
		initialdir=project_root,title="Select election results datafile",
		filetypes=(("text files","*.txt"),("csv files","*.csv"),("all files","*.*")))
	print(f'The datafile you chose is:\n\t{r.filename}')
	# TODO if datafile is already in db, don't create new record, but read from existing
	filename = ntpath.basename(r.filename)
	datafile_record_d, datafile_enumeration_name_d = create_record_in_db(
		sess,project_root,'_datafile','short_name',known_info_d={'file_name':filename})
	# TODO typing url into debug window opens the webpage; want it to just act like a string
	return datafile_record_d, datafile_enumeration_name_d, r.filename


def pick_paramfile(r,project_root):
	print('Use pop-up window to pick the parameter file for your postgreSQL database.')
	r.filename = filedialog.askopenfilename(
		initialdir=project_root,title="Select database initialization file",
		filetypes=(("db initialization","*.ini"),("all files","*.*")))
	print(f'The parameter file you chose is:\n\t'
		  f'{r.filename}')
	return r.filename


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


def pick_one(df,return_col,item='row',required=False):
	"""Returns index and <return_col> value of item chosen by user"""
	# TODO check that index entries are positive ints (and handle error)
	if df.empty:
		return None, None
	print(df)
	choice = max(df.index) + 1  # guaranteed not to be in df.index at start

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
				print(f'You must enter a number {req_str}, then hit return. Please try again.')
	print(f'Chosen {item} is {df.loc[choice,return_col]}\n\n')
	return choice, df.loc[choice,return_col]


def resolve_nulls(df,source_file,col_list=None,kwargs={}):
	working_df = df.copy()
	if col_list:
		working_df = working_df[col_list]
	while df.isna().any().any():
		input(f'There are nulls. Edit {source_file} to remove any nulls or blanks, then hit return to continue.\n')
		df = pd.read_csv(source_file,**kwargs)
	return


def show_sample(st,items,condition,outfile='shown_items.txt',dir=None):
	print(f'There are {len(st)} {items} that {condition}:')
	if len(st) == 0:
		return
	st = list(st)
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
				print(f'\t{r}')
	if dir is None:
		dir = input(f'Export all {len(st)} {items} that {condition}? If so, enter directory for export\n'
					f'(Current directory is {os.getcwd()})\n')
	elif os.path.isdir(dir):
		export = input(f'Export all {len(st)} {items} that {condition} to {outfile} (y/n)?\n')
		if export == 'y':
			with open(os.path.join(dir,outfile),'a') as f:
				f.write('\n'.join(st))
			print(f'{items} exported to {os.path.join(dir,outfile)}')
	elif dir != '':
		print(f'Directory {dir} does not exist.')

	return


def pick_database(paramfile,db_name=None):
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
	db_df = pd.DataFrame(dbr.query('SELECT datname FROM pg_database',[],[],con,cur))
	db_idx,desired_db = pick_one(db_df,0,item='database')

	if db_idx == None:	# if we're going to need a brand new db

		desired_db = input('Enter name for new database (alphanumeric only):\n')
		dbr.create_database(con,cur,desired_db)

	if desired_db != con.info.dbname:
		cur.close()
		con.close()
		con = dbr.establish_connection(paramfile,db_name=desired_db)
		cur = con.cursor()

	if db_idx == None: 	# if our db is brand new
		eng,meta = dbr.sql_alchemy_connect(paramfile=paramfile,db_name=desired_db)
		# TODO remove second arg from sql_alchemy_connect?
		Session = sessionmaker(bind=eng)
		pick_db_session = Session()

		db_cdf.create_common_data_format_tables(
			pick_db_session,dirpath=os.path.join(
				project_root,'election_anomaly/CDF_schema_def_info/'),delete_existing=False)
		db_cdf.fill_cdf_enum_tables(pick_db_session,None,dirpath=os.path.join(project_root,'election_anomaly/CDF_schema_def_info/'))

	# clean up
	if cur:
		cur.close()
	if con:
		con.close()
	return desired_db


def check_count_columns(df,file,mungerdir,CDF_schema_def_dir):
	"""Checks that <df> is a proper count_columns dataframe;
	If not, guides user to correct <file> and then upload its
	contents to a proper count_columns dataframe, which it returns"""

	# get count types from CDF_schema_def_dir and raw cols from munger directory once at beginning
	with open(os.path.join(CDF_schema_def_dir,'enumerations/CountItemType.txt'),'r') as f:
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


def pick_state(con,schema,path_to_states='jurisdictions/',state_name=None):
	"""Returns a State object.
	If <state_short_name> is given, this just initializes based on info
	in the folder with that name; """
	if state_name is None:
		choice_list = [x for x in os.listdir(path_to_states) if os.path.isdir(os.path.join(path_to_states,x))]
		state_df = pd.DataFrame(choice_list,columns=['State'])
		state_idx,state_name = pick_one(state_df,'State', item='state')

		if state_idx is None:
			# user chooses state short_name
			state_name = input('Enter a short name (alphanumeric only, no spaces) for your state '
							   '(e.g., \'NC\')\n')
		state_path = os.path.join(path_to_states,state_name)

		# create state directory
		try:
			os.mkdir(state_path)
		except FileExistsError:
			print(f'Directory {state_path} already exists, will be preserved')
		else:
			print(f'Directory {state_path} created')

		# create subdirectories
		subdir_list = ['context','data','output']
		for sd in subdir_list:
			sd_path = os.path.join(state_path,sd)
			try:
				os.mkdir(sd_path)
			except FileExistsError:
				print(f'Directory {sd_path} already exists, will be preserved')
			else:
				print(f'Directory {sd_path} created')

		# ensure context directory has what it needs
		context_file_list = ['Office.txt','Party.txt','ReportingUnit.txt','remark.txt']
		if not all([os.path.isfile(os.path.join(state_path,'context',x)) for x in context_file_list]):
			# pull necessary enumeration from db: ReportingUnitType
			ru_type = pd.read_sql_table('ReportingUnitType',con,schema=schema,index_col='Id')
			standard_ru_types = set(ru_type[ru_type.Txt != 'other']['Txt'])
			ru = fill_context_file(os.path.join(state_path,'context'),
							  os.path.join(path_to_states,'context_templates'),
								'ReportingUnit',standard_ru_types,'ReportingUnitType')
			ru_list = ru['Name'].to_list()
			fill_context_file(os.path.join(state_path,'context'),
							  os.path.join(path_to_states,'context_templates'),
								'Office',None,None)  # note check that ElectionDistricts are RUs happens below
			# Party.txt
			fill_context_file(os.path.join(state_path,'context'),
							  os.path.join(path_to_states,'context_templates'),
								'Party',None,None)
			# TODO remark
			remark_path = os.path.join(state_path,'context','remark.txt')
			open(remark_path,'a').close()	# creates file if it doesn't exist already
			with open(remark_path,'r') as f:
				remark = f.read()
			print(f'Current contents of remark.txt is:\n{remark}\n')
			input(f'In the file context/remark.txt, add or correct anything that user should know about the state {state_name}.\n'
						f'Then hit return to continue.')
	else:
		print(f'Directory {state_name} is assumed to exist and have the required contents.')
	# initialize the state
	ss = sf.State(state_name,path_to_states)
	ss.check_election_districts()
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


def confirm_or_correct_cdf_table_file(cdf_table_file,raw_cols):
	"""
	Checks that <cdf_table_file> has the right columns and contest;
	if not, guides user to correcting
	"""
	element_list = [
		'Office','ReportingUnit','Party','Candidate','CandidateContest',
		'BallotMeasureContest','BallotMeasureSelection']
	cdft_df = pd.read_csv(cdf_table_file,sep='\t')  # note index

	# check column headings
	while len(cdft_df.columns) != 2 or cdft_df.columns.to_list() != ['cdf_element','raw_identifier_formula']:
		input(f'The file {cdf_table_file} should tab-separated with two columns\n'
			  f'labeled \'cdf_element\' and \'raw_identifier_formula\'.\n'
			  f'Correct the file as necessary and hit return to continue')
		cdft_df = pd.read_csv(cdf_table_file,sep='\t')  # note index

	# check for missing rows
	missing_elements = [x for x in element_list if x not in cdft_df.cdf_element.to_list()]
	while missing_elements:
		input(f'Rows are missing from {cdf_table_file}:\n'
			  f'{",".join(missing_elements)}\n'
			  f'Add them and hit return to continue')
		cdft_df = pd.read_csv(cdf_table_file,sep='\t')  # note index
		missing_elements = [x for x in element_list if x not in cdft_df.cdf_element.to_list()]

	# check that formulas refer to existing columns of raw file
	bad_formulas = [1]
	while bad_formulas:
		bad_formulas = []
		misspellings = set()
		for idx,row in cdft_df.iterrows():
			new_misspellings = format_check_formula(row['raw_identifier_formula'],raw_cols)
			if new_misspellings:
				misspellings = misspellings.union(new_misspellings)
				bad_formulas.append(row.cdf_element)
		if misspellings:
			# TODO if raw file has col name matching an element, that name is changed in raw_cols but not in formulas in cdf_tables.txt
			print(f'Some formula parts are not recognized as raw column labels:\n'
				  f'{",".join([f"<{m}>" for m in misspellings])}\n\n'
				  f'Raw columns are: {",".join(raw_cols)}\n')

		if bad_formulas:
			print(f'Unusable formulas for {",".join(bad_formulas)}.\n')
			input(f'Fix the cdf_tables.txt file (or possibly the raw_columns.txt file)\n and hit return to continue.\n')
		cdft_df = pd.read_csv(cdf_table_file,sep='\t')  # note index
	return cdft_df


def confirm_or_correct_ballot_measure_style(options_file,bms_file,sep='\t'):
	bmso_df = pd.read_csv(options_file,sep=sep)
	try:
		with open(bms_file,'r') as f:
			bms = f.read()
	except FileNotFoundError:
		bms = None
	if bms not in bmso_df['short_name'].to_list():
		print('Ballot measure style not recognized. Please pick a new one.')
		bms_idx,bms = pick_one(bmso_df,'short_name',item='ballot measure style',required=True)
		with open(bms_file,'w') as f:
			f.write(bms)
	bms_description = bmso_df[bmso_df.short_name==bms]['description'].iloc[0]
	return bms, bms_description


def create_file_from_template(template_file,new_file,sep='\t'):
	"""For tab-separated files (or others, using <sep>); does not replace existing file
	but creates <new_file> with the proper header row
	taking the headers from the <template_file>"""
	template = pd.read_csv(template_file,sep=sep,header=0,dtype=str)
	if not os.path.isfile(new_file):
		# create file with just header row
		template.iloc[0:0].to_csv(new_file,index=None,sep=sep)
	return


def fill_context_file(context_path,template_dir_path,element,test_list,test_field,sep='\t'):
	"""Creates file context/<element>.txt if necessary.
	In any case, runs format and dupe checks on that file, inviting user corrections.
	Also checks that each <element> passes the test, i.e., that
	each <element.test_field> is in <test_list>."""
	template_file = os.path.join(template_dir_path,f'{element}.txt')
	template = pd.read_csv(template_file,sep='\t')
	context_file = os.path.join(context_path,f'{element}.txt')
	create_file_from_template(template_file,context_file,sep=sep)
	in_progress = 'y'
	while in_progress == 'y':
		# check format of file
		context_df = pd.read_csv(context_file,sep=sep,header=0,dtype=str)
		dupes,deduped = find_dupes(context_df.Name)
		if not context_df.columns.to_list() == template.columns.to_list():
			print(f'WARNING: {element}.txt is not in the correct format.')		# TODO refine error msg?
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
			print(f'File {context_path}\n has duplicates in the Name column.')
			show_sample(dupes,'names','appear on more than one line')
			input(f'Please correct and hit return to continue.\n')
		else:
			# report contents of file
			print(f'\nCurrent contents of {element}.txt:\n{context_df}')

			# check test conditions
			if test_list is None:
				in_progress = 'n'
			else:
				bad_set = {x for x in context_df[test_field] if x not in test_list}
				if len(bad_set) == 0:
					print(f'Congratulations! Contents of context/{element}.txt look good!')
					in_progress = 'n'
				else:  # if test condition fails
					print(f'\tStandard {test_field}s are not required, but you probably want to use them when you can.'
						  f'\n\tYour file has non-standard {test_field}s:')
					for rut in bad_set: print(f'\t\t{rut}')
					# TODO bug: this prints out long long list of ElectionDistricts,
					# TODO then suggests altering only Office.txt. Should be more graceful.
					print(f'\tStandard {test_field}s are:')
					print(f'\t\t{",".join(test_list)}')

					# invite input
					in_progress = input(f'Would you like to alter {element}.txt (y/n)?\n')
					if in_progress == 'y':
						input('Make alterations, then hit return to continue')
	return context_df


def pick_munger(sess,munger_dir='mungers/',column_list=None,root='.'):
	"""pick (or create) a munger """
	choice_list = os.listdir(munger_dir)
	for choice in os.listdir(munger_dir):
		p = os.path.join(munger_dir,choice)
		if not os.path.isdir(p):	# remove non-directories from list
			choice_list.remove(choice)
		elif not os.path.isfile(os.path.join(p,'raw_columns.txt')):
			pass  # list any munger that doesn't have raw_columns.txt file yet
		else:
			# remove from list if columns don't match
			raw_columns = pd.read_csv(os.path.join(p,'raw_columns.txt'),header=0,dtype=str,sep='\t')
			if raw_columns.name.to_list() != column_list:
				choice_list.remove(choice)

	munger_df = pd.DataFrame(choice_list,columns=['Munger'])
	munger_idx,munger_name = pick_one(munger_df,'Munger', item='munger')
	if munger_idx is None:
		# user chooses state munger
		munger_name = input('Enter a short name (alphanumeric only, no spaces) for your munger'
						   '(e.g., \'nc_primary18\')\n')
	need_to_check_munger = input(f'Check compatibility of munger {munger_name} (y/n)?\n')
	if need_to_check_munger == 'y':
		template_dir = os.path.join(root,'templates/munger_templates')
		check_munger(sess,munger_name,munger_dir,template_dir,column_list)

	munger_path = os.path.join(munger_dir,munger_name)
	munger = sf.Munger(munger_path,cdf_schema_def_dir=os.path.join(root,'election_anomaly/CDF_schema_def_info'))
	return munger


def check_munger(sess,munger_name,munger_dir,template_dir,column_list):
	munger_path = os.path.join(munger_dir,munger_name)
	# create munger directory
	try:
		os.mkdir(munger_path)
	except FileExistsError:
		print(f'Directory {munger_path} already exists, will be preserved')
	else:
		print(f'Directory {munger_path} created')

	file_list = ['raw_columns.txt','count_columns.txt','cdf_tables.txt','raw_identifiers.txt']
	if not all([os.path.isfile(os.path.join(munger_path,x)) for x in file_list]):
		for ff in file_list:
			create_file_from_template(os.path.join(template_dir,ff),os.path.join(munger_path,ff))
		# write column_list to raw_columns.txt
		if column_list:
			# np.savetxt(os.path.join(munger_path,ff),np.asarray([[x] for x in column_list]),header='name')
			pd.DataFrame(np.asarray([[x] for x in column_list]),columns=['name']).to_csv(
				os.path.join(munger_path,'raw_columns.txt'),sep='\t',index=False)
		else:
			input(f"""The file raw_columns.txt should have one row for each column 
				in the raw datafile to be processed with the munger {munger_name}. 
				The columns must be listed in the order in which they appear in the raw datafile'
				Check the file and correct as necessary. Then hit return to continue.\n""")

		# create ballot_measure_style.txt
		bms,bms_description = confirm_or_correct_ballot_measure_style(
			os.path.join(munger_dir,'ballot_measure_style_options.txt'),
				os.path.join(munger_path,'ballot_measure_style.txt'))

		# create/correct count_columns.txt
		print(f"""The file count_columns.txt should have one row for each vote-count column  
			in the raw datafile to be processed with the munger {munger_name}. 
			Each row should have the RawName of the column and the CountItemType. 
			Standard CountItemTypes are not required, but are recommended:""")
		cit = pd.read_sql_table('CountItemType',sess.bind)
		print(cit['Txt'].to_list())
		input('Check the file and correct as necessary.  Then hit return to continue.\n')
		# TODO check file against standard CountItemTypes?

		# create atomic_reporting_unit_type.txt
		rut_df = pd.read_sql_table('ReportingUnitType',sess.bind,index_col='Id')
		try:
			with open(os.path.join(munger_path,'atomic_reporting_unit_type.txt'),'r') as f:
				arut=f.read()
			change = input(f'Atomic ReportingUnit type is {arut}. Do you need to change it (y/n)?\n')
		except FileNotFoundError:
			change = 'y'
		if change == 'y':
			arut_idx,arut = pick_one(rut_df,'Txt',item='\'atomic\' reporting unit type for results file',required=True)
			with open(os.path.join(munger_path,'atomic_reporting_unit.txt'),'w') as f:
				f.write(arut)

		# prepare cdf_tables.txt
		prepare_cdf_tables_file(munger_path,bms)

		# prepare raw_identifiers.txt
		prepare_raw_identifiers_file(munger_path,bms)

	return


def prepare_raw_identifiers_file(dir_path,ballot_measure_style):
	if ballot_measure_style == 'yes_and_no_are_candidates':
		print('\nMake sure to list all raw ballot measure selections, with cdf_internal_name \'Yes\' or \'No\'')
	input('Prepare raw_identifiers.txt and hit return to continue.')
	# TODO add guidance
	return


def prepare_cdf_tables_file(dir_path,ballot_measure_style):
	guided = input(f'Would you like guidance in preparing the cdf_tables.txt file (y/n)?\n')
	if guided != 'y':
		input('Prepare cdf_tables.txt and hit return to continue.')
	else:
		elt_list = ['Office','ReportingUnit','Party','Candidate','CandidateContest',
						'BallotMeasureContest']
		out_lines = []
		if ballot_measure_style == 'yes_and_no_are_candidates':
			elt_list.append('BallotMeasureSelection')
		for element in ['Office','ReportingUnit','Party','Candidate','CandidateContest',
						'BallotMeasureContest','BallotMeasureSelection']:
			print(f'''Enter your formulas for reading the common-data-format elements from each row
					of the results file. Put raw column names in brackets (<>).
					For example if the raw file has columns \'County\' and \'Precinct\',
					the formula for ReportingUnit might be \'<County>;<Precinct>\'.''')
			formula = input(f'Formula for {element}:\n')
			# TODO error check formula against raw_columns.txt and count_columns.txt in <dir_path>
			out_lines.append(f'{element}\t{formula}')
		with open(os.path.join(dir_path,'cdf_tables.txt'),'a') as f:
			f.write('\n'.join(out_lines))
	return


def create_munger(column_list=None):
	# TODO walk user through munger creation
	#
	munger = None # TODO temp
	return munger


def pick_state_from_db(sess):
	ru = pd.read_sql_table('ReportingUnit',sess.bind,index_col='Id')
	rut = pd.read_sql_table('ReportingUnitType',sess.bind,index_col='Id')
	assert rut[rut.Txt=='state'].shape[0] == 1, '\'ReportingUnitType\' table does not have exactly one \'state\' entry'
	state_type_id = rut[rut.Txt=='state'].first_valid_index()
	states = ru[ru.ReportingUnitType_Id==state_type_id]
	state_idx, state_internal_db_name = pick_one(states,'Name','state')
	return state_idx, state_internal_db_name


def get_or_create_election_in_db(sess):
	"""Get id and electiontype from database, creating record first if necessary"""
	print('Specify the election:')
	election_df = pd.read_sql_table('Election',sess.bind,index_col='Id')
	election_idx, election = pick_one(election_df,'Name','election')
	electiontype_df = pd.read_sql_table('ElectionType',sess.bind,index_col='Id')
	if election_idx is None:
		election_record_d, election_enum_d = create_record_in_db(sess,project_root,'Election')
		election_idx = election_record_d['Id']
		elecitontype = election_enum_d['ElectionType']
	else:
		et_row = election_df.loc[:,['ElectionType_Id','OtherElectionType']].merge(
			electiontype_df,left_on='ElectionType_Id',right_index=True)
		if et_row.loc[election_idx,'OtherElectionType'] != '':
			electiontype = et_row.loc[election_idx,'OtherElectionType']
		else:
			electiontype = et_row.loc[election_idx,'Txt']
	return election_idx,electiontype


def create_record_in_db(sess,root_dir,table,name_field='Name',known_info_d={}):
	"""create record in <table> table in database from user input
	<enums is a dict of enumeration dataframes"""

	print(f'Enter information to be entered in the corresponding record in the {table} table in the database.')
	d = {}
	df = {}
	enum_val = {}
	for f in ['fields','enumerations','other_element_refs']:
		df[f] = pd.read_csv(
			os.path.join(root_dir,'election_anomaly/CDF_schema_def_info/Tables',table,f'{f}.txt'),
			sep='\t')

	for idx,row in df['fields'].iterrows():
		if row["fieldname"] in known_info_d.keys():
			d[row["fieldname"]] = known_info_d[row["fieldname"]]
		else:
			d[row["fieldname"]] = enter_and_check_datatype(f'Enter the {row["fieldname"]}.',row['datatype'])

	for idx, row in df['enumerations'].iterrows():
		enum_df = pd.read_sql_table(row['enumeration'],sess.bind,index_col='Id')
		d[f'{row["enumeration"]}_Id'], enum_txt = pick_one(enum_df,'Txt',row['enumeration'],required=True)
		if enum_txt == 'other':
			std_enum_list = list(enum_df['Txt'].remove('other'))
			d[f'Other{row["enumeration"]}'] = input('Enter the election type:\n')
			if d[f'Other{row["enumeration"]}'] in std_enum_list:
				d[f'{row["enumeration"]}_Id'] = enum_df[enum_df.Txt == d[f'Other{row["enumeration"]}']].first_valid_index()
				d[f'Other{row["enumeration"]}'] = ''
		else:
			d[f'Other{row["enumeration"]}'] = ''
		enum_val[row["enumeration"]] = enum_txt

	table_df = dbr.dframe_to_sql(pd.DataFrame(d,index=[-1]),sess,None,table)
	table_idx = table_df[table_df[name_field]==d[name_field]].iloc[0]['Id']
	return dict(table_df.loc[0]), enum_val


def enter_and_check_datatype(question,datatype):
	"""Datatype is typically 'Integer', 'String' or 'Date' """
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
		else:
			good = True
	return answer


def new_datafile(raw_file,raw_file_sep,session,project_root='.',state_short_name=None,encoding='utf-8'):
	"""Guide user through process of uploading data in <raw_file>
	into common data format.
	Assumes cdf db exists already"""

	state = pick_state(
		session.bind,None,
		path_to_states=os.path.join(project_root,'jurisdictions'),
		state_name=state_short_name)
	# TODO finalize ReportingUnits once for both kinds of files?

	state_idx, state_internal_db_name = pick_state_from_db(session)
	# TODO feature: write routine to deduce BallotMeasureContest district from the data?!?
	# update db from state context file

	election_idx, electiontype = get_or_create_election_in_db(session)
	# read file in as dataframe of strings, replacing any nulls with the empty string
	raw = pd.read_csv(raw_file,sep=raw_file_sep,dtype=str,encoding=encoding,quoting=csv.QUOTE_MINIMAL).fillna('')
	column_list = raw.columns.to_list()
	print('Specify the munger:')
	munger = pick_munger(
		session,column_list=column_list,munger_dir=os.path.join(project_root,'mungers'),root=project_root)
	print(f'Munger {munger.name} has been chosen and prepared.\n'
		  f'Next we check compatibility of the munger with the datafile.')

	munger.check_ballot_measure_selections()
	munger.check_atomic_ru_type()

	bmc_results,cc_results = mr.contest_type_split(raw,munger)
	if bmc_results.empty:
		print('Datafile has only Candidate Contests, and no Ballot Measure Contests')
		contest_type_df = ['Candidate']
	elif cc_results.empty:
		print('Datafile has only Ballot Measure Contests, andno Candidate Contests')
		contest_type_df = ['Ballot Measure']
	else:
		print('What types of contests would you like to analyze from the datafile?')
		contest_type_df = pd.DataFrame([
			['Candidate'], ['Ballot Measure'], ['Both Candidate and Ballot Measure']
		], columns=['Contest Type'])
		contest_type_idx, contest_type = pick_one(contest_type_df,'Contest Type', item='contest type',required=True)

	if contest_type in ['Candidate','Both Candidate and Ballot Measure']:
		munger.check_new_results_dataset(cc_results,state,session,'Candidate',project_root=project_root)
	if contest_type in ['Ballot Measure','Both Candidate and Ballot Measure']:
		munger.check_new_results_dataset(cc_results,state,session,'BallotMeasure',project_root=project_root)

	# TODO process new results dataset(s)
	if contest_type in ['Candidate','Both Candidate and Ballot Measure']:
		mr.raw_elements_to_cdf(session,munger,cc_results,'Candidate',election_idx,electiontype,state_idx)
	if contest_type in ['Ballot Measure','Both Candidate and Ballot Measure']:
		mr.raw_elements_to_cdf(session,munger,bmc_results,'BallotMeasure',election_idx,electiontype,state_idx)
	return state, munger


if __name__ == '__main__':

	print('\nReady to load some election result data?\n'
		  'This program will walk you through the process of creating or checking\n'
		  'an automatic munger that will load your data into a database in the '
		  'NIST common data format.')

	project_root = get_project_root()

	# initialize root widget for tkinter
	tk_root = tk.Tk()

	# pick db to use
	db_paramfile = pick_paramfile(tk_root,project_root)
	db_name = pick_database(db_paramfile)

	# connect to db
	eng, meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	new_df_session = Session()

	# get datafile & info
	dfile_d, enum_d, raw_file = find_datafile(tk_root,project_root,new_df_session)
	# TODO store \t and , directly?
	if enum_d['_datafile_separator'] == 'tab':
		sep = '\t'
	elif enum_d['_datafile_separator'] == 'comma':
		sep = ','
	else:
		raise Exception(f'separator {enum_d["_datafile_separator"]} not recognized')


	#state_short_name = 'FL'
	state_short_name = None

	# load new datafile

	state, munger = new_datafile(
		raw_file,sep,new_df_session,state_short_name=state_short_name,encoding=dfile_d['encoding'],project_root=project_root)

	eng.dispose()
	print('Done! (user_interface)')
	exit()