#!usr/bin/python3
import db_routines as dbr
import db_routines.Create_CDF_db as db_cdf
import pandas as pd
from sqlalchemy.orm import sessionmaker
import os
import states_and_files as sf


def pick_one(df,return_col,item='row'):
	"""Returns index and <return_col> value of item chosen by user"""
	# TODO check that index entries are positive ints (and handle error)
	if df.empty:
		print(f'DataFrame is empty\n{df}')
		return None, None
	print(df)
	choice = max(df.index) + 1  # guaranteed not to be in df.index at start

	while choice not in df.index:
		choice_str = input(f'Enter the number of the desired {item} (or nothing if none is correct):\n')
		if choice_str == '':
			return None,None
		else:
			try:
				choice = int(choice_str)
				if choice not in df.index:
					print(f'Entry must in the leftmost column. Please try again.')
			except ValueError:
				print(f'You must enter a number (or nothing), then hit return. Please try again.')
	return choice, df.loc[choice,return_col]


def pick_database(paramfile):
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
		eng,meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=desired_db)
		Session = sessionmaker(bind=eng)
		pick_db_session = Session()

		db_cdf.create_common_data_format_tables(pick_db_session,None,
												dirpath=os.path.join(
													project_root,'election_anomaly/CDF_schema_def_info/'),
												delete_existing=False)
		db_cdf.fill_cdf_enum_tables(pick_db_session,None,dirpath=os.path.join(project_root,'election_anomaly/CDF_schema_def_info/'))

	# clean up
	if cur:
		cur.close()
	if con:
		con.close()
	return desired_db


def pick_election(session,schema):
	# TODO read elections from schema.Election table
	elections = pd.read_sql_table()
	# user picks existing or enters info for new
	# if election is new, enter its info into schema.Election
	election = None	# TODO remove
	return election


def pick_state(con,schema,path_to_states='../local_data/'):
	"""Returns a State object"""
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
							'Office',ru_list,'ElectionDistrict',reportingunittype_list=standard_ru_types)
		# Party.txt
		fill_context_file(os.path.join(state_path,'context'),
						  os.path.join(path_to_states,'context_templates'),
							'Party',None,None)
		# TODO remark
		remark_path = os.path.join(state_path,'context','remark.txt')
		open(remark_path,'a').close()	# creates file if it doesn't exist already
		with open(remark_path,'r') as f:
			remark = f.read()
		print(f'Current contents of remark.txt is:\n{remark}\n'
			  f'Please add or correct anything that user should know about the state {state_name}.')

	# initialize the state
	ss = sf.State(state_name,path_to_states)
	return ss


def fill_context_file(context_path,template_dir_path,element,test_list,test_field,reportingunittype_list=None,sep='\t'):
	if element == 'Office':
		assert reportingunittype_list, 'When processing Offices, need to pass non-empty reportingunittype_list'
	template_file_path = os.path.join(template_dir_path,f'{element}.txt')
	template = pd.read_csv(template_file_path,sep=sep,header=0,dtype=str)
	context_file = os.path.join(context_path,f'{element}.txt')
	if not os.path.isfile(context_file):
		# create file with just header row
		template.iloc[0:0].to_csv(context_file,index=None,sep=sep)
	in_progress = 'y'
	while in_progress == 'y':
		# TODO check for dupes
		# check format of file
		context_df = pd.read_csv(context_file,sep=sep,header=0,dtype=str)
		if not context_df.columns.to_list() == template.columns.to_list():
			print(f'WARNING: {element}.txt is not in the correct format.')		# TODO refine error msg?
			input('Please correct the file and hit return to continue.\n')
		else:
			# report contents of file
			print(f'\nCurrent contents of {element}.txt:\n{context_df}')

			# check test conditions
			if test_list is not None:
				if element == 'Office':	# need to reload from ReportingUnit.txt
					test_list = pd.read_csv(os.path.join(context_path,'ReportingUnit.txt'),
										sep=sep,header=0,dtype=str)['Name'].to_list()
				bad_set = {x for x in context_df[test_field] if x not in test_list}
				if len(bad_set) > 0:	# if test condition fails
					if element == 'Office':		# Office.ElectionDistrict must be in ReportingUnit.Name
						print(f'The ElectionDistrict for each Office must be listed in ReportingUnit.txt.\n'
							  f'Here are the {test_field}s in Office.txt that fail this condition:\n')
						print(f'{",".join(bad_set)}')
						print(f'To solve the problem, you must either alter the Name column in ReportingUnit.txt '
							  f'to add/correct the missing items,'
							  f'or remove/correct the {test_field} column in the offending row of Office.txt ')
						edit_test_element = input(f'Would you like to edit ReportingUnit.txt (y/n)?\n')
						if edit_test_element:
							fill_context_file(context_path,template_dir_path,'ReportingUnit',reportingunittype_list,'ReportingUnitType')
					else:
						print(f'\tStandard {test_field}s are not required, but you probably want to use them when you can.'
							  f'\n\tYour file has non-standard {test_field}s:')
						for rut in bad_set: print(f'\t\t{rut}')
						print(f'\tStandard {test_field}s are:')
						print(f'\t\t{",".join(test_list)}')

			# invite input
			in_progress = input(f'Would you like to alter {element}.txt (y/n)?\n')
			if in_progress == 'y':
				input('Make alterations, then hit return to continue')
	return context_df


def pick_munger(munger_dir='../mungers/',column_list=None):
	choice_list = os.listdir(munger_dir)
	for choice in os.listdir(munger_dir):
		p = os.path.join(munger_dir,choice)
		if not os.path.isdir(p):	# remove non-directories from list
			choice_list.remove(choice)
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
	munger_path = os.path.join(munger_dir,munger_name)
	# create munger directory
	try:
		os.mkdir(munger_path)
	except FileExistsError:
		print(f'Directory {munger_path} already exists, will be preserved')
	else:
		print(f'Directory {munger_path} created')

	# TODO create/correct raw_columns.txt
	# TODO create/correct ballot_measure_style.txt
	# TODO create/correct ballot_measure_selections.txt
	# TODO create/correct count_columns.txt
	# TODO create/correct atomic_reporting_unit_type.txt
	# TODO create/correct cdf_tables.txt
	# TODO create/correct raw_identifiers.txt

	return munger_name


def create_munger(column_list=None):
	# TODO walk user through munger creation
	#
	munger = None # TODO temp
	return munger


def new_datafile(raw_file,raw_file_sep,db_paramfile):
	"""Guide user through process of uploading data in <raw_file>
	into common data format.
	Assumes cdf db exists already"""
	# connect to postgres to create schema if necessary

	db_name = pick_database(db_paramfile)

	eng, meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	new_df_session = Session()

	state = pick_state(new_df_session.bind,None,path_to_states=os.path.join(project_root,'local_data'))

	election_idx, election = pick_one(pd.read_sql_table('Election',new_df_session.bind,index_col='Id'),'Name','election')
	if election_idx is None:
		# create record in Election table
		election_name = input('Enter a unique short name for election\n') # TODO error check
		electiontype_idx,electiontype = \
			pick_one(pd.read_sql_table('ElectionType',new_df_session.bind,index_col='Id'),'Txt','election type')
		if electiontype == 'other':
			otherelectiontype = input('Enter the election type:\n')	# TODO assert type is not in standard list, not ''
		else:
			otherelectiontype = ''
		elections_df = dbr.dframe_to_sql(pd.DataFrame({'Name':election_name,'EndDate':
			input('Enter the end date of the election, a.k.a. \'Election Day\' (YYYY-MM-DD)\n'),'StartDate':
			input('Enter the start date of the election (YYYY-MM-DD)\n'),'ElectionType_Id':
			electiontype_idx,'OtherElectionType':otherelectiontype},index= [-1]),new_df_session,None,'Election')

	raw = pd.read_csv(raw_file,sep=raw_file_sep)
	column_list = raw.columns.to_list()
	munger = pick_munger(column_list=column_list,munger_dir=os.path.join(project_root,'mungers'))

	if munger == None:
		munger = create_munger(column_list=column_list)

	# TODO once munger is chosen, walk user through steps to make sure munger
	# TODO can handle datafile
	munger.check_new_datafile(raw_file,state,new_df_session)

	contest_type_df = pd.DataFrame([
		['Candidate'], ['Ballot Measure'], ['Both Candidate and Ballot Measure']
	], columns=['Contest Type'])
	contest_type_list = pick_one(contest_type_df, item='contest type')

	return

if __name__ == '__main__':
	project_root = os.getcwd().split('election_anomaly')[0]
	raw_file = os.path.join(project_root,'local_data/NC/data/2018g/nc_general/results_pct_20181106.txt')
	raw_file_sep = '\t'
	db_paramfile = os.path.join(project_root,'local_data/database.ini')
	new_datafile(raw_file, raw_file_sep, db_paramfile)
	print('Done! (user_interface)')