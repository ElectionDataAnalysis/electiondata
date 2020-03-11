#!usr/bin/python3
import db_routines as dbr
import pandas as pd
from sqlalchemy.orm import sessionmaker
import os
import states_and_files as sf


def pick_one(df,return_col,item='row'):
	"""Returns index of item chosen by user"""
	# TODO check that index entries are positive ints (and handle error)
	print(df)
	index_name = df.index.name or 'row number'
	choice_str = input(f'Enter the {index_name} of the desired {item} '
						   f'(or nothing if none is correct):\n')
	if choice_str == '':
		return None,None
	else:
		choice = -1	# not in df.index
		while choice not in df.index:
			choice_str = input(f'Entry must be in {index_name} column. Please try again.\n')
			try:
				choice = int(choice_str)
			except ValueError:
				choice_str = input(f'You must enter a number (or nothing), then hit return.\n')

		return choice, df.loc[choice,return_col]


def pick_schema(paramfile):
	# TODO tell user to choose desired db via paramfile
	con = dbr.establish_connection(paramfile=paramfile)  # TODO error handling for paramfile
	print(f'Connection established to database {con.info.dbname}')
	cur = con.cursor()
	db_df = pd.DataFrame(dbr.query('SELECT datname FROM pg_database',[],[],con,cur))

	db_idx,desired_db = pick_one(db_df,0,item='database')
	if desired_db != con.info.dbname:
		cur.close()
		con.close()
		con = dbr.establish_connection(paramfile,db_name=desired_db)
		cur = con.cursor()

	schema_df = pd.DataFrame(dbr.query('SELECT schema_name FROM information_schema.schemata',[],[],con,cur))
	schema_idx,schema = pick_one(schema_df,0,item='schema')

	# TODO show user list of schemas existing in db
	# TODO if new, create new schema with all the necessary cdf tables
	# TODO if old, check that schema has right format
	# clean up
	if cur:
		cur.close()
	if con:
		con.close()
	return schema


def pick_election(session,schema):
	# TODO read elections from schema.Election table
	# user picks existing or enters info for new
	# if election is new, enter its info into schema.Election
	return election


def pick_state(path_to_states='../local_data/'):
	"""Returns a State object"""
	if path_to_states[-1] != '/': path_to_states += '/'
	state_df = pd.DataFrame(os.listdir(path_to_states),columns='State')
	state_idx,state_name = pick_one(state_df,'State', item='state')
	if state_idx != None:
		# initialize the state
		spath = f'{path_to_states}{state_name}'
		ss = sf.State(state_name,spath)
	else:
		ss = create_state(path_to_states)
	# TODO if no state chosen, return None
	return ss


def create_state(parent_path):
	"""walk user through state creation, return State object"""
	# TODO
	state_name = input('Enter a short name (alphanumeric only, no spaces) for your state (e.g., \'NC\')')
	# TODO check alphanumeric only
	state_path = f'{parent_path}{state_name}/'

	# create state directory
	try:
		os.mkdir(state_path)
	except FileExistsError:
		print(f'Directory {state_path} already exists, will be preserved')
	else:
		print(f'Directory {state_path} created')

	subdir_list = ['context','data','output']
	for sd in subdir_list:
		sd_path = f'{state_path}{sd}'
		try:
			os.mkdir(sd_path)
		except FileExistsError:
			print(f'Directory {sd_path} already exists, will be preserved')
		else:
			print(f'Directory {sd_path} created')

	# TODO put required files into the context directory
	ss = sf.State(state_name,state_path)
	return ss


def pick_munger(path_to_munger_dir='../mungers/',column_list=None):
	# TODO if <column_list>, offer only mungers wtih that <column_list>
	# TODO if no munger chosen, return None
	return munger


def create_munger(column_list=None):
	# TODO walk user through munger creation
	return munger


def new_datafile(raw_file,raw_file_sep,db_paramfile):
	"""Guide user through process of uploading data in <raw_file>
	into common data format.
	Assumes cdf db exists already"""
	# connect to postgres to create schema if necessary

	schema = pick_schema(db_paramfile)

	state = pick_state()

	eng, meta = dbr.sql_alchemy_connect(paramfile=db_paramfile)
	Session = sessionmaker(bind=eng)
	new_df_session = Session()

	election = pick_election(new_df_session,schema)

	raw = pd.read_csv(raw_file,sep=raw_file_sep)
	column_list = raw.columns
	munger = pick_munger(column_list=column_list)

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
	raw_file = '../../local_data/NC/data/2018g/nc_general/results_pct_20181106.txt'
	raw_file_sep = '\t'
	db_paramfile = '../../local_data/database.ini'
	new_datafile(raw_file, raw_file_sep, db_paramfile)
	print('Done! (user_interface)')