#!usr/bin/python3
import db_routines as dbr
import pandas as pd
from sqlalchemy.orm import sessionmaker


def pick_one(df,item='row'):
	"""Returns index of item chosen by user"""
	# TODO check that index entries are ints (or handle non-int)
	print(df)
	choice = int(input(f'Enter the {df.index.name} of the desired {item} (or hit return if none is correct):\n'))
	if choice == '':
		return None
	else:
		while choice not in df.index:
			choice = input('Entry must be in {}. Please try again.'.format(df.index))
		return choice


def pick_schema(paramfile):
	# TODO tell user to choose desired db via paramfile
	con = dbr.establish_connection(paramfile=paramfile)  # TODO error handling for paramfile
	cur = con.cursor()

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


def pick_state(path_to_state_dir='../local_data/'):
	# TODO if no state chosen, return None
	return state


def create_state():
	# TODO walk user through state creation
	return state


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
	if state == None:
		state = create_state()

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

	return