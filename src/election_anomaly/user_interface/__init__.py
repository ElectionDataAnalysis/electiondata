#!usr/bin/python3
import db_routines as dbr

def pick_one(dataframe,item='row'):
	"""Returns index of item chosen by user"""
	# TODO check that index entries are ints (or handle non-int)
	print(dataframe)
	choice = int(input('Enter the {} of the desired {} (or hit return if none is correct):\n'.format(dataframe.index.name,item)))
	if choice == '':
		return None
	else:
		while choice not in dataframe.index:
			choice = input('Entry must be in {}. Please try again.'.format(dataframe.index))
		return choice


def pick_schema(con,cur):
	# TODO show user list of schemas existing in db
	# TODO if new, create new schema
	# TODO if old, check that schema has right format
	return schema


def pick_election(con,cur,schema):
	# TODO read elections from schema.Election table
	# user picks existing or enters info for new
	# if election is new, enter its info into schema.Election
	return election


def pick_munger(path_to_munger_dir='../mungers/',column_list=[]):
	# TODO if <column_list> != [], offer only mungers wtih that <column_list>
	return munger


def new_datafile(raw_file):
	"""Guide user through process of uploading data in <raw_file> into common data format"""
	# connect to postgres
	# TODO tell user to choose desired db via paramfile
	con = dbr.establish_connection()	# TODO error handling for paramfile
	cur = con.cursor()

	schema = pick_schema(con,cur)

	election = pick_election(con,cur,schema)

	raw = pd.read_csv(raw_file) # TODO separator, etc.
	column_list = raw.columns
	munger = pick_munger(column_list=column_list)

	return