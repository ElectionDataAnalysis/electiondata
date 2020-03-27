import db_routines as dbr
import user_interface as ui
from sqlalchemy.orm import sessionmaker
import numpy


if __name__ == '__main__':

	print('\nGet ready to load some election result data!\n')
	munger_needs_test = input('Do you want program to help check munger (y/n)\n')
	if munger_needs_test:
		print('This program will walk you through the process of creating or checking\n'
		'an automatic munger that will load your data into a database in the '
		'NIST common data format.')
		test_munger=True
	else:
		test_munger=False


	project_root = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/'

	# pick db to use
	db_paramfile = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/database.ini'
	db_name = 'NC'

	# connect to db
	eng, meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	new_df_session = Session()

	# get datafile & info
	dfile_d = {'short_name': 'nc_general2018', 'file_name': 'results_pct_20181106.txt',
			   'encoding': 'utf-8', 'source_url': 'NC website',
			   'file_date': numpy.datetime64('2222-11-11T00:00:00.000000000'),
			   'download_date': numpy.datetime64('2222-11-11T00:00:00.000000000')}
	enum_d = {'_datafile_separator': 'tab'}
	raw_file = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/NC_old/data/2018g/nc_general/results_pct_20181106.txt'

	# TODO store \t and , directly?
	if enum_d['_datafile_separator'] == 'tab':
		sep = '\t'
	elif enum_d['_datafile_separator'] == 'comma':
		sep = ','
	else:
		raise Exception(f'separator {enum_d["_datafile_separator"]} not recognized')


	#state_short_name = 'FL'
	state_short_name = 'NC'

	# load new datafile

	state, munger = ui.new_datafile(
		raw_file,sep,new_df_session,state_short_name=state_short_name,encoding=dfile_d['encoding'],
		project_root=project_root,test_munger=test_munger)

	eng.dispose()
	print('Done! (user_interface)')
	exit()