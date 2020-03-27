import db_routines as dbr
import user_interface as ui
from sqlalchemy.orm import sessionmaker
import numpy


if __name__ == '__main__':

	print('\nReady to load some election result data?\n'
		  'This program will walk you through the process of creating or checking\n'
		  'an automatic munger that will load your data into a database in the '
		  'NIST common data format.')

	project_root = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/'

	# pick db to use
	db_paramfile = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/database.ini'
	db_name = 'MD'

	# connect to db
	eng, meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	new_df_session = Session()

	# get datafile & info
	dfile_d = {'short_name': 'md_general2018', 'file_name': 'All_By_Precinct_2018_General.csv',
			   'encoding': 'iso-8859-1', 'source_url': 'MD website',
			   'file_date': numpy.datetime64('2222-11-11T00:00:00.000000000'),
			   'download_date': numpy.datetime64('2222-11-11T00:00:00.000000000'),
			   'note': 'source details incorrect', '_datafile_separator_Id': 37,
			   'Other_datafile_separator': '', 'Id': 66}
	enum_d = {'_datafile_separator': 'comma'}
	raw_file = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/MD_old/data/2018g/md_general/All_By_Precinct_2018_General.csv'

	# TODO store \t and , directly?
	if enum_d['_datafile_separator'] == 'tab':
		sep = '\t'
	elif enum_d['_datafile_separator'] == 'comma':
		sep = ','
	else:
		raise Exception(f'separator {enum_d["_datafile_separator"]} not recognized')


	#state_short_name = 'FL'
	state_short_name = 'MD'

	# load new datafile

	# TODO fix test_munger keyword argument for new_datafile() so it changes col names as necessary
	state, munger = ui.new_datafile(
		raw_file,sep,new_df_session,state_short_name=state_short_name,encoding=dfile_d['encoding'],project_root=project_root)

	eng.dispose()
	print('Done! (user_interface)')
	exit()