import db_routines as dbr
import user_interface as ui
from sqlalchemy.orm import sessionmaker
import os
import tkinter as tk


if __name__ == '__main__':
	print("""Ready to load some election result data?
			"This program will walk you through the process of creating or checking
			"an automatic munger that will load your data into a database in the 
			"NIST common data format.""")

	project_root = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/'

	# initialize root widget for tkinter

	# pick db to use
	db_paramfile = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/database.ini'

	db_name = 'NC_TEST'

	# connect to db
	eng, meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	new_df_session = Session()

	# pick munger
	munger = ui.pick_munger(
		new_df_session,munger_dir=os.path.join(project_root,'mungers'),
		root=project_root)


	# datafile & info
	raw_file_path = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/NC_old/data/2018g/nc_general/results_pct_20181106.txt'
	sep = '\t'
	juris_short_name = 'NC'

	# load new datafile
	# TODO handle default values more programmatically
	encoding =  'iso-8859-1'

	ui.new_datafile_NEW(new_df_session,munger,
		raw_file_path,sep,encoding,juris_short_name=juris_short_name,project_root=project_root)

	eng.dispose()
	print('Done! (user_interface)')

	exit()
