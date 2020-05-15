import db_routines as dbr
import user_interface as ui
from sqlalchemy.orm import sessionmaker
import tkinter as tk
import os


if __name__ == '__main__':
	print("""Ready to load some election result data?
			"This program will walk you through the process of creating or checking
			"an automatic munger that will load your data into a database in the 
			"NIST common data format.""")

	project_root = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/'

	# pick db to use
	db_paramfile = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/database.ini'

	db_name = 'Philadelphia'

	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	sess = Session()


	# datafile & info
	raw_file_dir = '/Users/Steph-Airbook/Documents/Temp/Philadelphia/data/'

	#raw_file_path = os.path.join(raw_file_dir,'small20181106.txt')
	raw_file_path = os.path.join(raw_file_dir,'2018_general.csv')

	# pick munger
	munger = ui.pick_munger(mungers_dir=os.path.join(project_root,'mungers'),
							project_root=project_root,session=sess)
	sep = munger.separator.replace('\\t','\t')  # TODO find right way to read \t
	encoding = munger.encoding
	juris_short_name = 'Phila'
	juris = ui.pick_juris_from_filesystem(project_root,juris_name=juris_short_name)




	# get datafile & info
	[dfile_d, enum_d, data_path] = ui.pick_datafile(project_root,sess)

	# check munger against datafile.
	munger.check_against_datafile(data_path)

	# TODO check datafile/munger against db?

	# load new datafile
	ui.new_datafile(
		sess,munger,data_path,juris=juris,project_root=project_root)

	eng.dispose()
	print('Done! (user_interface)')

	exit()
