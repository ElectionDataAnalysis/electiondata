import db_routines as dbr
import user_interface as ui
from sqlalchemy.orm import sessionmaker
import os
import munge_routines as mr
import pandas as pd
import csv


if __name__ == '__main__':
	print("Ready to load some election result data?\n"
			"This program will walk you through the process of creating or checking\n"
			"an automatic munger that will load your data into a database in the NIST common data format.")

	project_root = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/'

	# initialize root widget for tkinter

	# pick db to use
	db_paramfile = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/database.ini'

	db_name = 'NC'

	# connect to db
	eng, meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	new_df_session = Session()

	# pick munger
	munger = ui.pick_munger(
		new_df_session,munger_dir=os.path.join(project_root,'mungers'),
		root=project_root)

	# datafile & info
	raw_file_dir = '/Users/Steph-Airbook/Documents/Temp/'

	#raw_file_path = os.path.join(raw_file_dir,'small20181106.txt')
	raw_file_path = os.path.join(raw_file_dir,'results_pct_20181106.txt')

	sep = '\t'
	juris_short_name = 'NC_5'
	juris = ui.pick_juris_from_filesystem(
		project_root,path_to_jurisdictions=os.path.join(project_root,'jurisdictions'),
		jurisdiction_name=juris_short_name)

	# load new datafile
	# TODO handle default values more programmatically
	encoding = 'iso-8859-1'
	raw = pd.read_csv(
		raw_file_path,sep=sep,dtype=str,encoding=encoding,quoting=csv.QUOTE_MINIMAL,
		header=list(range(munger.header_row_count)))

	[raw,info_cols,num_cols] = mr.clean_raw_df(raw,munger)

	mr.raw_elements_to_cdf(new_df_session,project_root,juris,munger,raw,info_cols,num_cols,finalize=False)

	ui.new_datafile(new_df_session,munger,
					raw_file_path,sep,encoding,juris=juris,project_root=project_root,finalize_context=False)

	eng.dispose()
	print('Done!')

	exit()
