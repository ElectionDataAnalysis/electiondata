import db_routines as dbr
import user_interface as ui
from sqlalchemy.orm import sessionmaker
import os
import munge_routines as mr
import pandas as pd
import csv


if __name__ == '__main__':
	print("Ready to load some election result data?\n")

	project_root = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/'

	# pick db to use
	db_paramfile = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/database.ini'

	db_name = 'Philadelphia'

	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	new_df_session = Session()

	# pick munger
	munger = ui.pick_munger(mungers_dir=os.path.join(project_root,'mungers'),
							project_root=project_root,session=new_df_session)

	# datafile & info
	raw_file_dir = '/Users/Steph-Airbook/Documents/Temp/Philadelphia/data/'

	#raw_file_path = os.path.join(raw_file_dir,'small20181106.txt')
	raw_file_path = os.path.join(raw_file_dir,'2018_general.csv')

	sep = munger.separator.replace('\\t','\t')  # TODO find right way to read \t
	encoding = munger.encoding
	juris_short_name = 'Phila'
	juris = ui.pick_juris_from_filesystem(project_root,juris_name=juris_short_name)

	# load new datafile
	raw = pd.read_csv(
		raw_file_path,sep=sep,dtype=str,encoding=encoding,quoting=csv.QUOTE_MINIMAL,
		header=list(range(munger.header_row_count)))

	[raw,info_cols,num_cols] = mr.clean_raw_df(raw,munger)

	mr.raw_elements_to_cdf(new_df_session,project_root,juris,munger,raw,num_cols)

	eng.dispose()
	print('Done!')

	exit()
