import states_and_files as sf
import os
import user_interface as ui
import munge_routines as mr
import db_routines as dbr
from sqlalchemy.orm import sessionmaker



if __name__ == '__main__':
	print("""Ready to load some election result data?
			"This program will walk you through the process of creating or checking
			"an automatic munger that will load your data into a database in the 
			"NIST common data format.""")

	project_root = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/'

	j_path = os.path.join(project_root,'jurisdictions')

	juris_short_name = None
	juris = ui.pick_juris_from_filesystem(project_root,j_path,check_files=False)

	# pick db to use
	db_paramfile = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/database.ini'
	db_name = ui.pick_database(project_root,db_paramfile)

	# connect to db
	eng, meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	sess = Session()

	# pick munger (nb: session needed to read enum info from db)
	munger = ui.pick_munger(
		sess,munger_dir=os.path.join(project_root,'mungers'),
		root=project_root)

	eng.dispose()
	exit()
