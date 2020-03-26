#!usr/bin/python3

import user_interface as ui
import db_routines as dbr
from sqlalchemy.orm import sessionmaker


if __name__ == '__main__':
	project_root = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src'
	table = 'CandidateContest'
	name_field = 'Name'
	known = {}

	# connect to db
	db_paramfile = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/database.ini'
	db_name = 'MD'
	eng,meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	sess = Session()
	a,b = ui.create_record_in_db(sess,project_root,table,name_field=name_field,known_info_d=known)

	# cleanup
	eng.dispose()
	exit()
