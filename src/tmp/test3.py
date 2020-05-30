import user_interface as ui
from sqlalchemy.orm import sessionmaker
import db_routines as dbr

if __name__ == '__main__':
	project_root = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/'

	# initialize root widget for tkinter

	# pick db to use
	db_paramfile = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/database.ini'
	db_name = 'NC'
	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	sess = Session()

#
	table = 'ReportingUnit'
	known_info_d={'ReportingUnitType_Id':15}
	unique = [['Name'],['Name','ReportingUnitType_Id']]
	mode = 'database_and_filesystem'
	output = ui.new_record_info_from_user_OLD(sess,project_root,table,mode=mode,unique=unique)
	print(output)
	eng.dispose()
	print('Done!')

	exit()
