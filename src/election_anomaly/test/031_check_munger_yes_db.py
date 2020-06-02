import states_and_files as sf
import os
import user_interface as ui
import munge_routines as mr
import db_routines as dbr
from sqlalchemy.orm import sessionmaker


if __name__ == '__main__':

	interact = input('Run interactively (y/n)?\n')
	if interact == 'y':
		project_root = ui.get_project_root()
		db_paramfile = ui.pick_paramfile()
		db_name = ui.pick_database(project_root, db_paramfile)
	else:
		d = ui.config(section='election_anomaly',msg='Pick a parameter file.')
		project_root = d['project_root']
		db_paramfile = d['db_paramfile']
		db_name = d['db_name']

	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	sess = Session()

	# pick munger (nb: session=None by default, so info pulled from file system, not db)
	munger = ui.pick_munger(mungers_dir=os.path.join(project_root,'mungers'),project_root=project_root,session=sess)

	eng.dispose()
	exit()
