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
		juris_name = None
		db_paramfile = ui.pick_paramfile()
		munger_name = None

	else:
		d = ui.config(section='election_anomaly',msg='Pick a parameter file.')
		project_root = d['project_root']
		juris_name = None
		db_paramfile = d['db_paramfile']
		munger_name = d['munger_name']



	j_path = os.path.join(project_root,'jurisdictions')
	juris = ui.pick_juris_from_filesystem(project_root,j_path,check_files=False)

	# connect to db
	db_name = ui.pick_database(project_root,db_paramfile)
	eng = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	sess = Session()

	juris.load_context_to_db(sess,project_root)

	eng.dispose()
	exit()
