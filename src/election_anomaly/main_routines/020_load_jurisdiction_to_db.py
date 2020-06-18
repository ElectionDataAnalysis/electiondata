import os
from election_anomaly import user_interface as ui
from election_anomaly import db_routines as dbr
from sqlalchemy.orm import sessionmaker

if __name__ == '__main__':
	d, error = ui.get_runtime_parameters(
		['project_root','juris_name','db_paramfile','db_name'])

	j_path = os.path.join(d['project_root'],'jurisdictions')
	juris = ui.pick_juris_from_filesystem(
		d['project_root'],j_path,check_files=False,juris_name=d['juris_name'])

	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=d['db_paramfile'],db_name=d['db_name'])
	Session = sessionmaker(bind=eng)
	sess = Session()

	juris.load_juris_to_db(sess,d['project_root'])

	eng.dispose()
	exit()
