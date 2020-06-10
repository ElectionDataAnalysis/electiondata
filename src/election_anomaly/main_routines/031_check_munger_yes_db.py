import os
from election_anomaly import user_interface as ui
from election_anomaly import db_routines as dbr
from sqlalchemy.orm import sessionmaker


if __name__ == '__main__':

	d = ui.get_runtime_parameters(
		['project_root','db_paramfile','db_name','munger_name'])

	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=d['db_paramfile'],db_name=d['db_name'])
	Session = sessionmaker(bind=eng)
	sess = Session()

	# pick munger (nb: session=None by default, so info pulled from file system, not db)
	munger = ui.pick_munger(
		mungers_dir=os.path.join(d['project_root'],'mungers'),project_root=d['project_root'],
		session=sess,munger_name=d['munger_name'])

	eng.dispose()
	exit()
