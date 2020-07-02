from election_anomaly import db_routines as dbr
from election_anomaly import user_interface as ui
from sqlalchemy.orm import sessionmaker
import os


if __name__ == '__main__':

	d, error = ui.get_runtime_parameters(
		['project_root','juris_name','db_paramfile','db_name','munger_name','results_file'])

	# pick jurisdiction
	juris, juris_error = ui.pick_juris_from_filesystem(d['project_root'],juris_name=d['juris_name'],check_files=True)

	# create db if it does not already exist
	error = dbr.establish_connection(paramfile=d['db_paramfile'],db_name=d['db_name'])
	if error:
		dbr.create_new_db(d['project_root'], d['db_paramfile'], d['db_name'])

	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=d['db_paramfile'],db_name=d['db_name'])
	Session = sessionmaker(bind=eng)
	sess = Session()

	error = juris.load_juris_to_db(sess,d['project_root'])

	ui.track_results_file(d['project_root'],sess,d['results_file'])

	# pick munger
	munger, error = ui.pick_munger(
		project_root=d['project_root'],
		mungers_dir=os.path.join(d['project_root'],'mungers'),session=sess,munger_name=d['munger_name'])

	# load new datafile
	ui.new_datafile(sess,munger,d['results_file'],juris=juris,project_root=d['project_root'])

	eng.dispose()
	print('Done!')

	exit()
