import db_routines as dbr
import user_interface as ui
from sqlalchemy.orm import sessionmaker
import os


if __name__ == '__main__':

	d = ui.get_runtime_parameters(
		['project_root','juris_name','db_paramfile','db_name','rollup_directory','munger_name'])

	# pick jurisdiction
	juris = ui.pick_juris_from_filesystem(d['project_root'],juris_name=d['juris_name'])

	# TODO create db if it does not already exist
	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=d['db_paramfile'],db_name=d['db_name'])
	Session = sessionmaker(bind=eng)
	sess = Session()
	skip_load = input('Skip context loading (y/n)?\n')
	if skip_load == 'y':
		print('Warning: results for contests, selections and reporting units not loaded will not be processed.')
	else:
		juris.load_context_to_db(sess,d['project_root'])

	ui.track_results_file(d['project_root'],sess,d['results_file'])

	# pick munger
	munger = ui.pick_munger(
		project_root=d['project_root'],
		mungers_dir=os.path.join(d['project_root'],'mungers'),session=sess,munger_name=d['munger_name'])

	# load new datafile
	ui.new_datafile(sess,munger,d['data_path'],juris=juris,project_root=d['project_root'])

	eng.dispose()
	print('Done!')

	exit()
