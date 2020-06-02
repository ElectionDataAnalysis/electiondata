import db_routines as dbr
import user_interface as ui
from sqlalchemy.orm import sessionmaker
import tkinter as tk
import os


if __name__ == '__main__':
	print("""Ready to load some election result data?
			"This program will walk you through the process of creating or checking
			"an automatic munger that will load your data into a database in the 
			"NIST common data format.""")

	interact = input('Run interactively (y/n)?\n')
	if interact == 'y':
		project_root = ui.get_project_root()
		juris_name = None
		db_paramfile = ui.pick_paramfile()
		db_name = ui.pick_database(project_root,db_paramfile)
		munger_name = None

	else:
		d = ui.config(section='election_anomaly',msg='Pick a paramfile for 050.')
		project_root = d['project_root']
		juris_name = d['juris_name']
		db_paramfile = d['db_paramfile']
		db_name = d['db_name']
		munger_name = d['munger_name']

	# pick jurisdiction
	juris = ui.pick_juris_from_filesystem(project_root,juris_name=juris_name)

	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	sess = Session()
	skip_load = input('Skip context loading (y/n)?\n')
	if skip_load == 'y':
		print('Warning: results for contests, selections and reporting units not loaded will not be processed.')
	else:
		juris.load_context_to_db(sess,project_root)

	if interact == 'y':
		[dfile_d,enum_d,data_path] = ui.pick_datafile(project_root,sess)
	else:
		data_path = d['data_path']

	# pick munger
	munger = ui.pick_munger(mungers_dir=os.path.join(project_root,'mungers'),
							project_root=project_root,session=sess,munger_name=munger_name)

	# get datafile & info
	# load new datafile
	ui.new_datafile(sess,munger,data_path,juris=juris,project_root=project_root)

	eng.dispose()
	print('Done!')

	exit()
