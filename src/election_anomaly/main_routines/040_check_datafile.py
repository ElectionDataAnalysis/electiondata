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
	d = ui.get_runtime_parameters(
		['project_root','results_file','db_paramfile','db_name','juris_name','munger_name'])

	# initialize root widget for tkinter
	tk_root = tk.Tk()

	# pick jurisdiction
	juris = ui.pick_juris_from_filesystem(d['project_root'],juris_name=d['juris_name'])

	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=d['db_paramfile'],db_name=d['db_name'])
	Session = sessionmaker(bind=eng)
	sess = Session()

	# pick munger
	munger = ui.pick_munger(
		mungers_dir=os.path.join(d['project_root'],'mungers'),project_root=d['project_root'],
		session=sess,munger_name=d['munger_name'])

	# check munger against datafile.
	munger.check_against_datafile(d['results_file'])

	eng.dispose()
	print('Done!')

	exit()
