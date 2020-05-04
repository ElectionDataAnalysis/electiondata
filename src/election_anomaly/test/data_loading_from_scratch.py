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

	project_root = ui.get_project_root()

	# initialize root widget for tkinter
	tk_root = tk.Tk()

	# pick db to use
	db_paramfile = ui.pick_paramfile(project_root)
	db_name = ui.pick_database(project_root,db_paramfile)

	# connect to db
	eng, meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	new_df_session = Session()


	# pick munger
	munger = ui.pick_munger(mungers_dir=os.path.join(project_root,'mungers'),
							project_root=project_root,session=new_df_session)

	# get datafile & info
	dfile_d, enum_d, raw_file = ui.pick_datafile(project_root,new_df_session)
	# TODO store \t and , directly?
	if enum_d['_datafile_separator'] == 'tab':
		sep = '\t'
	elif enum_d['_datafile_separator'] == 'comma':
		sep = ','
	else:
		raise Exception(f'separator {enum_d["_datafile_separator"]} not recognized')
	encoding = dfile_d['encoding']

	juris_short_name = None

	# load new datafile
	ui.new_datafile(
		new_df_session,munger,raw_file,sep,juris=juris_short_name,encoding=encoding,project_root=project_root)

	eng.dispose()
	print('Done! (user_interface)')

	exit()
