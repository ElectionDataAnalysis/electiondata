import db_routines as dbr
import user_interface as ui
from sqlalchemy.orm import sessionmaker
import tkinter as tk


if __name__ == '__main__':
	print("""Ready to load some election result data?
			"This program will walk you through the process of creating or checking
			"an automatic munger that will load your data into a database in the 
			"NIST common data format.""")

	project_root = ui.get_project_root()

	# initialize root widget for tkinter
	tk_root = tk.Tk()

	# pick db to use
	db_paramfile = ui.pick_paramfile(tk_root,project_root)
	db_name = ui.pick_database(project_root,db_paramfile)

	# connect to db
	eng, meta = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	new_df_session = Session()

	# get datafile & info
	dfile_d, enum_d, raw_file = ui.find_datafile(tk_root,project_root,new_df_session)
	# TODO store \t and , directly?
	if enum_d['_datafile_separator'] == 'tab':
		sep = '\t'
	elif enum_d['_datafile_separator'] == 'comma':
		sep = ','
	else:
		raise Exception(f'separator {enum_d["_datafile_separator"]} not recognized')

	state_short_name = None

	# load new datafile
	# TODO handle default values more programmatically
	encoding = dfile_d['encoding']
	if encoding == '':
		encoding = 'utf-8'

	state, mnger = ui.new_datafile(
		raw_file,sep,new_df_session,state_short_name=state_short_name,encoding=encoding,project_root=project_root)

	eng.dispose()
	print('Done! (user_interface)')

	exit()
