import states_and_files as sf
import os
import user_interface as ui


if __name__ == '__main__':
	print("""Ready to load some election result data?
			"This program will walk you through the process of creating or checking
			"an automatic munger that will load your data into a database in the 
			"NIST common data format.""")

	project_root = ui.get_project_root()
	j_path = os.path.join(project_root,'jurisdictions')

	juris = ui.pick_juris_from_filesystem(project_root,j_path,check_files=True)

	print(f'Context files for {juris.short_name} are internally consistent.')
	exit()
