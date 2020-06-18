import os
from election_anomaly import user_interface as ui


if __name__ == '__main__':
	d, error = ui.get_runtime_parameters(['project_root','juris_name'])
	j_path = os.path.join(d['project_root'],'jurisdictions')

	juris = ui.pick_juris_from_filesystem(
		d['project_root'],j_path,check_files=True,juris_name=d['juris_name'])

	print(f'Context files for {juris.short_name} are internally consistent.')
	exit()
