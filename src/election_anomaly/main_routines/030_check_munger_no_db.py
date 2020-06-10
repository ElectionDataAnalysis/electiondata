import os
from election_anomaly import user_interface as ui


if __name__ == '__main__':

	d = ui.get_runtime_parameters(
		['project_root','munger_name'])

	# pick munger (nb: session=None by default, so info pulled from file system, not db)
	munger = ui.pick_munger(
		mungers_dir=os.path.join(d['project_root'],'mungers'),project_root=d['project_root'],
		munger_name=d['munger_name'])

	exit()
