import os
import user_interface as ui


if __name__ == '__main__':

	interact = input('Run interactively (y/n)?\n')
	if interact == 'y':
		project_root = ui.get_project_root()


	else:
		d = ui.config(section='election_anomaly',msg='Pick a paramfile for 050.')
		project_root = d['project_root']

	# project_root = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/'

	# pick munger (nb: session=None by default, so info pulled from file system, not db)
	munger = ui.pick_munger(mungers_dir=os.path.join(project_root,'mungers'),project_root=project_root)

	exit()
