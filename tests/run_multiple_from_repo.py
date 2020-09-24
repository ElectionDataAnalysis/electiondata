import os
import re
import datetime
from pathlib import Path
import election_data_analysis as e
from election_data_analysis import user_interface as ui

param_file = os.path.join(Path(__file__).parents[1],"src","run_time.ini")
# create a copy of the git directory in current directory
cmd = 'git clone https://github.com/ElectionDataAnalysis/TestingData.git'
if not os.path.isdir('TestingData'):
    os.system(cmd)
else:
    print(f"directory TestingData already exists")

# create unique name for test database
ts = datetime.datetime.now().strftime("%m%d_%H%M")
db = f"test_{ts}"

# get runtime parameters from run_time.ini in current directory
params, err = ui.get_runtime_parameters(
    required_keys=e.multi_data_loader_pars,
    param_file=param_file,
    header="election_data_analysis",
    err=dict(),
)

# alter the run_time.ini file to use test database
with open(param_file, "r") as f:
    original_params = f.read()
new_params = re.sub("dbname=[^\n]*\n", f"dbname={db}\n", original_params)
new_params = re.sub("results_dir=[^\n]*\n", "results_dir=TestingData\n", new_params)
with open(param_file, "w") as f:
    f.write(new_params)

# Load the data
dl = e.DataLoader()
dl.load_all()

# move to tests directory
original_dir = Path(__file__).parent.absolute()
test_dir =
os.system(f"cd {test_dir}")

# run pytest
os.system("pytest")

# move back to original directory
os.system(f"cd {original_dir}")

# return run_time.ini to its original state
with open(param_file, "w") as f:
    f.write(original_params)

# remove testing data
os.system(f"rm -rf TestingData")

# TODO remove database

exit()