import os
import re
import datetime
from pathlib import Path
import shutil
import election_data_analysis as e
from election_data_analysis import database as d
from election_data_analysis import user_interface as ui


def get_testing_data(url: str, target: str = "TestingData"):
    # if there is no testing data directory
    if not os.path.isdir(target):
        # create a shallow copy of the git directory in current directory
        cmd = f"git clone --depth 1 -b main {url}"
        os.system(cmd)
        # remove the git information
        shutil.rmtree(os.path.join(target, ".git"), ignore_errors=True)
        os.remove(os.path.join(target, ".gitignore"))

        print(f"Files downloaded from {url} into {Path(target).absolute()}")
    else:
        print(f"Tests will use data in existing directory: {Path(target).absolute()}")
    return


def run(load_data: bool = True, dbname: str = None):
    reference_param_file = test_param_file = os.path.join(Path(__file__).parents[1], "src", "run_time.ini")
    dataloader_param_file = "run_time.ini"

    original_dir = Path(__file__).parent.absolute()
    test_dir = Path(__file__).parent.absolute()

    if load_data:
        get_testing_data("https://github.com/ElectionDataAnalysis/TestingData.git", "TestingData")

    if dbname is None:
        # create unique name for test database
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        dbname = f"test_{ts}"

    with open(reference_param_file, "r") as f:
        original_parameter_text = f.read()
    new_parameter_text = re.sub("dbname=[^\n]*\n", f"dbname={dbname}\n", original_parameter_text)
    new_parameter_text = re.sub("results_dir=[^\n]*\n", "results_dir=TestingData\n", new_parameter_text)

    if load_data:
        # create local run_time.ini for dataloader, based on existing
        with open(dataloader_param_file, "w") as f:
            f.write(new_parameter_text)

        # Load the data
        dl = e.DataLoader()
        dl.load_all(move_files=True)

    # move to tests directory
    os.chdir(test_dir)

    # create run_time.ini for testing routines to use
    with open(test_param_file, "w") as f:
        f.write(new_parameter_text)

    # run pytest
    os.system("pytest")

    # move back to original directory
    os.chdir(original_dir)

    # allow user to inspect database if desired
    input(f"Hit return to continue (and remove test db {dbname} and test data)")

    # remove database
    new_params, err = ui.get_runtime_parameters(
        required_keys=["host","port","user","password","dbname"],
        param_file=test_param_file,
        header="postgresql",
        err=dict(),
    )

    if load_data:
        d.remove_database(new_params)

    # return run_time.ini to its original state (necessary only when it was in the current directory)
    if reference_param_file == test_param_file:
        with open(reference_param_file, "w") as f:
            f.write(original_parameter_text)

    # remove testing data
    if load_data:
        os.system(f"rm -rf TestingData")
    return


if __name__ == "__main__":
#    run(load_data=False,dbname='test_0924_1643')
    run()
    exit()