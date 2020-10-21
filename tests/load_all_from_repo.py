import os
import datetime
from pathlib import Path
import shutil
from typing import Optional
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


def run2(load_data: bool = True, dbname: str = None):
    reference_param_file = test_param_file = os.path.join(Path(__file__).parents[1], "src", "run_time.ini")
    with open(reference_param_file, "r") as f:
        original_parameter_text = f.read()
    test_dir = Path(__file__).parent.absolute()

    # name the db
    if dbname is None:
        # create unique name for test database
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        dbname = f"test_{ts}"

    # get db parameters
    required_keys = {"host", "user"}
    optional_keys = {"password", "port"}
    db_params, err1 = ui.get_runtime_parameters(
        required_keys=required_keys,
        optional_keys=optional_keys,
        header="postgresql",
        param_file="run_time.ini"
    )
    db_params["dbname"] = dbname

    if load_data:
        get_testing_data("https://github.com/ElectionDataAnalysis/TestingData.git", "TestingData")

    if load_data:
        # create local run_time.ini for dataloader, based on existing
        mdlp, err2 = ui.get_runtime_parameters(
            required_keys=e.multi_data_loader_pars,
            header="election_data_analysis",
            param_file="run_time.ini",
        )

        ui.create_param_file(db_params, mdlp, test_dir)

        # Load the data
        dl = e.DataLoader()
        dl.load_all(move_files=False)

    ui.run_tests(test_dir, test_param_file, db_params)

    # allow user to inspect database if desired
    input(f"Hit return to continue (and remove test db {dbname} and test data)")

    # remove database
    new_params, err = ui.get_runtime_parameters(
        required_keys=["host", "port", "user", "password", "dbname"],
        param_file=test_param_file,
        header="postgresql",
        err=dict(),
    )

    if load_data:
        d.remove_database(new_params)
    ui.run_tests(test_dir, test_param_file, db_params)

    if load_data:
        # allow user to pause, option to remove db
        remove_db = input(f"Remove test db {dbname} (y/n)?")

        if remove_db == "y":
            # remove database
            new_params, err = ui.get_runtime_parameters(
                required_keys=["host","port","user","password","dbname"],
                param_file=test_param_file,
                header="postgresql",
                err=dict(),
            )
            d.remove_database(new_params)

        remove_dir = input("Remove TestingData directory (y/n)?")
        if remove_dir == "y":
            # remove testing data
            os.system(f"rm -rf TestingData")

    # return run_time.ini to its original state (necessary only when it was in the current directory)
    if reference_param_file == test_param_file:
        with open(reference_param_file, "w") as f:
            f.write(original_parameter_text)


    return


if __name__ == "__main__":
    run2()
    exit()