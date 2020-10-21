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


def run_tests(test_dir: str, test_param_file: str, db_params: dict):
    """ move to tests directory, run tests, move back
    db_params must have host, user, pass, db_name"""

    # note current directory
    original_dir = os.getcwd()

    # move to tests directory
    os.chdir(test_dir)

    # create run_time.ini for testing routines to use
    # default port is 5432; default password is ""
    if "port" not in db_params.keys():
        db_params["port"] = "5432"
    if "password" not in db_params.keys():
        db_params["password"] = ""

    new_parameter_text = f"[postgresql]\n" \
                         f"host={db_params['host']}\n" \
                         f"port={db_params['port']}\n" \
                         f"dbname={db_params['dbname']}\n" \
                         f"user={db_params['user']}\n" \
                         f"password={db_params['password']}"
    with open(test_param_file, "w") as f:
        f.write(new_parameter_text)

    # run pytest
    os.system("pytest")

    # move back to original directory
    os.chdir(original_dir)
    return


def create_param_file(db_params: dict, multi_data_loader_pars: dict, target_dir: str) -> Optional[str]:
    err_str = None
    if not os.path.isdir(target_dir):
        return f"Directory not found: {target_dir}"
    db_params_str = "\n".join([f"{s}={db_params[s]}" for s in db_params.keys()])
    mdlp_str = "\n".join([f"{s}={multi_data_loader_pars[s]}" for s in multi_data_loader_pars.keys()])

    with open(os.path.join(target_dir,"run_time.ini"),"w") as f:
        f.write("[postgresql]\n" + db_params_str + "\n\n[election_data_analysis]\n" + mdlp_str)

    return err_str


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

        create_param_file(db_params, mdlp, test_dir)

        # Load the data
        dl = e.DataLoader()
        dl.load_all(move_files=False)

    run_tests(test_dir, test_param_file, db_params)

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

    # return run_time.ini to its original state (necessary only when it was in the current directory)
    if reference_param_file == test_param_file:
        with open(reference_param_file, "w") as f:
            f.write(original_parameter_text)

    # remove testing data
    if load_data:
        os.system(f"rm -rf TestingData")
    return


if __name__ == "__main__":
    run2()
    exit()