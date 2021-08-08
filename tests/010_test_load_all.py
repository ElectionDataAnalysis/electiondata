import os
import sys
import getopt
import datetime
from pathlib import Path
import shutil
from typing import Optional
import electiondata as eda
from electiondata import database as db
from electiondata import userinterface as ui
import inspect



def io(argv) -> Optional[list]:
    election = None
    jurisdiction = None
    file_name = "load_all_from_repo.py"
    try:
        opts, args = getopt.getopt(argv, "he:j:", ["election=", "juris="])
    except getopt.GetoptError:
        print(f"{file_name} -e <election> -j <jurisdiction>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print(f"{file_name} -e <election> -j <jurisdiction>")
            sys.exit()
        elif opt in ("-e", "--election"):
            election = arg
        elif opt in ("-j", "--juris"):
            jurisdiction = arg
    print(f"Election is {election}")
    print(f"Jurisdiction is {jurisdiction}")
    if (not election) or (not jurisdiction):
        ej_list = None
    else:
        ej_list = [(election, jurisdiction)]
    return ej_list


def optional_remove(dl: eda.DataLoader, dir_path: str) -> (Optional[dict], bool):
    err = None
    db_removed = False
    # give user option to remove db
    if dl is None:
        err = ui.add_new_error(
            err, "system",f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"DataLoader parameter is None"
        )
        return err, db_removed
    remove_db = input(f"Remove test db {dl.d['dbname']} (y/n)?\n")

    if remove_db == "y":
        err = dl.close_and_erase()
        if not err:
            db_removed = True
            print(f"db removed")
        else:
            print(f"db not removed due to error: {err['system']}")
        # define parameters to connect to postgres db
    return err, db_removed


def close_and_erase(dl: eda.DataLoader) -> Optional[dict]:
    db_params = {
        "host": dl.engine.url.host,
        "port": dl.engine.url.port,
        "user": dl.engine.url.username,
        "password": dl.engine.url.password,
        "dbname": dl.engine.url.database,
    }
    # point dataloader to default database
    dl.change_db("postgres")
    # remove the db
    err = db.remove_database(db_params)
    return err


def get_testing_data(
    url: Optional[str] = None,
    results_dir: Optional[str] = "TestingData",
):
    # if there is no target directory
    if not os.path.isdir(results_dir):
        # create a shallow copy of the git directory in current directory
        cmd = f"git clone --depth 1 -b main {url}"
        os.system(cmd)
        # remove the git information
        shutil.rmtree(os.path.join(results_dir, ".git"), ignore_errors=True)
        os.remove(os.path.join(results_dir, ".gitignore"))
        print(f"Files downloaded from {url} into {Path(results_dir).absolute()}")

    else:
        print(
            f"Tests will load data from existing directory: {Path(results_dir).absolute()}"
        )
    return


def run2(
    load_data: bool = True,
    dbname: Optional[str] = None,
    param_file: Optional[str] = None,
    test_dir: Optional[str] = None,
    election_jurisdiction_list: Optional[list] = None,
    rollup: bool = False,
) -> Optional[dict]:
    dl = None  # to keep syntax-checker happy

    err = None
    if not test_dir:
        # set the test_dir to the results-testing subdirectory of the directory containing this file
        test_dir = os.path.join(
            Path(__file__).parent.absolute(),"results_tests"
        )

    # name the db
    if dbname is None:
        # create unique name for test database
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        dbname = f"test_{ts}"

    # get absolute path for run_time.ini if no param_file is given
    if param_file is None:
        param_file = Path("run_time.ini").absolute()

    if load_data:
        try:
            # Load the data
            dl = eda.DataLoader(dbname=dbname, param_file=param_file)
            if not dl:
                err = ui.add_new_error(
                    err,
                    "ini",
                    f"{os.getcwd()}/run_time.ini",
                    "Error creating DataLoader.",
                )
                return err

            # restrict elections and jurisdictions to those given (if given)
            # otherwise use all in TestingData
            if not election_jurisdiction_list:
                election_jurisdiction_list = ui.election_juris_list(
                    dl.d["ini_dir"],
                    results_path="TestingData",
                )

            # load data for each election-jurisdiction pair in the list
            dl.change_db(dbname)
            dl.change_dir("results_dir", "TestingData")
            success, failure, err = dl.load_all(
                move_files=False,
                election_jurisdiction_list=election_jurisdiction_list,
                rollup=rollup,
            )
            if success:
                print(f"Files loaded:\n{success}")
            else:
                print(f"No files loaded")
            if failure:
                print(f"Files failing to load:\n{failure}")
            else:
                print("All files loaded to test db.")

        except Exception as exc:
            print(f"Exception occurred: {exc}")
            if dl:
                optional_remove(dl, "TestingData")
            err = ui.add_new_error(
                err,
                "file",
                "TestingData",
                f"Exception during data loading: {exc}",
            )
            return err

        if ui.fatal_error(err):
            optional_remove(dl, "TestingData")
            return err
    loaded_ej_list = [k.split(";") for k in success.keys()]
    report_dir = os.path.join(dl.d["reports_and_plots_dir"], f"tests_{ts}")
    failures = ui.run_tests(
        test_dir,
        dbname,
        election_jurisdiction_list=loaded_ej_list,
        report_dir=report_dir,
        param_file=param_file,
    )
    if failures:
        print("At least one test failed")
    else:
        print("All tests passed for loaded files.")
    if test_dir:
        for k in failures.keys():
            err = ui.add_new_error(
                err,
                "warn-test",
                k,
                failures[k],
            )

    if load_data:
        remove_err, db_removed = optional_remove(dl, "TestingData")
        if remove_err:
            err = ui.consolidate_errors([err, remove_err])
    return err


if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) == 1:
        ejs = None
    else:
        ejs = io(sys.argv[1:])

    error = run2(
        election_jurisdiction_list=ejs,
        rollup=True,
    )
    if error:
        params, new_err = ui.get_parameters(
            required_keys=["reports_and_plots_dir"],
            param_file="run_time.ini",
            header="electiondata",
        )
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        if not new_err:
            report_dir = os.path.join(
                params["reports_and_plots_dir"], f"load_and_test_all_{ts}"
            )
            ui.report(error, report_dir)
        else:
            print(
                f"No reports_and_plots_dir specified in run_time.ini. Errors:\n{error}"
            )
    exit()
