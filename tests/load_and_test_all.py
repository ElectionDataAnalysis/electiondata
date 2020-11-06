import os
import sys
import getopt
import datetime
from pathlib import Path
import shutil
from typing import Optional
import election_data_analysis as eda
from election_data_analysis import database as db
from election_data_analysis import user_interface as ui


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
    remove_db = input(f"Remove test db {dl.d['dbname']} (y/n)?\n")

    if remove_db == "y":
        err = close_and_erase(dl)
        if not err:
            db_removed = True
        # define parameters to connect to postgres db

    # give user option to remove directory
    remove_dir = input(f"Remove {dir_path} directory and all its contents (y/n)?\n")
    if remove_dir == "y":
        # remove testing data
        os.system(f"rm -rf {dir_path}")

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


def run2(
    load_data: bool = True,
    dbname: Optional[str] = None,
    test_dir: Optional[str] = None,
    election_jurisdiction_list: Optional[list] = None,
) -> Optional[dict]:
    dl = None  # to keep syntax-checker happy

    err = None
    db_removed = False
    if not test_dir:
        # set the test_dir to the directory containing this file
        test_dir = Path(__file__).parent.absolute()

    # name the db
    if dbname is None:
        # create unique name for test database
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        dbname = f"test_{ts}"

    if load_data:
        get_testing_data(
            "https://github.com/ElectionDataAnalysis/TestingData.git", "TestingData"
        )

    # restrict elections and jurisdictions to those given (if given)
    # otherwise use all in TestingData
    if not election_jurisdiction_list:
        election_jurisdiction_list = ui.election_juris_list("TestingData")

    if load_data:
        try:
            # Load the data
            dl = eda.DataLoader()
            dl.change_db(dbname)

            dl.change_dir("results_dir", "TestingData")
            err, success = dl.load_all(
                move_files=False, election_jurisdiction_list=election_jurisdiction_list
            )
            if not success:
                print("At least one file did not load correctly.")
                err, db_removed = optional_remove(dl, "TestingData")
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

    if not db_removed:
        ui.run_tests(
            test_dir, dbname, election_jurisdiction_list=election_jurisdiction_list
        )

        if load_data:
            err, db_removed = optional_remove(dl, "TestingData")
    return err


if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) == 1:
        ejs = None
    else:
        ejs = io(sys.argv[1:])

    error = run2(
        election_jurisdiction_list=ejs,
    )
    if error:
        print(error)
    exit()
