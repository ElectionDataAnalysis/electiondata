import os
import datetime
from pathlib import Path
import shutil
from typing import Optional
import election_data_analysis as e
from election_data_analysis import database as db
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


def run2(load_data: bool = True, dbname: Optional[str] = None, test_dir: Optional[str] = None):
    if not test_dir:
        # set the test_dir to the directory containing this file
        test_dir = Path(__file__).parent.absolute()

    # name the db
    if dbname is None:
        # create unique name for test database
        ts = datetime.datetime.now().strftime("%m%d_%H%M")
        dbname = f"test_{ts}"

    if load_data:
        get_testing_data("https://github.com/ElectionDataAnalysis/TestingData.git", "TestingData")

        # Load the data
        dl = e.DataLoader()
        dl.change_db(dbname)


        dl.change_dir("results_dir","TestingData")
        dl.load_all(move_files=False)

    ui.run_tests(test_dir, dbname)

    # allow user to inspect database if desired
    input(f"Hit return to continue (and remove test db {dbname} and test data)")

    if load_data:
        # allow user to pause, option to remove db
        remove_db = input(f"Remove test db {dbname} (y/n)?")

        if remove_db == "y":
            # define parameters to connect to postgres db
            db_params = {
                "host": dl.engine.url.host,
                "port": dl.engine.url.port,
                "user": dl.engine.url.username,
                "password": dl.engine.url.password,
                "dbname": dl.engine.url.database,
            }
            # close the connection to the db
            dl.engine.dispose()
            # remove the db
            db.remove_database(db_params)

        remove_dir = input("Remove TestingData directory (y/n)?")
        if remove_dir == "y":
            # remove testing data
            os.system(f"rm -rf TestingData")
    return


if __name__ == "__main__":
    run2()
    exit()