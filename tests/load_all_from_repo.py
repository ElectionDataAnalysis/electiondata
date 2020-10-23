import os
import sys, getopt
import datetime
from pathlib import Path
import shutil
from typing import Optional
import election_data_analysis as e
from election_data_analysis import database as db
from election_data_analysis import user_interface as ui


def io(argv) -> Optional[list]:
    election = None
    jurisdiction = None
    file_name = 'load_all_from_repo.py'
    try:
        opts, args = getopt.getopt(argv,"he:j:",["election=","juris="])
    except getopt.GetoptError:
        print (f'{file_name} -e <election> -j <jurisdiction>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print (f'{file_name} -e <election> -j <jurisdiction>')
            sys.exit()
        elif opt in ("-i", "--election"):
            election = arg
        elif opt in ("-o", "--juris"):
            jurisdiction = arg
    print (f'Election file is {election}')
    print (f'Jurisdiction is {jurisdiction}')
    if (not election) or (not jurisdiction):
        ej_list = None
    else:
        ej_list = [(election, jurisdiction)]
    return ej_list


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
        election_jurisdiction_list: Optional[list] = None
):
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

    ui.run_tests(test_dir, dbname, election_jurisdiction_list=election_jurisdiction_list)

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

    if len(sys.argv) == 1:
        election_jurisdiction_list = None
    else:
        election_jurisdiction_list = io(sys.argv[1:])

    run2(election_jurisdiction_list=election_jurisdiction_list)
    exit()
