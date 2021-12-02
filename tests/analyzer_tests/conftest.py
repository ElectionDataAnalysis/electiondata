import pytest
import os
import datetime
from pathlib import Path
from electiondata import DataLoader


def pytest_addoption(parser):
    parser.addoption(
        "--param_file",
        action="store",
        default="run_time.ini",
    )


# set up fixtures so tests can call them as arguments
@pytest.fixture(scope="session")
def param_file(request):
    return request.config.getoption("--param_file")


@pytest.fixture(scope="session")
def analyzer(param_file):
    # NB: don't create temp db yet -- it will be created in test_load_test_data
    dl = DataLoader(param_file=param_file)

    # name new db
    ts = datetime.datetime.now().strftime("%m%d_%H%M")
    new_dbname = f"pytest_{ts}"

    # Now load test data
    tests_path = os.path.join(Path(dl.d["repository_content_root"]).parent, "tests")
    db_dump = os.path.join(
        tests_path, "000_data_for_pytest", "postgres_test_db_dump.tar"
    )
    err_str = dl.load_data_from_db_dump(
        dbname=new_dbname, dump_file=db_dump, delete_existing=True
    )

    # point dl to new db
    dl.change_db(new_db_name=new_dbname, db_param_file=param_file, db_params=None)
    yield dl.analyzer
    dl.close_and_erase()


tests_path = Path(__file__).parents[1].absolute()


@pytest.fixture(scope="session")
def tests_path():
    return Path(__file__).parents[1].absolute()
