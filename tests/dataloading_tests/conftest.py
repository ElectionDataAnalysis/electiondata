import pytest
import os
from pathlib import Path
import electiondata as ed
import datetime


# add options to pytest
def pytest_addoption(parser):
    parser.addoption(
        "--param_file",
        action="store",
        default="run_time.ini",
    )
    parser.addoption(
        "--test_data_url",
        action="store",
        default="https://github.com/ElectionDataAnalysis/TestingData.git",
    )


# set up fixtures so tests can call them as arguments
@pytest.fixture(scope="session")
def param_file(request):
    return request.config.getoption("--param_file")


@pytest.fixture(scope="session")
def test_data_url(request):
    return request.config.getoption("--test_data_url")


@pytest.fixture(scope="function")
def dataloader(param_file):
    ts = datetime.datetime.now().strftime("%m%d_%H%M")
    dbname = f"test_{ts}"
    dl = ed.DataLoader(dbname=dbname, param_file=param_file)
    yield dl
    # code after yield statement runs during post-testing clean-up
    dl.close_and_erase()
