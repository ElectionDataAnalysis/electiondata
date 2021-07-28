import pytest
from _pytest.fixtures import FixtureRequest
import electiondata as ed
import pandas as pd
from electiondata import database as db


# add options to pytest
def pytest_addoption(parser):
    parser.addoption("--dbname", action="store", default=None)
    parser.addoption("--param_file", action="store", default="run_time.ini")
    parser.addoption("--election", action="store", default=None)
    parser.addoption("--jurisdiction", action="store", default=None)
    parser.addoption("--reference", action="store", default=None)  # file with results known to be correct


# set up fixtures so tests can call them as arguments
@pytest.fixture(scope="session")
def analyzer(request):
    an = ed.Analyzer(
        dbname=request.config.getoption("--dbname"),param_file=request.config.getoption("--param_file")
    )
    yield an
    # code after yield statement runs during post-testing clean-up
    an.session.bind.dispose()


@pytest.fixture(scope="session")
def election(request):
    # change any hyphens in election name to spaces
    return request.config.getoption("--election").replace("-"," ")


@pytest.fixture(scope="session")
def jurisdiction(request):
    # change any hyphens in jurisdiction name to spaces
    return request.config.getoption("--jurisdiction").replace("-"," ")


@pytest.fixture(scope="session")
def major_subdiv_type(analyzer, jurisdiction):
    return db.get_major_subdiv_type(analyzer.session, jurisdiction)


@pytest.fixture(scope="session")
def load_data(request):
    df = pd.read_csv(request.config.getoption("--reference"), sep="\t")
    # TODO check format of data read from reference file
    # TODO check contest names, reporting unit and warn user if not found
    # run tests only on official-final lines
    # TODO handle Status = 'preliminary' with as-of date (by adding status parameter to test?)
    return [dic for dic in df.to_dict("records") if dic["Status"] == "official-final"]


