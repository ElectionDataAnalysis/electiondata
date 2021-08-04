import pytest
import electiondata as ed
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
def dbname(request):
    return request.config.getoption("--dbname")


@pytest.fixture(scope="session")
def param_file(request):
    return request.config.getoption("--param_file")


@pytest.fixture(scope="session")
def election(request):
    el = request.config.getoption("--election")
    # change any hyphens in election name to spaces
    if el:
        el = el.replace("-"," ")
    return el


@pytest.fixture(scope="session")
def jurisdiction(request):
    ju = request.config.getoption("--jurisdiction")
    # change any hyphens in jurisdiction name to spaces
    if ju:
        ju = ju.replace("-"," ")
    return ju


@pytest.fixture(scope="session")
def major_subdiv_type(analyzer, jurisdiction):
    return db.get_major_subdiv_type(analyzer.session, jurisdiction)


@pytest.fixture(scope="session")
def analyzer(dbname, param_file):
    print(f"dbname: {dbname}, param_file: {param_file}")  # TODO remove diagnostic print
    an = ed.Analyzer(
        dbname=dbname, param_file=param_file
    )
    yield an
    # code after yield statement runs during post-testing clean-up
    an.session.bind.dispose()

