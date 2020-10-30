import pytest

def pytest_addoption(parser):
    parser.addoption("--dbname", action="store", default=None)

def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
    option_value = metafunc.config.option.dbname
    if 'dbname' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("dbname", [option_value])