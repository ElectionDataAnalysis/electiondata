import pytest
import os
from pathlib import Path
from electiondata import data_exists, external_data_exists


def pytest_addoption(parser):
    parser.addoption(
        "--param_file",
        action="store",
        default=os.path.join(Path(__file__).parents[1].absolute(), "run_time.ini"),
    )


def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
    option_value = metafunc.config.option.param_file
    if "param_file" in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("param_file", [option_value])


@pytest.fixture(scope="session")
def ok(param_file):
    return {
        "ga16g": data_exists("2016 General", "Georgia", param_file=param_file),
        "ga18g": data_exists("2018 General", "Georgia", param_file=param_file),
        "ga20p": data_exists("2020 Primary", "Georgia", param_file=param_file),
        "nc18g": data_exists("2018 General", "North Carolina", param_file=param_file),
        "ak16g": data_exists("2016 General", "Alaska", param_file=param_file),
        "ga18census": external_data_exists(
            "2018 General", "Georgia", param_file=param_file
        ),
    }
