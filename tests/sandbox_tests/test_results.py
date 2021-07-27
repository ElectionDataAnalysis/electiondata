import pytest
import os
import pandas as pd
import electiondata as ed
from electiondata import userinterface as ui, database as db


@pytest.fixture
def get_params(param_file):
    params, err = ui.get_parameters(
        param_file=param_file, header="electiondata", required_keys=["repository_content_root"]
    )
    # TODO handle err
    pytest.params = params


@pytest.fixture
def get_major_subdiv_type(dbname, param_file, jurisdiction):
    an = ed.Analyzer(dbname=dbname, param_file=param_file)
    pytest.major_subdiv_type = db.get_major_subdiv_type(
        an.session, jurisdiction, pytest.params["repository_content_root"]
    )


@pytest.fixture
def get_testing_info(param_file, election, jurisdiction):
    data_file = os.path.join(
        pytest.params["repository_content_root"], "results_for_testing",election, f"{jurisdiction}.tsv"
    )
    df = pd.read_csv(data_file, sep="\t")
    # TODO check contest names, reporting unit and warn user if not found
    # run tests only on official-final lines
    # TODO handle Status = 'preliminary' with as-of date (by adding status parameter to test?)
    pytest.testing_info = [dic for dic in df.to_dict("records") if dic["Status"] == "official-final"]


def test_data_exists(dbname, param_file, election, jurisdiction):
    assert ed.data_exists(election,jurisdiction,dbname=dbname,p_path=param_file)


def test_standard_vote_types(dbname, param_file, election, jurisdiction):
    assert ed.check_count_types_standard(election,jurisdiction,dbname=dbname,param_file=param_file)


def test_vote_type_counts_consistent(dbname, param_file, election, jurisdiction):
    assert ed.check_totals_match_vote_types(election,jurisdiction,dbname=dbname,param_file=param_file)


def test_all_candidates_known(dbname, param_file, election, jurisdiction):
    assert (
            ed.get_contest_with_unknown_candidates(election,jurisdiction,dbname=dbname,param_file=param_file)
            == []
    )


@pytest.mark.parametrize("testing_info_dictionary", pytest.testing_info)
def test_contest(testing_info_dictionary, dbname, param_file, election, jurisdiction):
    assert ed.contest_total(
                    election,
                    jurisdiction,
                    testing_info_dictionary["Contest"],
                    sub_unit_type=pytest.major_subdiv_type,
                    reporting_unit=testing_info_dictionary["ReportingUnit"],
                    vote_type=testing_info_dictionary["VoteType"],
                    dbname=dbname,
                    param_file=param_file,
                ) == testing_info_dictionary["Count"]


