import pytest
import os
import pandas as pd
import electiondata as ed
from electiondata import userinterface as ui
from electiondata import constants


@pytest.fixture
def get_major_subdiv(dbname, param_file, jurisdiction)

@pytest.fixture
def get_testing_info(param_file, election, jurisdiction):
    params, err = ui.get_parameters(
        param_file=param_file, header="electiondata", required_keys=["repository_content_root"]
    )
    # TODO handle err
    data_file = os.path.join(
        params["repository_content_root"], "results_for_testing",election, f"{jurisdiction}.tsv"
    )
    df = pd.read_csv(data_file, sep="\t")
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
    assert (
            ed.contest_total(
            election,
            jurisdiction,
            testing_info_dictionary["Contest"],
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
            == testing_info_dictionary["Count"]
    )


def test_district_1(dbname):
    assert (
            ed.contest_total(
            election,
            jurisdiction,
            district_contest_1,
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
            == total_district_contest_1
    )


def test_count_type_subtotal(dbname):
    assert (
            ed.contest_total(
            election,
            jurisdiction,
            top_contest,
            dbname=dbname,
            sub_unit_type=county_or_other,
            vote_type=single_vote_type,
        )
            == top_contest_vote_type
    )


def test_county_subtotal(dbname):
    assert (
            ed.contest_total(
            election,
            jurisdiction,
            top_contest,
            dbname=dbname,
            county=single_county,
            sub_unit_type=county_or_other,
        )
            == top_contest_votes_county
    )
