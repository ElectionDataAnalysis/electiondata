import results
import os
from election_data_analysis import Analyzer
from election_data_analysis import database as db
from election_data_analysis import data_exists
from election_data_analysis import census_data_exists
from psycopg2 import sql
import pytest


def get_analyzer(p_path: str = None):
    one_up = os.path.dirname(os.getcwd())
    if p_path:
        param_file = p_path
    else:
        param_file = os.path.join(one_up, "src", "run_time.ini")
    a = Analyzer(param_file)
    return a


analyzer = get_analyzer()
ok = {
    "ga16g": data_exists("2016 General", "Georgia"),
    "ga18g": data_exists("2018 General", "Georgia"),
    "ga20p": data_exists("2020 Primary", "Georgia"),
    "nc18g": data_exists("2018 General", "North Carolina"),
    "ak16g": data_exists("2016 General", "Alaska"),
    "ga18census": census_data_exists("2018 General", "Georgia")
}


# should be non-null on DB with any data
def test_election_display():
    assert analyzer.display_options("election", verbose=True)


# should be non-null on DB with any data
def test_jurisdiction_display():
    assert analyzer.display_options("jurisdiction", verbose=True)


# should be non-null on DB with data from 2018 General
def test_jurisdiction_display_filtered():
    assert analyzer.display_options(
        "jurisdiction", verbose=True, filters=["2018 General"]
    )


### Test bar chart flow ###
@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_contest_type_display():
    assert (
        analyzer.display_options(
            "contest_type", verbose=True, filters=["2018 General", "Georgia"]
        )
        == results.ga_2018_contest_types
    )


@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_contest_display():
    assert (
        analyzer.display_options(
            "contest",
            verbose=True,
            filters=["2018 General", "Georgia", "Congressional"],
        )
        == results.ga_2018_congressional_contests
    )


@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_bar_congressional():
    assert (
        analyzer.bar(
            "2018 General", "Georgia", "Congressional", "US House GA District 3"
        )
        == results.ga_2018_bar_congressional
    )


@pytest.mark.skipif(not ok["nc18g"], reason="No North Carolina 2018 General data")
def test_bar_all_state():
    assert (
        analyzer.bar("2018 General", "North Carolina", "State House", "All State House")
        == results.nc_2018_bar_statehouse
    )


@pytest.mark.skipif(not ok["nc18g"], reason="No North Carolina 2018 General data")
def test_bar_all_congressional():
    assert (
        analyzer.bar(
            "2018 General", "North Carolina", "Congressional", "All Congressional"
        )
        == results.nc_2018_bar_congressional
    )


### check scatter flow ###
# should be non-null if there is any georgia data in the DB
@pytest.mark.skipif(
    not ok["ga16g"] and not ok["ga18g"] and not ok["ga20p"], reason="No Georgai data"
)
def test_election_display():
    assert analyzer.display_options(
        "election", verbose=True, filters=["Georgia", "county"]
    )


@pytest.mark.skipif(not ok["nc18g"], reason="No North Carolina 2018 General data")
def test_category_display():
    assert (
        analyzer.display_options(
            "category", verbose=True, filters=["North Carolina", "county", "2018 General"]
        )
        == results.nc_2018_category
    )


@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_count_display():
    assert (
        analyzer.display_options(
            "count",
            verbose=True,
            filters=["Georgia", "county", "2018 General", "Candidate total"],
        )
        == results.ga_2018_count
    )


@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_scatter_candidates():
    assert (
        analyzer.scatter(
            "Georgia",
            "2018 General",
            "Candidate total",
            "Chris Carr",
            "2018 General",
            "Candidate total",
            "Charlie Bailey",
        )
        == results.ga_2018_scatter_candidates
    )


@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_scatter_candidates_longname():
    assert (
        analyzer.scatter(
            "Georgia",
            "2018 General",
            "Candidate total",
            "Chris Carr - R - GAAttGen",
            "2018 General",
            "Candidate total",
            "Charlie Bailey - D - GAAttGen",
        )
        == results.ga_2018_scatter_candidates
    )


@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_scatter_candidates_votetype():
    assert (
        analyzer.scatter(
            "Georgia",
            "2018 General",
            "Candidate absentee-mail",
            "Stacey Abrams",
            "2018 General",
            "Candidate election-day",
            "Stacey Abrams",
        )
        == results.ga_2018_scatter_candidates_votetype
    )


@pytest.mark.skipif(
    not ok["ga18g"] or not ok["ga16g"], reason="No Georgia 2016 or 2018 General data"
)
def test_scatter_multi_electio():
    assert (
        analyzer.scatter(
            "Georgia",
            "2018 General",
            "Contest total",
            "US House GA District 14",
            "2016 General",
            "Contest total",
            "US House GA District 14",
        )
        == results.ga_multi_election
    )


@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_scatter_party():
    assert (
        analyzer.scatter(
            "Georgia",
            "2018 General",
            "Party total",
            "Republican State House",
            "2018 General",
            "Party total",
            "Democratic State House",
        )
        == results.ga_2018_scatter_party
    )


@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_scatter_party_votetype():
    assert (
        analyzer.scatter(
            "Georgia",
            "2018 General",
            "Party absentee-mail",
            "Republican State House",
            "2018 General",
            "Candidate absentee-mail",
            "Stacey Abrams",
        )
        == results.ga_2018_scatter_party_votetype
    )


# check that rollup to county level works correctly
@pytest.mark.skipif(not ok["nc18g"], reason="No North Carolina 2018 General data")
def test_scatter_county_rollup():
    assert (
        analyzer.scatter(
            "North Carolina",
            "2018 General",
            "Candidate total",
            "Mark Harris - R - USHouse9",
            "2018 General",
            "Candidate total",
            "Dan Mccready - D - USHouse9",
        )
        == results.nc_2018_scatter_county_rollup
    )


# check that search works correctly
@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_candidate_search_display():
    assert (
        analyzer.display_options(
            "bishop",
            verbose=True,
            filters=["Georgia", "county", "2018 General", "Congressional"],
        )
        == results.ga_2018_candidate_search_display
    )


@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_count_contest_display():
    assert (
        analyzer.display_options(
            "count",
            verbose=True,
            filters=["Georgia", "county", "2018 General", "Contest total"],
        )
        == results.ga_2018_count_contest
    )


@pytest.mark.skipif(not ok["ga18g"], reason="No Georgia 2018 General data")
def test_contest_updatelabels_display():
    assert (
        analyzer.display_options(
            "contest",
            verbose=True,
            filters=["2018 General", "Georgia", "State Senate"],
        )
        == results.ga_2018_congressional_contests_state_senate
    )


@pytest.mark.skipif(not ok["ak16g"], reason="No Alaska 2016 General data")
def test_alaska_non_county_hierarchy():
    assert (
        analyzer.scatter(
            "Alaska",
            "2016 General",
            "Contest early",
            "US President (AK)",
            "2016 General",
            "Contest election-day",
            "US President (AK)"
        )
        == results.ak_2016_scatter
    )


@pytest.mark.skipif(
    not ok["ga18g"] or not ok["ga18census"], reason="No Georgia 2018 General or Census data"
)
def test_census_count_display():
    assert (
        analyzer.display_options(
            "count",
            verbose=True,
            filters=["Georgia", "2018 General", "Census data"]
        )
        == results.ga_2018_census_display_count
    )


@pytest.mark.skipif(
    not ok["ga18g"] or not ok["ga18census"], reason="No Georgia 2018 General or Census data"
)
def test_census_category_display():
    assert (
        analyzer.display_options(
            "category",
            verbose=True,
            filters=["Georgia", "county", "2018 General"]
        )
        == results.ga_2018_census_display_category
    )


@pytest.mark.skipif(
    not ok["ga18g"] or not ok["ga18census"], reason="No Georgia 2018 General or Census data"
)
def test_census_scatter():
    assert (
        analyzer.scatter(
            "Georgia",
            "2018 General",
            "Candidate absentee-mail",
            "Stacey Abrams",
            "2018 General",
            "Census data",
            "White",
        )
        == results.ga_2018_census_scatter
    )