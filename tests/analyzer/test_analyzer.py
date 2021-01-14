import results
import os
from election_data_analysis import Analyzer
from election_data_analysis import database as db
from election_data_analysis import data_exists
from election_data_analysis import census_data_exists
from psycopg2 import sql
import pytest


@pytest.fixture
def data(runtime):
    pytest.ok = {
        "ga16g": data_exists("2016 General", "Georgia", p_path=runtime),
        "ga18g": data_exists("2018 General", "Georgia", p_path=runtime),
        "ga20p": data_exists("2020 Primary", "Georgia", p_path=runtime),
        "ga20g": data_exists("2020 General", "Georgia", p_path=runtime),
        "nc18g": data_exists("2018 General", "North Carolina", p_path=runtime),
        "ak16g": data_exists("2016 General", "Alaska", p_path=runtime),
        "ga18census": census_data_exists("2018 General", "Georgia", p_path=runtime),
    }


# Required to initialize the pytest.ok variable
def test_config(data):
    return pytest.ok


# should be non-null on DB with any data
def test_election_display(runtime):
    analyzer = Analyzer(runtime)
    assert analyzer.display_options("election", verbose=True)


# should be non-null on DB with any data
def test_jurisdiction_display(runtime):
    analyzer = Analyzer(runtime)
    assert analyzer.display_options("jurisdiction", verbose=True)


# should be non-null on DB with data from 2018 General
def test_jurisdiction_display_filtered(runtime):
    analyzer = Analyzer(runtime)
    assert analyzer.display_options(
        "jurisdiction", verbose=True, filters=["2018 General"]
    )


# ### Test bar chart flow ###
def test_contest_type_display(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.display_options(
            "contest_type", verbose=True, filters=["2018 General", "Georgia"]
        )
        == results.ga_2018_contest_types
    )


def test_contest_display(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.display_options(
            "contest",
            verbose=True,
            filters=["2018 General", "Georgia", "Congressional"],
        )
        == results.ga_2018_congressional_contests
    )


def test_bar_congressional(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.bar(
            "2018 General", "Georgia", "Congressional", "US House GA District 3"
        )
        == results.ga_2018_bar_congressional
    )


def test_bar_all_state(runtime):
    assert pytest.ok["nc18g"], "No North Carolina 2018 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.bar("2018 General", "North Carolina", "State House", "All State House")
        == results.nc_2018_bar_statehouse
    )


def test_bar_all_congressional(runtime):
    assert pytest.ok["nc18g"], "No North Carolina 2018 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.bar(
            "2018 General", "North Carolina", "Congressional", "All Congressional"
        )
        == results.nc_2018_bar_congressional
    )


### check scatter flow ###
# should be non-null if there is any georgia data in the DB
def test_election_display(runtime):
    assert (
        pytest.ok["ga16g"] or pytest.ok["ga18g"] or pytest.ok["ga20p"]
    ), "No Georgia data"
    analyzer = Analyzer(runtime)
    assert analyzer.display_options(
        "election", verbose=True, filters=["Georgia", "county"]
    )


def test_category_display(runtime):
    assert pytest.ok["nc18g"], "No North Carolina 2018 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.display_options(
            "category",
            verbose=True,
            filters=["North Carolina", "county", "2018 General"],
        )
        == results.nc_2018_category
    )


def test_count_display(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.display_options(
            "count",
            verbose=True,
            filters=["Georgia", "county", "2018 General", "Candidate total"],
        )
        == results.ga_2018_count
    )


def test_scatter_candidates(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
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


def test_scatter_candidates_longname(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
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


def test_scatter_candidates_votetype(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
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


def test_scatter_multi_election(runtime):
    assert pytest.ok["ga16g"] and pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
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


def test_scatter_party(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
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


def test_scatter_party_votetype(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
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
def test_scatter_county_rollup(runtime):
    assert pytest.ok["nc18g"], "No North Carolina 2018 General data"
    analyzer = Analyzer(runtime)
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
def test_candidate_search_display(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.display_options(
            "bishop",
            verbose=True,
            filters=["Georgia", "county", "2018 General", "Congressional"],
        )
        == results.ga_2018_candidate_search_display
    )


def test_count_contest_display(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.display_options(
            "count",
            verbose=True,
            filters=["Georgia", "county", "2018 General", "Contest total"],
        )
        == results.ga_2018_count_contest
    )


def test_contest_updatelabels_display(runtime):
    assert pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.display_options(
            "contest",
            verbose=True,
            filters=["2018 General", "Georgia", "State Senate"],
        )
        == results.ga_2018_congressional_contests_state_senate
    )


def test_alaska_non_county_hierarchy(runtime):
    assert pytest.ok["ak16g"], "No Alaska 2016 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.scatter(
            "Alaska",
            "2016 General",
            "Contest early",
            "US President (AK)",
            "2016 General",
            "Contest election-day",
            "US President (AK)",
        )
        == results.ak_2016_scatter
    )


def test_census_count_display(runtime):
    assert pytest.ok["ga16g"] and pytest.ok["ga18g"], "No Georgia 2018 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.display_options(
            "count", verbose=True, filters=["Georgia", "2018 General", "Census data"]
        )
        == results.ga_2018_census_display_count
    )


def test_census_category_display(runtime):
    assert (
        pytest.ok["ga18g"] and pytest.ok["ga18census"]
    ), "No Georgia 2018 General or Census data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.display_options(
            "category", verbose=True, filters=["Georgia", "county", "2018 General"]
        )
        == results.ga_2018_census_display_category
    )


def test_census_scatter(runtime):
    assert (
        pytest.ok["ga18g"] and pytest.ok["ga18census"]
    ), "No Georgia 2018 General or Census data"
    analyzer = Analyzer(runtime)
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


def test_georgia_runoff_display(runtime):
    assert pytest.ok["ga20g"], "No Georgia 2020 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.display_options(
            "count",
            verbose=True,
            filters=["Georgia", "county", "2020 General", "Candidate total"],
        )
        == results.ga_2020_candidates
    )


def test_georgia_runoff_scatter(runtime):
    assert pytest.ok["ga20g"], "No Georgia 2020 General data"
    analyzer = Analyzer(runtime)
    assert (
        analyzer.scatter(
            "Georgia",
            "2020 General",
            "Candidate total",
            "Raphael Warnock - D - USSenRunoff",
            "2020 General",
            "Candidate total",
            "Raphael Warnock - D - USSen",
        )
        == results.ga_2020_warnock
    )
