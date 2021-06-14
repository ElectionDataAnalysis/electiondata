import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "Wisconsin"


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Wisconsin", dbname=dbname)


def test_wi_presidential_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Wisconsin",
            "US President (WI) (Democratic Party)",
            dbname=dbname,
        )
        == 925065
    )


# No results available for additional statewide elections.


def test_wi_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Wisconsin",
            "WI Senate District 28 (Republican Party)",
            dbname=dbname,
        )
        == 20309
    )


def test_wi_house_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Wisconsin",
            "WI House District 4 (Democratic Party)",
            dbname=dbname,
        )
        == 4724
    )


def test_us_house_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Wisconsin",
            "US House WI District 1 (Republican Party)",
            dbname=dbname,
        )
        == 40404
    )


# results not available by vote type


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
