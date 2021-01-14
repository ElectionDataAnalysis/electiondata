import election_data_analysis as e

election = "2016 General"
jurisdiction = "Colorado"


def test_data_exists(dbname):
    assert e.data_exists("2016 General", "Colorado", dbname=dbname)


def test_co_presidential_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Colorado",
            "US President (CO)",
            dbname=dbname,
        )
        == 2780247
    )


def test_co_statewide_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Colorado",
            "US Senate CO",
            dbname=dbname,
        )
        == 2743029
    )


def test_co_senate_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Colorado",
            "CO Senate District 14",
            dbname=dbname,
        )
        == 85788
    )


def test_co_rep_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Colorado",
            "CO House District 60",
            dbname=dbname,
        )
        == 41303
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
