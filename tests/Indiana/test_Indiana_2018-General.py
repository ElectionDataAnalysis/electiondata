import election_data_analysis as e

election = "2018 General"
jurisdiction = "Indiana"


def test_data_exists(dbname):
    assert e.data_exists("2018 General", "Indiana", dbname=dbname)


def test_in_statewide_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Indiana",
            "US Senate IN",
            dbname=dbname,
        )
        == 2282565
    )


def test_in_senate_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Indiana",
            "IN Senate District 14",
            dbname=dbname,
        )
        == 34542
    )


def test_in_house_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Indiana",
            "IN House District 27",
            dbname=dbname,
        )
        == 12238
    )


# Vote Type not available


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
