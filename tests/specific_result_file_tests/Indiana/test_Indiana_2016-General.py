import election_data_analysis as e

election = "2016 General"
jurisdiction = "Indiana"


def test_data_exists(dbname):
    assert e.data_exists("2016 General", "Indiana", dbname=dbname)


def test_in_presidential_16(dbname):
    assert (
        not e.data_exists("2016 General", "Indiana", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Indiana",
            "US President (IN)",
            dbname=dbname,
        )
        == 2728138
    )


def test_in_statewide_totals_16(dbname):
    assert (
        not e.data_exists("2016 General", "Indiana", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Indiana",
            "IN Attorney General",
            dbname=dbname,
        )
        == 2635832
    )


def test_in_senate_totals_16(dbname):
    assert (
        not e.data_exists("2016 General", "Indiana", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Indiana",
            "IN Senate District 7",
            dbname=dbname,
        )
        == 50622
    )


def test_in_house_totals_16(dbname):
    assert (
        not e.data_exists("2016 General", "Indiana", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Indiana",
            "IN House District 13",
            dbname=dbname,
        )
        == 26712
    )


# Vote Type not available


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
