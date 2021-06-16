import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "Colorado"


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Colorado", dbname=dbname)


def test_co_presidential_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Colorado",
            "US President (CO) (Democratic Party)",
            dbname=dbname,
        )
        == 960128
    )


def test_co_statewide_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Colorado",
            "US Senate CO (Republican Party)",
            dbname=dbname,
        )
        == 554806
    )


def test_co_senate_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Colorado",
            "CO Senate District 21 (Republican Party)",
            dbname=dbname,
        )
        == 6320
    )


def test_co_rep_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Colorado",
            "CO House District 20 (Democratic Party)",
            dbname=dbname,
        )
        == 10011
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
