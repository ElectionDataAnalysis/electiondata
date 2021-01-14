import election_data_analysis as e

election = "2018 General"
jurisdiction = "Illinois"


def test_data_exists(dbname):
    assert e.data_exists("2018 General", "Illinois", dbname=dbname)


def test_statewide_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Illinois",
            "IL Governor",
            dbname=dbname,
        )
        == 4547657
    )


def test_senate_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Illinois",
            "IL Senate District 14",
            dbname=dbname,
        )
        == 65275
    )


def test_house_totals(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Illinois",
            "IL House District 10",
            dbname=dbname,
        )
        == 31649
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
