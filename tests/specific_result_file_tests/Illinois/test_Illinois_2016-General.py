import election_data_analysis as e

election = "2016 General"
jurisdiction = "Illinois"


def test_data_exists(dbname):
    assert e.data_exists("2016 General", "Illinois", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Illinois",
            "US President (IL)",
            dbname=dbname,
        )
        == 5536424
    )


def test_statewide_totals(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Illinois",
            "IL Comptroller",
            dbname=dbname,
        )
        == 5412543
    )


def test_senate_totals(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Illinois",
            "IL Senate District 14",
            dbname=dbname,
        )
        == 79949
    )


def test_house_totals(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "Illinois",
            "IL House District 13",
            dbname=dbname,
        )
        == 40831
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
