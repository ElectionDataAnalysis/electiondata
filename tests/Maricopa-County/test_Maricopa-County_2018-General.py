import election_data_analysis as e

election = "2018 General"
jurisdiction = "Maricopa County"


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_ussenate_20(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "AZ Secretary of State (Maricopa County)",
            dbname=dbname,
        )
        == 1407445
    )


def test_senate_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "AZ Senate District 13 (Maricopa County)",
            dbname=dbname,
        )
        == 56975
    )


def test_rep(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "AZ House District 25 (Maricopa County)",
            dbname=dbname,
        )
        == 130966
    )


def test_congress(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "US House AZ District 9 (Maricopa County)",
            dbname=dbname,
        )
        == 261559
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
