import election_data_analysis as e

election = "2018 General"
jurisdiction = "Philadelphia"


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_us_senator(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "US Senate PA (Phila)",
            dbname=dbname,
        )
        == 554604
    )


def test_gov(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "PA Governor (Phila)",
            dbname=dbname,
        )
        == 554264
    )


def test_state_senate(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "PA Senate District 8 (Phila)",
            dbname=dbname,
        )
        == 63295
    )


def test_state_rep(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "PA House District 197 (Phila)",
            dbname=dbname,
        )
        == 15480
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
