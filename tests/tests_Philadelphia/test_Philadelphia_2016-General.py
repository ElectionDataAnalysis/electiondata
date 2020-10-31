import election_data_analysis as e

election = "2016 General"
jurisdiction = "Philadelphia"


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_attorney_general(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "PA Attorney General (Phila)",
            dbname=dbname,
        )
        == 675504
    )


def test_president(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "US President (Phila)",
            dbname=dbname,
        )
        == 709618
    )


def test_state_senate_dem(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "PA Senate District 1 (Phila)",
            dbname=dbname,
        )
        == 114370
    )


def test_state_rep(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "PA House District 182 (Phila)",
            dbname=dbname,
        )
        == 31806
    )


def test_state_treasurer_republican(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "PA State Treasurer",
            dbname=dbname,
        )
        == 21082
    )
