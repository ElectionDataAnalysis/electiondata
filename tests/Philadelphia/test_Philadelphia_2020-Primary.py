import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "Philadelphia"


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_attorney_general_dem(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "PA Attorney General (Phila) (Democratic Party)",
            dbname=dbname,
        )
        == 228504
    )


def test_president_dem(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "US President (PA) (Phila) (Democratic Party)",
            dbname=dbname,
        )
        == 304190
    )


def test_state_senate_dem(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "PA Senate District 1 (Phila) (Democratic Party)",
            dbname=dbname,
        )
        == 62840
    )


def test_state_rep_republican(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "PA House District 184 (Phila) (Republican Party)",
            dbname=dbname,
        )
        == 1192
    )


def test_state_treasurer_republican(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            "PA Treasurer (Phila) (Republican Party)",
            dbname=dbname,
        )
        == 21082
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
