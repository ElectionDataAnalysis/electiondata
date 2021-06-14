import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "Montana"


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Montana", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Montana",
            "US President (MT) (Democratic Party)",
            dbname=dbname,
        )
        == 149973
    )


def test_statewide_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Montana",
            "US Senate MT (Republican Party)",
            dbname=dbname,
        )
        == 219205
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Montana",
            "MT Senate District 17 (Republican Party)",
            dbname=dbname,
        )
        == 4896
    )


def test_state_rep_totals(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Montana",
            "MT House District 4 (Republican Party)",
            dbname=dbname,
        )
        == 2782
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
